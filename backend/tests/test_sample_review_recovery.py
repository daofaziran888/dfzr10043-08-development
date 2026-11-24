"""抽检复核与交接恢复测试：复核后重新采集有效行、样本更正明细、交接恢复。

场景主线：某批货号和数量因原图模糊一同驳回；新图只让货号恢复通过；
旧退回回执在接收方核验更正件时到达。覆盖：按通过项推进样本状态、
恢复中断时未处理样本仍待抽检、同一货号并发提交只交付一次、
原驳回证据与已签收旧样本分别回查、更正件只覆盖货号来源行。
"""
from __future__ import annotations

import threading

from app.pipeline import sample_review_handoff_recovery
from app.services import service_for

BATCH_CODE = "CAPT-REVIEW"


def _setup_rejected_batch():
    """构造批次：货号+数量被驳回的样本、已通过样本、待抽检样本、已退回交接包。"""
    batch_svc = service_for("capture_batch")
    batch, _ = batch_svc.create_entry({
        "批次编号": BATCH_CODE, "关联执行": "RUN-REVIEW", "记录行数": "20",
    })
    batch_svc.run_action(batch["id"], "启动校验")
    batch_svc.run_action(batch["id"], "确认批次")

    sample_svc = service_for("sample_check")
    rejected, _ = sample_svc.create_entry({
        "样本编号": "SAMP-REJ", "关联批次": BATCH_CODE,
        "抽样比例": "0.2", "抽样行数": "4",
    })
    sample_svc.run_action(rejected["id"], "启动抽检")
    sample_svc.reject_sample_with_fields(
        rejected["id"], ["货号", "数量"], "原图模糊", "原始影像v1")

    passed, _ = sample_svc.create_entry({
        "样本编号": "SAMP-OK", "关联批次": BATCH_CODE,
        "抽样比例": "0.2", "抽样行数": "4",
    })
    sample_svc.run_action(passed["id"], "启动抽检")
    sample_svc.accept_sample(passed["id"])

    pending, _ = sample_svc.create_entry({
        "样本编号": "SAMP-PENDING", "关联批次": BATCH_CODE,
        "抽样比例": "0.2", "抽样行数": "4",
    })

    handoff_svc = service_for("handoff_pack")
    pack, _ = handoff_svc.create_entry({
        "交接编号": "HAND-REVIEW", "关联批次": BATCH_CODE,
        "目标系统": "datalake_downstream",
    })
    handoff_svc.run_action(pack["id"], "启动打包")
    handoff_svc.reject_handoff(pack["id"], "样本驳回")
    handoff_svc.receive_return_receipt(pack["id"], "RCPT-1", "原退回回执")
    return batch, rejected, passed, pending, pack


class TestSampleReviewHandoffRecoveryScenario:
    """需求场景：先签收未驳回样本 → 提交货号新图 → 暂停交接 →
    重传旧回执与重复上报 → 恢复后签收更正件并回查。"""

    def test_full_recovery_scenario(self):
        _, rejected, passed, pending, pack = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        handoff_svc = service_for("handoff_pack")

        # 先签收该批次未被驳回的样本，已驳回样本跳过
        signoff = handoff_svc.signoff_samples(pack["id"])
        assert rejected["id"] in signoff["skipped_rejected"]
        assert passed["id"] in signoff["signed"]

        # 提交货号新图：只有货号恢复通过，数量仍待复核
        review = sample_svc.submit_field_review(rejected["id"], "货号", "新影像v2", True)
        assert review["ok"] is True
        assert review["correction"] is not None
        # 复核结论变更后按通过项推进样本状态：部分通过 → 抽检中
        assert sample_svc.get_entry(rejected["id"])["status"] == "抽检中"
        assert sample_svc.pending_review_fields(rejected["id"]) == ["数量"]

        # 准备输出恢复单时把交接暂停
        handoff_svc.run_action(pack["id"], "启动打包")
        sheet = handoff_svc.prepare_recovery_sheet(pack["id"])["sheet"]
        assert sheet["覆盖字段"] == ["货号"]
        handoff_svc.pause_handoff(pack["id"])

        # 恢复中断：尚未处理的样本仍待抽检
        interrupted = handoff_svc.apply_recovery(pack["id"])
        assert interrupted["status"] == "paused"
        assert sample_svc.get_entry(pending["id"])["status"] == "待抽检"

        # 此时重传原退回回执，同时重复上报货号的通过结论
        receipt = handoff_svc.receive_return_receipt(pack["id"], "RCPT-1", "重传")
        assert receipt["duplicate"] is True
        dup = sample_svc.submit_field_review(rejected["id"], "货号", "新影像v2", True)
        assert dup["deduplicated"] is True
        assert len(sample_svc.correction_details(rejected["id"])) == 1

        # 恢复交接：同一货号只交付一次，随后签收更正件
        handoff_svc.resume_handoff(pack["id"])
        recovery = handoff_svc.apply_recovery(pack["id"])
        assert recovery["status"] == "recovered"
        assert recovery["delivered"] == ["CORR-SAMP-REJ-货号"]
        again = handoff_svc.apply_recovery(pack["id"])
        assert again["newly_delivered"] == []
        assert again["delivered"] == ["CORR-SAMP-REJ-货号"]
        sign = handoff_svc.signoff_correction(pack["id"])
        assert sign["signed"] == ["CORR-SAMP-REJ-货号"]

        # 核对更正件只覆盖货号的来源行，数量仍待复核
        detail = handoff_svc.recovery_detail(pack["id"])
        assert detail["覆盖字段"] == ["货号"]
        delivered_rows = [r for d in detail["已交付更正"] for r in d["来源行"]]
        assert delivered_rows
        assert all(":货号:" in r for r in delivered_rows)
        assert all(":数量:" not in r for r in delivered_rows)
        assert detail["待复核字段"] == ["数量"]

        # 第一次签收与原驳回都能按当时样本分别回查
        rejections = sample_svc.sample_history(rejected["id"], "样本驳回")
        assert len(rejections) == 1
        assert rejections[0]["证据"] == "原始影像v1"
        assert rejections[0]["当时状态"] == "已驳回"
        signoffs = sample_svc.sample_history(passed["id"], "样本签收")
        assert len(signoffs) == 1
        assert signoffs[0]["样本编号"] == "SAMP-OK"

        # 已恢复的来源行已关联到采集样本
        sample_entry = sample_svc.get_entry(rejected["id"])
        assert sample_entry["_recovered_rows"]["货号"] == [
            f"SAMP-REJ:货号:R{i}" for i in range(1, 5)
        ]


class TestFieldReview:
    """字段级复核：更正明细生成、状态推进、幂等与并发。"""

    def test_review_pass_generates_correction_detail(self):
        _, rejected, *_ = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        result = sample_svc.submit_field_review(rejected["id"], "货号", "新影像v2", True)
        correction = result["correction"]
        assert correction["字段"] == "货号"
        assert correction["原结论"] == "驳回"
        assert correction["复核结论"] == "通过"
        assert correction["恢复行数"] == "4"
        assert correction["签收状态"] == "待签收"

    def test_status_advances_by_passed_items(self):
        _, rejected, *_ = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        sample_svc.submit_field_review(rejected["id"], "货号", "新影像v2", True)
        assert sample_svc.get_entry(rejected["id"])["status"] == "抽检中"
        sample_svc.submit_field_review(rejected["id"], "数量", "新影像v3", True)
        assert sample_svc.get_entry(rejected["id"])["status"] == "已通过"

    def test_conclusion_change_readvances_status(self):
        _, rejected, *_ = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        sample_svc.submit_field_review(rejected["id"], "货号", "新影像v2", True)
        sample_svc.submit_field_review(rejected["id"], "数量", "新影像v3", True)
        assert sample_svc.get_entry(rejected["id"])["status"] == "已通过"
        # 货号结论由通过变更为驳回：更正撤回，状态回落
        changed = sample_svc.submit_field_review(rejected["id"], "货号", "复核影像v4", False)
        assert changed["changed"] is True
        assert sample_svc.get_entry(rejected["id"])["status"] == "抽检中"
        fields = [c["字段"] for c in sample_svc.correction_details(rejected["id"])]
        assert fields == ["数量"]

    def test_review_requires_rejected_sample(self):
        _, _, passed, pending, _ = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        result = sample_svc.submit_field_review(passed["id"], "货号", "新影像", True)
        assert result["ok"] is False
        result = sample_svc.submit_field_review(pending["id"], "货号", "新影像", True)
        assert result["ok"] is False

    def test_review_rejects_unknown_field(self):
        _, rejected, *_ = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        result = sample_svc.submit_field_review(rejected["id"], "单价", "新影像", True)
        assert result["ok"] is False

    def test_concurrent_submission_generates_one_correction(self):
        _, rejected, *_ = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        results = []

        def submit():
            results.append(
                sample_svc.submit_field_review(rejected["id"], "货号", "新影像v2", True))

        threads = [threading.Thread(target=submit) for _ in range(6)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert all(r["ok"] for r in results)
        assert sum(1 for r in results if r.get("deduplicated")) == 5
        assert len(sample_svc.correction_details(rejected["id"])) == 1


class TestHandoffRecovery:
    """交接恢复：旧回执冲突、暂停中断、交付幂等。"""

    def test_old_receipt_does_not_undo_recovered_fields(self):
        _, rejected, _, _, pack = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        handoff_svc = service_for("handoff_pack")
        sample_svc.submit_field_review(rejected["id"], "货号", "新影像v2", True)
        handoff_svc.run_action(pack["id"], "启动打包")
        handoff_svc.apply_recovery(pack["id"])

        # 旧退回回执迟到重传：已恢复的货号仍可交接，未更正的数量依然不可用
        receipt = handoff_svc.receive_return_receipt(pack["id"], "RCPT-1", "迟到重传")
        assert receipt["duplicate"] is True
        assert receipt["recovered_fields"] == ["货号"]
        detail = handoff_svc.recovery_detail(pack["id"])
        assert detail["status"] == "已交接"
        assert detail["覆盖字段"] == ["货号"]
        assert detail["待复核字段"] == ["数量"]
        assert len(detail["退回回执"]) == 2

    def test_paused_handoff_interrupts_recovery(self):
        _, rejected, _, pending, pack = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        handoff_svc = service_for("handoff_pack")
        sample_svc.submit_field_review(rejected["id"], "货号", "新影像v2", True)
        handoff_svc.run_action(pack["id"], "启动打包")
        handoff_svc.prepare_recovery_sheet(pack["id"])
        handoff_svc.pause_handoff(pack["id"])

        result = handoff_svc.apply_recovery(pack["id"])
        assert result["ok"] is False
        assert result["status"] == "paused"
        # 恢复中断时，尚未处理的样本仍待抽检
        assert sample_svc.get_entry(pending["id"])["status"] == "待抽检"
        assert handoff_svc.recovery_detail(pack["id"])["已交付更正"] == []

    def test_delivery_is_idempotent_under_concurrency(self):
        _, rejected, *_ , pack = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        handoff_svc = service_for("handoff_pack")
        sample_svc.submit_field_review(rejected["id"], "货号", "新影像v2", True)
        handoff_svc.run_action(pack["id"], "启动打包")
        handoff_svc.prepare_recovery_sheet(pack["id"])

        outcomes = []

        def recover():
            outcomes.append(handoff_svc.apply_recovery(pack["id"]))

        threads = [threading.Thread(target=recover) for _ in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        total_new = sum(len(o.get("newly_delivered", [])) for o in outcomes)
        assert total_new == 1
        detail = handoff_svc.recovery_detail(pack["id"])
        assert len(detail["已交付更正"]) == 1

    def test_uncorrected_quantity_stays_unavailable(self):
        _, rejected, *_ , pack = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        handoff_svc = service_for("handoff_pack")
        sample_svc.submit_field_review(rejected["id"], "货号", "新影像v2", True)
        handoff_svc.run_action(pack["id"], "启动打包")
        handoff_svc.apply_recovery(pack["id"])
        detail = handoff_svc.recovery_detail(pack["id"])
        assert "数量" not in detail["覆盖字段"]
        assert detail["待复核字段"] == ["数量"]
        # 数量未生成更正明细
        corrections = sample_svc.correction_details(rejected["id"])
        assert all(c["字段"] != "数量" for c in corrections)

    def test_correction_signoff_marks_sample_side(self):
        _, rejected, *_ , pack = _setup_rejected_batch()
        sample_svc = service_for("sample_check")
        handoff_svc = service_for("handoff_pack")
        sample_svc.submit_field_review(rejected["id"], "货号", "新影像v2", True)
        handoff_svc.run_action(pack["id"], "启动打包")
        handoff_svc.apply_recovery(pack["id"])
        handoff_svc.signoff_correction(pack["id"], "CORR-SAMP-REJ-货号")
        correction = sample_svc.correction_details(rejected["id"])[0]
        assert correction["签收状态"] == "已签收"
        events = sample_svc.sample_history(rejected["id"], "更正签收")
        assert len(events) == 1


class TestPipelineLink11_SampleReviewRecovery:
    """链路11: 抽检复核 → 重新采集有效行 → 样本更正明细 → 交接包恢复"""

    def test_link11_recovers_handoff_with_corrections(self):
        _, rejected, _, _, pack = _setup_rejected_batch()
        handoff_svc = service_for("handoff_pack")
        handoff_svc.run_action(pack["id"], "启动打包")
        result = sample_review_handoff_recovery(
            rejected["id"], pack["id"], {"货号": True, "数量": False}, "新影像v2")
        assert result["status"] == "recovered"
        assert result["sample_status"] == "抽检中"
        assert result["pending_fields"] == ["数量"]
        assert len(result["corrections"]) == 1
        assert result["corrections"][0]["字段"] == "货号"
        pack_entry = handoff_svc.get_entry(pack["id"])
        assert pack_entry["status"] == "已交接"
        assert pack_entry["记录行数"] == "4"

    def test_link11_all_fields_pass_completes_sample(self):
        _, rejected, _, _, pack = _setup_rejected_batch()
        handoff_svc = service_for("handoff_pack")
        handoff_svc.run_action(pack["id"], "启动打包")
        result = sample_review_handoff_recovery(
            rejected["id"], pack["id"], {"货号": True, "数量": True}, "新影像v2")
        assert result["status"] == "recovered"
        assert result["sample_status"] == "已通过"
        assert result["pending_fields"] == []
        assert result["sheet"]["恢复行数"] == 8
