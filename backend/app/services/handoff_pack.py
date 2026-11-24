"""交接包服务：批次打包、清单校验、下游交接与驳回恢复。

把已确认批次连同字段映射、校验摘要和清单打包成交接包，
供下游数仓直接消费并回溯来源。退回后批次不得再次交接。

恢复能力：交接包可消费抽检样本的更正明细，把已恢复的来源行关联回
采集样本；同一更正并发或重复交付只生效一次；迟到的旧退回回执与当前
复核冲突时，已恢复字段仍可交接，未更正字段保持不可用。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


class HandoffPackService(BaseModuleService):
    MODULE = "handoff_pack"
    ENTITY = "交接包"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "交接编号", "关联批次", "目标系统", "清单文件数", "记录行数",
        "校验摘要", "交接版本", "交接时间",
    ]
    REQUIRED_FIELDS = ["交接编号", "关联批次", "目标系统"]
    STATUS_ORDER = ["待打包", "打包中", "已交接", "交接退回"]
    ACTION_RULES = {"启动打包": "打包中", "确认交接": "已交接", "记录退回": "交接退回"}
    NEGATIVE_ACTIONS = []

    def __init__(self) -> None:
        # 恢复交付串行化：同一更正并发交付只生效一次
        self._recovery_lock = threading.Lock()

    def reject_handoff(self, pack_id: int, reason: str) -> dict[str, Any]:
        """驳回交接包，退回后批次不得进入交接。"""
        entry = self.get_entry(pack_id)
        if entry is None:
            return {"ok": False, "reason": "交接包不存在"}
        self.run_action(pack_id, "记录退回")
        return {"ok": True, "pack_id": pack_id, "reason": reason, "action": "交接退回"}

    def get_delivered_packs(self, target_system: str | None = None) -> list[dict[str, Any]]:
        """获取已交接的包列表，可按目标系统筛选。"""
        items, _ = self.list_entries(size=10000)
        delivered = [it for it in items if it.get("status") == "已交接"]
        if target_system:
            delivered = [it for it in delivered if it.get("目标系统") == target_system]
        return delivered

    # ── 样本签收 ───────────────────────────────────────────────

    def signoff_samples(
        self, pack_id: int, sample_ids: list[int] | None = None
    ) -> dict[str, Any]:
        """签收该批次未被驳回的样本；已驳回样本跳过，签收记录可按样本回查。"""
        from app.services import service_for

        pack = self.get_entry(pack_id)
        if pack is None:
            return {"ok": False, "reason": "交接包不存在"}
        sample_svc = service_for("sample_check")
        all_samples, _ = sample_svc.list_entries(size=10000)
        batch_samples = [
            s for s in all_samples if s.get("关联批次") == pack.get("关联批次")
        ]
        if sample_ids is not None:
            batch_samples = [s for s in batch_samples if s["id"] in sample_ids]
        signed, skipped = [], []
        for sample in batch_samples:
            if sample.get("status") == "已驳回":
                skipped.append(sample["id"])
                continue
            sample_svc.record_signoff(
                sample["id"], f"交接包{pack.get('交接编号')}签收样本"
            )
            pack.setdefault("_sample_signoffs", []).append({
                "样本编号": sample.get("样本编号"),
                "样本ID": sample["id"],
                "交接编号": pack.get("交接编号"),
                "签收时间": _now_iso(),
            })
            signed.append(sample["id"])
        return {
            "ok": True, "pack_id": pack_id,
            "signed": signed, "skipped_rejected": skipped,
        }

    # ── 交接恢复 ───────────────────────────────────────────────

    def prepare_recovery_sheet(self, pack_id: int) -> dict[str, Any]:
        """准备恢复单：汇总关联批次内样本复核通过的更正明细。"""
        from app.services import service_for

        pack = self.get_entry(pack_id)
        if pack is None:
            return {"ok": False, "reason": "交接包不存在"}
        sample_svc = service_for("sample_check")
        all_samples, _ = sample_svc.list_entries(size=10000)
        corrections = []
        for sample in all_samples:
            if sample.get("关联批次") != pack.get("关联批次"):
                continue
            for corr in sample.get("_corrections", []):
                if corr.get("复核结论") == "通过":
                    corrections.append(dict(corr))
        sheet = {
            "恢复单编号": f"RECV-{pack.get('交接编号', pack_id)}",
            "交接编号": pack.get("交接编号", ""),
            "关联批次": pack.get("关联批次", ""),
            "更正明细": corrections,
            "覆盖字段": sorted({c["字段"] for c in corrections}),
            "恢复行数": sum(int(c.get("恢复行数", 0) or 0) for c in corrections),
            "准备时间": _now_iso(),
        }
        pack["_recovery_sheet"] = sheet
        return {"ok": True, "pack_id": pack_id, "sheet": sheet}

    def pause_handoff(self, pack_id: int, reason: str = "接收方核验更正件") -> dict[str, Any]:
        """暂停交接（如准备输出恢复单时等待接收方核验）。"""
        pack = self.get_entry(pack_id)
        if pack is None:
            return {"ok": False, "reason": "交接包不存在"}
        pack["_paused"] = True
        pack["_pause_reason"] = reason
        return {"ok": True, "pack_id": pack_id, "paused": True, "reason": reason}

    def resume_handoff(self, pack_id: int) -> dict[str, Any]:
        """恢复已暂停的交接。"""
        pack = self.get_entry(pack_id)
        if pack is None:
            return {"ok": False, "reason": "交接包不存在"}
        pack["_paused"] = False
        return {"ok": True, "pack_id": pack_id, "paused": False}

    def receive_return_receipt(
        self, pack_id: int, receipt_no: str, note: str = ""
    ) -> dict[str, Any]:
        """登记退回回执。

        迟到或重复到达的旧回执只登记留证：与当前复核冲突时，
        不撤销已恢复字段，已恢复字段保持可交接。
        """
        pack = self.get_entry(pack_id)
        if pack is None:
            return {"ok": False, "reason": "交接包不存在"}
        receipts = pack.setdefault("_return_receipts", [])
        duplicate = any(r.get("回执编号") == receipt_no for r in receipts)
        receipts.append({
            "回执编号": receipt_no,
            "到达时间": _now_iso(),
            "说明": note,
            "重复到达": duplicate,
        })
        recovered = sorted({d["字段"] for d in pack.get("_delivered", {}).values()})
        return {
            "ok": True, "pack_id": pack_id, "duplicate": duplicate,
            "recovered_fields": recovered,
            "message": (
                "旧回执与当前复核冲突，已恢复字段保持可交接"
                if duplicate else "退回回执已登记"
            ),
        }

    def apply_recovery(self, pack_id: int) -> dict[str, Any]:
        """执行交接恢复：把已恢复来源行关联到采集样本。

        幂等：同一更正并发或重复交付只生效一次；
        暂停时恢复中断，尚未处理的样本仍保持待抽检。
        """
        from app.services import service_for

        with self._recovery_lock:
            pack = self.get_entry(pack_id)
            if pack is None:
                return {"ok": False, "reason": "交接包不存在"}
            if pack.get("_paused"):
                return {
                    "ok": False, "status": "paused",
                    "reason": "交接已暂停，恢复中断，未处理样本仍待抽检",
                }
            sheet = pack.get("_recovery_sheet")
            if sheet is None:
                prepared = self.prepare_recovery_sheet(pack_id)
                sheet = prepared["sheet"]
            sample_svc = service_for("sample_check")
            delivered = pack.setdefault("_delivered", {})
            newly_delivered = []
            try:
                for corr in sheet.get("更正明细", []):
                    key = corr.get("更正编号")
                    if not key or key in delivered:
                        continue  # 同一更正只交付一次
                    delivered[key] = {
                        "更正编号": key,
                        "样本编号": corr.get("样本编号", ""),
                        "字段": corr.get("字段", ""),
                        "来源行": list(corr.get("来源行", [])),
                        "交付时间": _now_iso(),
                    }
                    # 把已恢复来源行关联回采集样本
                    self._link_rows_to_sample(sample_svc, pack, corr)
                    newly_delivered.append(key)
            except Exception as exc:  # 恢复中断：未处理的更正留待下次交付
                return {
                    "ok": False, "status": "interrupted",
                    "reason": f"恢复中断：{exc}",
                    "delivered": sorted(delivered),
                    "pending": [
                        c["更正编号"] for c in sheet.get("更正明细", [])
                        if c.get("更正编号") not in delivered
                    ],
                }
            recovered_rows = sum(len(d["来源行"]) for d in delivered.values())
            if delivered:
                pack["记录行数"] = str(recovered_rows)
                pack["_recovered"] = True
                self.run_action(pack_id, "确认交接")
            return {
                "ok": True, "status": "recovered",
                "pack_id": pack_id,
                "newly_delivered": newly_delivered,
                "delivered": sorted(delivered),
                "recovered_rows": recovered_rows,
            }

    def signoff_correction(
        self, pack_id: int, correction_no: str | None = None
    ) -> dict[str, Any]:
        """签收更正件：接收方确认已交付的更正明细，样本侧同步标记已签收。"""
        from app.services import service_for

        pack = self.get_entry(pack_id)
        if pack is None:
            return {"ok": False, "reason": "交接包不存在"}
        delivered = pack.get("_delivered", {})
        targets = [
            key for key in delivered
            if correction_no is None or key == correction_no
        ]
        if not targets:
            return {"ok": False, "reason": "无已交付的更正件可签收"}
        sample_svc = service_for("sample_check")
        signed = []
        for key in targets:
            delivery = delivered[key]
            sample = self._find_sample_by_code(sample_svc, delivery.get("样本编号", ""))
            if sample is not None:
                for corr in sample.get("_corrections", []):
                    if corr.get("更正编号") == key:
                        corr["签收状态"] = "已签收"
                sample_svc.record_signoff(
                    sample["id"], f"更正件{key}已签收", kind="更正签收"
                )
            pack.setdefault("_correction_signoffs", []).append({
                "更正编号": key,
                "签收时间": _now_iso(),
            })
            signed.append(key)
        return {"ok": True, "pack_id": pack_id, "signed": signed}

    def recovery_detail(self, pack_id: int) -> dict[str, Any] | None:
        """恢复详情：已交付更正、待复核字段、退回回执与签收记录。"""
        from app.services import service_for

        pack = self.get_entry(pack_id)
        if pack is None:
            return None
        sample_svc = service_for("sample_check")
        all_samples, _ = sample_svc.list_entries(size=10000)
        pending_fields = sorted({
            field
            for sample in all_samples
            if sample.get("关联批次") == pack.get("关联批次")
            for field in sample_svc.pending_review_fields(sample["id"])
        })
        delivered = pack.get("_delivered", {})
        return {
            "交接编号": pack.get("交接编号", ""),
            "status": pack.get("status"),
            "paused": bool(pack.get("_paused")),
            "恢复单": pack.get("_recovery_sheet"),
            "已交付更正": [dict(d) for d in delivered.values()],
            "覆盖字段": sorted({d["字段"] for d in delivered.values()}),
            "待复核字段": pending_fields,
            "退回回执": list(pack.get("_return_receipts", [])),
            "样本签收": list(pack.get("_sample_signoffs", [])),
            "更正签收": list(pack.get("_correction_signoffs", [])),
        }

    # ── 内部工具 ───────────────────────────────────────────────

    def _link_rows_to_sample(self, sample_svc, pack, corr) -> None:
        """把已恢复的来源行关联到采集样本，样本历史留存交付记录。"""
        sample = self._find_sample_by_code(sample_svc, corr.get("样本编号", ""))
        if sample is None:
            return
        sample.setdefault("_recovered_rows", {})
        sample["_recovered_rows"][corr.get("字段", "")] = list(corr.get("来源行", []))
        sample_svc.record_signoff(
            sample["id"],
            f"更正{corr.get('更正编号')}随交接包{pack.get('交接编号')}交付，"
            f"{len(corr.get('来源行', []))}行来源行恢复",
            kind="交接恢复",
        )

    @staticmethod
    def _find_sample_by_code(sample_svc, sample_code: str) -> dict[str, Any] | None:
        all_samples, _ = sample_svc.list_entries(size=10000)
        for sample in all_samples:
            if sample.get("样本编号") == sample_code:
                return sample
        return None
