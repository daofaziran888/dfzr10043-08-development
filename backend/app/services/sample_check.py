"""抽检样本服务：抽样检查、字段通过率、缺陷记录与复核更正。

对已确认批次做抽样检查，记录字段通过率和缺陷描述，
驳回后批次不得进入交接。支持按比例和行数两种抽样方式。

复核能力：已驳回样本可按来源行字段（货号、数量等）提交重新识别结论，
复核通过的字段重新采集有效行并生成样本更正明细，供交接包恢复；
样本状态按通过项推进，同一字段重复上报相同结论不会重复生成更正，
原驳回证据与签收记录都留在样本历史里供分别回查。
"""
from __future__ import annotations

import threading
from datetime import datetime, timedelta, timezone
from typing import Any

from app.services.base import BaseModuleService


def _now_iso() -> str:
    return datetime.now(timezone(timedelta(hours=8))).isoformat(timespec="seconds")


class SampleCheckService(BaseModuleService):
    MODULE = "sample_check"
    ENTITY = "抽检样本"
    OPERATION_TERM = "关联"
    LIST_FIELDS = [
        "样本编号", "关联批次", "抽样比例", "抽样行数", "字段通过率",
        "缺陷描述", "抽检结论", "抽检时间",
    ]
    REQUIRED_FIELDS = ["样本编号", "关联批次", "抽样比例"]
    STATUS_ORDER = ["待抽检", "抽检中", "已通过", "已驳回"]
    ACTION_RULES = {
        "启动抽检": "抽检中",
        "通过样本": "已通过",
        "驳回样本": "已驳回",
        "提交复核": "抽检中",
    }
    NEGATIVE_ACTIONS = ["驳回样本"]

    def __init__(self) -> None:
        # 复核提交串行化：同一已复核字段并发上报只生成一次更正
        self._review_lock = threading.Lock()

    def accept_sample(self, sample_id: int) -> dict[str, Any]:
        """通过抽检样本。"""
        entry = self.get_entry(sample_id)
        if entry is None:
            return {"ok": False, "reason": "样本不存在"}
        self.run_action(sample_id, "通过样本")
        self._append_history(entry, "样本通过", "抽检结论为通过")
        return {"ok": True, "sample_id": sample_id}

    def reject_sample(self, sample_id: int, defect: str) -> dict[str, Any]:
        """驳回抽检样本，记录缺陷描述。"""
        entry = self.get_entry(sample_id)
        if entry is None:
            return {"ok": False, "reason": "样本不存在"}
        entry["缺陷描述"] = defect
        entry["抽检结论"] = "驳回"
        self.run_action(sample_id, "驳回样本")
        self._append_history(entry, "样本驳回", f"样本因{defect}被驳回", evidence=defect)
        return {"ok": True, "sample_id": sample_id, "defect": defect}

    # ── 复核与更正 ─────────────────────────────────────────────

    def reject_sample_with_fields(
        self, sample_id: int, fields: list[str], defect: str, evidence: str
    ) -> dict[str, Any]:
        """按来源行字段驳回样本，留存驳回证据（如原图模糊）供回查。"""
        entry = self.get_entry(sample_id)
        if entry is None:
            return {"ok": False, "reason": "样本不存在"}
        field_state = entry.setdefault("_fields", {})
        for field in fields:
            field_state[field] = {
                "结论": "驳回",
                "驳回证据": evidence,
                "复核结论": "待复核",
            }
        entry["缺陷描述"] = defect
        entry["抽检结论"] = "驳回"
        self.run_action(sample_id, "驳回样本")
        self._append_history(
            entry, "样本驳回",
            f"字段{'、'.join(fields)}因{defect}被驳回",
            evidence=evidence,
        )
        return {"ok": True, "sample_id": sample_id, "fields": list(fields)}

    def submit_field_review(
        self, sample_id: int, field: str, evidence: str, passed: bool
    ) -> dict[str, Any]:
        """提交字段级复核结论（重新识别结果）。

        幂等：同一字段重复上报相同结论只保留首次结果，不重复生成更正；
        结论变更时按最新通过项重新推进样本状态。
        """
        with self._review_lock:
            entry = self.get_entry(sample_id)
            if entry is None:
                return {"ok": False, "reason": "样本不存在"}
            if entry.get("status") not in ("已驳回", "抽检中", "已通过"):
                return {"ok": False, "reason": f"样本状态「{entry.get('status')}」不支持复核"}
            fields_state = entry.setdefault("_fields", {})
            if field not in fields_state:
                return {"ok": False, "reason": f"字段{field}不在驳回范围"}
            state = fields_state[field]
            conclusion = "通过" if passed else "驳回"
            correction = self._find_correction(entry, field)
            if state.get("复核结论") == conclusion and state.get("复核证据") == evidence:
                # 重复上报同一结论：直接返回首次结果，不重复生成更正
                return {
                    "ok": True, "sample_id": sample_id, "field": field,
                    "conclusion": conclusion, "deduplicated": True,
                    "correction": correction,
                }
            changed = state.get("复核结论") not in (None, "待复核") and state["复核结论"] != conclusion
            state["复核结论"] = conclusion
            state["复核证据"] = evidence
            if passed and correction is None:
                # 复核通过：重新采集该字段有效行，生成样本更正明细
                correction = self._build_correction(entry, field, evidence)
                entry.setdefault("_corrections", []).append(correction)
            elif not passed and correction is not None:
                # 结论由通过变更为驳回：撤回已生成的更正
                entry["_corrections"] = [
                    c for c in entry["_corrections"] if c.get("更正编号") != correction["更正编号"]
                ]
                correction = None
            self._append_history(
                entry, "样本复核",
                f"字段{field}复核结论{conclusion}" + ("（结论变更）" if changed else ""),
                evidence=evidence,
            )
            self._advance_status(entry)
            return {
                "ok": True, "sample_id": sample_id, "field": field,
                "conclusion": conclusion, "changed": changed,
                "correction": correction,
                "sample_status": entry.get("status"),
            }

    def correction_details(self, sample_id: int) -> list[dict[str, Any]]:
        """样本更正明细：复核通过后重新采集的有效行，供交接恢复消费。"""
        entry = self.get_entry(sample_id)
        if entry is None:
            return []
        return [dict(c) for c in entry.get("_corrections", [])]

    def pending_review_fields(self, sample_id: int) -> list[str]:
        """仍未通过复核的驳回字段（如数量仍待复核）。"""
        entry = self.get_entry(sample_id)
        if entry is None:
            return []
        return [
            field for field, state in entry.get("_fields", {}).items()
            if state.get("结论") == "驳回" and state.get("复核结论") != "通过"
        ]

    def sample_history(self, sample_id: int, event_type: str | None = None) -> list[dict[str, Any]]:
        """按样本回查历史：驳回证据、复核结论、签收记录可按事件类型分别查询。"""
        entry = self.get_entry(sample_id)
        if entry is None:
            return []
        events = entry.get("_history", [])
        if event_type:
            events = [e for e in events if e.get("事件") == event_type]
        return [dict(e) for e in events]

    def record_signoff(self, sample_id: int, note: str, kind: str = "样本签收") -> dict[str, Any]:
        """记录一次样本签收，签收时的样本状态一并留存供回查。"""
        entry = self.get_entry(sample_id)
        if entry is None:
            return {"ok": False, "reason": "样本不存在"}
        event = self._append_history(entry, kind, note)
        return {"ok": True, "sample_id": sample_id, "event": event}

    # ── 内部工具 ───────────────────────────────────────────────

    def _advance_status(self, entry: dict[str, Any]) -> None:
        """复核结论变更后按通过项推进样本状态。"""
        fields_state = entry.get("_fields", {})
        rejected = [f for f, s in fields_state.items() if s.get("结论") == "驳回"]
        passed = [f for f in rejected if fields_state[f].get("复核结论") == "通过"]
        if rejected and len(passed) == len(rejected):
            self.run_action(int(entry["id"]), "通过样本")
        elif passed:
            self.run_action(int(entry["id"]), "提交复核")
        elif entry.get("status") != "已驳回":
            # 通过项被撤回：样本回到已驳回，等待重新复核
            self.run_action(int(entry["id"]), "驳回样本")

    def _find_correction(self, entry: dict[str, Any], field: str) -> dict[str, Any] | None:
        for corr in entry.get("_corrections", []):
            if corr.get("字段") == field:
                return corr
        return None

    def _build_correction(self, entry: dict[str, Any], field: str, evidence: str) -> dict[str, Any]:
        """重新采集该字段的有效来源行，生成样本更正明细。"""
        rows = self._source_rows(entry, field)
        return {
            "更正编号": f"CORR-{entry.get('样本编号', entry['id'])}-{field}",
            "样本编号": entry.get("样本编号", ""),
            "关联批次": entry.get("关联批次", ""),
            "字段": field,
            "来源行": rows,
            "恢复行数": str(len(rows)),
            "原结论": "驳回",
            "复核结论": "通过",
            "复核证据": evidence,
            "复核时间": _now_iso(),
            "签收状态": "待签收",
        }

    def _source_rows(self, entry: dict[str, Any], field: str) -> list[str]:
        """重新采集该字段覆盖的有效来源行标识。"""
        try:
            total = int(str(entry.get("抽样行数") or "1"))
        except ValueError:
            total = 1
        total = max(1, total)
        prefix = entry.get("样本编号", entry["id"])
        return [f"{prefix}:{field}:R{i}" for i in range(1, total + 1)]

    def _append_history(
        self, entry: dict[str, Any], event: str, detail: str, evidence: str = ""
    ) -> dict[str, Any]:
        record = {
            "事件": event,
            "细节": detail,
            "证据": evidence,
            "样本编号": entry.get("样本编号", ""),
            "时间": _now_iso(),
            "当时状态": entry.get("status", ""),
        }
        entry.setdefault("_history", []).append(record)
        return record
