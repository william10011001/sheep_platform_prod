import json
from typing import Any, Dict, List, Optional


KNOWN_REVIEW_STATUSES = {
    "auto_managed",
    "queued",
    "running",
    "passed",
    "rejected",
    "error",
    "not_eligible",
}


def _as_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return float(default)


def _as_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return int(default)


def parse_progress_json(raw: Any) -> Dict[str, Any]:
    if isinstance(raw, dict):
        return dict(raw)
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
        return dict(parsed) if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def evaluate_thresholds(
    metrics: Dict[str, Any],
    min_trades: int,
    min_total_return_pct: float,
    max_drawdown_pct: float,
    min_sharpe: float,
) -> Dict[str, Any]:
    trades = _as_int(metrics.get("trades"), 0)
    total_return_pct = _as_float(metrics.get("total_return_pct"), 0.0)
    max_drawdown = _as_float(metrics.get("max_drawdown_pct"), 0.0)
    sharpe = _as_float(metrics.get("sharpe"), 0.0)

    failures: List[Dict[str, Any]] = []

    if trades < int(min_trades):
        failures.append(
            {
                "code": "min_trades",
                "label": "交易筆數",
                "actual": trades,
                "threshold": int(min_trades),
                "comparator": ">=",
                "message": f"交易筆數僅 {trades} 筆，小於門檻 {int(min_trades)} 筆",
            }
        )
    if total_return_pct < float(min_total_return_pct):
        failures.append(
            {
                "code": "min_total_return_pct",
                "label": "總報酬",
                "actual": round(total_return_pct, 4),
                "threshold": float(min_total_return_pct),
                "comparator": ">=",
                "message": f"總報酬僅 {total_return_pct:.2f}%，小於門檻 {float(min_total_return_pct):.2f}%",
            }
        )
    if max_drawdown > float(max_drawdown_pct):
        failures.append(
            {
                "code": "max_drawdown_pct",
                "label": "最大回撤",
                "actual": round(max_drawdown, 4),
                "threshold": float(max_drawdown_pct),
                "comparator": "<=",
                "message": f"最大回撤為 {max_drawdown:.2f}%，高於門檻 {float(max_drawdown_pct):.2f}%",
            }
        )
    if sharpe < float(min_sharpe):
        failures.append(
            {
                "code": "min_sharpe",
                "label": "夏普指標",
                "actual": round(sharpe, 6),
                "threshold": float(min_sharpe),
                "comparator": ">=",
                "message": f"夏普指標僅 {sharpe:.2f}，小於門檻 {float(min_sharpe):.2f}",
            }
        )

    return {
        "passed": len(failures) == 0,
        "reason": failures[0]["message"] if failures else "OK",
        "failures": failures,
        "metrics": {
            "trades": trades,
            "total_return_pct": total_return_pct,
            "max_drawdown_pct": max_drawdown,
            "sharpe": sharpe,
        },
    }


def _normalized_failures(progress: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = progress.get("review_failures")
    if not isinstance(raw, list):
        return []
    failures: List[Dict[str, Any]] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        failures.append(
            {
                "code": str(item.get("code") or "").strip(),
                "label": str(item.get("label") or "").strip(),
                "actual": item.get("actual"),
                "threshold": item.get("threshold"),
                "comparator": str(item.get("comparator") or "").strip(),
                "message": str(item.get("message") or "").strip(),
            }
        )
    return failures


def normalize_review_fields(progress_like: Any, task_status: str = "") -> Dict[str, Any]:
    progress = parse_progress_json(progress_like)
    task_status_norm = str(task_status or "").strip().lower()

    best_any_passed = bool(progress.get("best_any_passed") or False)
    best_any_score = progress.get("best_any_score")
    try:
        best_any_score = float(best_any_score) if best_any_score is not None else None
    except Exception:
        best_any_score = None

    explicit_status = str(progress.get("review_status") or "").strip().lower()
    explicit_oos_status = str(progress.get("oos_status") or "").strip().lower()
    review_failures = _normalized_failures(progress)
    review_reason = str(progress.get("review_reason") or "").strip()
    last_reject_reason = str(progress.get("last_reject_reason") or "").strip()
    last_error = str(progress.get("last_error") or "").strip()

    if not review_reason and review_failures:
        review_reason = str(review_failures[0].get("message") or "").strip()
    if not review_reason and last_reject_reason:
        review_reason = last_reject_reason

    review_status = ""
    if explicit_status in KNOWN_REVIEW_STATUSES:
        review_status = explicit_status
    elif explicit_oos_status in KNOWN_REVIEW_STATUSES:
        review_status = explicit_oos_status
    elif best_any_passed:
        if task_status_norm in ("running", "syncing"):
            review_status = "running"
        elif task_status_norm in ("assigned", "queued"):
            review_status = "queued"
        else:
            review_status = "auto_managed"
    elif review_failures or review_reason or last_reject_reason or ("未達標" in last_error):
        review_status = "rejected"
    elif task_status_norm == "error":
        review_status = "error"
    elif task_status_norm in ("running", "syncing"):
        review_status = "running"
    elif task_status_norm in ("assigned", "queued"):
        review_status = "queued"
    else:
        review_status = "not_eligible"

    if not review_reason:
        if review_status == "auto_managed":
            review_reason = "已達標，後續流程由系統自動管理中"
        elif review_status == "queued":
            review_reason = "已達標，等待系統安排後續自動管理流程"
        elif review_status == "running":
            review_reason = "已達標，系統正在執行後續自動管理流程"
        elif review_status == "passed":
            review_reason = "審核已通過"
        elif review_status == "rejected":
            review_reason = last_error or "未通過目前的達標門檻"
        elif review_status == "error":
            review_reason = str(progress.get("verify_error") or last_error or "審核流程發生異常").strip()
        elif review_status == "not_eligible":
            review_reason = last_error or "本次任務尚未進入後續管理流程"

    oos_status = explicit_oos_status
    if not oos_status:
        if review_status in {"auto_managed", "queued", "running", "passed", "rejected", "error"}:
            oos_status = review_status
        else:
            oos_status = "not_eligible"

    return {
        "best_any_passed": bool(best_any_passed),
        "best_any_score": best_any_score,
        "review_status": review_status,
        "review_reason": review_reason,
        "review_failures": review_failures,
        "oos_status": oos_status,
    }


def enrich_task_row(task: Dict[str, Any]) -> Dict[str, Any]:
    row = dict(task or {})
    progress = parse_progress_json(row.get("progress_json"))
    review = normalize_review_fields(progress, str(row.get("status") or ""))

    row["best_any_passed"] = bool(review["best_any_passed"])
    row["best_any_score"] = review["best_any_score"]
    row["review_status"] = review["review_status"]
    row["review_reason"] = review["review_reason"]
    row["review_failures"] = review["review_failures"]
    row["oos_status"] = review["oos_status"]
    return row


def count_review_pipeline_tasks(tasks: List[Dict[str, Any]]) -> int:
    total = 0
    for task in tasks or []:
        row = enrich_task_row(task)
        if not bool(row.get("best_any_passed")):
            continue
        if str(row.get("review_status") or "") in {"rejected", "error"}:
            continue
        total += 1
    return total
