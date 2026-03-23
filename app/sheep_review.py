import json
from typing import Any, Dict, List, Optional


KNOWN_REVIEW_STATUSES = {
    "auto_managed",
    "queued",
    "running",
    "rejected",
    "error",
    "not_eligible",
}

PIPELINE_REVIEW_STATUSES = {
    "auto_managed",
    "queued",
    "running",
}

REJECT_KEYWORDS = (
    "reject",
    "rejected",
    "threshold",
    "not eligible",
    "未達",
    "門檻",
    "淘汰",
)

ERROR_KEYWORDS = (
    "error",
    "exception",
    "failed",
    "timeout",
    "crash",
    "錯誤",
    "異常",
    "失敗",
)


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
                "label": "最少交易數",
                "actual": trades,
                "threshold": int(min_trades),
                "comparator": ">=",
                "message": f"最少交易數為 {trades}，低於門檻 {int(min_trades)}。",
            }
        )
    if total_return_pct < float(min_total_return_pct):
        failures.append(
            {
                "code": "min_total_return_pct",
                "label": "總報酬率",
                "actual": round(total_return_pct, 4),
                "threshold": float(min_total_return_pct),
                "comparator": ">=",
                "message": f"總報酬率 {total_return_pct:.2f}% 低於門檻 {float(min_total_return_pct):.2f}%。",
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
                "message": f"最大回撤 {max_drawdown:.2f}% 高於門檻 {float(max_drawdown_pct):.2f}%。",
            }
        )
    if sharpe < float(min_sharpe):
        failures.append(
            {
                "code": "min_sharpe",
                "label": "夏普值",
                "actual": round(sharpe, 6),
                "threshold": float(min_sharpe),
                "comparator": ">=",
                "message": f"夏普值 {sharpe:.2f} 低於門檻 {float(min_sharpe):.2f}。",
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


def _keyword_match(text: str, keywords: tuple[str, ...]) -> bool:
    haystack = str(text or "").strip().lower()
    if not haystack:
        return False
    return any(keyword in haystack for keyword in keywords)


def _default_review_reason(review_status: str, progress: Dict[str, Any], last_error: str) -> str:
    if review_status == "auto_managed":
        return "已通過審核並進入自動管理流程。"
    if review_status == "queued":
        return "已達標，等待進入後續自動管理流程。"
    if review_status == "running":
        return "已達標，正在執行後續自動管理流程。"
    if review_status == "rejected":
        return last_error or "未通過門檻審核。"
    if review_status == "error":
        return str(progress.get("verify_error") or last_error or "審核流程發生錯誤。").strip()
    if review_status == "not_eligible":
        return last_error or "尚未達到進入後續自動管理的資格。"
    return ""


def _has_reject_signal(
    progress: Dict[str, Any],
    *,
    review_reason: str = "",
    last_reject_reason: str = "",
    last_error: str = "",
) -> bool:
    if _normalized_failures(progress):
        return True
    if str(last_reject_reason or "").strip():
        return True
    return _keyword_match(" ".join([review_reason, last_reject_reason, last_error]), REJECT_KEYWORDS)


def _has_error_signal(last_error: str, task_status_norm: str) -> bool:
    if task_status_norm == "error":
        return True
    return _keyword_match(last_error, ERROR_KEYWORDS)


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
    if explicit_status == "passed":
        explicit_status = "auto_managed"
    if explicit_oos_status == "passed":
        explicit_oos_status = "auto_managed"
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
    elif _has_reject_signal(progress, review_reason=review_reason, last_reject_reason=last_reject_reason, last_error=last_error):
        review_status = "rejected"
    elif _has_error_signal(last_error, task_status_norm):
        review_status = "error"
    elif task_status_norm in ("running", "syncing"):
        review_status = "running"
    elif task_status_norm in ("assigned", "queued"):
        review_status = "queued"
    else:
        review_status = "not_eligible"

    if not review_reason:
        review_reason = _default_review_reason(review_status, progress, last_error)

    oos_status = explicit_oos_status
    if oos_status not in KNOWN_REVIEW_STATUSES:
        if review_status in KNOWN_REVIEW_STATUSES - {"not_eligible"}:
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


def _merge_review_fields(progress: Dict[str, Any], review: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(progress or {})
    merged["best_any_passed"] = bool(review.get("best_any_passed") or False)
    if review.get("best_any_score") is not None or "best_any_score" in merged:
        merged["best_any_score"] = review.get("best_any_score")
    merged["review_status"] = str(review.get("review_status") or "not_eligible")
    merged["review_reason"] = str(review.get("review_reason") or "")
    merged["review_failures"] = list(review.get("review_failures") or [])
    merged["oos_status"] = str(review.get("oos_status") or "not_eligible")
    return merged


def rebuild_review_progress(
    progress_like: Any,
    task_status: str = "",
    *,
    has_follow_on_strategy: bool = False,
) -> Dict[str, Any]:
    progress = parse_progress_json(progress_like)
    explicit_status = str(progress.get("review_status") or "").strip().lower()
    explicit_oos_status = str(progress.get("oos_status") or "").strip().lower()
    explicit_valid = explicit_status in KNOWN_REVIEW_STATUSES or explicit_oos_status in KNOWN_REVIEW_STATUSES

    if explicit_valid:
        if has_follow_on_strategy and not bool(progress.get("best_any_passed") or False):
            progress["best_any_passed"] = True
        return _merge_review_fields(progress, normalize_review_fields(progress, task_status))

    if has_follow_on_strategy:
        progress["best_any_passed"] = True
        progress["review_status"] = "auto_managed"
        progress["oos_status"] = "auto_managed"
        progress["review_failures"] = []
        progress["last_reject_reason"] = ""
        progress["review_reason"] = str(progress.get("review_reason") or "已通過審核並進入自動管理流程。").strip()
        return _merge_review_fields(progress, normalize_review_fields(progress, task_status))

    if bool(progress.get("best_any_passed") or False):
        progress["best_any_passed"] = True
        return _merge_review_fields(progress, normalize_review_fields(progress, task_status))

    last_error = str(progress.get("last_error") or "").strip()
    if _has_reject_signal(
        progress,
        review_reason=str(progress.get("review_reason") or "").strip(),
        last_reject_reason=str(progress.get("last_reject_reason") or "").strip(),
        last_error=last_error,
    ):
        progress["best_any_passed"] = False
        progress["review_status"] = "rejected"
        progress["oos_status"] = "rejected"
        return _merge_review_fields(progress, normalize_review_fields(progress, task_status))

    if _has_error_signal(last_error, str(task_status or "").strip().lower()):
        progress["best_any_passed"] = False
        progress["review_status"] = "error"
        progress["oos_status"] = "error"
        return _merge_review_fields(progress, normalize_review_fields(progress, task_status))

    progress["best_any_passed"] = False
    progress["review_status"] = "not_eligible"
    progress["oos_status"] = "not_eligible"
    return _merge_review_fields(progress, normalize_review_fields(progress, task_status))


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
        if str(row.get("review_status") or "") not in PIPELINE_REVIEW_STATUSES:
            continue
        total += 1
    return total


def _stable_progress_json(progress: Dict[str, Any]) -> str:
    return json.dumps(progress or {}, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def rebuild_review_state(
    *,
    db_module=None,
    task_ids: Optional[List[int]] = None,
    limit: int = 0,
    dry_run: bool = False,
) -> Dict[str, Any]:
    if db_module is None:
        import sheep_platform_db as db_module

    params: List[Any] = []
    query = """
        SELECT
            t.id,
            t.status,
            t.progress_json,
            EXISTS(
                SELECT 1
                FROM candidates c
                JOIN submissions su ON su.candidate_id = c.id
                WHERE c.task_id = t.id
                  AND COALESCE(su.status, 'approved') IN ('approved', 'active')
            ) AS has_approved_submission,
            EXISTS(
                SELECT 1
                FROM candidates c
                JOIN submissions su ON su.candidate_id = c.id
                JOIN strategies st ON st.submission_id = su.id
                WHERE c.task_id = t.id
                  AND COALESCE(st.status, '') = 'active'
            ) AS has_active_strategy
        FROM mining_tasks t
        WHERE t.status = 'completed'
    """
    task_id_list = [int(task_id) for task_id in (task_ids or []) if int(task_id or 0) > 0]
    if task_id_list:
        placeholders = ",".join("?" for _ in task_id_list)
        query += f" AND t.id IN ({placeholders})"
        params.extend(task_id_list)
    query += " ORDER BY t.id ASC"
    if int(limit or 0) > 0:
        query += " LIMIT ?"
        params.append(int(limit))

    conn = db_module._conn()
    summary = {
        "scanned": 0,
        "updated": 0,
        "explicit_preserved": 0,
        "auto_managed_repairs": 0,
        "rejected_repairs": 0,
        "error_repairs": 0,
        "not_eligible_repairs": 0,
        "dry_run": bool(dry_run),
    }
    try:
        rows = conn.execute(query, params).fetchall()
        now_iso = db_module._now_iso()
        for row in rows:
            task_id = int(row["id"])
            task_status = str(row["status"] or "")
            current_progress = parse_progress_json(row["progress_json"])
            explicit_status = str(current_progress.get("review_status") or "").strip().lower()
            explicit_oos_status = str(current_progress.get("oos_status") or "").strip().lower()
            if explicit_status in KNOWN_REVIEW_STATUSES or explicit_oos_status in KNOWN_REVIEW_STATUSES:
                summary["explicit_preserved"] += 1

            has_follow_on = bool(row["has_approved_submission"] or False) or bool(row["has_active_strategy"] or False)
            rebuilt = rebuild_review_progress(current_progress, task_status, has_follow_on_strategy=has_follow_on)
            rebuilt_status = str(rebuilt.get("review_status") or "").strip().lower()

            if rebuilt_status == "auto_managed":
                summary["auto_managed_repairs"] += 1
            elif rebuilt_status == "rejected":
                summary["rejected_repairs"] += 1
            elif rebuilt_status == "error":
                summary["error_repairs"] += 1
            elif rebuilt_status == "not_eligible":
                summary["not_eligible_repairs"] += 1

            summary["scanned"] += 1
            if _stable_progress_json(current_progress) == _stable_progress_json(rebuilt):
                continue

            summary["updated"] += 1
            if dry_run:
                continue

            conn.execute(
                "UPDATE mining_tasks SET progress_json = ?, updated_at = ? WHERE id = ?",
                (json.dumps(rebuilt, ensure_ascii=False), now_iso, task_id),
            )

        if not dry_run:
            conn.commit()
        return summary
    finally:
        conn.close()
