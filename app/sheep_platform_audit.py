import math
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd


def _audit_failure(code: str, label: str, actual: Any, threshold: Any, comparator: str, message: str) -> Dict[str, Any]:
    return {
        "code": str(code or ""),
        "label": str(label or ""),
        "actual": actual,
        "threshold": threshold,
        "comparator": str(comparator or ""),
        "message": str(message or ""),
    }


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(default)


def _segment_df(df: pd.DataFrame, start_idx: int, end_idx: int) -> pd.DataFrame:
    start_idx = max(0, int(start_idx))
    end_idx = min(len(df), int(end_idx))
    if end_idx <= start_idx:
        return df.iloc[0:0].copy()
    return df.iloc[start_idx:end_idx].copy()


def _split_indices(n: int, ratios: Tuple[float, float, float]) -> Tuple[Tuple[int, int], Tuple[int, int], Tuple[int, int]]:
    r1, r2, r3 = ratios
    s1 = int(round(n * r1))
    s2 = int(round(n * (r1 + r2)))
    s1 = min(max(s1, 0), n)
    s2 = min(max(s2, s1), n)
    return (0, s1), (s1, s2), (s2, n)


def _metric_pack(bt_result: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "total_return_pct": _safe_float(bt_result.get("total_return_pct")),
        "max_drawdown_pct": _safe_float(bt_result.get("max_drawdown_pct")),
        "sharpe": _safe_float(bt_result.get("sharpe")),
        "sortino": _safe_float(bt_result.get("sortino")),
        "calmar": _safe_float(bt_result.get("calmar")),
        "trades": int(bt_result.get("trades") or 0),
        "win_rate_pct": _safe_float(bt_result.get("win_rate_pct")),
        "expectancy_pct": _safe_float(bt_result.get("expectancy_pct")),
    }


def audit_candidate(
    df: pd.DataFrame,
    run_backtest_fn: Callable[..., Dict[str, Any]],
    family: str,
    family_params: Dict[str, Any],
    tp: float,
    sl: float,
    max_hold: int,
    risk_overrides: Optional[Dict[str, Any]] = None,
    ratios: Tuple[float, float, float] = (0.6, 0.2, 0.2),
    min_trades: int = 40,
    min_oos_return_pct: float = 0.0,
    min_forward_return_pct: float = 0.0,
    min_sharpe_oos: float = 0.0,
    max_drawdown_oos: float = 40.0,
    stress_fee_mult: float = 1.5,
    stress_slippage_mult: float = 1.5,
    monte_carlo_n: int = 200,
    monte_carlo_min_winrate: float = 0.55,
    seed: int = 7,
) -> Dict[str, Any]:
    """
    Overfitting audit for a single parameter set.
    - Time split: in-sample / out-of-sample / forward
    - Stress: higher fee and slippage
    - Monte Carlo: shuffle trade returns and estimate positive-equity probability
    """

    risk_overrides = dict(risk_overrides or {})
    fee_side = float(risk_overrides.get("fee_side", 0.0002))
    slippage = float(risk_overrides.get("slippage", 0.0))
    worst_case = bool(risk_overrides.get("worst_case", True))
    reverse_mode = bool(risk_overrides.get("reverse_mode", False))

    n = len(df)
    (a0, a1), (b0, b1), (c0, c1) = _split_indices(n, ratios)

    df_is = _segment_df(df, a0, a1)
    df_oos = _segment_df(df, b0, b1)
    df_fw = _segment_df(df, c0, c1)

    res_is = run_backtest_fn(df_is, family, family_params, tp, sl, max_hold, fee_side=fee_side, slippage=slippage, worst_case=worst_case, reverse_mode=reverse_mode)
    res_oos = run_backtest_fn(df_oos, family, family_params, tp, sl, max_hold, fee_side=fee_side, slippage=slippage, worst_case=worst_case, reverse_mode=reverse_mode)
    res_fw = run_backtest_fn(df_fw, family, family_params, tp, sl, max_hold, fee_side=fee_side, slippage=slippage, worst_case=worst_case, reverse_mode=reverse_mode)

    m_is = _metric_pack(res_is)
    m_oos = _metric_pack(res_oos)
    m_fw = _metric_pack(res_fw)

    # Stress test on OOS
    res_oos_stress = run_backtest_fn(
        df_oos,
        family,
        family_params,
        tp,
        sl,
        max_hold,
        fee_side=fee_side * float(stress_fee_mult),
        slippage=slippage * float(stress_slippage_mult),
        worst_case=worst_case,
        reverse_mode=reverse_mode,
    )
    m_oos_stress = _metric_pack(res_oos_stress)

    # Monte Carlo on OOS trades
    rng = random.Random(int(seed))
    trade_returns = np.array(res_oos.get("trades_detail") or [], dtype=object)
    tr = []
    try:
        for t in res_oos.get("trades_detail") or []:
            # net_ret is in decimal form; convert to return multiplier delta
            v = t.get("net_ret")
            if v is None:
                continue
            tr.append(float(v))
    except Exception:
        tr = []
    tr = np.array(tr, dtype=np.float64) if tr else np.array([], dtype=np.float64)

    mc_prob_pos = None
    mc_avg_end = None
    if tr.size >= 10 and int(monte_carlo_n) > 0:
        ends = []
        wins = 0
        for _ in range(int(monte_carlo_n)):
            idx = list(range(tr.size))
            rng.shuffle(idx)
            seq = tr[idx]
            eq = 1.0
            for r in seq:
                eq *= (1.0 + r)
            ends.append(eq)
            if eq > 1.0:
                wins += 1
        mc_prob_pos = wins / float(monte_carlo_n)
        mc_avg_end = float(np.mean(ends))

    # Rules
    reasons: List[str] = []
    failure_details: List[Dict[str, Any]] = []

    if m_oos["trades"] < int(min_trades):
        reasons.append("out_of_sample_trade_count")
        failure_details.append(
            _audit_failure(
                "out_of_sample_trade_count",
                "OOS 交易筆數",
                int(m_oos["trades"]),
                int(min_trades),
                ">=",
                f"OOS 交易筆數僅 {int(m_oos['trades'])} 筆，小於門檻 {int(min_trades)} 筆",
            )
        )
    if m_oos["total_return_pct"] < float(min_oos_return_pct):
        reasons.append("out_of_sample_return")
        failure_details.append(
            _audit_failure(
                "out_of_sample_return",
                "OOS 總報酬",
                round(float(m_oos["total_return_pct"]), 4),
                float(min_oos_return_pct),
                ">=",
                f"OOS 總報酬僅 {float(m_oos['total_return_pct']):.2f}%，小於門檻 {float(min_oos_return_pct):.2f}%",
            )
        )
    if m_fw["total_return_pct"] < float(min_forward_return_pct):
        reasons.append("forward_return")
        failure_details.append(
            _audit_failure(
                "forward_return",
                "Forward 總報酬",
                round(float(m_fw["total_return_pct"]), 4),
                float(min_forward_return_pct),
                ">=",
                f"Forward 總報酬僅 {float(m_fw['total_return_pct']):.2f}%，小於門檻 {float(min_forward_return_pct):.2f}%",
            )
        )
    if m_oos["sharpe"] < float(min_sharpe_oos):
        reasons.append("out_of_sample_sharpe")
        failure_details.append(
            _audit_failure(
                "out_of_sample_sharpe",
                "OOS 夏普指標",
                round(float(m_oos["sharpe"]), 6),
                float(min_sharpe_oos),
                ">=",
                f"OOS 夏普指標僅 {float(m_oos['sharpe']):.2f}，小於門檻 {float(min_sharpe_oos):.2f}",
            )
        )
    if m_oos["max_drawdown_pct"] > float(max_drawdown_oos):
        reasons.append("out_of_sample_drawdown")
        failure_details.append(
            _audit_failure(
                "out_of_sample_drawdown",
                "OOS 最大回撤",
                round(float(m_oos["max_drawdown_pct"]), 4),
                float(max_drawdown_oos),
                "<=",
                f"OOS 最大回撤為 {float(m_oos['max_drawdown_pct']):.2f}%，高於門檻 {float(max_drawdown_oos):.2f}%",
            )
        )
    if m_oos_stress["total_return_pct"] < 0.0:
        reasons.append("stress_return")
        failure_details.append(
            _audit_failure(
                "stress_return",
                "壓力測試報酬",
                round(float(m_oos_stress["total_return_pct"]), 4),
                0.0,
                ">=",
                f"壓力測試報酬為 {float(m_oos_stress['total_return_pct']):.2f}%，未達 0%",
            )
        )
    if mc_prob_pos is not None and mc_prob_pos < float(monte_carlo_min_winrate):
        reasons.append("monte_carlo_probability")
        failure_details.append(
            _audit_failure(
                "monte_carlo_probability",
                "蒙地卡羅正報酬機率",
                round(float(mc_prob_pos), 6),
                float(monte_carlo_min_winrate),
                ">=",
                f"蒙地卡羅正報酬機率僅 {float(mc_prob_pos):.2%}，小於門檻 {float(monte_carlo_min_winrate):.0%}",
            )
        )

    # Robustness score: prioritize OOS and forward
    score = (
        0.45 * m_oos["total_return_pct"]
        + 0.35 * m_fw["total_return_pct"]
        + 0.20 * m_is["total_return_pct"]
        - 0.25 * m_oos["max_drawdown_pct"]
    )

    passed = len(reasons) == 0

    return {
        "passed": bool(passed),
        "score": float(score),
        "splits": {
            "in_sample": {"idx": [a0, a1], "metrics": m_is},
            "out_of_sample": {"idx": [b0, b1], "metrics": m_oos},
            "forward": {"idx": [c0, c1], "metrics": m_fw},
        },
        "stress_oos": {"metrics": m_oos_stress, "fee_mult": float(stress_fee_mult), "slippage_mult": float(stress_slippage_mult)},
        "monte_carlo": {"n": int(monte_carlo_n), "prob_positive": mc_prob_pos, "avg_end_equity": mc_avg_end},
        "reasons": reasons,
        "reason_messages": [str(item.get("message") or "") for item in failure_details],
        "failure_details": failure_details,
        "reason_message": str(failure_details[0].get("message") or "") if failure_details else "",
    }
