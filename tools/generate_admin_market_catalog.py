from __future__ import annotations

import hashlib
import json
import math
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List


REPO_ROOT = Path(__file__).resolve().parents[1]
APP_DIR = REPO_ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from sheep_combo_stats import family_combo_count, pool_combo_count, risk_combo_count

OUTPUT_PATH = REPO_ROOT / "catalogs" / "admin_batch_market_catalog_v1.json"
ACTIVE_OUTPUT_PATH = REPO_ROOT / "catalogs" / "admin_batch_market_catalog_active_fine_v1.json"


FAMILIES: List[str] = [
    "RSI",
    "SMA_Cross",
    "EMA_Cross",
    "HMA_Cross",
    "MACD_Cross",
    "PPO_Cross",
    "Bollinger_Touch",
    "Stoch_Oversold",
    "CCI_Oversold",
    "WillR_Oversold",
    "MFI_Oversold",
    "Donchian_Breakout",
    "ADX_DI_Cross",
    "Aroon_Cross",
    "ROC_Threshold",
    "KAMA_Cross",
    "TRIX_Cross",
    "DPO_Revert",
    "CMF_Threshold",
    "OBV_Slope",
    "EFI_Threshold",
    "ATR_Band_Break",
    "Vortex_Cross",
    "PVO_Cross",
    "DEMA_Cross",
    "TEMA_Cross",
    "WMA_Cross",
    "BB_PercentB_Revert",
    "ADL_Slope",
    "Aroon_Osc_Threshold",
    "Volatility_Squeeze",
    "OB_FVG",
    "SMC",
    "LaguerreRSI_TEMA",
    "TEMA_RSI",
]


TF_PERIOD_FACTOR = {
    15: 0.85,
    30: 1.0,
    60: 1.15,
    240: 1.45,
}


@dataclass(frozen=True)
class SymbolProfile:
    symbol: str
    directions: List[str]
    timeframes: List[int]
    years: int
    period_factor: float
    risk_factor: float
    mintick: float
    ref_price: float
    fee_side: float
    slippage: float
    tier: str


SYMBOLS: List[SymbolProfile] = [
    SymbolProfile("BTC_USDT", ["short"], [15, 60, 240], 3, 1.00, 1.00, 0.1, 70692.0, 0.00045, 0.00012, "core"),
    SymbolProfile("ETH_USDT", ["short"], [15, 60, 240], 3, 0.95, 1.08, 0.01, 2151.0, 0.00045, 0.00015, "core"),
    SymbolProfile("XRP_USDT", ["long", "short"], [30, 60, 240], 3, 0.90, 1.18, 0.0001, 1.43, 0.00055, 0.00030, "alt"),
    SymbolProfile("XAUT_USDT", ["long", "short"], [60, 240], 3, 1.18, 0.70, 0.01, 5136.0, 0.00040, 0.00010, "metal"),
    SymbolProfile("XAG_USDT", ["long", "short"], [60, 240], 3, 1.12, 0.82, 0.001, 84.9, 0.00045, 0.00014, "metal"),
    SymbolProfile("SOL_USDT", ["long", "short"], [30, 60, 240], 3, 0.88, 1.25, 0.01, 91.44, 0.00055, 0.00032, "alt"),
    SymbolProfile("ADA_USDT", ["long", "short"], [30, 60, 240], 3, 0.92, 1.18, 0.0001, 0.26135, 0.00055, 0.00028, "alt"),
    SymbolProfile("DOGE_USDT", ["long", "short"], [30, 60, 240], 3, 0.86, 1.28, 0.00001, 0.094244, 0.00060, 0.00036, "alt"),
    SymbolProfile("NEAR_USDT", ["long", "short"], [30, 60, 240], 3, 0.90, 1.22, 0.001, 1.31, 0.00060, 0.00034, "alt"),
    SymbolProfile("AVAX_USDT", ["long", "short"], [30, 60, 240], 3, 0.88, 1.26, 0.01, 9.55, 0.00055, 0.00033, "alt"),
    SymbolProfile("UNI_USDT", ["long", "short"], [30, 60, 240], 3, 0.94, 1.15, 0.001, 3.57, 0.00055, 0.00028, "alt"),
    SymbolProfile("AAVE_USDT", ["long", "short"], [30, 60, 240], 3, 0.92, 1.18, 0.01, 109.84, 0.00055, 0.00030, "alt"),
    SymbolProfile("INJ_USDT", ["long", "short"], [30, 60, 240], 3, 0.86, 1.30, 0.01, 3.08, 0.00060, 0.00036, "alt"),
]


def qint(value: float, minimum: int = 1) -> int:
    return max(int(minimum), int(round(float(value))))


def qfloat(value: float, digits: int = 4, minimum: float | None = None) -> float:
    out = round(float(value), digits)
    if minimum is not None and out < minimum:
        out = minimum
    return round(out, digits)


def period_factor(profile: SymbolProfile, timeframe_min: int) -> float:
    return float(profile.period_factor) * float(TF_PERIOD_FACTOR.get(int(timeframe_min), 1.0))


def interval_text(timeframe_min: int) -> str:
    tf = int(timeframe_min)
    if tf % 1440 == 0:
        return f"{tf // 1440}d"
    if tf % 60 == 0:
        return f"{tf // 60}h"
    return f"{tf}m"


def stable_seed(key: str) -> int:
    seed = int(hashlib.sha1(key.encode("utf-8")).hexdigest()[:8], 16) & 0x7FFFFFFF
    return int(seed or 42)


def risk_spec_for(
    profile: SymbolProfile,
    timeframe_min: int,
    direction: str,
    family: str,
    *,
    fine_grain: bool = False,
) -> Dict[str, Any]:
    risk = float(profile.risk_factor)
    tf = int(timeframe_min)
    hold_map = {
        15: (24, 144, 24),
        30: (16, 96, 16),
        60: (12, 72, 12),
        240: (6, 36, 6),
    }
    mh_min, mh_max, mh_step = hold_map.get(tf, (12, 72, 12))
    if fine_grain:
        mh_step = max(1, int(math.ceil(float(mh_step) / 2.0)))
    if family in {"TEMA_RSI", "LaguerreRSI_TEMA"}:
        return {
            "max_hold_min": int(mh_min),
            "max_hold_max": int(mh_max),
            "max_hold_step": int(mh_step),
            "fee_side": qfloat(profile.fee_side, 6, 0.0),
            "slippage": qfloat(profile.slippage, 6, 0.0),
            "worst_case": True,
            "reverse_mode": direction == "short",
        }

    if profile.tier == "metal":
        tp_base = (0.8, 3.2, 0.8)
        sl_base = (0.6, 2.4, 0.6)
    elif profile.tier == "core":
        tp_base = (1.1, 5.1, 1.0)
        sl_base = (0.8, 4.0, 0.8)
    else:
        tp_base = (1.6, 7.6, 1.2)
        sl_base = (1.1, 5.9, 1.2)

    if tf >= 240:
        tp_mul = 1.20
        sl_mul = 1.15
    elif tf >= 60:
        tp_mul = 1.0
        sl_mul = 1.0
    else:
        tp_mul = 0.88
        sl_mul = 0.92

    tp_min = qfloat(tp_base[0] * risk * tp_mul, 2, 0.3)
    tp_max = qfloat(tp_base[1] * risk * tp_mul, 2, tp_min)
    tp_step = qfloat(tp_base[2] * max(0.8, risk) * tp_mul, 2, 0.1)
    sl_min = qfloat(sl_base[0] * risk * sl_mul, 2, 0.3)
    sl_max = qfloat(sl_base[1] * risk * sl_mul, 2, sl_min)
    sl_step = qfloat(sl_base[2] * max(0.8, risk) * sl_mul, 2, 0.1)

    return {
        "tp_min": tp_min,
        "tp_max": max(tp_min, tp_max),
        "tp_step": tp_step,
        "sl_min": sl_min,
        "sl_max": max(sl_min, sl_max),
        "sl_step": sl_step,
        "max_hold_min": int(mh_min),
        "max_hold_max": int(mh_max),
        "max_hold_step": int(mh_step),
        "fee_side": qfloat(profile.fee_side, 6, 0.0),
        "slippage": qfloat(profile.slippage, 6, 0.0),
        "worst_case": True,
        "reverse_mode": direction == "short",
    }


def tick_grid(profile: SymbolProfile, trail_percents: List[float]) -> tuple[int, int, int]:
    ticks = []
    for pct in trail_percents:
        raw = (profile.ref_price * pct) / profile.mintick
        ticks.append(max(1, int(round(raw))))
    ticks = sorted(set(ticks))
    if len(ticks) == 1:
        return ticks[0], ticks[0], 1
    if len(ticks) == 2:
        step = max(1, ticks[1] - ticks[0])
        return ticks[0], ticks[1], step
    return ticks[0], ticks[-1], max(1, ticks[1] - ticks[0])


def grid_spec_for(profile: SymbolProfile, timeframe_min: int, direction: str, family: str) -> Dict[str, Any]:
    pf = period_factor(profile, timeframe_min)

    def cross_grid() -> Dict[str, Any]:
        fast_min = qint(4 * pf, 3)
        fast_step = qint(max(1.0, 2 * pf), 1)
        fast_max = fast_min + fast_step * 3
        slow_min = max(fast_max + fast_step, qint(20 * pf, fast_max + 1))
        slow_step = qint(max(2.0, 8 * pf), fast_step + 1)
        slow_max = slow_min + slow_step * 3
        return {
            "fast_min": fast_min,
            "fast_max": fast_max,
            "fast_step": fast_step,
            "slow_min": slow_min,
            "slow_max": slow_max,
            "slow_step": slow_step,
        }

    def threshold_period_grid(thr_min: float, thr_max: float, thr_step: float, *, thr_digits: int = 3) -> Dict[str, Any]:
        p_min = qint(8 * pf, 5)
        p_step = qint(max(1.0, 4 * pf), 1)
        p_max = p_min + p_step * 3
        return {
            "p_min": p_min,
            "p_max": p_max,
            "p_step": p_step,
            "thr_min": qfloat(thr_min, thr_digits),
            "thr_max": qfloat(thr_max, thr_digits),
            "thr_step": qfloat(thr_step, thr_digits, 0.001),
        }

    if family == "RSI":
        p_min = qint(7 * pf, 5)
        p_step = qint(max(1.0, 3 * pf), 1)
        p_max = p_min + p_step * 3
        if profile.tier == "metal":
            lv_min, lv_max, lv_step = 24, 42, 6
        elif profile.tier == "core":
            lv_min, lv_max, lv_step = 20, 38, 6
        else:
            lv_min, lv_max, lv_step = 18, 34, 4
        return {
            "rsi_p_min": p_min,
            "rsi_p_max": p_max,
            "rsi_p_step": p_step,
            "rsi_lv_min": lv_min,
            "rsi_lv_max": lv_max,
            "rsi_lv_step": lv_step,
        }

    if family in {"SMA_Cross", "EMA_Cross", "HMA_Cross", "DEMA_Cross", "TEMA_Cross", "WMA_Cross"}:
        return cross_grid()

    if family in {"MACD_Cross", "PPO_Cross", "PVO_Cross"}:
        spec = cross_grid()
        sig_min = qint(5 * pf, 3)
        sig_step = qint(max(1.0, 2 * pf), 1)
        spec.update({"sig_min": sig_min, "sig_max": sig_min + sig_step * 2, "sig_step": sig_step})
        return spec

    if family == "Bollinger_Touch":
        p_min = qint(14 * pf, 10)
        p_step = qint(max(1.0, 4 * pf), 1)
        p_max = p_min + p_step * 3
        nstd_min = 1.6 if profile.tier != "metal" else 1.4
        return {
            "bb_p_min": p_min,
            "bb_p_max": p_max,
            "bb_p_step": p_step,
            "bb_n_min": qfloat(nstd_min, 2),
            "bb_n_max": qfloat(nstd_min + 0.8, 2),
            "bb_n_step": 0.4,
        }

    if family == "Stoch_Oversold":
        k_min = qint(8 * pf, 5)
        k_step = qint(max(1.0, 3 * pf), 1)
        return {
            "k_min": k_min,
            "k_max": k_min + k_step * 2,
            "k_step": k_step,
            "d_min": 3,
            "d_max": 7,
            "d_step": 2,
            "stoch_lv_min": 15 if profile.tier != "metal" else 20,
            "stoch_lv_max": 35 if profile.tier != "metal" else 40,
            "stoch_lv_step": 10,
        }

    if family == "CCI_Oversold":
        p_min = qint(10 * pf, 8)
        p_step = qint(max(1.0, 4 * pf), 1)
        return {
            "p_min": p_min,
            "p_max": p_min + p_step * 3,
            "p_step": p_step,
            "lv_min": -180 if profile.tier == "alt" else -160,
            "lv_max": -80,
            "lv_step": 40,
        }

    if family == "WillR_Oversold":
        p_min = qint(10 * pf, 8)
        p_step = qint(max(1.0, 4 * pf), 1)
        return {
            "p_min": p_min,
            "p_max": p_min + p_step * 3,
            "p_step": p_step,
            "lv_min": -90,
            "lv_max": -70,
            "lv_step": 10,
        }

    if family == "MFI_Oversold":
        p_min = qint(10 * pf, 8)
        p_step = qint(max(1.0, 4 * pf), 1)
        return {
            "p_min": p_min,
            "p_max": p_min + p_step * 3,
            "p_step": p_step,
            "lv_min": 15,
            "lv_max": 35,
            "lv_step": 10,
        }

    if family == "Donchian_Breakout":
        look_min = qint(12 * pf, 8)
        look_step = qint(max(2.0, 8 * pf), 2)
        return {"look_min": look_min, "look_max": look_min + look_step * 3, "look_step": look_step}

    if family in {"ADX_DI_Cross", "Aroon_Cross"}:
        p_min = qint(8 * pf, 6)
        p_step = qint(max(1.0, 4 * pf), 1)
        return {"p_min": p_min, "p_max": p_min + p_step * 3, "p_step": p_step}

    if family == "ROC_Threshold":
        base = 0.007 if profile.tier == "metal" else (0.010 if profile.tier == "core" else 0.014)
        return threshold_period_grid(base, base * (2.5 if profile.tier == "metal" else 3.0), base, thr_digits=4)

    if family in {"KAMA_Cross", "TRIX_Cross", "DPO_Revert", "Vortex_Cross"}:
        p_min = qint(8 * pf, 6)
        p_step = qint(max(1.0, 4 * pf), 1)
        return {"p_min": p_min, "p_max": p_min + p_step * 3, "p_step": p_step}

    if family == "CMF_Threshold":
        return threshold_period_grid(0.02, 0.12 if profile.tier != "metal" else 0.08, 0.05, thr_digits=3)

    if family == "OBV_Slope":
        return {}

    if family == "EFI_Threshold":
        p_min = qint(8 * pf, 5)
        p_step = qint(max(1.0, 4 * pf), 1)
        return {"p_min": p_min, "p_max": p_min + p_step * 2, "p_step": p_step, "thr_min": 0.0, "thr_max": 0.0, "thr_step": 1.0}

    if family == "ATR_Band_Break":
        p_min = qint(10 * pf, 8)
        p_step = qint(max(1.0, 4 * pf), 1)
        mult_min = 1.2 if profile.tier != "metal" else 1.0
        return {
            "p_min": p_min,
            "p_max": p_min + p_step * 3,
            "p_step": p_step,
            "mult_min": qfloat(mult_min, 2),
            "mult_max": qfloat(mult_min + 1.2, 2),
            "mult_step": 0.4,
        }

    if family == "BB_PercentB_Revert":
        p_min = qint(14 * pf, 10)
        p_step = qint(max(1.0, 4 * pf), 1)
        return {"p_min": p_min, "p_max": p_min + p_step * 3, "p_step": p_step, "thr_min": 0.03, "thr_max": 0.12, "thr_step": 0.03}

    if family == "ADL_Slope":
        return {}

    if family == "Aroon_Osc_Threshold":
        p_min = qint(10 * pf, 8)
        p_step = qint(max(1.0, 4 * pf), 1)
        return {"p_min": p_min, "p_max": p_min + p_step * 3, "p_step": p_step, "thr_min": 10.0, "thr_max": 40.0, "thr_step": 10.0}

    if family == "Volatility_Squeeze":
        p_min = qint(14 * pf, 10)
        p_step = qint(max(1.0, 4 * pf), 1)
        return {
            "p_min": p_min,
            "p_max": p_min + p_step * 3,
            "p_step": p_step,
            "nstd_min": 1.8 if profile.tier != "metal" else 1.6,
            "nstd_max": 2.4 if profile.tier != "metal" else 2.2,
            "nstd_step": 0.3,
            "q_min": 0.15,
            "q_max": 0.30,
            "q_step": 0.05,
        }

    if family == "OB_FVG":
        base_r = 0.0012 if profile.tier == "core" else (0.0018 if profile.tier == "alt" else 0.0008)
        return {
            "obfvg_n_min": 2,
            "obfvg_n_max": 4,
            "obfvg_n_step": 2,
            "obfvg_w_min": 20,
            "obfvg_w_max": 30,
            "obfvg_w_step": 10,
            "obfvg_r_min": qfloat(base_r, 4),
            "obfvg_r_max": qfloat(base_r * 2, 4),
            "obfvg_r_step": qfloat(base_r, 4, 0.0001),
            "obfvg_h_min": 12,
            "obfvg_h_max": 24,
            "obfvg_h_step": 12,
            "obfvg_g_min": 0.6,
            "obfvg_g_max": 1.0,
            "obfvg_g_step": 0.4,
            "obfvg_a_min": 0.65,
            "obfvg_a_max": 0.90,
            "obfvg_a_step": 0.25,
            "obfvg_thr_min": 1.001,
            "obfvg_thr_max": 1.003,
            "obfvg_thr_step": 0.002,
            "obfvg_rsi_p_min": 14,
            "obfvg_rsi_p_max": 14,
            "obfvg_rsi_p_step": 1,
            "obfvg_rsi_diff_min": 0.00,
            "obfvg_rsi_diff_max": 0.10,
            "obfvg_rsi_diff_step": 0.10,
            "obfvg_x": 1.0,
            "obfvg_y": 0.0,
            "obfvg_ob_range_based": False,
            "obfvg_reverse": direction == "short",
        }

    if family == "SMC":
        return {
            "smc_len_min": 4,
            "smc_len_max": 10,
            "smc_len_step": 3,
            "smc_limit_min": 2,
            "smc_limit_max": 6,
            "smc_limit_step": 2,
            "smc_reverse": direction == "short",
        }

    if family == "LaguerreRSI_TEMA":
        tema_min = qint(24 * pf, 16)
        tema_step = max(2, qint(12 * pf, 2))
        return {
            "gamma_min": 0.45,
            "gamma_max": 0.55,
            "gamma_step": 0.10,
            "tema_min": tema_min,
            "tema_max": tema_min + tema_step,
            "tema_step": tema_step,
            "sl_c_min": 1.0,
            "sl_c_max": 1.4,
            "sl_c_step": 0.4,
            "tp_c_min": 1.6,
            "tp_c_max": 2.4,
            "tp_c_step": 0.8,
            "tsd_min": 0.9,
            "tsd_max": 1.3,
            "tsd_step": 0.4,
            "tsa_min": 0.8,
            "tsa_max": 1.2,
            "tsa_step": 0.4,
        }

    if family == "TEMA_RSI":
        if profile.tier == "metal":
            tp_vals = [1.0, 1.8]
            sl_vals = [2.4, 3.6]
            act_vals = [0.5, 1.0]
            trail_pcts = [0.003, 0.006]
        elif profile.tier == "core":
            tp_vals = [1.8, 3.0]
            sl_vals = [4.0, 6.0]
            act_vals = [0.8, 1.6]
            trail_pcts = [0.004, 0.008]
        else:
            tp_vals = [2.2, 4.0]
            sl_vals = [4.8, 7.2]
            act_vals = [1.0, 2.0]
            trail_pcts = [0.008, 0.016]
        tr_min, tr_max, tr_step = tick_grid(profile, trail_pcts)
        fast_min = qint(3 * max(0.9, pf), 3)
        fast_step = qint(max(1.0, pf), 1)
        slow_min = qint(48 * pf, fast_min + 10)
        slow_step = max(8, qint(24 * pf, 8))
        return {
            "fast_min": fast_min,
            "fast_max": fast_min + fast_step * 2,
            "fast_step": fast_step,
            "slow_min": slow_min,
            "slow_max": slow_min + slow_step,
            "slow_step": slow_step,
            "rsi_thr_min": 20 if profile.tier != "metal" else 24,
            "rsi_thr_max": 30 if profile.tier != "metal" else 34,
            "rsi_thr_step": 10,
            "tp_min": tp_vals[0],
            "tp_max": tp_vals[-1],
            "tp_step": round(tp_vals[-1] - tp_vals[0], 2),
            "sl_min": sl_vals[0],
            "sl_max": sl_vals[-1],
            "sl_step": round(sl_vals[-1] - sl_vals[0], 2),
            "act_min": act_vals[0],
            "act_max": act_vals[-1],
            "act_step": round(act_vals[-1] - act_vals[0], 2),
            "tr_tick_min": tr_min,
            "tr_tick_max": tr_max,
            "tr_tick_step": max(1, tr_step),
            "mintick": profile.mintick,
            "stake_pct": 95.0,
        }

    raise ValueError(f"unsupported family: {family}")


def midpoint_family_params(family: str, grid_spec: Dict[str, Any], direction: str) -> Dict[str, Any]:
    def imid(a: int, b: int, step: int) -> int:
        count = ((int(b) - int(a)) // int(step)) + 1
        return int(a) + int(step) * (count // 2)

    def fmid(a: float, b: float, step: float, digits: int = 4) -> float:
        count = int(math.floor((float(b) - float(a)) / float(step) + 1e-12)) + 1
        return round(float(a) + float(step) * (count // 2), digits)

    reverse = direction == "short"

    if family == "RSI":
        return {"period": imid(grid_spec["rsi_p_min"], grid_spec["rsi_p_max"], grid_spec["rsi_p_step"]), "enter_level": imid(grid_spec["rsi_lv_min"], grid_spec["rsi_lv_max"], grid_spec["rsi_lv_step"])}
    if family in {"SMA_Cross", "EMA_Cross", "HMA_Cross", "DEMA_Cross", "TEMA_Cross", "WMA_Cross"}:
        fast = imid(grid_spec["fast_min"], grid_spec["fast_max"], grid_spec["fast_step"])
        slow = imid(grid_spec["slow_min"], grid_spec["slow_max"], grid_spec["slow_step"])
        if slow <= fast:
            slow = fast + max(1, int(grid_spec["slow_step"]))
        return {"fast": fast, "slow": slow}
    if family in {"MACD_Cross", "PPO_Cross", "PVO_Cross"}:
        payload = midpoint_family_params("EMA_Cross", {k: grid_spec[k] for k in ("fast_min", "fast_max", "fast_step", "slow_min", "slow_max", "slow_step")}, direction)
        payload["signal"] = imid(grid_spec["sig_min"], grid_spec["sig_max"], grid_spec["sig_step"])
        return payload
    if family == "Bollinger_Touch":
        return {"period": imid(grid_spec["bb_p_min"], grid_spec["bb_p_max"], grid_spec["bb_p_step"]), "nstd": fmid(grid_spec["bb_n_min"], grid_spec["bb_n_max"], grid_spec["bb_n_step"], 2)}
    if family == "Stoch_Oversold":
        return {"k": imid(grid_spec["k_min"], grid_spec["k_max"], grid_spec["k_step"]), "d": imid(grid_spec["d_min"], grid_spec["d_max"], grid_spec["d_step"]), "enter_level": imid(grid_spec["stoch_lv_min"], grid_spec["stoch_lv_max"], grid_spec["stoch_lv_step"])}
    if family in {"CCI_Oversold", "WillR_Oversold", "MFI_Oversold"}:
        return {"period": imid(grid_spec["p_min"], grid_spec["p_max"], grid_spec["p_step"]), "enter_level": imid(grid_spec["lv_min"], grid_spec["lv_max"], grid_spec["lv_step"])}
    if family == "Donchian_Breakout":
        return {"lookback": imid(grid_spec["look_min"], grid_spec["look_max"], grid_spec["look_step"])}
    if family in {"ADX_DI_Cross", "Aroon_Cross"}:
        return {"period": imid(grid_spec["p_min"], grid_spec["p_max"], grid_spec["p_step"])}
    if family in {"ROC_Threshold", "CMF_Threshold", "EFI_Threshold", "BB_PercentB_Revert", "Aroon_Osc_Threshold"}:
        return {"period": imid(grid_spec["p_min"], grid_spec["p_max"], grid_spec["p_step"]), "enter_thr": fmid(grid_spec["thr_min"], grid_spec["thr_max"], grid_spec["thr_step"], 4)}
    if family in {"KAMA_Cross", "TRIX_Cross", "DPO_Revert", "Vortex_Cross"}:
        return {"period": imid(grid_spec["p_min"], grid_spec["p_max"], grid_spec["p_step"])}
    if family == "ATR_Band_Break":
        return {"period": imid(grid_spec["p_min"], grid_spec["p_max"], grid_spec["p_step"]), "mult": fmid(grid_spec["mult_min"], grid_spec["mult_max"], grid_spec["mult_step"], 2)}
    if family == "OBV_Slope":
        return {}
    if family == "ADL_Slope":
        return {}
    if family == "Volatility_Squeeze":
        return {"period": imid(grid_spec["p_min"], grid_spec["p_max"], grid_spec["p_step"]), "nstd": fmid(grid_spec["nstd_min"], grid_spec["nstd_max"], grid_spec["nstd_step"], 2), "quantile": fmid(grid_spec["q_min"], grid_spec["q_max"], grid_spec["q_step"], 2)}
    if family == "OB_FVG":
        return {"N": imid(grid_spec["obfvg_n_min"], grid_spec["obfvg_n_max"], grid_spec["obfvg_n_step"]), "w": imid(grid_spec["obfvg_w_min"], grid_spec["obfvg_w_max"], grid_spec["obfvg_w_step"]), "r": fmid(grid_spec["obfvg_r_min"], grid_spec["obfvg_r_max"], grid_spec["obfvg_r_step"], 4), "h": imid(grid_spec["obfvg_h_min"], grid_spec["obfvg_h_max"], grid_spec["obfvg_h_step"]), "g": fmid(grid_spec["obfvg_g_min"], grid_spec["obfvg_g_max"], grid_spec["obfvg_g_step"], 2), "a": fmid(grid_spec["obfvg_a_min"], grid_spec["obfvg_a_max"], grid_spec["obfvg_a_step"], 2), "rise_thr": fmid(grid_spec["obfvg_thr_min"], grid_spec["obfvg_thr_max"], grid_spec["obfvg_thr_step"], 4), "rsi_period": imid(grid_spec["obfvg_rsi_p_min"], grid_spec["obfvg_rsi_p_max"], grid_spec["obfvg_rsi_p_step"]), "rsi_diff": fmid(grid_spec["obfvg_rsi_diff_min"], grid_spec["obfvg_rsi_diff_max"], grid_spec["obfvg_rsi_diff_step"], 2), "x": float(grid_spec["obfvg_x"]), "y": float(grid_spec["obfvg_y"]), "ob_range_based": bool(grid_spec.get("obfvg_ob_range_based", False)), "reverse": reverse}
    if family == "SMC":
        return {"len": imid(grid_spec["smc_len_min"], grid_spec["smc_len_max"], grid_spec["smc_len_step"]), "limit": imid(grid_spec["smc_limit_min"], grid_spec["smc_limit_max"], grid_spec["smc_limit_step"]), "reverse": reverse}
    if family == "LaguerreRSI_TEMA":
        return {"tema_len": imid(grid_spec["tema_min"], grid_spec["tema_max"], grid_spec["tema_step"]), "gamma": fmid(grid_spec["gamma_min"], grid_spec["gamma_max"], grid_spec["gamma_step"], 2), "ema1_w": 9, "ema2_w": 20, "ema3_w": 40, "low_lookback": 10, "sl_coef": fmid(grid_spec["sl_c_min"], grid_spec["sl_c_max"], grid_spec["sl_c_step"], 2), "tp_coef": fmid(grid_spec["tp_c_min"], grid_spec["tp_c_max"], grid_spec["tp_c_step"], 2), "ts_dist_coef": fmid(grid_spec["tsd_min"], grid_spec["tsd_max"], grid_spec["tsd_step"], 2), "ts_act_coef": fmid(grid_spec["tsa_min"], grid_spec["tsa_max"], grid_spec["tsa_step"], 2), "atr_sltp_len": 15, "atr_trail_len": 18, "atr_act_len": 20}
    if family == "TEMA_RSI":
        return {"fast_len": imid(grid_spec["fast_min"], grid_spec["fast_max"], grid_spec["fast_step"]), "slow_len": imid(grid_spec["slow_min"], grid_spec["slow_max"], grid_spec["slow_step"]), "rsi_len": 14, "rsi_thr": imid(grid_spec["rsi_thr_min"], grid_spec["rsi_thr_max"], grid_spec["rsi_thr_step"]), "tp_pct_strat": fmid(grid_spec["tp_min"], grid_spec["tp_max"], grid_spec["tp_step"], 2), "sl_pct_strat": fmid(grid_spec["sl_min"], grid_spec["sl_max"], grid_spec["sl_step"], 2), "activation_pct": fmid(grid_spec["act_min"], grid_spec["act_max"], grid_spec["act_step"], 2), "trail_ticks": imid(grid_spec["tr_tick_min"], grid_spec["tr_tick_max"], grid_spec["tr_tick_step"]), "mintick": float(grid_spec["mintick"]), "stake_pct": float(grid_spec.get("stake_pct", 95.0))}
    raise ValueError(f"unsupported family midpoint: {family}")


def choose_partitions(
    profile: SymbolProfile,
    family: str,
    total_combos: int,
    *,
    fine_grain: bool = False,
) -> int:
    if fine_grain:
        if family in {"OB_FVG", "TEMA_RSI"}:
            target = 96
        elif family in {"LaguerreRSI_TEMA", "Volatility_Squeeze", "MACD_Cross", "PPO_Cross", "PVO_Cross"}:
            target = 144
        else:
            target = 192
        max_partitions = 128
    else:
        if family in {"OB_FVG", "TEMA_RSI"}:
            target = 120
        elif family in {"LaguerreRSI_TEMA", "Volatility_Squeeze", "MACD_Cross", "PPO_Cross", "PVO_Cross"}:
            target = 180
        else:
            target = 240
        max_partitions = 64
    target = int(target / max(0.8, min(1.35, profile.risk_factor)))
    partitions = max(1, math.ceil(int(total_combos) / max(1, target)))
    return max(4, min(max_partitions, int(partitions)))


def build_catalog(*, active_pools: bool = False, fine_grain: bool = False) -> Dict[str, Any]:
    factor_pools: List[Dict[str, Any]] = []
    strategies: List[Dict[str, Any]] = []
    for profile in SYMBOLS:
        for timeframe_min in profile.timeframes:
            tf_text = interval_text(timeframe_min)
            for family in FAMILIES:
                for direction in profile.directions:
                    key = f"mktv1__{profile.symbol.lower()}__{tf_text}__{family.lower()}__{direction}"
                    grid_spec = grid_spec_for(profile, timeframe_min, direction, family)
                    risk_spec = risk_spec_for(profile, timeframe_min, direction, family, fine_grain=fine_grain)
                    total_combos = pool_combo_count(family, grid_spec, risk_spec)
                    partitions = choose_partitions(profile, family, total_combos, fine_grain=fine_grain)
                    pool_name = f"{profile.symbol} {tf_text} {family} {direction.upper()}"
                    factor_pools.append({"key": key, "name": pool_name, "symbol": profile.symbol, "family": family, "direction": direction, "timeframe_min": int(timeframe_min), "years": int(profile.years), "grid_spec": grid_spec, "risk_spec": risk_spec, "num_partitions": partitions, "seed": stable_seed(key), "active": bool(active_pools), "auto_expand": False})
                    family_params = midpoint_family_params(family, grid_spec, direction)
                    strategies.append({"key": f"{key}__template", "name": f"{pool_name} Template", "family": family, "symbol": profile.symbol, "direction": direction, "interval": tf_text, "family_params": family_params, "tp_pct": float(family_params.get("tp_pct_strat", 0.0) or 0.0), "sl_pct": float(family_params.get("sl_pct_strat", 0.0) or 0.0), "max_hold_bars": int((int(risk_spec["max_hold_min"]) + int(risk_spec["max_hold_max"])) // 2), "stake_pct": 0.0, "status": "disabled", "enabled": False})
    return {"schema_version": 1, "factor_pools": factor_pools, "strategies": strategies}


def catalog_combo_total(payload: Dict[str, Any]) -> int:
    total = 0
    for pool in list(payload.get("factor_pools") or []):
        family = str(pool.get("family") or "")
        grid_spec = dict(pool.get("grid_spec") or {})
        risk_spec = dict(pool.get("risk_spec") or {})
        total += int(pool_combo_count(family, grid_spec, risk_spec))
    return int(total)


def catalog_partition_total(payload: Dict[str, Any]) -> int:
    return int(sum(int(pool.get("num_partitions") or 0) for pool in list(payload.get("factor_pools") or [])))


def main() -> None:
    payload = build_catalog(active_pools=False, fine_grain=False)
    active_payload = build_catalog(active_pools=True, fine_grain=True)
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    ACTIVE_OUTPUT_PATH.write_text(json.dumps(active_payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Wrote {OUTPUT_PATH}")
    print(f"factor_pools={len(payload['factor_pools'])}")
    print(f"strategies={len(payload['strategies'])}")
    print(f"combos={catalog_combo_total(payload)}")
    print(f"partitions={catalog_partition_total(payload)}")
    print(f"Wrote {ACTIVE_OUTPUT_PATH}")
    print(f"factor_pools={len(active_payload['factor_pools'])}")
    print(f"strategies={len(active_payload['strategies'])}")
    print(f"combos={catalog_combo_total(active_payload)}")
    print(f"partitions={catalog_partition_total(active_payload)}")


if __name__ == "__main__":
    main()
