import json
import math
from typing import Any, Dict


def _as_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, str):
        try:
            loaded = json.loads(value)
            if isinstance(loaded, dict):
                return dict(loaded)
        except Exception:
            return {}
    return {}


def _int_range_count(start: Any, end: Any, step: Any) -> int:
    try:
        step_i = int(step or 0)
        if step_i <= 0:
            return 0
        start_i = int(start or 0)
        end_i = int(end or 0)
        if end_i < start_i:
            return 0
        return ((end_i - start_i) // step_i) + 1
    except Exception:
        return 0


def _float_range_count(start: Any, end: Any, step: Any) -> int:
    try:
        step_f = float(step or 0.0)
        if step_f <= 0.0:
            return 0
        start_f = float(start or 0.0)
        end_f = float(end or 0.0)
        if end_f + 1e-12 < start_f:
            return 0
        return int(math.floor((end_f - start_f) / step_f + 1e-12)) + 1
    except Exception:
        return 0


def risk_combo_count(family: str, risk_spec: Any) -> int:
    spec = _as_dict(risk_spec)
    hold_count = _int_range_count(spec.get("max_hold_min"), spec.get("max_hold_max"), spec.get("max_hold_step"))
    if str(family or "") in {"TEMA_RSI", "LaguerreRSI_TEMA"}:
        return max(1, hold_count)
    tp_count = _float_range_count(spec.get("tp_min"), spec.get("tp_max"), spec.get("tp_step"))
    sl_count = _float_range_count(spec.get("sl_min"), spec.get("sl_max"), spec.get("sl_step"))
    return max(1, tp_count) * max(1, sl_count) * max(1, hold_count)


def family_combo_count(family: str, grid_spec: Any) -> int:
    spec = _as_dict(grid_spec)
    family_name = str(family or "")

    if family_name == "RSI":
        return _int_range_count(spec.get("rsi_p_min"), spec.get("rsi_p_max"), spec.get("rsi_p_step")) * _int_range_count(
            spec.get("rsi_lv_min"), spec.get("rsi_lv_max"), spec.get("rsi_lv_step")
        )
    if family_name in {"SMA_Cross", "EMA_Cross", "HMA_Cross", "DEMA_Cross", "TEMA_Cross", "WMA_Cross"}:
        total = 0
        fast_min = int(spec.get("fast_min") or 0)
        fast_max = int(spec.get("fast_max") or 0)
        fast_step = max(1, int(spec.get("fast_step") or 1))
        slow_min = int(spec.get("slow_min") or 0)
        slow_max = int(spec.get("slow_max") or 0)
        slow_step = max(1, int(spec.get("slow_step") or 1))
        fast_vals = range(fast_min, fast_max + 1, fast_step)
        slow_vals = list(range(slow_min, slow_max + 1, slow_step))
        for fast in fast_vals:
            total += sum(1 for slow in slow_vals if slow > fast)
        return max(0, total)
    if family_name in {"MACD_Cross", "PPO_Cross", "PVO_Cross"}:
        base = family_combo_count(
            "EMA_Cross",
            {
                key: spec.get(key)
                for key in ("fast_min", "fast_max", "fast_step", "slow_min", "slow_max", "slow_step")
            },
        )
        return base * _int_range_count(spec.get("sig_min"), spec.get("sig_max"), spec.get("sig_step"))
    if family_name == "Bollinger_Touch":
        return _int_range_count(spec.get("bb_p_min"), spec.get("bb_p_max"), spec.get("bb_p_step")) * _float_range_count(
            spec.get("bb_n_min"), spec.get("bb_n_max"), spec.get("bb_n_step")
        )
    if family_name == "Stoch_Oversold":
        return (
            _int_range_count(spec.get("k_min"), spec.get("k_max"), spec.get("k_step"))
            * _int_range_count(spec.get("d_min"), spec.get("d_max"), spec.get("d_step"))
            * _int_range_count(spec.get("stoch_lv_min"), spec.get("stoch_lv_max"), spec.get("stoch_lv_step"))
        )
    if family_name in {"CCI_Oversold", "WillR_Oversold", "MFI_Oversold"}:
        return _int_range_count(spec.get("p_min"), spec.get("p_max"), spec.get("p_step")) * _int_range_count(
            spec.get("lv_min"), spec.get("lv_max"), spec.get("lv_step")
        )
    if family_name == "Donchian_Breakout":
        return _int_range_count(spec.get("look_min"), spec.get("look_max"), spec.get("look_step"))
    if family_name in {"ADX_DI_Cross", "Aroon_Cross", "KAMA_Cross", "TRIX_Cross", "DPO_Revert", "Vortex_Cross"}:
        return _int_range_count(spec.get("p_min"), spec.get("p_max"), spec.get("p_step"))
    if family_name in {"ROC_Threshold", "CMF_Threshold", "EFI_Threshold", "BB_PercentB_Revert", "Aroon_Osc_Threshold"}:
        return _int_range_count(spec.get("p_min"), spec.get("p_max"), spec.get("p_step")) * _float_range_count(
            spec.get("thr_min"), spec.get("thr_max"), spec.get("thr_step")
        )
    if family_name == "ATR_Band_Break":
        return _int_range_count(spec.get("p_min"), spec.get("p_max"), spec.get("p_step")) * _float_range_count(
            spec.get("mult_min"), spec.get("mult_max"), spec.get("mult_step")
        )
    if family_name == "Volatility_Squeeze":
        return (
            _int_range_count(spec.get("p_min"), spec.get("p_max"), spec.get("p_step"))
            * _float_range_count(spec.get("nstd_min"), spec.get("nstd_max"), spec.get("nstd_step"))
            * _float_range_count(spec.get("q_min"), spec.get("q_max"), spec.get("q_step"))
        )
    if family_name in {"OBV_Slope", "ADL_Slope"}:
        return 1
    if family_name == "OB_FVG":
        return (
            _int_range_count(spec.get("obfvg_n_min"), spec.get("obfvg_n_max"), spec.get("obfvg_n_step"))
            * _int_range_count(spec.get("obfvg_w_min"), spec.get("obfvg_w_max"), spec.get("obfvg_w_step"))
            * _float_range_count(spec.get("obfvg_r_min"), spec.get("obfvg_r_max"), spec.get("obfvg_r_step"))
            * _int_range_count(spec.get("obfvg_h_min"), spec.get("obfvg_h_max"), spec.get("obfvg_h_step"))
            * _float_range_count(spec.get("obfvg_g_min"), spec.get("obfvg_g_max"), spec.get("obfvg_g_step"))
            * _float_range_count(spec.get("obfvg_a_min"), spec.get("obfvg_a_max"), spec.get("obfvg_a_step"))
            * _float_range_count(spec.get("obfvg_thr_min"), spec.get("obfvg_thr_max"), spec.get("obfvg_thr_step"))
            * _int_range_count(spec.get("obfvg_rsi_p_min"), spec.get("obfvg_rsi_p_max"), spec.get("obfvg_rsi_p_step"))
            * _float_range_count(spec.get("obfvg_rsi_diff_min"), spec.get("obfvg_rsi_diff_max"), spec.get("obfvg_rsi_diff_step"))
        )
    if family_name == "SMC":
        return _int_range_count(spec.get("smc_len_min"), spec.get("smc_len_max"), spec.get("smc_len_step")) * _int_range_count(
            spec.get("smc_limit_min"), spec.get("smc_limit_max"), spec.get("smc_limit_step")
        )
    if family_name == "LaguerreRSI_TEMA":
        return (
            _float_range_count(spec.get("gamma_min"), spec.get("gamma_max"), spec.get("gamma_step"))
            * _int_range_count(spec.get("tema_min"), spec.get("tema_max"), spec.get("tema_step"))
            * _float_range_count(spec.get("sl_c_min"), spec.get("sl_c_max"), spec.get("sl_c_step"))
            * _float_range_count(spec.get("tp_c_min"), spec.get("tp_c_max"), spec.get("tp_c_step"))
            * _float_range_count(spec.get("tsd_min"), spec.get("tsd_max"), spec.get("tsd_step"))
            * _float_range_count(spec.get("tsa_min"), spec.get("tsa_max"), spec.get("tsa_step"))
        )
    if family_name == "TEMA_RSI":
        return (
            _int_range_count(spec.get("fast_min"), spec.get("fast_max"), spec.get("fast_step"))
            * _int_range_count(spec.get("slow_min"), spec.get("slow_max"), spec.get("slow_step"))
            * _int_range_count(spec.get("rsi_thr_min"), spec.get("rsi_thr_max"), spec.get("rsi_thr_step"))
            * _float_range_count(spec.get("tp_min"), spec.get("tp_max"), spec.get("tp_step"))
            * _float_range_count(spec.get("sl_min"), spec.get("sl_max"), spec.get("sl_step"))
            * _float_range_count(spec.get("act_min"), spec.get("act_max"), spec.get("act_step"))
            * _int_range_count(spec.get("tr_tick_min"), spec.get("tr_tick_max"), spec.get("tr_tick_step"))
        )
    raise ValueError(f"unsupported family for count: {family_name}")


def pool_combo_count(family: str, grid_spec: Any, risk_spec: Any) -> int:
    return int(max(0, family_combo_count(family, grid_spec)) * max(0, risk_combo_count(family, risk_spec)))


def extract_progress_counters(progress_like: Any) -> Dict[str, Any]:
    progress = _as_dict(progress_like)

    def _to_int(value: Any) -> int:
        try:
            parsed = int(float(value or 0))
        except Exception:
            parsed = 0
        return max(0, parsed)

    def _to_float(value: Any) -> float:
        try:
            parsed = float(value or 0.0)
        except Exception:
            parsed = 0.0
        return max(0.0, parsed)

    combos_done = _to_int(progress.get("combos_done", progress.get("done", 0)))
    combos_total = _to_int(progress.get("combos_total", progress.get("total", 0)))
    elapsed_s = _to_float(progress.get("elapsed_s", progress.get("elapsed", 0.0)))
    if combos_total > 0 and combos_done > combos_total:
        combos_done = combos_total
    return {
        "combos_done": combos_done,
        "combos_total": combos_total,
        "elapsed_s": elapsed_s,
    }
