#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import logging
import sys
import warnings
from pathlib import Path

import urllib3

ROOT = Path(__file__).resolve().parent
APP_DIR = ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

from sheep_runtime_paths import import_backtest_panel, project_root


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
warnings.simplefilter(action="ignore", category=FutureWarning)
logging.getLogger("streamlit.runtime.caching.cache_data_api").setLevel(logging.CRITICAL)


def _print(message: str) -> None:
    print(message)


def main() -> int:
    bt, import_error = import_backtest_panel(project_root())
    if bt is None:
        print(f"[fatal] {import_error}")
        return 1

    from sheep_holy_grail_runtime import run_holy_grail_build

    print("=" * 70)
    print("Auto Holy Grail Builder")
    print("=" * 70)
    _print("[status] starting shared holy grail runtime")

    result = run_holy_grail_build(bt_module=bt, log=_print)
    if not result.ok:
        print(f"[fatal] {result.message}")
        if result.warnings:
            for warning in result.warnings:
                print(warning)
        return 1

    print(f"[status] api base: {result.api_base}")
    print(
        "[status] fetched "
        f"{result.strategies_count} strategies, flattened {result.flattened_count}, "
        f"candidates {result.candidate_count}, backtested {result.backtested_count}, "
        f"selected {result.selected_count}"
    )
    print()
    print("Portfolio")
    for row in result.selected_portfolio:
        print(
            f"[{int(row['rank']):02d}] {row['strategy_key']:<32} "
            f"Sharpe={float(row['sharpe']):.2f} Corr={float(row['avg_corr_to_portfolio']):+.4f}"
        )

    metrics = result.portfolio_metrics
    print()
    print("Summary")
    print(
        f"Sharpe={float(metrics.get('sharpe', 0.0)):.2f} | "
        f"CAGR={float(metrics.get('cagr_pct', 0.0)):.2f}% | "
        f"MaxDD={float(metrics.get('max_drawdown_pct', 0.0)):.2f}%"
    )
    if result.report_paths:
        print(f"Summary CSV: {result.report_paths.get('summary_report', '')}")
        print(f"Trades CSV: {result.report_paths.get('trades_report', '')}")
        print(f"Full CSV: {result.report_paths.get('final_report', '')}")
    if result.warnings:
        print()
        print("Warnings")
        for warning in result.warnings:
            print(warning)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
