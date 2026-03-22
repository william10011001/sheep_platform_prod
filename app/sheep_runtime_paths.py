from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from typing import Iterable, List, Optional, Tuple


def find_project_root(start: Optional[Path | str] = None) -> Path:
    current = Path(start or __file__).resolve()
    if current.is_file():
        current = current.parent
    for candidate in [current, *current.parents]:
        if (candidate / "app" / "backtest_panel2.py").exists():
            return candidate
    return Path(__file__).resolve().parents[1]


_PROJECT_ROOT = find_project_root(__file__)


def project_root() -> Path:
    return _PROJECT_ROOT


def app_dir() -> Path:
    return project_root() / "app"


def _resolve_env_path(env_name: str, default_path: Path, *, anchor: Optional[Path] = None) -> Path:
    raw = str(os.environ.get(env_name, "") or "").strip()
    if not raw:
        return default_path.resolve()
    path = Path(os.path.expanduser(os.path.expandvars(raw)))
    if not path.is_absolute():
        path = (anchor or project_root()) / path
    return path.resolve()


def runtime_dir() -> Path:
    path = _resolve_env_path("SHEEP_RUNTIME_DIR", project_root(), anchor=project_root())
    path.mkdir(parents=True, exist_ok=True)
    return path


def data_dir() -> Path:
    path = _resolve_env_path("SHEEP_DATA_DIR", app_dir() / "data", anchor=project_root())
    path.mkdir(parents=True, exist_ok=True)
    return path


def data_search_dirs() -> List[Path]:
    dirs: List[Path] = []
    seen = set()
    for candidate in (
        data_dir(),
        app_dir() / "data",
        project_root() / "data",
        runtime_dir() / "data",
    ):
        resolved = candidate.resolve()
        if resolved in seen:
            continue
        seen.add(resolved)
        dirs.append(resolved)
    return dirs


def realtime_dir() -> Path:
    root = project_root()
    for child in root.iterdir():
        if not child.is_dir():
            continue
        try:
            if (child / "tema_rsi_gui_config.json").exists():
                return child.resolve()
            if (child / "tema_rsi_gui.log").exists():
                return child.resolve()
        except Exception:
            continue
    return root


def report_dir() -> Path:
    path = runtime_dir() / "Factor_Dependency_Report"
    path.mkdir(parents=True, exist_ok=True)
    return path


def realtime_config_path() -> Path:
    return realtime_dir() / "tema_rsi_gui_config.json"


def realtime_log_path() -> Path:
    return realtime_dir() / "tema_rsi_gui.log"


def realtime_state_path() -> Path:
    return realtime_dir() / "tema_rsi_state.json"


def realtime_exec_log_dir() -> Path:
    path = realtime_dir() / "execution_logs"
    path.mkdir(parents=True, exist_ok=True)
    return path


def default_worker_id_path() -> Path:
    return runtime_dir() / ".sheep_worker_id"


def compute_worker_id_path() -> Path:
    return data_dir() / ".sheep_compute_worker_id"


def ensure_parent(path: Path | str) -> Path:
    resolved = Path(path).resolve()
    resolved.parent.mkdir(parents=True, exist_ok=True)
    return resolved


def ensure_app_on_syspath(root: Optional[Path | str] = None) -> Path:
    base = Path(root).resolve() if root is not None else project_root()
    app_path = base / "app"
    app_path_str = str(app_path)
    if app_path_str not in sys.path:
        sys.path.insert(0, app_path_str)
    return app_path


def import_backtest_panel(root: Optional[Path | str] = None) -> Tuple[object | None, str | None]:
    try:
        app_path = ensure_app_on_syspath(root)
        module = importlib.import_module("backtest_panel2")
        return module, None
    except Exception as exc:
        app_path = Path(root).resolve() / "app" if root is not None else app_dir()
        return None, f"Unable to import backtest_panel2 from {app_path}: {exc}"


def normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().replace("/", "_").replace(":", "_")


def timeframe_min_to_label(step_min: int) -> str:
    step_min = int(step_min)
    if step_min < 60:
        return f"{step_min}m"
    if step_min == 60:
        return "1h"
    if step_min == 120:
        return "2h"
    if step_min == 240:
        return "4h"
    if step_min == 1440:
        return "1d"
    if step_min == 10080:
        return "1w"
    if step_min == 43200:
        return "1mo"
    if step_min < 1440 and step_min % 60 == 0:
        return f"{step_min // 60}h"
    return f"{step_min}m"


def timeframe_candidate_labels(step_min: int) -> List[str]:
    labels: List[str] = []
    canonical = timeframe_min_to_label(int(step_min))
    legacy = f"{int(step_min)}m"
    for label in (canonical, legacy):
        if label not in labels:
            labels.append(label)
    return labels


def timeframe_interval_string(step_min: int) -> str:
    return timeframe_min_to_label(int(step_min))


def kline_candidate_paths(symbol: str, step_min: int, *, years: int = 3) -> List[Path]:
    safe_symbol = normalize_symbol(symbol)
    paths: List[Path] = []
    seen = set()
    for directory in data_search_dirs():
        for label in timeframe_candidate_labels(step_min):
            for filename in (
                f"{safe_symbol}_{label}_{int(years)}y.csv",
                f"{safe_symbol}_{label}.csv",
            ):
                path = (directory / filename).resolve()
                if path in seen:
                    continue
                seen.add(path)
                paths.append(path)
    return paths


def unique_existing_paths(paths: Iterable[Path | str]) -> List[Path]:
    out: List[Path] = []
    seen = set()
    for path in paths:
        resolved = Path(path).resolve()
        if resolved in seen or not resolved.exists():
            continue
        seen.add(resolved)
        out.append(resolved)
    return out
