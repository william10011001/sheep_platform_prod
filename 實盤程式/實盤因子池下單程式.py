#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from pathlib import Path
import sys


_WRAPPER_FILE = Path(__file__).resolve()
PROJECT_ROOT = _WRAPPER_FILE.parents[1]
APP_DIR = PROJECT_ROOT / "app"
if str(APP_DIR) not in sys.path:
    sys.path.insert(0, str(APP_DIR))

_RUNTIME_FILE = APP_DIR / "sheep_realtime" / "runtime_legacy.py"
_ORIGINAL_NAME = __name__
_ORIGINAL_FILE = __file__
globals()["__file__"] = str(_RUNTIME_FILE)
globals()["__name__"] = "sheep_realtime_runtime_legacy_embedded"
exec(compile(_RUNTIME_FILE.read_text(encoding="utf-8-sig"), str(_RUNTIME_FILE), "exec"), globals(), globals())
globals()["__name__"] = _ORIGINAL_NAME
globals()["__file__"] = _ORIGINAL_FILE


if __name__ == "__main__":
    main()
