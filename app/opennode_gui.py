from __future__ import annotations

import datetime as dt
import json
import logging
import multiprocessing
import os
import queue
import sys
import threading
import time
import traceback
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional

import tkinter as tk
from tkinter import messagebox, ttk

from sheep_runtime_paths import default_worker_id_path, ensure_parent, runtime_dir


if sys.stdout is not None:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if sys.stderr is not None:
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")


APP_NAME = "OpenNode"
DEFAULT_SERVER_URL = os.environ.get("SHEEP_WORKER_BASE_URL", "https://sheep123.com/sheep123").strip()
APP_RUNTIME_DIR = (runtime_dir() / "OpenNode").resolve()
CONFIG_FILE = ensure_parent(APP_RUNTIME_DIR / "worker_config.json")
LOG_FILE = ensure_parent(APP_RUNTIME_DIR / "logs" / "opennode.log")


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _setup_logger() -> logging.Logger:
    logger = logging.getLogger("opennode")
    if logger.handlers:
        return logger
    logger.setLevel(logging.INFO)
    handler = RotatingFileHandler(str(LOG_FILE), maxBytes=2 * 1024 * 1024, backupCount=5, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
    logger.addHandler(handler)
    logger.propagate = False
    return logger


LOGGER = _setup_logger()
_WORKER_MODULE = None


def _worker_client():
    global _WORKER_MODULE
    if _WORKER_MODULE is None:
        import sheep_worker_client as worker_module

        _WORKER_MODULE = worker_module
    return _WORKER_MODULE


class OpenNodeApp(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title(APP_NAME)
        self.geometry("620x620")
        self.minsize(560, 560)
        self.configure(bg="#0b0f14")

        self.queue: "queue.Queue[Dict[str, Any]]" = queue.Queue()
        self.worker_thread: Optional[threading.Thread] = None
        self.is_running = False

        self.server_url_var = tk.StringVar(value=DEFAULT_SERVER_URL)
        self.token_var = tk.StringVar(value="")
        self.status_var = tk.StringVar(value="待命中")
        self.progress_var = tk.StringVar(value="0%")
        self.detail_var = tk.StringVar(value="尚未開始")
        self.speed_var = tk.StringVar(value="0.0 H/s")

        self._build_style()
        self._build_ui()
        self._load_config()
        self.after(180, self._drain_queue)
        self.protocol("WM_DELETE_WINDOW", self._on_close)

    def _build_style(self) -> None:
        style = ttk.Style()
        try:
            style.theme_use("clam")
        except Exception:
            pass
        style.configure("Cyber.TFrame", background="#0f1720")
        style.configure("Cyber.TLabel", background="#0f1720", foreground="#e2e8f0")
        style.configure("Muted.TLabel", background="#0f1720", foreground="#94a3b8")
        style.configure("Accent.TButton", padding=8)
        style.configure("Tree.Horizontal.TProgressbar", troughcolor="#111827", background="#10b981", bordercolor="#111827", lightcolor="#10b981", darkcolor="#10b981")

    def _build_ui(self) -> None:
        root = ttk.Frame(self, style="Cyber.TFrame", padding=18)
        root.pack(fill="both", expand=True)

        ttk.Label(root, text="OpenNode", style="Cyber.TLabel", font=("Segoe UI", 22, "bold")).pack(anchor="w")
        ttk.Label(root, text="穩定版節點啟動器", style="Muted.TLabel").pack(anchor="w", pady=(4, 16))

        form = ttk.Frame(root, style="Cyber.TFrame")
        form.pack(fill="x")

        ttk.Label(form, text="網站 API Base URL", style="Muted.TLabel").grid(row=0, column=0, sticky="w", pady=(0, 4))
        self.server_entry = ttk.Entry(form, textvariable=self.server_url_var, width=56)
        self.server_entry.grid(row=1, column=0, sticky="ew", padx=(0, 8))
        ttk.Button(form, text="自檢連線", command=self._check_connection).grid(row=1, column=1, sticky="ew")

        ttk.Label(form, text="專屬節點 Token", style="Muted.TLabel").grid(row=2, column=0, sticky="w", pady=(14, 4))
        self.token_entry = ttk.Entry(form, textvariable=self.token_var, width=56)
        self.token_entry.grid(row=3, column=0, sticky="ew", padx=(0, 8))
        ttk.Button(form, text="清空", command=lambda: self.token_var.set("")).grid(row=3, column=1, sticky="ew")
        form.columnconfigure(0, weight=1)

        button_bar = ttk.Frame(root, style="Cyber.TFrame")
        button_bar.pack(fill="x", pady=(18, 12))
        self.start_btn = ttk.Button(button_bar, text="開始挖礦", style="Accent.TButton", command=self._action_start)
        self.start_btn.pack(side="left")
        self.pause_btn = ttk.Button(button_bar, text="暫停", command=self._action_pause, state="disabled")
        self.pause_btn.pack(side="left", padx=8)
        ttk.Button(button_bar, text="重新載入設定", command=self._load_config).pack(side="left")

        status_card = ttk.Frame(root, style="Cyber.TFrame")
        status_card.pack(fill="x", pady=(0, 12))
        ttk.Label(status_card, text="狀態", style="Muted.TLabel").grid(row=0, column=0, sticky="w")
        ttk.Label(status_card, textvariable=self.status_var, style="Cyber.TLabel", font=("Segoe UI", 12, "bold")).grid(row=1, column=0, sticky="w")
        ttk.Label(status_card, text="進度", style="Muted.TLabel").grid(row=0, column=1, sticky="e")
        ttk.Label(status_card, textvariable=self.progress_var, style="Cyber.TLabel", font=("Consolas", 12, "bold")).grid(row=1, column=1, sticky="e")
        status_card.columnconfigure(0, weight=1)
        status_card.columnconfigure(1, weight=1)

        self.progress = ttk.Progressbar(root, maximum=100.0, style="Tree.Horizontal.TProgressbar")
        self.progress.pack(fill="x")

        info_bar = ttk.Frame(root, style="Cyber.TFrame")
        info_bar.pack(fill="x", pady=(8, 14))
        ttk.Label(info_bar, textvariable=self.detail_var, style="Muted.TLabel").pack(side="left")
        ttk.Label(info_bar, textvariable=self.speed_var, style="Muted.TLabel").pack(side="right")

        ttk.Label(root, text=f"設定檔：{CONFIG_FILE}", style="Muted.TLabel", wraplength=560).pack(anchor="w", pady=(0, 8))
        ttk.Label(root, text=f"日誌：{LOG_FILE}", style="Muted.TLabel", wraplength=560).pack(anchor="w", pady=(0, 8))

        self.log_text = tk.Text(root, height=18, bg="#020617", fg="#d1fae5", insertbackground="#d1fae5", wrap="word", relief="flat")
        self.log_text.pack(fill="both", expand=True)
        self.log_text.configure(state="disabled")

    def _append_log(self, message: str) -> None:
        text = str(message or "").strip()
        if not text:
            return
        line = f"[{_now_iso()}] {text}"
        LOGGER.info(text)
        self.log_text.configure(state="normal")
        self.log_text.insert("end", line + "\n")
        self.log_text.see("end")
        self.log_text.configure(state="disabled")

    def _load_config(self) -> None:
        try:
            if CONFIG_FILE.exists():
                payload = json.loads(CONFIG_FILE.read_text(encoding="utf-8"))
                self.token_var.set(str(payload.get("token") or ""))
                self.server_url_var.set(str(payload.get("server_url") or DEFAULT_SERVER_URL))
                self._append_log(f"已載入設定 {CONFIG_FILE}")
        except Exception as exc:
            self._append_log(f"載入設定失敗：{exc}")

    def _save_config(self) -> None:
        payload = {
            "token": str(self.token_var.get() or "").strip(),
            "server_url": str(self.server_url_var.get() or "").strip(),
            "updated_at": _now_iso(),
        }
        CONFIG_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def _set_running_state(self, running: bool) -> None:
        self.is_running = bool(running)
        self.start_btn.configure(state="disabled" if running else "normal")
        self.pause_btn.configure(state="normal" if running else "disabled")
        if not running:
            self.pause_btn.configure(text="暫停")

    def _normalize_server_url(self) -> str:
        worker = _worker_client()
        raw = str(self.server_url_var.get() or "").strip() or DEFAULT_SERVER_URL
        return worker.normalize_api_base_url(raw)

    def _check_connection(self) -> None:
        token = str(self.token_var.get() or "").strip()
        if not token:
            messagebox.showwarning("缺少 Token", "請先貼上專屬節點 Token。")
            return
        try:
            worker = _worker_client()
            worker_id = worker._load_or_create_worker_id(str(default_worker_id_path()))
            api = worker.ApiClient(base_url=self._normalize_server_url(), token=token, worker_id=worker_id)
            snap = api.get_settings_snapshot()
            self._append_log(f"連線自檢成功，worker_min_version={snap.get('worker_min_version')}")
            self.status_var.set("連線正常")
        except Exception as exc:
            self._append_log(f"連線自檢失敗：{exc}")
            messagebox.showerror("連線失敗", str(exc))

    def _action_start(self) -> None:
        token = str(self.token_var.get() or "").strip()
        if len(token) < 20:
            messagebox.showwarning("Token 不完整", "請貼上網站發出的專屬節點 Token。")
            return
        try:
            self._save_config()
        except Exception as exc:
            messagebox.showerror("設定保存失敗", str(exc))
            return
        self._set_running_state(True)
        self.status_var.set("正在初始化")
        self.detail_var.set("準備與 sheep123.com 建立連線")
        self.progress.configure(value=0.0)
        self.progress_var.set("0%")
        self.speed_var.set("0.0 H/s")
        self.worker_thread = threading.Thread(target=self._worker_loop, args=(token,), daemon=True)
        self.worker_thread.start()

    def _action_pause(self) -> None:
        worker = _worker_client()
        paused = bool(getattr(worker, "GUI_PAUSED", False))
        worker.GUI_PAUSED = not paused
        self.pause_btn.configure(text="繼續挖礦" if not paused else "暫停")
        self.status_var.set("已暫停" if not paused else "恢復中")
        self._append_log("已切換暫停狀態")

    def _drain_queue(self) -> None:
        while not self.queue.empty():
            try:
                item = self.queue.get_nowait()
            except queue.Empty:
                break
            msg_type = str(item.get("type") or "")
            if msg_type == "status":
                self.status_var.set(str(item.get("msg") or ""))
                self._append_log(str(item.get("msg") or ""))
                frac = item.get("frac")
                if frac is not None:
                    try:
                        pct = max(0.0, min(100.0, float(frac) * 100.0))
                        self.progress.configure(value=pct)
                        self.progress_var.set(f"{pct:.0f}%")
                    except Exception:
                        pass
            elif msg_type == "progress":
                total = max(1.0, float(item.get("total") or 1.0))
                done = max(0.0, float(item.get("done") or 0.0))
                pct = max(0.0, min(100.0, done / total * 100.0))
                self.progress.configure(value=pct)
                self.progress_var.set(f"{pct:.0f}%")
                self.detail_var.set(f"進度 {done:.0f} / {total:.0f}")
                speed = float(item.get("speed") or 0.0)
                self.speed_var.set(f"{speed:.1f} H/s" if speed < 1000 else f"{speed / 1000.0:.2f} KH/s")
            elif msg_type == "error":
                title = str(item.get("title") or "系統錯誤")
                msg = str(item.get("msg") or "")
                self._append_log(f"{title}: {msg}")
                messagebox.showerror(title, msg)
            elif msg_type == "ui_state" and str(item.get("state") or "") == "reset":
                self._set_running_state(False)
        self.after(180, self._drain_queue)

    def _worker_loop(self, token: str) -> None:
        worker = _worker_client()
        worker.GUI_QUEUE = self.queue
        worker.GUI_PAUSED = False
        try:
            worker_id = worker._load_or_create_worker_id(str(default_worker_id_path()))
            api = worker.ApiClient(base_url=self._normalize_server_url(), token=token, worker_id=worker_id)
            self.queue.put({"type": "status", "msg": "正在驗證 Token..."})
            settings = api.get_settings_snapshot()
            thresholds = worker.Thresholds.from_dict(settings.get("thresholds") or {})
            self.queue.put({"type": "status", "msg": "驗證成功，開始待命..."})
            while self.is_running:
                if getattr(worker, "GUI_PAUSED", False):
                    try:
                        api.heartbeat(None)
                    except Exception:
                        pass
                    time.sleep(1.0)
                    continue

                try:
                    flags = api.flags()
                except Exception as exc:
                    self.queue.put({"type": "status", "msg": f"無法取得伺服器狀態：{exc}"})
                    time.sleep(3.0)
                    continue

                run_enabled = bool(flags.get("run_enabled"))
                pending_task_count = int(flags.get("pending_task_count") or 0)
                token_kind = str(flags.get("token_kind") or "")
                reason = str(flags.get("reason") or "")

                if not run_enabled:
                    if reason == "legacy_web_session_token" or token_kind == "web_session":
                        status_msg = "目前貼上的 Token 不是專屬節點 Token，請回網站重新複製。"
                    elif reason == "run_disabled":
                        status_msg = "網站端尚未啟動個人派工，請先在 sheep123.com 按下開始挖礦。"
                    else:
                        status_msg = "已連線，但目前沒有可執行的個人派工。"
                    self.queue.put({"type": "status", "msg": status_msg})
                    self.queue.put({"type": "progress", "done": 0, "total": 1, "speed": 0.0})
                    try:
                        api.heartbeat(None)
                    except Exception:
                        pass
                    time.sleep(3.0)
                    continue

                try:
                    api.heartbeat(None)
                except Exception:
                    pass

                if pending_task_count > 0:
                    self.queue.put({"type": "status", "msg": f"偵測到 {pending_task_count} 個待領取任務，正在嘗試領取..."})
                else:
                    self.queue.put({"type": "status", "msg": "已啟動個人派工，等待新任務..."})

                try:
                    task = api.claim_task()
                except Exception as exc:
                    self.queue.put({"type": "status", "msg": f"領取任務失敗：{exc}"})
                    time.sleep(2.0)
                    continue

                if not isinstance(task, dict) or "task_id" not in task:
                    self.queue.put({"type": "status", "msg": "目前沒有可分配任務，持續待命中..."})
                    self.queue.put({"type": "progress", "done": 0, "total": 1, "speed": 0.0})
                    time.sleep(2.0)
                    continue

                self.queue.put(
                    {
                        "type": "status",
                        "msg": f"執行任務 #{task.get('task_id')} | {task.get('symbol')} | {task.get('timeframe_min')}m",
                    }
                )
                self.queue.put({"type": "progress", "done": 0, "total": 1, "speed": 0.0})
                worker.run_task(api, task, thresholds, flag_poll_s=5.0, commit_every=25)
                time.sleep(1.0)
        except Exception as exc:
            LOGGER.error("OpenNode worker loop crash: %s\n%s", exc, traceback.format_exc())
            self.queue.put(
                {
                    "type": "error",
                    "title": "OpenNode 執行失敗",
                    "msg": f"{exc}\n\n詳細堆疊已寫入 {LOG_FILE}",
                }
            )
        finally:
            self.queue.put({"type": "ui_state", "state": "reset"})
            self.is_running = False

    def _on_close(self) -> None:
        try:
            self.is_running = False
            worker = _WORKER_MODULE
            if worker is not None:
                worker.GUI_PAUSED = True
            if self.worker_thread and self.worker_thread.is_alive():
                self.worker_thread.join(timeout=5.0)
        finally:
            self.destroy()


def main() -> None:
    multiprocessing.freeze_support()
    app = OpenNodeApp()
    app.mainloop()


if __name__ == "__main__":
    main()
