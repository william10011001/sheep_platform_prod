from __future__ import annotations

import os
import time
from datetime import datetime

import paramiko


VM_IP = os.environ.get("SHEEP_FETCH_VM_IP", "").strip()
SSH_PORT = int(os.environ.get("SHEEP_FETCH_VM_PORT", "22") or 22)
USERNAME = os.environ.get("SHEEP_FETCH_VM_USER", "").strip()
PASSWORD = os.environ.get("SHEEP_FETCH_VM_PASS", "").strip()
REMOTE_CSV_PATH = os.environ.get(
    "SHEEP_FETCH_REMOTE_CSV_PATH",
    "/home/wm105020/repo/deploy/top_strategies_report_full.csv",
).strip()
LOCAL_CSV_PATH = os.environ.get("SHEEP_FETCH_LOCAL_CSV_PATH", "top_strategies_report_full.csv").strip()
INTERVAL = int(os.environ.get("SHEEP_FETCH_INTERVAL_SECONDS", "10") or 10)
REMOTE_COMMAND = os.environ.get("SHEEP_FETCH_REMOTE_COMMAND", "").strip()


def _require_env() -> None:
    if not VM_IP or not USERNAME or not PASSWORD:
        raise RuntimeError("Missing SHEEP_FETCH_VM_IP / SHEEP_FETCH_VM_USER / SHEEP_FETCH_VM_PASS")


def fetch_csv_loop() -> None:
    _require_env()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] connecting to {VM_IP} over SSH ...")

    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    ssh.connect(hostname=VM_IP, port=SSH_PORT, username=USERNAME, password=PASSWORD)
    sftp = ssh.open_sftp()

    try:
        while True:
            if REMOTE_COMMAND:
                ssh.exec_command(REMOTE_COMMAND)
            sftp.get(REMOTE_CSV_PATH, LOCAL_CSV_PATH)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] fetched {REMOTE_CSV_PATH} -> {LOCAL_CSV_PATH}")
            time.sleep(max(1, INTERVAL))
    finally:
        try:
            sftp.close()
        finally:
            ssh.close()


if __name__ == "__main__":
    fetch_csv_loop()
