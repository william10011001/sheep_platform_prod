import os
import time
import json
import traceback
import requests
import pandas as pd
from datetime import datetime

# ==========================================
# [配置區] 請根據您的環境調整以下設定
# ==========================================
# [重要] 若伺服器架設在遠端 (例如 DigitalOcean VM)，請務必將 127.0.0.1 改為該伺服器的真實 IP 或網域
API_BASE_URL = os.environ.get("SHEEP_API_URL", "https://sheep123.com/api")
ADMIN_USER = os.environ.get("SHEEP_COMPUTE_USER", "sheep")
# [重要] 請將 YOUR_ADMIN_PASSWORD_HERE 改為您的管理員真實密碼
ADMIN_PASS = os.environ.get("SHEEP_COMPUTE_PASS", "@@Wm105020") 
EXCEL_FILE_PATH = "candidates_report.xlsx"
SYNC_INTERVAL_SECONDS = 30  # 每 30 秒同步一次

class SyncDaemon:
    def __init__(self):
        self.token = ""
        self.last_auth_time = 0

    def _get_token(self):
        """[專家級防護] 負責取得並維護 Admin Token，具備自動重試與錯誤顯示"""
        now = time.time()
        # Token 每 12 小時強制換發一次
        if self.token and (now - self.last_auth_time < 43200):
            return self.token

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在向伺服器申請 Admin Token (目標: {API_BASE_URL})...")
        url = f"{API_BASE_URL.rstrip('/')}/token"
        try:
            r = requests.post(url, json={
                "username": ADMIN_USER,
                "password": ADMIN_PASS,
                "ttl_seconds": 86400,
                "name": "excel_sync_daemon"
            }, timeout=10)
            r.raise_for_status()
            
            try:
                resp_json = r.json()
            except Exception as json_err:
                print(f"\n[FATAL] 伺服器成功連線，但回傳了「非 JSON 格式」的內容 (通常是 HTML 網頁)！")
                print("可能原因與解法：")
                print(f"1. 您的 API_BASE_URL 目前為 '{API_BASE_URL}'，這似乎不是真正的後端 API 伺服器網址。")
                print("2. 如果這是網頁前端 (UI) 的網址，請改成後端 API 的網址 (例如您的真實 IP 加上 port 8000)。")
                print("3. 如果您填寫的是假網域 (例如 sheep123.com)，請換成您的伺服器真實 IP。")
                print(f"【伺服器實際回傳內容擷取 (看一眼就知道連到哪了)】:\n{r.text[:400]}\n")
                return None
                
            self.token = resp_json.get("token")
            self.last_auth_time = now
            print(f"[{datetime.now().strftime('%H:%M:%S')}] Token 申請成功！")
            return self.token
        except requests.exceptions.ConnectionError:
            print(f"\n[FATAL] 無法連線至伺服器 ({API_BASE_URL})！")
            print("可能原因與解法：")
            print("1. 您的伺服器 (API) 尚未啟動。")
            print("2. 您在本地電腦執行此腳本，但忘記將上方的 API_BASE_URL 改為雲端 VM 的真實 IP。")
            print("➡️ 請修改程式碼上方的 API_BASE_URL 變數！\n")
            return None
        except requests.exceptions.HTTPError as he:
            if he.response.status_code == 401:
                print(f"\n[FATAL] 登入失敗 (401 Unauthorized)！")
                print(f"請確認上方 [配置區] 的 ADMIN_USER ({ADMIN_USER}) 與 ADMIN_PASS 是否填寫正確。")
                print("➡️ 若您尚未修改密碼，請將 'YOUR_ADMIN_PASSWORD_HERE' 替換為真實密碼！\n")
            else:
                print(f"\n[FATAL] 伺服器回傳錯誤 (HTTP {he.response.status_code}): {he.response.text}\n")
            return None
        except Exception as e:
            print(f"[FATAL] Token 申請發生未預期錯誤！\n{traceback.format_exc()}")
            return None

    def fetch_and_save(self):
        """[專家級防護] 負責拉取資料並安全寫入 Excel，攔截被用戶打開鎖定的檔案錯誤"""
        token = self._get_token()
        if not token:
            return

        print(f"[{datetime.now().strftime('%H:%M:%S')}] 正在從 API 拉取最新組合資料...")
        url = f"{API_BASE_URL.rstrip('/')}/admin/candidates/all"
        headers = {"Authorization": f"Bearer {token}"}
        
        try:
            r = requests.get(url, headers=headers, timeout=30)
            if r.status_code == 403:
                print("[ERROR] 權限被拒！請確定使用的帳號具有 admin 權限。")
                self.token = "" # 強制下次重新登入
                return
            r.raise_for_status()
            data = r.json().get("candidates", [])
        except Exception as e:
            print(f"[ERROR] 呼叫 API 拉取資料時發生異常:\n{traceback.format_exc()}")
            return

        if not data:
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 伺服器目前沒有任何資料。")
            return

        # 整理為 Dataframe
        processed = []
        for row in data:
            # 安全防護：避免部分 JSON 欄位為 None 導致崩潰
            p_json = row.get("params_json") or "{}"
            m_json = row.get("metrics_json") or "{}"
            t_json = row.get("task_progress") or "{}"
            
            processed.append({
                "candidate_id": row.get("candidate_id"),
                "用戶帳號": row.get("username"),
                "策略池名稱": row.get("pool_name"),
                "綜合分數": row.get("score"),
                "策略參數組合": p_json if isinstance(p_json, str) else json.dumps(p_json, ensure_ascii=False),
                "樣本內績效_IS": m_json if isinstance(m_json, str) else json.dumps(m_json, ensure_ascii=False),
                "任務進度與樣本外績效_OOS": t_json if isinstance(t_json, str) else json.dumps(t_json, ensure_ascii=False),
                "是否已提交": "是" if row.get("is_submitted") else "否",
                "產出時間": row.get("created_at")
            })

        df = pd.DataFrame(processed)

        # 寫入 Excel，並具備強大的檔案鎖定捕捉機制
        try:
            df.to_excel(EXCEL_FILE_PATH, index=False)
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 成功更新 Excel 檔案：{EXCEL_FILE_PATH} (共 {len(df)} 筆)")
        except PermissionError:
            print(f"[WARN] 寫入失敗！Excel 檔案 '{EXCEL_FILE_PATH}' 目前正被開啟，請關閉 Excel 讓系統更新資料。程式將於下一週期重試...")
        except Exception as e:
            print(f"[ERROR] 寫入 Excel 時發生未預期錯誤:\n{traceback.format_exc()}")

    def run_forever(self):
        print("==================================================")
        print("  Sheep Platform 自動同步 Excel 服務已啟動")
        print(f"  目標伺服器: {API_BASE_URL}")
        print(f"  輸出檔案: {EXCEL_FILE_PATH}")
        print(f"  更新頻率: {SYNC_INTERVAL_SECONDS} 秒")
        print("==================================================")
        while True:
            self.fetch_and_save()
            time.sleep(SYNC_INTERVAL_SECONDS)

if __name__ == "__main__":
    daemon = SyncDaemon()
    try:
        daemon.run_forever()
    except KeyboardInterrupt:
        print("\n使用者手動終止程式。")