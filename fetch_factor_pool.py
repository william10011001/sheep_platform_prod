#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
sheep123.com 實盤因子池狀態 (Factor Pool) - 專家級遠端獲取工具
特色：
1. 最大化除錯流程：攔截所有可能的 HTTP 與 JSON 崩潰，並精準輸出 Server 原始回應。
2. API 路由自動探測：自動掃描 /sheep123 或 /api 前綴，防止 404 Not Found。
3. Compute 偽裝登入：宣告 name="compute" 以嘗試繞過前端滑塊驗證碼 (Captcha)。
4. 完美輸出：直接將 Admin 策略池陣列轉存為 analyze_factor_pool.py 可直讀的本地 JSON。
"""

import os
import sys
import json
import time
import logging
import argparse
import requests
import urllib3
from getpass import getpass

# 關閉 SSL 憑證警告 (防止本地端測試時因自簽憑證報錯)
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ==========================================
# 1. 專家級日誌系統設定 (極致詳細輸出)
# ==========================================
class DebugFormatter(logging.Formatter):
    def format(self, record):
        # 依照錯誤級別給予不同的前綴標籤
        if record.levelno >= logging.ERROR:
            prefix = "[❌ 致命錯誤]"
        elif record.levelno == logging.WARNING:
            prefix = "[⚠️ 系統警告]"
        elif record.levelno == logging.INFO:
            prefix = "[✅ 執行狀態]"
        else:
            prefix = "[🔍 網路偵錯]"
        return f"[{self.formatTime(record, '%H:%M:%S')}] {prefix} {record.getMessage()}"

handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(DebugFormatter())
logger = logging.getLogger("PoolFetcher")
logger.setLevel(logging.DEBUG)
logger.addHandler(handler)

# ==========================================
# 2. 核心網路請求與探測模組
# ==========================================
def detect_api_base(host_url):
    """自動偵測 API 正確的路由前綴"""
    host_url = host_url.rstrip('/')
    prefixes = ["/sheep123", "/api", ""]
    
    logger.debug(f"開始自動探測伺服器 {host_url} 的有效 API 路由...")
    for p in prefixes:
        test_url = f"{host_url}{p}/healthz"
        logger.debug(f"  -> 探測路徑: GET {test_url}")
        try:
            res = requests.get(test_url, verify=False, timeout=5)
            if res.status_code == 200 and "ok" in res.text.lower():
                logger.info(f"成功鎖定 API 基礎路由: {host_url}{p}")
                return f"{host_url}{p}"
        except requests.exceptions.RequestException:
            pass
            
    logger.warning("無法透過 /healthz 自動探測到有效路由，將強制使用使用者輸入的網址。")
    return host_url

def safe_json_request(method, url, **kwargs):
    """具備極限除錯能力的 HTTP 請求包裝器"""
    try:
        t0 = time.perf_counter()
        if method.upper() == 'GET':
            resp = requests.get(url, verify=False, **kwargs)
        else:
            resp = requests.post(url, verify=False, **kwargs)
        dt = time.perf_counter() - t0
        
        logger.debug(f"HTTP {method} {url} | 狀態碼: {resp.status_code} | 耗時: {dt:.2f}s | 大小: {len(resp.content)} bytes")
        
        # 攔截 Cloudflare / Nginx 502/503 錯誤
        if resp.status_code >= 500:
            logger.error(f"伺服器端錯誤 (5xx)。伺服器可能離線或正在重啟。\n--- 伺服器原始回應預覽 ---\n{resp.text[:500]}")
            sys.exit(1)
            
        try:
            return resp, resp.json()
        except json.JSONDecodeError:
            logger.error(f"伺服器回傳了非 JSON 格式的內容 (可能被防火牆或錯誤頁面攔截)！\n--- 回應預覽 ---\n{resp.text[:500]}")
            sys.exit(1)
            
    except requests.exceptions.ConnectionError as e:
        logger.error(f"網路連線失敗，請檢查網址或您的網路狀態: {e}")
        sys.exit(1)
    except requests.exceptions.Timeout:
        logger.error(f"請求超時！伺服器未能在規定時間內回應: {url}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"發生未預期的系統例外: {e}")
        sys.exit(1)

# ==========================================
# 3. 主流程邏輯
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="Sheep123.com 實盤因子池資料下載器")
    parser.add_argument("--host", type=str, default="https://sheep123.com", help="網站根目錄 (預設: https://sheep123.com)")
    parser.add_argument("--user", type=str, help="管理員帳號")
    parser.add_argument("--pwd", type=str, help="管理員密碼")
    parser.add_argument("--token", type=str, help="直接提供 Token (若提供則跳過登入)")
    parser.add_argument("--out", type=str, default="實盤因子池狀態.json", help="輸出的 JSON 檔名")
    args = parser.parse_args()

    print("=" * 70)
    print("🐑 羊肉爐實盤因子池遠端資料拉取系統 (Expert Mode) 🐑")
    print("=" * 70)

    # 1. 路由探測
    api_base = detect_api_base(args.host)
    token = args.token

    # 2. 身分驗證 (若無 Token)
    if not token:
        logger.info("未提供 API Token，進入登入授權流程...")
        user = args.user or input("請輸入管理員帳號 (預設為 compute): ") or "compute"
        pwd = args.pwd or getpass("請輸入管理員密碼: ")
        
        login_url = f"{api_base}/token"
        payload = {
            "username": user,
            "password": pwd,
            "name": "compute"  # 偽裝/宣告為 compute 算力節點，繞過前端滑塊驗證碼限制
        }
        
        logger.debug(f"發送授權請求至 {login_url} ...")
        resp, data = safe_json_request("POST", login_url, json=payload, timeout=10)
        
        if resp.status_code != 200:
            logger.error(f"登入失敗！請確認帳號密碼是否正確。伺服器訊息: {data.get('detail', data)}")
            sys.exit(1)
            
        token = data.get("token")
        role = data.get("role")
        
        if role != "admin":
            logger.warning(f"您的帳號身分為 '{role}'，而非 'admin'，接下來的讀取極可能遭遇 403 權限拒絕！")
        else:
            logger.info("授權成功！已取得系統管理員 (Admin) 高階存取憑證。")

    # 3. 獲取實盤策略池
    strategies_url = f"{api_base}/admin/strategies"
    logger.info("開始請求遠端實盤因子池大數據...")
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    resp, data = safe_json_request("GET", strategies_url, headers=headers, timeout=60)
    
    if resp.status_code == 403:
        logger.error("403 Forbidden：您提供的 Token 或帳號無權存取管理員路由！")
        sys.exit(1)
    elif resp.status_code != 200:
        logger.error(f"獲取策略池失敗，狀態碼 {resp.status_code}。訊息: {data.get('detail', data)}")
        sys.exit(1)
        
    if not data.get("ok"):
        logger.error(f"API 業務邏輯報錯: {data.get('msg', data)}")
        sys.exit(1)

    strategies = data.get("strategies", [])
    if not strategies:
        logger.warning("伺服器回傳的策略池為空！(目前可能沒有通過審核的實盤策略)")
    else:
        logger.info(f"完美命中！共拉取 {len(strategies)} 筆活躍的實盤網格大數據 (策略參數與運算進度)。")

    # 4. 寫入本地檔案
    out_path = args.out
    try:
        with open(out_path, "w", encoding="utf-8") as f:
            # 轉換為 analyze_factor_pool.py 可直接讀取的陣列格式
            json.dump(strategies, f, ensure_ascii=False, indent=2)
        logger.info(f"大數據已成功寫入本地硬碟: {os.path.abspath(out_path)}")
        
        # 顯示資料摘要，協助確認資料健康度
        total_size_mb = os.path.getsize(out_path) / (1024 * 1024)
        logger.debug(f"JSON 檔案大小: {total_size_mb:.2f} MB")
        
        print("\n" + "=" * 70)
        print(f"🎉 任務圓滿達成！您可以直接執行 `python analyze_factor_pool.py` 來進行深度分析了！")
        print("=" * 70)
        
    except Exception as e:
        logger.error(f"寫入檔案時發生錯誤 (請檢查權限或磁碟空間): {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()