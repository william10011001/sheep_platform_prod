import threading
try:
    import webview
except ImportError:
    print(" [系統錯誤] 找不到 pywebview 模組！這是使用新版 HTML UI 的必要組件。")
    print("請於終端機執行: pip install pywebview")
    import sys
    sys.exit(1)
import queue
import json
import os
import time
import datetime
import traceback
import sys

# 【核心級防護】強制設定標準輸出與錯誤輸出為 UTF-8 編碼，並啟用安全替換機制
# 徹底解決 Windows 預設 CP950 編碼無法解析特殊字元 (如 ⚠️, 🚨) 導致的 UnicodeEncodeError 崩潰
if sys.stdout is not None:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if sys.stderr is not None:
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# 匯入微調後的底層模組
import sheep_worker_client

# 設定伺服器位址 (打包前請確認改為正式機網址)
SERVER_URL = "https://sheep123.com/api"
CONFIG_FILE = "worker_config.json"

UI_HTML = """
<!DOCTYPE html>
<html lang="zh-Hant">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>OpenNode UI</title>
    <script src="https://cdn.tailwindcss.com"></script>
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Noto+Sans+TC:wght@300;400;700&family=Inter:wght@300;400;600&display=swap');
        :root {
            --bg-color-top: #3e5252;
            --bg-color-bottom: #1e2b2b;
            --accent-color: #ffffff;
            --input-border: rgba(255, 255, 255, 0.2);
            --text-secondary: rgba(255, 255, 255, 0.4);
        }
        body {
            font-family: 'Inter', 'Noto Sans TC', sans-serif;
            background: radial-gradient(circle at center, var(--bg-color-top) 0%, var(--bg-color-bottom) 100%);
            background-attachment: fixed;
            height: 100vh;
            margin: 0;
            display: flex;
            /* 移除 center 避免上下雙向溢出裁切，改用 flex-start 從頂部開始安全排版 */
            align-items: flex-start;
            justify-content: center;
            color: white;
            /* 將整個畫面精準往下推移 35px，確保頂部不被裁切 */
            padding-top: 35px;
            box-sizing: border-box;
            overflow: hidden;
        }
        .input-group { position: relative; margin-bottom: 2rem; width: 100%; }
        .input-label { display: block; font-size: 10px; text-transform: uppercase; letter-spacing: 0.2em; color: var(--text-secondary); margin-bottom: 8px; }
        .input-field { width: 100%; background: transparent; border: none; border-bottom: 1px solid var(--input-border); padding: 8px 0; color: white; font-size: 14px; font-weight: 300; letter-spacing: 0.05em; transition: border-color 0.3s ease; outline: none; }
        .input-field:focus { border-bottom-color: rgba(255, 255, 255, 0.7); }
        .help-icon { position: absolute; right: 0; bottom: 10px; width: 18px; height: 18px; background: rgba(255, 255, 255, 0.1); border-radius: 4px; display: flex; align-items: center; justify-content: center; font-size: 10px; color: var(--text-secondary); cursor: pointer; }
        .btn-login { width: 100%; background: rgba(255, 255, 255, 0.15); border: none; padding: 14px; color: white; font-size: 12px; letter-spacing: 0.3em; text-transform: uppercase; margin-top: 1rem; cursor: pointer; transition: background 0.3s ease; backdrop-filter: blur(5px); }
        .btn-login:hover { background: rgba(255, 255, 255, 0.25); }
        .btn-login:active { transform: scale(0.98); }
        .btn-login:disabled { opacity: 0.3; cursor: not-allowed; }
        .progress-container { width: 100%; margin-top: 3rem; }
        .progress-info { display: flex; justify-content: space-between; font-size: 10px; letter-spacing: 0.1em; color: var(--text-secondary); margin-bottom: 10px; text-transform: uppercase; }
        .progress-track { width: 100%; height: 1px; background: rgba(255, 255, 255, 0.1); position: relative; }
        .progress-bar { position: absolute; top: 0; left: 0; height: 100%; background: white; width: 0%; transition: width 0.3s ease; box-shadow: 0 0 8px rgba(255, 255, 255, 0.3); }
        .stats-footer { display: flex; justify-content: space-between; margin-top: 15px; font-size: 9px; color: rgba(255, 255, 255, 0.15); letter-spacing: 0.2em; text-transform: uppercase; }
    </style>
</head>
<body>
    <div class="w-full max-w-[320px] flex flex-col items-center">
        <div class="text-center mb-10">
            <h1 class="text-4xl font-black tracking-tighter bg-clip-text text-transparent bg-gradient-to-r from-white to-emerald-400">OpenNode</h1>
            <p class="text-xs text-emerald-400/60 mt-2 uppercase tracking-widest">一  個 策 略 挖 礦 平 台</p>
        </div>
        <div class="input-group">
            <label class="input-label">TOKEN</label>
            <input type="text" class="input-field" value="">
            <div class="help-icon">?</div>
        </div>
        <div class="w-full flex flex-col gap-3">
            <button id="startBtn" class="btn-login">開始挖礦</button>
            <button id="pauseBtn" class="btn-login" style="background: transparent; border: 1px solid rgba(255,255,255,0.1); display: none;">暫停</button>
        </div>
        <div class="progress-container">
            <div class="progress-info">
                <span id="statusLabel">Status: 準備中</span>
                <span id="progressText">0%</span>
            </div>
            <div class="progress-track">
                <div id="progressBar" class="progress-bar"></div>
            </div>
            <div class="stats-footer">
                <span id="progDetail">進度: 0.00%</span>
            </div>
        </div>
        <div class="mt-12 mb-6 text-[9px] tracking-[0.4em] text-white/10 uppercase">OpenNode V2</div>
    </div>

    <script>
        const startBtn = document.getElementById('startBtn');
        const pauseBtn = document.getElementById('pauseBtn');
        const progressBar = document.getElementById('progressBar');
        const statusLabel = document.getElementById('statusLabel');
        const progressText = document.getElementById('progressText');
        const progDetail = document.getElementById('progDetail');
        const tokenInput = document.querySelector('.input-field');

        // 當 Python 端準備好後，取得上次儲存的 Token
        window.addEventListener('pywebviewready', function() {
            window.pywebview.api.ui_ready().then(function(token) {
                if(token) tokenInput.value = token;
            });
            
            let lastUpdateTs = Date.now();
            let lastSpeed = 0;
            let lastPct = 0;
            
            // 【核心防崩潰機制】前端主動輪詢取代後端強制推播，徹底解決 WinForms/Edge 遞迴崩潰
            setInterval(async () => {
                if (!window.pywebview || !window.pywebview.api) return;
                try {
                    const updates = await window.pywebview.api.get_ui_updates();
                    let hasProgress = false;
                    
                    if (updates && updates.length > 0) {
                        for (const msg of updates) {
                            if (msg.type === 'status') {
                                uiUpdateStatus(msg.msg);
                                if (msg.frac !== undefined) {
                                    lastPct = msg.frac * 100;
                                    lastUpdateTs = Date.now();
                                }
                            } else if (msg.type === 'progress') {
                                lastPct = msg.total > 0 ? (msg.done / msg.total * 100) : 0;
                                // 【算力精準擷取】即使是 0 也不能丟失，並確保數值有效
                                if (msg.speed !== undefined && msg.speed !== null) {
                                    lastSpeed = parseFloat(msg.speed);
                                }
                                lastUpdateTs = Date.now();
                                hasProgress = true;
                            } else if (msg.type === 'error') {
                                uiShowError(msg.title, msg.msg);
                            } else if (msg.type === 'ui_state') {
                                if (msg.state === 'starting') window.uiSetStarting();
                                else if (msg.state === 'reset') window.uiSetReset();
                                else if (msg.state === 'paused') window.uiSetPaused(msg.is_paused);
                            }
                        }
                    }
                    
                    // 【智慧算力衰減機制】放寬至 5 秒無任何更新才開始緩慢衰減，避免因為策略區塊運算較久導致的假死歸零
                    if (!hasProgress) {
                        let idleTime = Date.now() - lastUpdateTs;
                        if (idleTime > 5000 && lastSpeed > 0) {
                            lastSpeed = Math.max(0, lastSpeed * 0.9); // 每 200ms 衰減 10%，呈現平滑下降
                            if (lastSpeed < 1) lastSpeed = 0;
                        }
                    }
                    
                    // 總是渲染最新且動態調整的算力
                    let speedStr = lastSpeed > 1000 ? (lastSpeed / 1000).toFixed(2) + " KH/s" : lastSpeed.toFixed(1) + " H/s";
                    uiUpdateProgress(lastPct, speedStr);

                } catch (e) {
                    console.error("UI同步錯誤:", e);
                }
            }, 200);
        });

        startBtn.addEventListener('click', () => {
            const token = tokenInput.value.trim();
            if (!token || token.length < 20) {
                alert(`警告\n請輸入完整的 Token`);
                return;
            }
            window.pywebview.api.action_start(token);
        });

        pauseBtn.addEventListener('click', () => {
            window.pywebview.api.action_pause();
        });

        // 以下為供 Python 後端呼叫的控制函數
        window.uiSetStarting = function() {
            startBtn.style.display = 'none';
            pauseBtn.style.display = 'block';
            pauseBtn.innerText = '暫停';
            tokenInput.disabled = true;
        };

        window.uiSetPaused = function(isPaused) {
            if (isPaused) {
                pauseBtn.innerText = '繼續挖礦';
                statusLabel.innerText = 'Status: 已暫停';
            } else {
                pauseBtn.innerText = '暫停';
                statusLabel.innerText = 'Status: 恢復運算...';
            }
        };

        window.uiSetReset = function() {
            startBtn.style.display = 'block';
            startBtn.innerText = '開始挖礦';
            pauseBtn.style.display = 'none';
            tokenInput.disabled = false;
        };

        window.uiUpdateStatus = function(msg) {
            statusLabel.innerText = 'Status: ' + msg;
        };

        window.uiUpdateProgress = function(pct, speedStr) {
            progressBar.style.width = pct + '%';
            progressText.innerText = Math.floor(pct) + '%';
            progDetail.innerText = `進度: ${pct.toFixed(1)}%`;
        };

        window.uiShowError = function(title, msg) {
            alert(`${title}\n${msg}`);
        };
    </script>
</body>
</html>
"""

class OpenNodeApp:
    def __init__(self):
        self.window = None
        self.q = queue.Queue()
        sheep_worker_client.GUI_QUEUE = self.q
        sheep_worker_client.GUI_PAUSED = False
        self.is_running = False
        self.worker_thread = None

    def set_window(self, window):
        self.window = window

    # JS API: 供前端初始化讀取 Token
    def ui_ready(self):
        token = ""
        if os.path.exists(CONFIG_FILE):
            try:
                with open(CONFIG_FILE, "r", encoding="utf-8") as f:
                    cfg = json.load(f)
                    token = cfg.get("token", "")
            except Exception:
                pass
        return token

    def save_config(self, token):
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump({"token": token}, f)

    def on_closing(self):
        if self.is_running and self.worker_thread and self.worker_thread.is_alive():
            print("\n偵測到關閉視窗，準備向伺服器釋放任務...")
            self.q.put({"type": "status", "msg": "正在安全釋放任務並退出..."})
            
            sheep_worker_client.GUI_PAUSED = True
            self.is_running = False
            
            # 阻塞式等待底層回報釋放完畢 (最多等待 8 秒)
            self.worker_thread.join(timeout=8.0)
            if self.worker_thread.is_alive():
                print("釋放任務超時，強制退出程式。")
            else:
                print("任務已成功釋放，安全退出程式。")
        else:
            print("\n目前無執行中任務，直接關閉。")
        os._exit(0)

    # JS API: 前端定期獲取更新 (避免 evaluate_js 造成的 WinForms 遞迴崩潰)
    def get_ui_updates(self):
        updates = []
        while not self.q.empty():
            try:
                msg = self.q.get_nowait()
                if msg.get("type") == "status":
                    current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
                    print(f"[{current_time}] 狀態: {msg.get('msg')}", flush=True)
                updates.append(msg)
            except queue.Empty:
                break
            except Exception as e:
                print(f"UI 讀取異常: {e}", flush=True)
        return updates

    def start_resource_monitor(self):
        def monitor():
            import time
            try:
                import psutil
                has_psutil = True
            except ImportError:
                has_psutil = False
                print(" 無法獲取資源，若要開啟即時監控，請終止程式並於終端機輸入: pip install psutil", flush=True)

            while self.is_running:
                if has_psutil:
                    try:
                        mem = psutil.virtual_memory().percent
                        if mem > 85:
                            print(f" 記憶體已達 {mem}%。系統將啟動虛擬記憶體，運算速度將下降！", flush=True)
                    except Exception as e:
                        print(f"  獲取系統資源失敗: {str(e)}", flush=True)
                    time.sleep(2)
                else:
                    time.sleep(5)
                    
        m_thread = threading.Thread(target=monitor, daemon=True)
        m_thread.start()

    # JS API: 啟動挖礦
    def action_start(self, token):
        self.q.put({"type": "ui_state", "state": "starting"})
        
        self.is_running = True
        sheep_worker_client.GUI_PAUSED = False
        
        self.q.put({"type": "status", "msg": "正在驗證 Token..."})
        
        self.start_resource_monitor()
        self.worker_thread = threading.Thread(target=self.worker_loop, args=(token,), daemon=True)
        self.worker_thread.start()

    # JS API: 暫停切換
    def action_pause(self):
        if sheep_worker_client.GUI_PAUSED:
            sheep_worker_client.GUI_PAUSED = False
            self.q.put({"type": "ui_state", "state": "paused", "is_paused": False})
        else:
            sheep_worker_client.GUI_PAUSED = True
            self.q.put({"type": "ui_state", "state": "paused", "is_paused": True})

    def reset_ui(self):
        self.is_running = False
        self.q.put({"type": "ui_state", "state": "reset"})

    def worker_loop(self, token):
        try:
            # 1. 直接儲存 Token 並跳過舊的帳密驗證流程
            self.save_config(token)

            # 2. 初始化 API
            worker_id = sheep_worker_client._load_or_create_worker_id(".sheep_worker_id")
            api = sheep_worker_client.ApiClient(base_url=SERVER_URL, token=token, worker_id=worker_id)
            
            self.q.put({"type": "status", "msg": "正在與伺服器建立連線..."})
            
            # 【專家級診斷】驗證 Token 並測量伺服器反應時間
            t_auth_start = time.time()
            try:
                snap = api.get_settings_snapshot()
                auth_elapsed = time.time() - t_auth_start
                print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}]  Token 驗證耗時: {auth_elapsed:.2f}s", flush=True)
            except Exception as e:
                err_str = str(e)
                self.q.put({"type": "status", "msg": "驗證失敗，請檢查 Token"})
                self.q.put({
                    "type": "error", 
                    "title": "Token 無效或過期", 
                    "msg": f"無法與伺服器連線，詳細錯誤：\n{err_str}\n\n解決方式：\n請回到 sheep123.com，在控制面板重新複製最新的 Token 貼上。"
                })
                self.reset_ui()
                return

            thr = sheep_worker_client.Thresholds.from_dict(snap.get("thresholds") or {})
            self.q.put({"type": "status", "msg": "驗證通過！正在同步任務..."})
            print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}]  系統就緒，開始自動輪詢任務。", flush=True)

            # 3. 核心派發迴圈
            while self.is_running:
                if sheep_worker_client.GUI_PAUSED:
                    try:
                        api.heartbeat(None)
                    except Exception:
                        pass
                    time.sleep(1)
                    continue

                # 【診斷式輪詢】紀錄每個 API 動作的耗時，揪出 5 分鐘卡頓的元兇
                try:
                    t_f_start = time.time()
                    flags = api.flags()
                    run_enabled = bool(flags.get("run_enabled"))
                    token_kind = str(flags.get("token_kind") or "")
                    reason = str(flags.get("reason") or "")
                    pending_task_count = int(flags.get("pending_task_count") or 0)
                    active_cycle_id = int(flags.get("active_cycle_id") or 0)
                except Exception as e:
                    import traceback
                    print(f"\n🚨 [{datetime.datetime.now().strftime('%H:%M:%S')}] 無法取得伺服器狀態 (Flags API): {str(e)}", flush=True)
                    print(traceback.format_exc(), flush=True)
                    run_enabled = False
                    token_kind = ""
                    reason = ""
                    pending_task_count = 0
                    active_cycle_id = 0

                if not run_enabled:
                    if reason == "legacy_web_session_token" or token_kind == "web_session":
                        status_msg = "目前貼上的 Token 是網站登入 Token，請改貼網站上的「專屬節點 Token」。本版暫時相容，但建議立即更新。"
                    elif reason == "run_disabled":
                        status_msg = "網站端尚未啟動個人派工，請先在頁面按下開始挖礦。"
                    elif reason == "no_active_cycle" or active_cycle_id <= 0:
                        status_msg = "目前尚未啟用新的任務週期，請等待系統發布。"
                    else:
                        status_msg = "伺服器尚未啟動個人派工，請確認網頁端已啟動..."
                    self.q.put({"type": "status", "msg": status_msg})
                    self.q.put({"type": "progress", "done": 0, "total": 1, "speed": 0.0})
                    try:
                        api.heartbeat(None) # 發送待命心跳
                    except Exception:
                        pass
                    time.sleep(3)
                    continue

                # 【啟動加速】領取任務前強制補送一次就緒心跳，解決伺服器端「沒看到人」的問題
                try:
                    api.heartbeat(None)
                except Exception:
                    pass
                
                # 【UX 防呆優化】進入最高 600 秒的長輪詢前，主動刷新面板，消除卡死錯覺
                if token_kind == "web_session":
                    self.q.put({"type": "status", "msg": "目前貼上的 Token 是網站登入 Token，建議改貼網站上的「專屬節點 Token」。本版暫時相容，正在嘗試領取任務..."})
                elif pending_task_count > 0:
                    self.q.put({"type": "status", "msg": f"已啟動個人派工，偵測到 {pending_task_count} 個待領取任務，正在嘗試領取..."})
                else:
                    self.q.put({"type": "status", "msg": "已啟動個人派工，但目前沒有可指派任務，持續待命中..."})

                t_c_start = time.time()
                # 領取任務在底層有 600s 超時，若伺服器沒任務會卡在此處 (背景靜默等待)
                try:
                    task = api.claim_task()
                except Exception as claim_err:
                    print(f"\n [{datetime.datetime.now().strftime('%H:%M:%S')}] 領取任務 API 發生崩潰: {claim_err}", flush=True)
                    task = None
                claim_elapsed = time.time() - t_c_start
                
                # 嚴格驗證任務格式，防止空殼任務引發 KeyError
                is_valid_task = isinstance(task, dict) and "task_id" in task
                
                if not is_valid_task:
                    if task is not None and task != {}:
                        print(f"\n [{datetime.datetime.now().strftime('%H:%M:%S')}] 伺服器回傳了未知的任務格式 (非預期結構): {str(task)[:200]}", flush=True)

                    if claim_elapsed > 10.0:
                        print(f"[{datetime.datetime.now().strftime('%H:%M:%S')}] ℹ 伺服器長輪詢共 {claim_elapsed:.1f}s，目前佇列中無可用任務。", flush=True)
                    
                    if token_kind == "web_session":
                        status_msg = "目前貼上的 Token 是網站登入 Token，請回到網站重新複製「專屬節點 Token」。"
                    elif reason == "no_pending_tasks" or pending_task_count <= 0:
                        status_msg = "已啟動，但目前沒有可指派任務，請稍候或查看網頁端評分專區。"
                    else:
                        status_msg = "排隊中：等待伺服器分配新任務區塊..."
                    self.q.put({"type": "status", "msg": status_msg})
                    self.q.put({"type": "progress", "done": 0, "total": 1, "speed": 0.0})
                    time.sleep(2)
                    continue
                
                print(f"\n[{datetime.datetime.now().strftime('%H:%M:%S')}]  成功取得任務 #{task.get('task_id')}，等待分配耗時 {claim_elapsed:.2f}s", flush=True)

                try:
                    api.heartbeat(int(task.get("task_id") or 0))
                except Exception as hb_err:
                    print(f" [{datetime.datetime.now().strftime('%H:%M:%S')}] 任務啟動心跳發送失敗 (不影響執行): {hb_err}", flush=True)

                self.q.put({"type": "status", "msg": f"執行任務 #{task.get('task_id')}: {task.get('symbol')} {task.get('timeframe_min')}m"})
                self.q.put({"type": "progress", "done": 0, "total": 1, "speed": 0.0})
                
                # 執行主要任務，並加入精確執行時間測量與更詳盡的防禦性錯誤捕捉
                start_time = time.time()
                current_time_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                print(f"\n[{current_time_str}]  開始執行任務 #{task.get('task_id')} | 交易對: {task.get('symbol')} | 週期: {task.get('timeframe_min')}m", flush=True)
                
                try:
                    # 進入 sheep_worker_client.run_task，內部的 K 線讀取耗時將透過日誌揭露
                    sheep_worker_client.run_task(api, task, thr, flag_poll_s=5.0, commit_every=25)
                except Exception as inner_e:
                    err_trace = traceback.format_exc()
                    print(f" 任務 #{task.get('task_id')} 發生例外錯誤:\n{err_trace}", flush=True)
                    self.q.put({"type": "status", "msg": f"任務發生異常: {str(inner_e)[:20]}"})
                
                elapsed = time.time() - start_time
                print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]  任務結束 | 總耗時: {elapsed:.2f} 秒\n", flush=True)
                
                # 【智慧防護機制】若耗時極短 (小於 2 秒，代表是無須運算的空區塊任務)
                # 則僅冷卻 0.2 秒，啟動「極速掃蕩」模式來快速清空排隊佇列
                if elapsed < 2.0:
                    time.sleep(0.2)
                else:
                    time.sleep(3)

        except Exception as e:
            err_msg = traceback.format_exc()
            print(f"Worker loop 發生嚴重例外:\n{err_msg}")
            
            brief_err = str(e).split('\n')[0][:30]
            self.q.put({"type": "status", "msg": f"系統異常: {brief_err}"})
            self.q.put({"type": "progress", "done": 0, "total": 1, "speed": 0.0})
            
            self.q.put({
                "type": "error", 
                "title": "系統錯誤", 
                "msg": f"核心迴圈發生崩潰，已幫您攔截並停止。請檢查網路或向管理員回報：\n\n{str(e)}\n\n(詳細錯誤追蹤已輸出至終端機)"
            })
            self.reset_ui()

# 【專家級防護】獨立的 JS 通訊橋樑類別
# 徹底阻絕 pywebview 的底層反射機制 (Reflection) 去掃描 OpenNodeApp 內的 window 與 thread 物件
# 凡是帶有 _ 開頭的屬性，pywebview 皆會忽略，從根本避免 COM 跨執行緒崩潰與無限遞迴
class JsApiBridge:
    def __init__(self, core_app):
        self._app = core_app
        
    def ui_ready(self):
        return self._app.get_ui_ready() if hasattr(self._app, 'get_ui_ready') else self._app.ui_ready()
        
    def action_start(self, token):
        self._app.action_start(token)
        
    def action_pause(self):
        self._app.action_pause()
        
    def get_ui_updates(self):
        return self._app.get_ui_updates()

if __name__ == "__main__":
    import multiprocessing
    import sys
    import os
    
    # 【極限效能關鍵】必須加入 freeze_support() 才能在打包後的 EXE 中正確啟動多進程 (ProcessPool) 核心，防止無限彈窗崩潰
    multiprocessing.freeze_support()
    
    # 【專家級防護】圖標路徑解析，自動適應開發環境與 PyInstaller 打包後的虛擬暫存目錄 (sys._MEIPASS)
    def get_resource_path(relative_path):
        if hasattr(sys, '_MEIPASS'):
            base_path = sys._MEIPASS
        else:
            base_path = os.path.abspath(os.path.dirname(__file__))
        return os.path.join(base_path, relative_path)
    
    app = OpenNodeApp()
    api_bridge = JsApiBridge(app)
    
    # 建立 WebView 視窗取代原本的 Tkinter
    window = webview.create_window(
        title="OpenNode", 
        html=UI_HTML,
        width=360, 
        height=480, 
        resizable=False,
        js_api=api_bridge,       # 改為綁定純淨的 api_bridge，杜絕遞迴掃描崩潰
        frameless=False,
        background_color='#1e2b2b'
    )
    
    app.set_window(window)
    
    # 綁定優雅關閉事件
    window.events.closing += app.on_closing
    
    # 解析圖標絕對路徑，請確保資料夾內有一張 opennode.ico 圖片
    icon_path = get_resource_path('opennode.ico')
    
    # 啟動應用程式並掛載左上角與工作列的圖標 (若圖標不存在則安全降級為預設值避免崩潰)
    if os.path.exists(icon_path):
        webview.start(icon=icon_path)
    else:
        print(f" [警告] 找不到圖標檔案: {icon_path}，將使用預設圖標啟動。")
        webview.start()
