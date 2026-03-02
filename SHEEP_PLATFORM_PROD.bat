SHEEP_PLATFORM_PROD
├── app                                      # 核心應用程式目錄
│   ├── __pycache__                          # Python 快取目錄
│   ├── assets                               # 前端靜態資源
│   ├── data                                 # 運行時資料（K線CSV、log、secret.key 等）
│   ├── migrations                           # Alembic 資料庫遷移腳本
│   │   └── versions
│   ├── static                               # 【最核心業務區】所有策略、回測、API、Worker 檔案
│   │   ├── brand_1.webm
│   │   ├── brand_2.webm
│   │   ├── 羊LOGO影片(去背).webm
│   │   ├── alembic.ini
│   │   ├── backtest_panel2.py               # 格點回測引擎（全系統核心）
│   │   ├── debug_ui.py                      # Streamlit UI 診斷工具
│   │   ├── requirements.txt
│   │   ├── sheep_compute_daemon.py          # 計算節點守護程式（跨用戶派工）
│   │   ├── sheep_platform_api.py            # FastAPI 主後端 API
│   │   ├── sheep_platform_app.py            # Streamlit 前端管理介面
│   │   ├── sheep_platform_audit.py          # 策略審核與加分制模組
│   │   ├── sheep_platform_cron.py           # 週結算與派息排程
│   │   ├── sheep_platform_db.py             # 資料庫操作層
│   │   ├── sheep_platform_jobs.py           # 任務排程器與 JobManager
│   │   ├── sheep_platform_rate_limit.py
│   │   ├── sheep_platform_security.py
│   │   ├── sheep_platform_version.py
│   │   ├── sheep_worker_client.py           # Worker 客戶端
│   │   └── sheep_worker_daemon.py           # Worker 守護程式
│   ├── env.py                               # Alembic 環境設定
│   └── script.py.mako                       # Alembic 遷移模板
│
├── deploy                                   # 生產部署與容器化配置
│   ├── nginx
│   │   └── conf.d
│   │       ├── app_http.conf.disabled
│   │       ├── app_https.conf               # 主生產 HTTPS 配置（已啟用）
│   │       └── app_https.conf.disabled
│   ├── html
│   │   ├── index.html                       # 入口預覽頁（OG 標籤用）
│   │   ├── logo.png
│   │   ├── og.png
│   │   └── S_13320194.jpg
│   ├── scripts
│   │   ├── entrypoint_api.sh                # API 容器啟動腳本
│   │   └── entrypoint_ui.sh                 # UI 容器啟動腳本
│   ├── .env.example                         # 環境變數範本
│   ├── docker-compose.yml                   # Docker Compose 主配置
│   ├── Dockerfile.api                       # API 服務 Dockerfile
│   ├── Dockerfile.ui                        # UI 服務 Dockerfile
│   ├── .gitignore
│   ├── patch.diff
│   └── README.md
│
├── docker-compose.yml                       # 根目錄備份配置
├── .gitignore
└── README.md