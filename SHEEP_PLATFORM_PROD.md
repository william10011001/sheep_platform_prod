SHEEP_PLATFORM_PROD
├── app                                   
│   ├── __pycache__                          # Python快取
│   ├── assets                               # 前端靜態資源
│   ├── data                                 # Bitmart加密貨幣市場的價格資料
│   ├── migrations                           # 遷移至VM主機使用
│   │   └── versions
│   ├── static                              
│   │   ├── brand1.webm
│   │   ├── brand2.webm
│   │   ├── 羊LOGO.webm
│   │   ├── alembic.ini
│   │   ├── backtest_panel2.py               # 格點搜尋之回測
│   │   ├── debug_ui.py                      # 除錯(前端網頁的)
│   │   ├── requirements.txt
│   │   ├── sheep_compute_daemon.py          # 分發任務給用戶
│   │   ├── sheep_platform_api.py            # 後端 API
│   │   ├── sheep_platform_app.py            # 前端介面
│   │   ├── sheep_platform_audit.py          # 策略審核、加分制
│   │   ├── sheep_platform_cron.py           # 週結算用戶利潤
│   │   ├── sheep_platform_db.py             # 資料庫
│   │   ├── sheep_platform_jobs.py           # 排程器
│   │   ├── sheep_platform_rate_limit.py
│   │   ├── sheep_platform_security.py
│   │   ├── sheep_platform_version.py
│   │   ├── sheep_worker_client.py          
│   │   └── sheep_worker_daemon.py           
│   ├── env.py                               # 環境設定
│   └── script.py.mako                       # Alembic模板
│
├── deploy                                   # 生產配置
│   ├── nginx
│   │   └── conf.d
│   │       ├── app_http.conf.disabled
│   │       ├── app_https.conf               # HTTPS配置
│   │       └── app_https.conf.disabled
│   ├── html
│   │   ├── index.html                       # 網站入口畫面(登入與註冊)
│   │   ├── logo.png
│   │   ├── og.png
│   │   └── S_13320194.jpg
│   ├── scripts
│   │   ├── entrypoint_api.sh                # API啟動
│   │   └── entrypoint_ui.sh                 # UI啟動
│   ├── .env.example                         # 變數範本
│   ├── docker-compose.yml                   # Docker Compose配置
│   ├── Dockerfile.api                       # API 服務
│   ├── Dockerfile.ui                        # UI 服務
│   ├── .gitignore
│   ├── patch.diff
│   └── README.md
│
├── docker-compose.yml                       # 資料備份
├── .gitignore
└── README.md