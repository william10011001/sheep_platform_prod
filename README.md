# 羊肉爐任務平台（Production-ready 版本）

這包是把你現有的 FastAPI + Streamlit + PostgreSQL 專案，直接升級成「可上線」的系統級部署版本：
- Docker / Docker Compose：環境一致
- Nginx 反代：Streamlit / Uvicorn 不碰 80/443
- Certbot：免費 HTTPS
- Alembic：資料庫版本控管（不再靠 CREATE TABLE IF NOT EXISTS 在程式裡硬搞）
- 自動重啟：Docker restart: always + healthcheck
- 登入滑動驗證碼（Slider Captcha）
- Streamlit UX：任務無縫銜接（開始一次就會一直跑），自動刷新預設勾選
- OG/SEO：貼 Discord/LINE 有標題、描述、預覽圖

---

## 0) 你會需要的東西（很現實但必要）
1. 一台 Linux 伺服器（有 public IP）
2. 一個網域（A Record 指到這台伺服器）
3. 伺服器開放 80 / 443
4. Docker + Docker Compose（新版 `docker compose`）

---

## 1) 部署檔案結構
- `app/`：Python 程式（FastAPI、Streamlit、DB、Alembic）
- `deploy/`：Compose、Nginx、Certbot、entrypoint

---

## 2) 第一次啟動（HTTP-only，先把服務跑起來）
原因：你還沒有 Let’s Encrypt 憑證，HTTPS Nginx 配置不能直接開，否則 nginx 會因為找不到 cert 檔案直接掛掉。

### 2.1 設定環境變數
進到 `deploy/` 資料夾，複製 env：
```bash
cd deploy
cp .env.example .env
```

打開 `.env`，至少改：
- `POSTGRES_PASSWORD=...`（請用強密碼）

### 2.2 設定 Nginx 網域與 OG meta
1) `deploy/nginx/conf.d/app.conf`（目前是 HTTP-only 配置）
- 把 `YOUR_DOMAIN` 換成你的真實網域（例如 `sheep.example.com`）

2) `deploy/nginx/html/index.html`
- 把 `YOUR_DOMAIN` 換成你的真實網域（Discord/LINE 最吃這個）

### 2.3 啟動（會自動跑 migrations）
```bash
docker compose up -d --build
```

確認：
- `http://YOUR_DOMAIN/` 會看到 landing page
- `http://YOUR_DOMAIN/app/` 會進 Streamlit
- `http://YOUR_DOMAIN/api/healthz` 會回 OK

---

## 3) 申請 HTTPS（Certbot）
### 3.1 先確保 Nginx (HTTP-only) 在跑
```bash
docker compose ps
```

### 3.2 申請憑證（webroot 模式）
把 `YOUR_DOMAIN`、`YOUR_EMAIL` 換掉：
```bash
docker compose run --rm certbot certonly   --webroot -w /var/www/certbot   -d YOUR_DOMAIN   -m YOUR_EMAIL   --agree-tos --no-eff-email
```

成功後，你會在 `certbot_conf` volume 裡得到憑證檔。

---

## 4) 切到 HTTPS 配置（正式上線）
### 4.1 切換 Nginx config
目前 `deploy/nginx/conf.d/` 內：
- `app.conf`：HTTP-only（目前啟用）
- `app_https.conf.disabled`：HTTPS（已準備好但未啟用）

執行：
```bash
mv nginx/conf.d/app.conf nginx/conf.d/app_http.conf.disabled
mv nginx/conf.d/app_https.conf.disabled nginx/conf.d/app.conf
```

打開 `deploy/nginx/conf.d/app.conf`（HTTPS 版），把 `YOUR_DOMAIN` 換成真實網域。

### 4.2 Reload Nginx
```bash
docker compose restart nginx
```

之後：
- `https://YOUR_DOMAIN/` 會自動導向、而且 Discord/LINE 會有漂亮預覽
- `/app/` / `/api/` 都走 HTTPS

---

## 5) 憑證續期（建議）
Let’s Encrypt 憑證 90 天。你至少要做「自動 renew」。

最簡做法：在 host 上加 cron（每天跑一次）：
```bash
docker compose run --rm certbot renew --webroot -w /var/www/certbot
docker compose restart nginx
```

---

## 6) 任務無縫銜接（你原本卡住的點已修掉）
你描述的痛點是：
- 跑完一輪後，要再點一次「開始全部任務」
- 還要雙擊「自動刷新」才會接到新任務

現在行為是：
1) 使用者點一次「開始全部任務」後，系統會記住「run_all=開」的狀態
2) 即使暫時沒有 running/queued，仍會維持自動刷新（只要 run_all 還是開）
3) 只要 DB 出現新的 assigned 任務（或新 cycle），會自動塞入隊列並繼續跑
4) 自動刷新 checkbox 預設永遠勾選

---

## 7) Alembic（DB schema 版本控管）
這版把 Postgres 改成 Alembic 管理 schema：
- 不再在程式啟動時 CREATE TABLE
- DB schema 缺失會直接 fail-fast，逼你先 migrate（這才是 production 正確姿勢）

你新增欄位（V3/V4）要做的流程：
```bash
# 進入 api 容器（它有 alembic）
docker compose exec api sh

# 產生 migration（需要你手寫或改模板）
alembic -c alembic.ini revision -m "add column xyz"

# 編輯 migrations/versions/xxxx.py，寫 upgrade/downgrade

# 套用
alembic -c alembic.ini upgrade head
```

---

## 8) 滑動驗證碼（Slider Captcha）
Streamlit 登入頁已加入滑桿驗證：
- 預設開啟（`SHEEP_CAPTCHA=1`）
- 太快拖完（<0.8s）會視為可疑，要求重拖一次
- 這不是 Turnstile 等級的機器人防護，但能擋掉大量低成本腳本撞庫

如果你把網域掛到 Cloudflare，建議直接上 Turnstile（強很多），但需要前端 component；這版先用無外部依賴的 slider 實作。

---

## 9) 一句實話（但很重要）
Streamlit 適合「內部工具 / 操作台」，不是理想的「公開網站前台」。  
你要 SEO、要極速首屏、要大量用戶同時進來？你最後一定會把前端換掉（Next.js/React），Streamlit 放在後台控制台就好。

但這包至少把你目前的系統「能安全上線」做到一個合理標準：HTTPS、有反代、有 migrations、有重啟守護、有基本反機器人、有 OG 預覽、有 UX 無縫銜接。

---

## 10) 常見排錯
- Nginx 起不來：通常是 conf 裡 domain 沒改、或 HTTPS 配置啟用但 cert 還沒申請。
- /api/docs 壞掉：你如果只要 worker 用 API，不需要 docs；要 docs 的話請確保 `SHEEP_API_ROOT_PATH=/api`（compose 已設定）。
- Streamlit 靜態資源 404：代表 baseUrlPath 或 Nginx proxy_pass 設錯；這包已對齊（Streamlit baseUrlPath=app + Nginx 不剝 prefix）。

Auto deploy test: 2026-02-19T22:22:42

