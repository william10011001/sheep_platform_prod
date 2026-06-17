# 多交易所即時買賣價 (BBO) 採集器

即時透過 **WebSocket** 監測 23 間現貨交易所、全部可交易幣種的最新買一價/賣一價,
正規化成統一的 `BASE/QUOTE` 符號後，寫入分割式 **zstd-Parquet**。

所有 WS 規格均經「實連抓真實樣本訊息」驗證（非靠文件/記憶）。

## 快速開始

```bash
# 面板(監控+錄製+延遲量測+多裝置比較)── 開瀏覽器看 http://localhost:8800
python -m bbo_collector.panel_server --exchanges all --out data/bbo --port 8800

# 純採集器(無面板)
python -m bbo_collector.main --exchanges all --out data/bbo            # 全量持續
python -m bbo_collector.main --exchanges all --duration 25 --max-symbols 40 --out data/bbo  # 煙霧測試

# 指定交易所
python -m bbo_collector.main --exchanges binance,okx,kucoin,htx --out data/bbo
```

### 面板能看什麼
- **各交易所健康/延遲**:upd/s、~X(顆粒度ms)、網路RTT p50/p99、抖動、feed-lag、**程式處理延遲 proc µs**(每筆更新的程式開銷,實測 p50 ~10-20µs、p99 ~50-90µs ⇒ 瓶頸 100% 在網路、不在程式)。
- **跨所比價**:選任一幣,並排所有交易所的買一/賣一/價差/該幣在該所的更新間隔X,標出最高買價、最低賣價與套利邊際(bps)。
- **多裝置比較**:5090/AWS 跑 `node_agent.py`(零依賴)把延遲推回面板,逐交易所標出最快裝置。

### 多裝置比較(找最適合的機器)
面板機開好後,在 5090 / AWS 各跑(只需 Python,免裝任何套件):
```bash
python node_agent.py --panel-url http://<面板機IP>:8800 --node-id aws-tokyo
python node_agent.py --panel-url http://<面板機IP>:8800 --node-id 5090-home
```
面板「Device comparison」表會逐交易所以綠底標出 RTT 最低的裝置。
完整多機部署(hub 放 5090 + 網域 + 隧道 + token 認證 + Parquet 彙整 R2/S3):見 [../deploy/DEPLOY.md](../deploy/DEPLOY.md)。

跑全套(hub + spoke + 認證)。token 放檔案(repo 外),不進指令/網址:
```bash
# 0) 每台寫同一把 token 到檔案:  python -c "import secrets;print(secrets.token_urlsafe(32))" > C:\SheepNode\panel_token.txt
# 5090 = hub:    python -m bbo_collector.panel_server --exchanges all --node-id 5090-home --token-file C:\SheepNode\panel_token.txt
# 5050/AWS=spoke: python -m bbo_collector.panel_server --exchanges all --node-id aws-tokyo --hub-url https://panel.<域名> --token-file ~/panel_token.txt
# 看板:  開 https://panel.<域名> → /login 登入頁貼 token(POST,不進網址)
# 彙整:  python sync_parquet.py --root data/bbo --bucket bbo-archive --prefix node=5090 --endpoint-url https://<ACCT>.r2.cloudflarestorage.com --interval 300
```
> 註:`feed-lag`(交易所事件時間→本地)對某些全市場feed(如 KuCoin ticker:all 的 time=最後成交時間)會被冷門幣的舊時間戳灌大;**衡量機器延遲請看 RTT 與 proc µs 這兩個乾淨指標**。
> 關於 X:WebSocket 是事件驅動,X 不是你設的旋鈕,而是「新價多久來一次」,下限=網路RTT。要把 X 壓更低 ⇒ 降RTT ⇒ 機器搬近交易所(AWS東京)。

依賴：`aiohttp`、`pyarrow`（已安裝）。純 WebSocket，公開行情**不需任何 API key**。

## 採集的交易所（23 家，全部實測通過）

| 交易所 | WS 模式 | 取得買賣價的管道 | 編碼/特例 | 報價幣 |
|---|---|---|---|---|
| Binance | 逐幣 | `<sym>@bookTicker` | — | USDT/USDC/TRY… |
| OKX | 逐幣 | `tickers` 頻道 | text ping | USDT/USDC… |
| Bybit | 逐幣 | `orderbook.1.<sym>` ⚠不是 tickers | — | USDT… |
| Coinbase | 列舉 | `ticker` 頻道 | — | USD/USDC/USDT |
| Gate.io | 逐幣 | `spot.book_ticker` | — | USDT… |
| KuCoin | **全市場一訂閱** | `/market/ticker:all` | 需 bullet-public token | USDT/BRL… |
| HTX(火幣) | 逐幣 | `market.<sym>.bbo` ⚠tickers無買賣價 | **gzip**、server ping | USDT… |
| Bitget | 逐幣 | `ticker` 頻道 | text ping | USDT… |
| MEXC | 逐幣 | `aggre.bookTicker.pb@100ms` | **protobuf**、30訂閱/連線 | USDT… |
| Crypto.com | 逐幣 | `ticker.<sym>` | 須回 heartbeat | USDT… |
| Bitfinex | 逐幣 | `ticker` 頻道 | **chanId→symbol 狀態**、USDT=UST | USD/UST |
| Bitstamp | 逐幣 | `order_book_<sym>` | symbol 只在 channel 欄 | USDT/USD |
| BitMart | 逐幣 | `spot/ticker:<sym>` | text ping | USDT… |
| CoinEx | 列舉 | `bbo.subscribe` | **gzip**、server ping | USDT… |
| WhiteBIT | **全市場一訂閱** | `bookTicker_subscribe` | 位置陣列、含永續需濾 | USDT… |
| Poloniex | 逐幣 | `book_lv2` | **快照+增量訂單簿重建** | USDT… |
| Upbit(韓) | 列舉 | `orderbook` | 二進位JSON、科學記號、QUOTE-BASE | KRW/USDT |
| Bithumb(韓) | 列舉 | `orderbook` | 二進位JSON、時間戳μs、QUOTE-BASE | KRW |
| XT | 逐幣 | `depth@<sym>,5` ⚠ticker無買賣價 | — | USDT… |
| AscendEX | 逐幣 | `bbo:<sym>` | server ping、BASE/QUOTE | USDT… |
| Phemex | **全市場一訂閱** | `spot_market24h` | **scaled-int /1e8**、無量、s前綴 | USDT… |
| DigiFinex | **全市場一訂閱** | `all_ticker` | **zlib(78da)** | USDT… |
| LBank | 逐幣 | `depth` | server ping、api.lbkex.com | USDT… |

> 未納入：**Kraken / Gemini**（未實測，可後續加；Kraken WS v2 為逐幣列舉、Gemini 無全市場買賣價且美國合規限制）。

## 達到的顆粒度

WebSocket 是**事件驅動**：交易所一變動就推送，不是輪詢。實測單一機器、一條 IP：

- 流動幣的有效顆粒度 ≈ **50–150ms**（即時推送，受網路延遲下限約束）。
- 不吃 REST 速率限制、不會因輪詢被封 IP。
- 25 秒煙霧測試(23家×40符號)：**129,700 筆、0 丟失、0 交叉報價(bid>ask)**。
- 單一 Binance 全市場(1364現貨對、自動分片)：25 秒 **86,328 筆、0 丟失**。

> 對照 REST 輪詢：顆粒度被最慢的所綁死(Kraken ~1s、Bitfinex ~2–9s)，且多家輪詢全市場 ticker 會封 IP。詳見專案根目錄的速率限制研究。

## 「共同幣」的真相（重要）

要求「同一顆幣在每間都有」會讓幣種暴跌：17 家全有 → 僅 **3 顆(嚴格USDT)/27 顆(任一美元報價)**。
正確做法是**選定交易所集合 + 維護每幣上市對照表**，跨所比價只在「同時上市且有量」的子集做。
參考量級（任一美元類報價）：8 大主流所交集 **131 顆**、8 大+Coinbase+Kraken **100 顆**。

## 儲存格式

分割：`data/bbo/exchange=<EX>/date=<YYYY-MM-DD>/hour=<HH>/part-<ns>.parquet`（zstd）。
每筆兩個時間戳：`ts_exchange_ns`(交易所事件時間，可空) + `ts_local_ns`(本地接收)。
每次 flush 一個新檔(append-free，避免半行損毀)，可由 cron 上傳物件儲存。

**為何不用 CSV**：以 30所×~300幣×混合5筆/秒估 ≈ 39億列/天，CSV ~310–470GB/天，
Parquet(zstd) 僅 ~25–45GB/天(約小10倍)。需即時查詢/儀表板可並行灌一份到 QuestDB。

讀回範例：
```python
import pyarrow.dataset as ds
t = ds.dataset("data/bbo", format="parquet", partitioning="hive").to_table()
```

## ⚠ 部署地點(關鍵)

Binance/OKX/Bybit/HTX/MEXC/Gate/Bitget/KuCoin 等**封鎖美國(常連帶新加坡/香港)IP**。
採集器請跑在**台灣或日本(AWS 東京)出口**；**勿跑在美國雲(us-east)或新加坡**，否則大面積 451/403。

## 架構

```
每交易所一個 asyncio 任務(逐幣多的會自動分片成多連線)
  連線 → (認證) → 訂閱(分批限速) → 收訊 → 解壓/解碼 → 解析成 canonical BBO
        → 心跳(client/server) → 自動重連(指數退避)
  └─ socket 讀取迴圈絕不阻塞：佇列滿則丟棄並計數(記錄器寧可掉資料也不能讓 socket 卡死)
有界佇列 → Parquet 寫入器(批次 5萬列或 2秒 flush)
```

- `registry.py` — 23 家轉接器(訂閱/解析/正規化宣告式定義)
- `collector.py` — 通用連線引擎(分片、心跳、重連、解壓、protobuf 路徑)
- `adapter.py` — 路徑式 JSON 解析器(支援陣列索引 `data.b[0][0]`)
- `custom.py` — MEXC protobuf、Bitfinex/Poloniex 狀態解析、Phemex scaled-int、KRW 正規化
- `writer.py` — 分割 Parquet 寫入；`symbols.py` — 符號正規化；`main.py` — 編排器

## 已知限制 / 後續

- **價格用 float64**：~15–16 位有效數字，記錄/分析足夠；要嚴格金融精度可改 decimal/縮放整數。
- **Phemex/MEXC 縮放**：目前 Phemex 固定 /1e8；嚴謹應從 REST `products` 讀各幣 `priceScale`。
- **Kraken / Gemini** 尚未納入(可加)。
- **斷線補洞**：目前重連會重新訂閱(快照型頻道自動補齊)；可再加「缺口標記列」與序號斷點偵測。
- 監控建議：每所 msgs/s、佇列深度、丟棄數、feed-lag(`ts_local-ts_exchange`)、staleness 告警。
```
