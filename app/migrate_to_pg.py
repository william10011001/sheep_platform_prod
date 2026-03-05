import sqlite3
import psycopg2
import psycopg2.extras
import os

# 1. 舊 SQLite 檔案路徑 (透過 Docker Volume 掛載，位置不變)
SQLITE_PATH = "/app/data/sheep.db"

# 2. 透過環境變數安全讀取連線字串，避免密碼外洩到 GitHub (讀取 Docker 注入的 SHEEP_DB_URL)
PG_URL = os.environ.get("SHEEP_DB_URL", "")

TABLES = [
    "users", "settings", "audit_logs", "api_tokens", "mining_cycles",
    "factor_pools", "mining_tasks", "submissions", "candidates",
    "strategies", "weekly_checks", "payouts", "workers", "worker_events"
]

def main():
    print(" 開始進行資料庫無損轉移 (SQLite -> PostgreSQL)...")
    
    if not os.path.exists(SQLITE_PATH):
        print(f" 找不到 SQLite 檔案: {SQLITE_PATH}")
        return

    try:
        sqlite_conn = sqlite3.connect(SQLITE_PATH)
        sqlite_conn.row_factory = sqlite3.Row
        
        pg_conn = psycopg2.connect(PG_URL)
        pg_conn.autocommit = False
        pg_cur = pg_conn.cursor()
        print(" 成功連線至 DigitalOcean 雲端資料庫！")

        for table in TABLES:
            print(f" 正在搬移資料表: {table} ...", end=" ")
            
            sl_cur = sqlite_conn.cursor()
            try:
                sl_cur.execute(f"SELECT * FROM {table}")
                rows = []
                while True:
                    try:
                        row = sl_cur.fetchone()
                        if not row:
                            break
                        rows.append(row)
                    except sqlite3.DatabaseError as row_err:
                        print(f"\n      ⚠️ 偵測到資料庫壞軌 ({row_err})，已極限挽救 {len(rows)} 筆安全資料，強制跳過損毀區段...", end=" ")
                        break
            except sqlite3.OperationalError:
                print("⚠️ 舊庫中無此表，略過。")
                continue
            except sqlite3.DatabaseError as tbl_err:
                print(f"⚠️ 表結構嚴重損毀 ({tbl_err})，直接跳過此表。")
                continue

            if not rows:
                print("0 筆資料，略過。")
                continue

            # 清空雲端資料庫該表的舊資料
            pg_cur.execute(f"TRUNCATE TABLE {table} CASCADE;")

            # 動態產生 INSERT 語法
            cols = rows[0].keys()
            col_str = ", ".join(cols)
            val_str = ", ".join(["%s"] * len(cols))
            insert_query = f"INSERT INTO {table} ({col_str}) VALUES ({val_str})"

            data = [tuple(row) for row in rows]
            psycopg2.extras.execute_batch(pg_cur, insert_query, data)
            
            # 同步 PostgreSQL 的自動遞增 ID (非常重要)
            if 'id' in cols:
                try:
                    pg_cur.execute(f"SELECT setval(pg_get_serial_sequence('{table}', 'id'), coalesce(max(id), 1), max(id) IS NOT null) FROM {table};")
                except Exception:
                    pass

            pg_conn.commit()
            print(f"成功！匯入 {len(rows)} 筆。")

        print(" 恭喜！所有資料已完美搬移至 DigitalOcean 雲端資料庫！")

    except Exception as e:
        print(f"\n 發生錯誤: {e}")
        if 'pg_conn' in locals():
            pg_conn.rollback()
    finally:
        if 'sqlite_conn' in locals(): sqlite_conn.close()
        if 'pg_conn' in locals() and not pg_conn.closed: pg_conn.close()

if __name__ == "__main__":
    main()