#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
實盤因子池 (Factor Pool) - 終極投資組合建構引擎
功能：
1. 讀取扁平化參數寬表，依夏普值 (Sharpe Ratio) 全局排序。
2. 實施嚴格約束：同家族 (Family)、同幣種 (Symbol)、同方向 (Direction) 最多取 2 名，篩選出 Top 150。
3. 呼叫底層 backtest_panel2.py 進行實彈回測，重構每日資金曲線。
4. 計算 Pearson 相依性矩陣，使用貪婪演算法選出相依性極低的前 20 名策略。
作者: Gemini (Expert Mode)
"""

import os
import sys
import json
import pandas as pd
import numpy as np
from datetime import datetime
import warnings

# 隱藏 Pandas 未來版本的警告
warnings.simplefilter(action='ignore', category=FutureWarning)

# 強制將 app 資料夾加入系統路徑，以確保能正確匯入底層模組
sys.path.append(os.path.join(os.path.dirname(__file__), 'app'))
try:
    import backtest_panel2 as bt
except ImportError as e:
    print(f"[致命錯誤] 無法匯入 backtest_panel2，請確保腳本在專案根目錄執行，且 app/backtest_panel2.py 存在。詳細錯誤: {e}")
    sys.exit(1)

# [專家級潔癖] 必須在 import backtest_panel2 (連帶匯入 streamlit)「之後」執行封鎖！
# 這樣才能攔截並摧毀 Streamlit 剛建立好的日誌處理器，徹底消滅所有 Thread 與 Cache 警告。
import logging
for name in list(logging.root.manager.loggerDict.keys()):
    if "streamlit" in name.lower():
        logger = logging.getLogger(name)
        logger.setLevel(logging.CRITICAL)
        logger.propagate = False

def load_kline_data(symbol: str, timeframe_min: int, cache: dict) -> pd.DataFrame:
    """智慧型 K 線快取載入器，避免重複讀取大型 CSV"""
    cache_key = f"{symbol}_{timeframe_min}m"
    if cache_key in cache:
        return cache[cache_key]
    
    # 預設路徑探測
    file_paths = [
        f"app/data/{symbol}_{timeframe_min}m_3y.csv",
        f"app/data/{symbol}_{timeframe_min}m.csv",
        f"data/{symbol}_{timeframe_min}m_3y.csv"
    ]
    
    for path in file_paths:
        if os.path.exists(path):
            print(f"  -> [I/O] 載入歷史資料: {path} ...")
            df = pd.read_csv(path)
            # [專家級修復] 強制將 ts 欄位轉換為 UTC datetime 物件，避免底層引擎使用 .dt 屬性時發生型別崩潰
            df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
            cache[cache_key] = df
            return df
            
    # [專家級潔癖] 靜默跳過缺失的資料，不污染終端機畫面
    return None

def build_daily_equity_curve(trades_detail: list) -> pd.Series:
    """從交易明細重構逐日資金曲線 (Daily Equity Curve)"""
    if not trades_detail:
        return pd.Series(dtype=float)
        
    records = []
    for t in trades_detail:
        # 兼容不同命名風格的出場時間
        exit_time = t.get('exit_time') or t.get('exit_ts') or t.get('Time')
        # [專家級修復] 精準對齊 backtest_panel2.py 的輸出鍵值 'net_return'
        net_ret = float(t.get('net_return') if t.get('net_return') is not None else t.get('net_ret', 0.0))
        
        if exit_time is not None:
            records.append({'time': exit_time, 'ret': net_ret})
            
    if not records:
        return pd.Series(dtype=float)
        
    df_trades = pd.DataFrame(records)
    # 處理時間戳或字串
    try:
        df_trades['time'] = pd.to_datetime(df_trades['time'])
    except Exception:
        df_trades['time'] = pd.to_datetime(df_trades['time'], unit='s')
        
    df_trades = df_trades.set_index('time').sort_index()
    
    # 計算複利累積淨值
    df_trades['equity'] = (1.0 + df_trades['ret']).cumprod()
    
    # 重採樣為每日最後淨值，並向前填充 (遇到無交易日維持淨值不變)
    daily_equity = df_trades['equity'].resample('1D').last().ffill()
    return daily_equity

def main():
    print("="*60)
    print("🚀 啟動終極量化投資組合建構引擎 (Top 150 -> Top 20)")
    print("="*60)
    
    csv_path = "Factor_Dependency_Report/Flattened_Factor_Data.csv"
    if not os.path.exists(csv_path):
        print(f"[錯誤] 找不到參數寬表 {csv_path}。請先執行前一個腳本生成資料。")
        return
        
    print("[1] 載入參數寬表並進行全域排序...")
    df_params = pd.read_csv(csv_path)
    
    # 確保依照 Sharpe 全域降冪排序
    if 'metric_sharpe' not in df_params.columns:
        print("[錯誤] 找不到 metric_sharpe 欄位，請檢查寬表。")
        return
        
    df_params = df_params.sort_values(by='metric_sharpe', ascending=False).reset_index(drop=True)
    
    # ==========================================
    # 階段 1: 實施嚴格過濾與 Top 150 篩選
    # ==========================================
    print("[2] 實施生態多樣性約束 (同家族、同幣種、同方向最多 2 個)...")
    
    selected_indices = []
    group_counts = {}
    
    for idx, row in df_params.iterrows():
        fam = str(row.get('family', 'Unknown'))
        sym = str(row.get('symbol', 'Unknown'))
        
        # 判斷方向 (若寬表有 reverse_mode 則使用，否則預設為 Long)
        direction = 'long'
        if 'param_reverse_mode' in row and str(row['param_reverse_mode']).lower() in ['true', '1', '1.0']:
            direction = 'short'
            
        group_key = (fam, sym, direction)
        
        if group_counts.get(group_key, 0) < 2:
            selected_indices.append(idx)
            group_counts[group_key] = group_counts.get(group_key, 0) + 1
            
        if len(selected_indices) >= 150:
            break
            
    pool_df = df_params.loc[selected_indices].copy()
    print(f"  -> 成功萃取 {len(pool_df)} 組最高質量且具備多樣性的參數組合。")
    
    # ==========================================
    # 階段 2: 呼叫回測引擎重構資金曲線
    # ==========================================
    print("\n[3] 啟動底層回測引擎，重構歷史資金曲線 (Equity Curves)...")
    kline_cache = {}
    equity_curves = {}
    
    # 保留標準保留字，以利區分哪些是 family_params
    standard_params = {'strategy_id', 'symbol', 'timeframe_min', 'pool_name', 'cand_score', 'family', 'param_tp', 'param_sl', 'param_max_hold'}
    
    success_count = 0
    
    for idx, row in pool_df.iterrows():
        symbol = row['symbol']
        tf_min = int(row['timeframe_min']) if pd.notna(row['timeframe_min']) else 15
        family = row['family']
        
        df_kline = load_kline_data(symbol, tf_min, kline_cache)
        if df_kline is None:
            continue
            
        # 組裝參數
        tp = float(row['param_tp']) if pd.notna(row['param_tp']) else 0.0
        sl = float(row['param_sl']) if pd.notna(row['param_sl']) else 0.0
        max_hold = int(row['param_max_hold']) if pd.notna(row['param_max_hold']) else 0
        
        family_params = {}
        reverse_mode = False
        
        for col in pool_df.columns:
            if col.startswith('param_') and col not in standard_params:
                key = col.replace('param_', '')
                val = row[col]
                if pd.notna(val):
                    if key == 'reverse_mode':
                        reverse_mode = str(val).lower() in ['true', '1', '1.0']
                    else:
                        # 嘗試轉型為 int (若原本是整數)
                        if isinstance(val, float) and val.is_integer():
                            family_params[key] = int(val)
                        else:
                            family_params[key] = val
                            
        # 顯示進度
        strat_name = f"{family}_{symbol}_{'SHORT' if reverse_mode else 'LONG'}_Score{row['cand_score']:.1f}"
        print(f"  -> 回測執行中: [{success_count+1}/150] {strat_name} ...", end='\r')
        
        try:
            # 呼叫底層 backtest_panel2 進行運算 (對齊最新版參數命名 tp_pct 與 sl_pct)
            res = bt.run_backtest(
                df=df_kline,
                family=family,
                family_params=family_params,
                tp_pct=tp,
                sl_pct=sl,
                max_hold=max_hold,
                reverse_mode=reverse_mode
            )
            
            trades = res.get('trades_detail', [])
            daily_eq = build_daily_equity_curve(trades)
            
            if not daily_eq.empty:
                # 記錄成功重構的資金曲線
                # Key: Rank號_家族_幣種 (例如: 001_TEMA_RSI_BTC_USDT)
                curve_key = f"{success_count+1:03d}_{family}_{symbol}"
                equity_curves[curve_key] = daily_eq
                # 將原參數列存入 dict 以利後續匯出
                pool_df.at[idx, 'Curve_Key'] = curve_key
                success_count += 1
                print(f"  -> 回測執行中: [{success_count}/150] {strat_name} (完成: 找到 {len(trades)} 筆交易)")
            else:
                print(f"  -> 回測執行中: [{success_count+1}/150] {strat_name} (跳過: 無交易明細)")
                
        except Exception as e:
            print(f"  -> [崩潰] {strat_name} 回測失敗: {e}")

    if success_count == 0:
        print("[錯誤] 所有回測皆無產生資金曲線，請檢查資料或回測引擎。")
        return

    # ==========================================
    # 階段 3: 相依性計算與貪婪降維選擇
    # ==========================================
    print(f"\n[4] 合併 {success_count} 條資金曲線，計算動態相依性矩陣...")
    
    # 合併為 DataFrame，並處理未重疊的時間段 (前向填充淨值，若最前方無值則補 1.0)
    equity_df = pd.DataFrame(equity_curves).ffill().fillna(1.0)
    
    # 將淨值曲線轉換為「每日報酬率」(Daily Returns)
    returns_df = equity_df.pct_change().dropna(how='all').fillna(0.0)
    
    # 計算 Pearson 相關係數矩陣
    # [專家級防護] 強制將零變異數 (平盤空倉期) 導致的 NaN 轉換為 0.0，避免貪婪演算法崩潰
    corr_matrix = returns_df.corr().fillna(0.0)
    
    print("\n[5] 啟動貪婪降相依演算法 (Greedy Decorrelation)...")
    
    # 確保索引依據排名排序 (因為 Key 是 '001_...', '002_...' 開頭)
    sorted_strat_keys = sorted(list(equity_curves.keys()))
    
    selected_strats = []
    
    # 直接選入第 1 名 (目前世界上算出來夏普最高的組合)
    selected_strats.append(sorted_strat_keys[0])
    
    # 貪婪選擇直到滿 20 個 (或耗盡名單)
    while len(selected_strats) < 20 and len(selected_strats) < len(sorted_strat_keys):
        best_next_strat = None
        lowest_avg_corr = float('inf')
        
        for strat in sorted_strat_keys:
            if strat in selected_strats:
                continue
                
            # 計算此備選策略與「所有已入選策略」的平均相關係數
            avg_corr = corr_matrix.loc[strat, selected_strats].mean()
            
            # [專家級修復] 防護 NaN 數學陷阱！將因空倉造成的 NaN 相關性強制視為 0.0 (絕對零相關)
            if pd.isna(avg_corr):
                avg_corr = 0.0
            
            # 若平均相關係數更低，則取代成為最佳候選人
            if avg_corr < lowest_avg_corr:
                lowest_avg_corr = avg_corr
                best_next_strat = strat
                
        if best_next_strat:
            selected_strats.append(best_next_strat)
        else:
            break

    # ==========================================
    # 階段 4: 輸出最終報告
    # ==========================================
    print("\n" + "="*60)
    print("🏆 最終獲選：低相依性 20 大聖杯投資組合 (Holy Grail Portfolio)")
    print("="*60)
    
    final_report_path = "Factor_Dependency_Report/Top20_Holy_Grail_Portfolio.csv"
    
    # 萃取最終 20 名的完整參數資料
    final_df = pool_df[pool_df['Curve_Key'].isin(selected_strats)].copy()
    
    # 依照選入順序重新排序
    final_df['Selection_Rank'] = pd.Categorical(final_df['Curve_Key'], categories=selected_strats, ordered=True)
    final_df = final_df.sort_values('Selection_Rank').drop(columns=['Selection_Rank'])
    
    # 匯出 CSV 報告
    final_df.to_csv(final_report_path, index=False, encoding='utf-8-sig')
    
    for i, s_key in enumerate(selected_strats):
        row_data = final_df[final_df['Curve_Key'] == s_key].iloc[0]
        fam = row_data['family']
        sym = row_data['symbol']
        sharpe = row_data['metric_sharpe']
        
        # 顯示與其他已選策略的平均相關性 (第一名為基準，無平均相關性)
        if i == 0:
            corr_text = "N/A (基準錨點)"
        else:
            avg_corr = corr_matrix.loc[s_key, selected_strats[:i]].mean()
            corr_text = f"{avg_corr:+.4f}"
            
        print(f"[{i+1:02d}] {s_key: <25} | 夏普: {sharpe:.2f} | 對組合平均相關性: {corr_text}")

    print("\n[★] 分析完成！最終 Top 20 絕對參數清單已匯出至:")
    print(f"    -> {final_report_path}")
    print("    您可以直接拿這份名單輸入實盤下單機，享受極低相依性帶來的風險對沖紅利！")

if __name__ == "__main__":
    main()