import pandas as pd
import numpy as np
import plotly.graph_objects as go

def generate_random_market(num_periods=200, start_price=3000, mu=0.0001, sigma=0.02, seed=None):
    """
    生成隨機市場的 OHLC 數據。
    
    參數:
    num_periods (int): 生成的 K 棒數量
    start_price (float): 初始價格 (例如設定為以太幣的價格級別)
    mu (float): 預期收益率 (Drift)
    sigma (float): 波動率 (Volatility)
    seed (int): 隨機種子，方便重現結果
    """
    if seed is not None:
        np.random.seed(seed)
        
    # 1. 生成每日的對數收益率 (常態分佈)
    # 使用幾何布朗運動的離散形式
    returns = np.random.normal(loc=mu, scale=sigma, size=num_periods)
    
    # 2. 計算收盤價 (累乘收益率)
    close_prices = start_price * np.cumprod(1 + returns)
    
    # 3. 建構 Open, High, Low
    # 開盤價：假設等於前一根的收盤價 (這裡簡化處理，不模擬跳空)
    open_prices = np.roll(close_prices, 1)
    open_prices[0] = start_price
    
    # 高低價：在開盤與收盤的基礎上，加入盤中波動雜訊
    # 確保 High 一定是大於等於 Open 和 Close 的最大值，Low 則是最小值
    intraday_volatility = sigma * 0.5 
    
    high_prices = np.maximum(open_prices, close_prices) * (1 + np.abs(np.random.normal(0, intraday_volatility, num_periods)))
    low_prices = np.minimum(open_prices, close_prices) * (1 - np.abs(np.random.normal(0, intraday_volatility, num_periods)))
    
    # 4. 整理成 DataFrame
    dates = pd.date_range(start='2026-01-01', periods=num_periods, freq='D')
    df = pd.DataFrame({
        'Open': open_prices,
        'High': high_prices,
        'Low': low_prices,
        'Close': close_prices
    }, index=dates)
    
    return df

# 生成數據
market_data = generate_random_market(num_periods=150, start_price=3000, sigma=0.03, seed=42)

# 使用 Plotly 畫出互動式 K 棒圖
fig = go.Figure(data=[go.Candlestick(x=market_data.index,
                open=market_data['Open'],
                high=market_data['High'],
                low=market_data['Low'],
                close=market_data['Close'])])

fig.update_layout(
    title='隨機生成的虛擬市場 K 線圖 (Geometric Brownian Motion)',
    yaxis_title='價格',
    xaxis_title='日期',
    template='plotly_dark' # 黑色主題比較有看盤軟體的感覺
)

fig.show()