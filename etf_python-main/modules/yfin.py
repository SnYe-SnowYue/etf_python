import yfinance as yf
import time
import pandas as pd
from datetime import datetime,timedelta
from yfinance.exceptions import YFRateLimitError
import asyncio
import aiohttp
from bs4 import BeautifulSoup
import numpy as np
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# 全域設定：股票清單與日期區間
start = '2023-01-01'
end = '2023-12-31'
tickers = ['2330.TW', '0050.TW', '00785B.TWO', '00862B.TWO']  # 可自行擴充

# 新增：資料庫連線與批次寫入
from connection.sql import main as get_db_conn, insert_stock_prices_batch

# 抓取新聞
NEWS_URL = "https://tw.stock.yahoo.com/"

async def fetch_article(session, link, title):
    try:
        async with session.get(link, timeout=10) as resp:
            html = await resp.text()
            soup = BeautifulSoup(html, "html.parser")
            date_tag = soup.select_one("time")
            if date_tag:
                date_str = date_tag.get("datetime") or date_tag.text
                date = pd.to_datetime(date_str).date()
            else:
                date = datetime.now().date()

            content_tag = soup.select_one(".caas-body")
            content = content_tag.get_text(strip=True) if content_tag else ""

            return {"title": title, "content": content, "date": date, "link": link}

    except Exception as e:
        print(f"Error fetching {link}: {e}")
        return None

async def fetch_news(limit=50):
    async with aiohttp.ClientSession() as session:
        res = await session.get(NEWS_URL)
        html = await res.text()
        soup = BeautifulSoup(html, "html.parser")
        news_links = soup.select("a[href^='/news']")[:limit]
        keywords = ['台積', '股價', 'ETF', '上市', '台股', '股票']
        news_links = [a for a in news_links if any(k in a.get_text() for k in keywords)]

        tasks = []
        for item in news_links:
            title = item.get_text(strip=True)
            if not title:
                continue
            link = "https://tw.stock.yahoo.com" + item["href"]
            tasks.append(fetch_article(session, link, title))

        results = await asyncio.gather(*tasks)
        return [r for r in results if r]

# 除息率
def get_dividend_data(ticker, retries=3):
    ticker_full = ticker

    for attempt in range(retries):
        try:
            stock = yf.Ticker(ticker_full)
            dividends = stock.dividends
            # print(yf.__version__)
            if dividends.empty:
                return None

            dividends_df = dividends.to_frame(name="Dividend").reset_index()
            dividends_df['year'] = dividends_df['Date'].dt.year
            dividends_df['month'] = dividends_df['Date'].dt.month
            dividends_df['Date'] = dividends_df['Date'].dt.strftime("%Y-%m-%d")
            return dividends_df

        except YFRateLimitError:
            # print(f"[{ticker}] 被限流，第 {attempt+1} 次重試中...")
            time.sleep(3 * (attempt + 1))  # 增加等待時間

        except Exception as e:
            # print(f"[{ticker}] 發生其他錯誤：{e}")
            break

    return None

# 成交價
def get_price_data(ticker, start_date=None, end_date=None, retries=3):
    for attempt in range(retries):
        try:
            stock = yf.Ticker(ticker)
            
            if not start_date:
                start_date = "2020-01-01"
            if not end_date:
                end_date = datetime.now().strftime("%Y-%m-%d")

            history = stock.history(start=start_date, end=end_date)

            if history.empty:
                return None

            price_df = history[['Open', 'Close']].reset_index()
            # 日期格式化
            price_df['Date'] = price_df['Date'].dt.strftime("%Y-%m-%d")
            # 四捨五入
            price_df['Close'] = price_df['Close'].round(2)
            price_df['Open'] = price_df['Open'].round(2)
            # 重新命名欄位
            price_df.rename(columns={'Close': 'Closing Price', 'Open': 'Open'}, inplace=True)

            return price_df

        except YFRateLimitError:
            time.sleep(3 * (attempt + 1))

        except Exception as e:
            print(f"[WARN] {ticker} 收盤價抓取失敗: {e}")
            break

    return None


# 殖利率
def merge_dividend_and_price(dividend_df, price_df):
    if dividend_df is None or price_df is None:
        return None

    # 合併兩張表格（以日期對齊）
    merged_df = pd.merge(dividend_df, price_df, on="Date", how="left")

    # 計算殖利率（Dividend / Closing Price）
    merged_df['Yield'] = (merged_df['Dividend'] / merged_df['Closing Price']).round(4)

    return merged_df

# 股票漲跌
def fetch_stock_info(ticker_symbol: str, display_name: str):
    try:
        ticker = yf.Ticker(ticker_symbol)

        # 取得最近兩日收盤價，用於計算漲跌
        hist = ticker.history(period="2d", interval="1d")
        if hist.empty or len(hist) < 1:
            return None  # 沒資料直接返回 None

        previous_close = hist['Close'].iloc[0]

        # 當日即時價格
        intraday = ticker.history(period="1d", interval="1m")
        if intraday.empty:
            current_price = previous_close
            open_price = previous_close
        else:
            first = intraday.iloc[0]
            latest = intraday.iloc[-1]
            open_price = first["Open"]
            current_price = latest["Close"]

        # 計算漲跌百分比（對今日開盤）
        percent_change = ((current_price - open_price) / open_price) * 100

        # 漲跌符號與顏色
        if percent_change > 0:
            sign, color = "▲", "text-success"  # 上漲 → 綠色
        elif percent_change < 0:
            sign, color = "▼", "text-danger"   # 下跌 → 紅色
        else:
            sign, color = "■", "text-secondary"  # 持平 → 灰色

        return {
            "name": display_name,
            "code": ticker_symbol.split('.')[0],
            "price": round(current_price, 2),
            "open": round(open_price, 2),
            "previous_close": round(previous_close, 2),
            "change": f"{abs(percent_change):.2f}",
            "sign": sign,
            "color": color
        }

    except Exception as e:
        # 抓取失敗自動返回 None
        print(f"[WARN] {ticker_symbol} 資料抓取失敗，跳過。錯誤: {e}")
        return None

# 損益率
def calculate_return(ticker_symbol, period_days=180):
    ticker = yf.Ticker(ticker_symbol)

    end_date = datetime.today()
    start_date = end_date - timedelta(days=period_days)

    # 取歷史資料，日期格式要用字串
    hist = ticker.history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))

    if hist.empty:
        # print("無歷史資料")
        return None

    start_price = hist['Close'][0]
    end_price = hist['Close'][-1]

    profit_rate = ((end_price - start_price) / start_price) * 100

    return profit_rate
# 平均殖利率
def calculate_average_yield(ticker, period_days=365):
    end_date = datetime.today()
    start_date = end_date - timedelta(days=period_days)
    hist = yf.Ticker(ticker).history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
    if hist.empty:
        return 0.0
    avg_price = hist['Close'].mean()
    dividends = get_dividend_data(ticker)
    if dividends is None:
        return 0.0
    dividends_in_period = dividends[(dividends['Date'] >= start_date.strftime('%Y-%m-%d')) & 
                                    (dividends['Date'] <= end_date.strftime('%Y-%m-%d'))]
    total_dividends = dividends_in_period['Dividend'].sum()
    if avg_price == 0:
        return 0.0
    return round((total_dividends / avg_price) * 100, 2)

# === 長期績效 & 風險指標 ===
def calculate_annualized_return(hist, period_days):
    start_price = hist['Close'].iloc[0]
    end_price = hist['Close'].iloc[-1]
    total_return = (end_price / start_price) - 1
    annualized_return = (1 + total_return) ** (365 / period_days) - 1
    return round(annualized_return * 100, 2)

def calculate_annualized_volatility(hist):
    daily_returns = hist['Close'].pct_change().dropna()
    volatility = daily_returns.std() * np.sqrt(252)
    return round(volatility * 100, 2)

def calculate_sharpe_ratio(hist, risk_free_rate=0.01):
    daily_returns = hist['Close'].pct_change().dropna()
    avg_daily_return = daily_returns.mean()
    excess_return = avg_daily_return - (risk_free_rate / 252)
    std_dev = daily_returns.std()
    sharpe_ratio = (excess_return / std_dev) * np.sqrt(252)
    return round(sharpe_ratio, 2)

def calculate_max_drawdown(hist):
    cum_returns = hist['Close'] / hist['Close'].iloc[0]
    rolling_max = cum_returns.cummax()
    drawdown = (cum_returns - rolling_max) / rolling_max
    return round(drawdown.min() * 100, 2)

def calculate_return(hist):
    start_price = hist['Close'].iloc[0]
    end_price = hist['Close'].iloc[-1]
    profit_rate = ((end_price - start_price) / start_price) * 100
    return round(profit_rate, 2)

# === 單檔ETF分析 ===
def analyze_single_etf(ticker_symbol, period_days=365):
    end_date = datetime.today()
    start_date = end_date - timedelta(days=period_days)
    hist = yf.Ticker(ticker_symbol).history(start=start_date.strftime('%Y-%m-%d'), end=end_date.strftime('%Y-%m-%d'))
    if hist.empty:
        return None
    return {
        "ETF": ticker_symbol,
        "期間(天)": period_days,
        "年化報酬率(%)": calculate_annualized_return(hist, period_days),
        "波動率(%)": calculate_annualized_volatility(hist),
        "Sharpe比率": calculate_sharpe_ratio(hist),
        "最大回撤(%)": calculate_max_drawdown(hist),
        "損益率(%)": calculate_return(hist),
        "平均殖利率(%)": calculate_average_yield(ticker_symbol, period_days)
    }


def get_hot_etf(symbols, names, period_days=365, top_n=5):
    etf_data = []

    for symbol, name in zip(symbols, names):
        try:
            perf = analyze_single_etf(symbol, period_days=period_days)  # 單一 ETF
            score = perf["年化報酬率(%)"] / (perf["波動率(%)"] + 1e-6)
            etf_data.append({
                "name": name,
                "ticker": symbol,
                "score": score,
                "perf": perf
            })
        except Exception as e:
            print(f"{name} 資料抓不到，跳過")
            continue

    # 排序並取前 top_n
    return sorted(etf_data, key=lambda x: x["score"], reverse=True)[:top_n]

# 如果不能用 pip install --upgrade yfinance


# 範例：批次抓取多檔股票價格並寫入資料庫
def save_prices_to_db(ticker_list, start_date=None, end_date=None):
    """
    批次抓取多檔股票歷史價格，並寫入 stock_prices 資料表。
    :param ticker_list: 股票代碼 list
    :param start_date: 起始日 (str, YYYY-MM-DD)
    :param end_date: 結束日 (str, YYYY-MM-DD)
    """
    conn = get_db_conn()
    if conn is None:
        print("[DB] 無法連線 MySQL，停止寫入。")
        return
    all_data = []
    for ticker in ticker_list:
        price_df = get_price_data(ticker, start_date, end_date)
        if price_df is None:
            continue
        for _, row in price_df.iterrows():
            closing = float(row['Closing Price']) if not pd.isna(row['Closing Price']) else None
            openp = float(row['Open']) if 'Open' in row and not pd.isna(row['Open']) else None
            all_data.append((ticker, row['Date'], closing, openp))
    if all_data:
        insert_stock_prices_batch(conn, all_data)
    conn.close()

# 使用範例：
# save_prices_to_db(['2330.TW', '0050.TW'], start_date='2023-01-01', end_date='2023-12-31')


# === 主程式整合 ===
start = '2023-01-01'
end = '2025-12-31'
tickers = ['2330.TW', '0050.TW', '00785B.TWO', '00862B.TWO']  # 可自行擴充

if __name__ == "__main__":
    # 1. 自動檢查並新增 open_price 欄位（如不存在）
    conn = get_db_conn()
    try:
        with conn.cursor() as cursor:
            cursor.execute("SHOW COLUMNS FROM stock_prices LIKE 'open_price'")
            result = cursor.fetchone()
            if not result:
                cursor.execute("ALTER TABLE stock_prices ADD COLUMN open_price FLOAT NULL AFTER closing_price;")
                conn.commit()
    except Exception:
        pass
    finally:
        conn.close()

    # 2. 批次寫入
    save_prices_to_db(tickers, start_date=start, end_date=end)