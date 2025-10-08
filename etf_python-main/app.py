from flask import Flask, request, jsonify, render_template, redirect, session, flash
import base64
import asyncio
import random
# === mysql連線 ===
from connection.sql import main
conn = main()

# === modules ===
from modules.yfin import get_dividend_data, get_price_data, merge_dividend_and_price,fetch_stock_info, fetch_news, get_hot_etf

# === Flask應用程式初始化 ===
app = Flask(__name__) 

app.secret_key = "etf_taiwan_2025_unversity_ocu_project"

# === 網站標題 ===
web_title = "台股ETF推薦管理平台"
# === 網站footer 標題 ===
web_footer = web_title+" © 2025 OCU專題"

# === navbar選單 登入前 ===
navbar_before_login = [
    {"name": "登入", "url": "/"},
    {"name": "註冊", "url": "/register"},
]

# === navbar選單 登入後 ===
navbar_after_login = [
    {"name": "首頁", "url": "/home"},
    {"name": "股票管理", "url": "/stock"},
    {"name": "會員個資", "url": "/profile"},
    {"name": "登出", "url": "/logout"},
]

# === session管理 ===
def is_logged_in():
    return 'user' in session and 'name' in session 

# === 登入首頁 ===
@app.route("/", methods=["GET"])
def login():
    # 檢查使用者是否已經登入
    islogged_in = is_logged_in()
    if islogged_in:
        return redirect("/home")
    
    return render_template("login.html", title=web_title, navbar=navbar_before_login, footer=web_footer)

# === 登入處理 ===
@app.route("/login", methods=["POST"])
def do_login():
    username = request.form.get("username")
    password = request.form.get("password")
    
    # 從資料庫驗證使用者
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        user = cursor.fetchone()

    password = base64.b64encode(password.encode()).decode() if password else None
    if user and user[6] == username and user[7] == password:
        session['user'] = user[6]  # 使用者名稱
        session['name'] = user[1]  # 使用者姓名
        return redirect("/home")
    else:
        if not user:
            # 使用Flask的flash功能來顯示錯誤訊息
            flash("帳號錯誤~請重新輸入!","error")
            return redirect('/')
        elif user[2] != password:
            # 使用Flask的flash功能來顯示錯誤訊息
            flash("密碼錯誤~請重新輸入!","error")
            return redirect('/')
        else:
            # 使用Flask的flash功能來顯示錯誤訊息
            flash("登入失敗~請稍後再試!","error")
            return redirect('/')

# === 註冊頁面 ===
@app.route("/register", methods=["GET"])
def register():
    # 檢查使用者是否已經登入
    islogged_in = is_logged_in()
    if islogged_in:
        return redirect("/home")
    
    return render_template("register.html", title=web_title, navbar=navbar_before_login, footer=web_footer)

# === 註冊處理 ===
@app.route("/register", methods=["POST"])
def do_register():  
    username = request.form.get("username")
    password = request.form.get("password")
    idcard = request.form.get("idcard")
    name = request.form.get("name")
    email = request.form.get("email")
    birthday = request.form.get("birthday")
    tel = request.form.get("tel")

    # 檢查使用者是否已經存在
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users WHERE username = %s", (username,))
        existing_user = cursor.fetchone()

    if existing_user:
        flash("使用者名稱已存在，請選擇其他名稱！", "error")
        return redirect("/register")

    # 將密碼進行base64編碼
    password = base64.b64encode(password.encode()).decode() if password else None

    # 新增使用者到資料庫
    with conn.cursor() as cursor:
        cursor.execute("""
            INSERT INTO users (username, password, idcard, name, email, birthday, tel) 
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (username, password, idcard, name, email, birthday, tel))
        conn.commit()

    flash("註冊成功！請登入。", "success")
    return redirect("/")

# === 登出 ===
@app.route("/logout", methods=["GET"])
def logout():
    session['user'] = None
    session['name'] = None
    session.clear()
    return redirect("/")


# === 登入後首頁 ===
@app.route("/home", methods=["GET"])
def home():
    # 檢查使用者是否已經登入
    islogged_in = is_logged_in()
    if not islogged_in:
        return redirect("/")
    
    # 1. 查詢隨機三檔股票基本資料
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM etf_tickers ORDER BY rand() LIMIT 3")
        tickers = cursor.fetchall()

    # 2. 查詢這三檔股票的最新價格、前一日價格、開盤價，計算漲跌幅等
    info_stock = []
    with conn.cursor(dictionary=True) as cursor:
        for symbol in tickers:
            ticker_code = symbol[2]
            # 查詢最新兩日的收盤價與開盤價
            cursor.execute("""
                SELECT date, closing_price, open_price
                FROM stock_prices
                WHERE ticker = %s
                ORDER BY date DESC LIMIT 2
            """, (ticker_code,))
            price_rows = cursor.fetchall()

            def safe_val(val, digits=2):
                if val is None:
                    return '-'
                if isinstance(val, str):
                    if val.strip() == '' or val.strip().lower() == 'none':
                        return '-'
                    try:
                        fval = float(val)
                        return f"{fval:.{digits}f}"
                    except Exception:
                        return val
                if isinstance(val, (int, float)):
                    return f"{val:.{digits}f}"
                return '-'

            if not price_rows:
                info_stock.append({
                    'name': symbol[1],
                    'code': ticker_code,
                    'price': '-',
                    'open': '-',
                    'previous_close': '-',
                    'change': '-',
                    'sign': '-',
                    'color': ''
                })
                continue
            latest = price_rows[0]
            previous = price_rows[1] if len(price_rows) > 1 else None
            # 計算漲跌幅
            latest_close = latest['closing_price'] if latest['closing_price'] is not None else None
            previous_close = previous['closing_price'] if previous and previous['closing_price'] is not None else None
            try:
                if previous_close is not None and latest_close is not None:
                    change = float(latest_close) - float(previous_close)
                    percent = (change / float(previous_close)) * 100 if float(previous_close) else 0
                else:
                    change = None
                    percent = None
            except Exception:
                change = None
                percent = None
            # 漲跌符號與顏色
            if percent is None:
                sign, color, percent_str = '-', '', '-'
            elif percent > 0:
                sign, color, percent_str = "▲", "text-success", f"{abs(percent):.2f}"
            elif percent < 0:
                sign, color, percent_str = "▼", "text-danger", f"{abs(percent):.2f}"
            else:
                sign, color, percent_str = "■", "text-secondary", f"{abs(percent):.2f}"
            info_stock.append({
                'name': symbol[1],
                'code': ticker_code,
                'price': safe_val(latest.get('closing_price')),
                'open': safe_val(latest.get('open_price')),
                'previous_close': safe_val(previous['closing_price']) if previous else '-',
                'change': percent_str if percent_str != '0.00' else '-',
                'sign': sign,
                'color': color
            })

    # 3. 熱門ETF與新聞維持原本流程
    symbols = []
    names = []
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM etf_tickers ORDER BY rand() LIMIT 5")
        for row in cursor.fetchall():
            symbols.append(row[2])
            names.append(row[1])
    hot_etf = get_hot_etf(symbols, names)
    news_data = asyncio.run(fetch_news())

    return render_template(
        "home.html",
        title=web_title,
        navbar=navbar_after_login,
        footer=web_footer,
        tickers=tickers,
        info_stock=info_stock,
        fetch_new=news_data,
        hot_etfs=hot_etf
    )

# === 會員個資頁面 ===
@app.route("/profile", methods=["GET"])
def profile():
    # 檢查使用者是否已經登入
    islogged_in = is_logged_in()
    if not islogged_in:
        return redirect("/")
    
    # 取得使用者資料
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users WHERE username = %s", (session['user'],))
        user = cursor.fetchone()

    if user:
        return render_template("userprofile.html", title=web_title, navbar=navbar_after_login, footer=web_footer, user=user)
    else:
        flash("無法找到使用者資料！", "error")
        return redirect("/home")

# === 會員個資編輯頁面 ===
@app.route("/profileEdit", methods=["GET"])
def profileEdit():
    # 檢查使用者是否已經登入
    islogged_in = is_logged_in()
    if not islogged_in:
        return redirect("/")
    
    # 取得使用者資料
    with conn.cursor() as cursor:
        cursor.execute("SELECT * FROM users WHERE username = %s", (session['user'],))
        user = cursor.fetchone()

    if user:
        return render_template("userprofileEdit.html", title=web_title, navbar=navbar_after_login, footer=web_footer, user=user)
    else:
        flash("無法找到使用者資料！", "error")
        return redirect("/home")

# === 會員個資編輯處理 ===
@app.route("/profileEdit", methods=["POST"])
def profileEditPost():
    # 檢查使用者是否已經登入
    islogged_in = is_logged_in()
    if not islogged_in:
        return redirect("/")

    # 取得表單資料
    idcard = request.form.get("idcard")
    name = request.form.get("name")
    email = request.form.get("email")
    birthday = request.form.get("birthday")
    tel = request.form.get("tel")

    # 更新使用者資料
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE users 
            SET idcard = %s, name= %s, email = %s, birthday = %s, tel = %s 
            WHERE username = %s
        """, (idcard, name, email, birthday, tel, session['user']))

        conn.commit()

    flash("個資更新成功！", "success")
    return redirect("/profile")

# === 股票管理頁面 ===
@app.route("/stock", methods=["GET"])
def stock():
    # 檢查使用者是否已經登入
    if not is_logged_in():
        return redirect("/")

    with conn.cursor(dictionary=True) as cursor:
        # 取出所有股票資料
        cursor.execute("SELECT * FROM stocks")
        stocks = cursor.fetchall()

        # 取出所有 ticker 對應關係，避免每次查一次資料庫
        cursor.execute("SELECT ticker, ticker_yfinance FROM etf_tickers")
        ticker_map = {row['ticker']: row['ticker_yfinance'] for row in cursor.fetchall()}

    for stock in stocks:
        symbol = stock.get("symbol")
        yfinance_ticker = ticker_map.get(symbol)

        if yfinance_ticker:
            dividend_data = get_dividend_data(yfinance_ticker)
            price_data = get_price_data(yfinance_ticker)

            merged = None
            if dividend_data is not None and price_data is not None:
                merged = merge_dividend_and_price(dividend_data, price_data)

            if merged is not None and not merged.empty:
                latest = merged.sort_values("Date", ascending=False).iloc[0]
                stock['date'] = latest.get('Date', '')
                stock['dividend'] = latest.get('Dividend', '')
                stock['closing_price'] = latest.get('Closing Price', '')
                stock['yield'] = latest.get('Yield', '')
            else:
                stock['dividend'] = ''
                stock['closing_price'] = ''
                stock['yield'] = ''
                stock['date'] = ''  
        else:
            stock['dividend'] = ''
            stock['closing_price'] = ''
            stock['yield'] = ''
            stock['date'] = ''

    if not stocks:
        flash("目前沒有股票資料！", "info")

    return render_template("stock.html", title=web_title, navbar=navbar_after_login, footer=web_footer, stocks=stocks)

@app.route("/stockAdd", methods=["GET"])
def stockAdd():
    # 檢查使用者是否已經登入
    islogged_in = is_logged_in()
    if not islogged_in:
        return redirect("/")

    return render_template("stockAdd.html", title=web_title, navbar=navbar_after_login, footer=web_footer)

# === 股票新增處理 ===
@app.route("/stockAdd", methods=["POST"])
def stockAddPost():
    # 檢查使用者是否已經登入
    islogged_in = is_logged_in()
    if not islogged_in:
        return redirect("/")

    # 取得表單資料
    stock_data = []
    names = request.form.getlist("Name[]")
    symbols = request.form.getlist("Symbol[]")
    quantities = request.form.getlist("Quantity[]")
    purchase_prices = request.form.getlist("Purchase_price[]")
    current_prices = request.form.getlist("Current_price[]")

    for i in range(len(names)):
        stock_data.append({
            "name": names[i],
            "symbol": symbols[i],
            "quantity": int(quantities[i]),
            "purchase_price": float(purchase_prices[i]),
            "current_price": float(current_prices[i])
        })
    
    # 新增股票資料到資料庫
    with conn.cursor() as cursor:
        for stock in stock_data:
            cursor.execute("""
                INSERT INTO stocks (name, symbol, quantity, purchase_price, current_price) 
                VALUES (%s, %s, %s, %s, %s)
            """, (stock['name'], stock['symbol'], stock['quantity'], stock['purchase_price'], stock['current_price']))
        conn.commit()

    flash("股票新增成功！", "success")
    return redirect("/stock")

# === 股票編輯頁面 === 
@app.route("/stockEdit/<int:id>", methods=["GET"])
def stockEdit(id):
    islogged_in = is_logged_in()
    if not islogged_in:
        return redirect("/")
    with conn.cursor(dictionary=True) as cursor:
        cursor.execute("SELECT * FROM stocks WHERE id = %s", (id,))
        stock = cursor.fetchone()
    if not stock:
        flash("找不到該股票資料！", "error")
        return redirect("/stock")
    return render_template("stockEdit.html", title=web_title, navbar=navbar_after_login, footer=web_footer, stock=stock)

# === 股票編輯處理 ===
@app.route("/stockEdit/<int:id>", methods=["POST"])
def stockEditPost(id):
    islogged_in = is_logged_in()
    if not islogged_in:
        return redirect("/")
    name = request.form.get("Name")
    symbol = request.form.get("Symbol")
    quantity = request.form.get("Quantity")
    purchase_price = request.form.get("Purchase_price")
    current_price = request.form.get("Current_price")
    with conn.cursor() as cursor:
        cursor.execute("""
            UPDATE stocks SET name=%s, symbol=%s, quantity=%s, purchase_price=%s, current_price=%s
            WHERE id=%s
        """, (name, symbol, quantity, purchase_price, current_price, id))
        conn.commit()
    flash("股票資料已更新！", "success")
    return redirect("/stock")

# === 股票刪除 ===
@app.route("/stockDelete/<int:id>", methods=["GET"])
def stockDelete(id):
    islogged_in = is_logged_in()
    if not islogged_in:
        return redirect("/")
    with conn.cursor() as cursor:
        cursor.execute("DELETE FROM stocks WHERE id = %s", (id,))
        conn.commit()
    flash("股票已刪除！", "success")
    return redirect("/stock")


# === 搜尋測試 ===

# 除息率(dividends)
@app.route("/ysearch", methods=["GET"])
def ysearch():
    # 檢查使用者是否已經登入
    islogged_in = is_logged_in()
    if not islogged_in:
        return redirect("/")

    get_dividend_tickers = request.args.get("ticker")
    get_dividend_year = request.args.get("year", type=int)
    get_dividend_month = request.args.get("month", type=int)

    if get_dividend_tickers:
        dividend_data = get_dividend_data(get_dividend_tickers)

        if dividend_data is not None:
            # 取得成交價資料範圍
            start = dividend_data['Date'].min()
            end = dividend_data['Date'].max()
            price_data = get_price_data(get_dividend_tickers, start_date=start, end_date=end)

            # 合併除息與成交價資料，計算殖利率
            merged_data = merge_dividend_and_price(dividend_data, price_data)

            if merged_data is None:
                return jsonify({"error": "資料合併失敗"}), 500

            # 篩選年份與月份
            filtered_data = merged_data
            if get_dividend_year:
                filtered_data = filtered_data[filtered_data['year'] == get_dividend_year].copy()
                if get_dividend_month:
                    filtered_data = filtered_data[filtered_data['month'] == get_dividend_month].copy()

            col_map = {
                "Date": "日期",
                "Dividend": "除息金額",
                "Closing Price": "成交價",
                "Yield": "殖利率"
            }

            filtered_data = filtered_data.drop(columns=[col for col in ['year', 'month'] if col in filtered_data.columns])
            filtered_data.rename(columns = col_map, inplace= True)

            return render_template(
                "dividend.html",
                title=web_title,
                navbar=navbar_after_login,
                footer=web_footer,
                dividend_data=filtered_data.to_dict(orient="records")
            )
        else:
            return render_template(
                "dividend.html",
                title=web_title,
                navbar=navbar_after_login,
                footer=web_footer,
                dividend_data= None
            )
    flash("缺少必要的參數","error")
    return render_template(
        "dividend.html",
        title=web_title,
        navbar=navbar_after_login,
        footer=web_footer,
        dividend_data= None
    )



# ======
if __name__ == "__main__":
    # 啟動Flask應用程式
    app.run(debug=False)
