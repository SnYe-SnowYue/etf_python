import mysql.connector

# == Connection to MySQL Database default setting ===
host = 'localhost'
user = 'root'
password = ''
database = 'etf'


# == Function to create a connection to the MySQL database ===

def create_connection(host, user, password, database):
    """Create a database connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        if connection.is_connected():
            print("Connection to MySQL DB successful")
            return connection
    except mysql.connector.Error as err:
        print(f"Error: {err}")
        return None


# Create a connection to the database
def main():
    conn = create_connection(host, user, password, database)
    return conn 


# 新增：寫入股票價格資料

# 單筆寫入（含 open_price）
def insert_stock_price(conn, ticker, date, closing_price, open_price):
    """
    將單筆股票價格資料寫入 stock_prices 資料表。
    :param conn: 資料庫連線
    :param ticker: 股票代碼 (str)
    :param date: 日期 (str, 格式: YYYY-MM-DD)
    :param closing_price: 收盤價 (float)
    :param open_price: 開盤價 (float)
    """
    try:
        cursor = conn.cursor()
        sql = """
            INSERT INTO stock_prices (ticker, date, closing_price, open_price)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE closing_price = VALUES(closing_price), open_price = VALUES(open_price)
        """
        cursor.execute(sql, (ticker, date, closing_price, open_price))
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"[DB] insert_stock_price error: {e}")


# 新增：批次寫入多筆股票價格資料

# 批次寫入（含 open_price）
def insert_stock_prices_batch(conn, data_list):
    """
    批次寫入多筆股票價格資料。
    :param conn: 資料庫連線
    :param data_list: List of (ticker, date, closing_price, open_price)
    """
    try:
        cursor = conn.cursor()
        sql = """
            INSERT INTO stock_prices (ticker, date, closing_price, open_price)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE closing_price = VALUES(closing_price), open_price = VALUES(open_price)
        """
        cursor.executemany(sql, data_list)
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"[DB] insert_stock_prices_batch error: {e}")
