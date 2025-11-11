import sqlite3
import os

DB_FILE = "demo.db"

if os.path.exists(DB_FILE):
    os.remove(DB_FILE)

conn = sqlite3.connect(DB_FILE)
cursor = conn.cursor()

# 1. 테이블 생성 (FK 포함)
cursor.execute(
    "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)"
)
cursor.execute(
    "CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price REAL)"
)
cursor.execute(
    """
    CREATE TABLE orders (
        id INTEGER PRIMARY KEY, 
        user_id INTEGER, 
        product_id INTEGER,
        order_date TEXT,
        FOREIGN KEY(user_id) REFERENCES users(id),
        FOREIGN KEY(product_id) REFERENCES products(id)
    )
    """
)

# 2. 뷰 생성
cursor.execute(
    """
    CREATE VIEW user_orders AS
    SELECT u.name as user_name, p.name as product_name, o.order_date
    FROM orders o
    JOIN users u ON o.user_id = u.id
    JOIN products p ON o.product_id = p.id
    """
)

# 3. 데이터 일부 삽입 (선택 사항)
cursor.execute(
    "INSERT INTO users (name, email) VALUES ('천재 사용자', 'genius@example.com')"
)
cursor.execute(
    "INSERT INTO products (name, price) VALUES ('Schema Scribe Pro', 99.9)"
)

conn.commit()
conn.close()

print(f"✅ 데모 데이터베이스 '{DB_FILE}' 생성 완료!")
