"""Generate synthetic customer orders data and optional MySQL load helper."""
import csv
import random
from datetime import datetime, timedelta
import argparse
import mysql.connector

def generate_csv(path: str, rows: int = 100000):
    random.seed(42)
    start = datetime(2023, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["order_id","customer_id","order_date","amount","status"])
        for i in range(1, rows+1):
            cust = random.randint(1, 5000)
            dt = start + timedelta(days=random.randint(0, 600))
            amt = round(random.uniform(5, 1000), 2)
            status = random.choice(["NEW","PAID","SHIPPED","CANCELLED","REFUNDED"])
            w.writerow([i, cust, dt.strftime("%Y-%m-%d"), amt, status])
    print(f"Wrote {rows} rows to {path}")

def load_to_mysql(csv_path: str, host: str, user: str, password: str, database: str, port: int = 3306):
    conn = mysql.connector.connect(host=host, user=user, password=password, database=database, port=port)
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE IF NOT EXISTS customer_orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        order_date DATE,
        amount DECIMAL(10,2),
        status VARCHAR(20)
    )
    """)
    conn.commit()
    with open(csv_path, newline="") as f:
        reader = csv.DictReader(f)
        rows = []
        for row in reader:
            rows.append((int(row["order_id"]), int(row["customer_id"]), row["order_date"], float(row["amount"]), row["status"]))
            if len(rows) >= 1000:
                cur.executemany("INSERT INTO customer_orders (order_id, customer_id, order_date, amount, status) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE customer_id=VALUES(customer_id), order_date=VALUES(order_date), amount=VALUES(amount), status=VALUES(status)", rows)
                conn.commit()
                rows.clear()
        if rows:
            cur.executemany("INSERT INTO customer_orders (order_id, customer_id, order_date, amount, status) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE customer_id=VALUES(customer_id), order_date=VALUES(order_date), amount=VALUES(amount), status=VALUES(status)", rows)
            conn.commit()
    cur.close()
    conn.close()
    print("Loaded CSV into MySQL.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100000)
    ap.add_argument("--csv", type=str, default="data/customer_orders.csv")
    ap.add_argument("--load-mysql", action="store_true")
    ap.add_argument("--host", default="localhost")
    ap.add_argument("--port", type=int, default=3306)
    ap.add_argument("--user", default="root")
    ap.add_argument("--password", default="yourpassword")
    ap.add_argument("--database", default="source_db")
    args = ap.parse_args()

    generate_csv(args.csv, rows=args.rows)
    if args.load_mysql:
        load_to_mysql(args.csv, args.host, args.user, args.password, args.database, args.port)
