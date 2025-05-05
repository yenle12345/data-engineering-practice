import psycopg2
import csv

def create_tables(cur):
    # Xoá bảng nếu đã tồn tại để tạo lại từ đầu (tránh lỗi schema cũ)
    cur.execute("DROP TABLE IF EXISTS transactions;")
    cur.execute("DROP TABLE IF EXISTS products;")
    cur.execute("DROP TABLE IF EXISTS accounts;")


    cur.execute("""
        CREATE TABLE IF NOT EXISTS accounts (
            customer_id INT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            address_1 TEXT,
            address_2 TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            join_date DATE
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id INT PRIMARY KEY,
            product_code TEXT,
            product_description TEXT
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            transaction_date DATE,
            product_id INT,
            product_code TEXT,
            product_description TEXT,
            quantity INT,
            account_id INT,
            FOREIGN KEY (product_id) REFERENCES products(product_id),
            FOREIGN KEY (account_id) REFERENCES accounts(customer_id)
        );
    """)

def insert_csv_data(cur, conn, table_name, file_path):
    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        headers = next(reader)  # bỏ qua header

        for row in reader:
            placeholders = ', '.join(['%s'] * len(row))
            sql = f"INSERT INTO {table_name} VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            cur.execute(sql, row)
        conn.commit()

def main():
    host = "postgres"
    database = "airflow"
    user = "airflow"
    pas = "airflow"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cur = conn.cursor()

    create_tables(cur)

    insert_csv_data(cur, conn, 'accounts', 'data/accounts.csv')
    insert_csv_data(cur, conn, 'products', 'data/products.csv')
    insert_csv_data(cur, conn, 'transactions', 'data/transactions.csv')

    print("Hoàn thành nạp dữ liệu.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
