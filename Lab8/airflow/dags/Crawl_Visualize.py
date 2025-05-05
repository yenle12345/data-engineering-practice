from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import time
import pandas as pd
import matplotlib.pyplot as plt
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

default_args = {
    'owner': 'airflow',
    'retries': 3,  # Tăng số lần thử lại để đảm bảo ổn định
    'retry_delay': timedelta(minutes=3),  # Giảm thời gian chờ khi lỗi
}

dag = DAG(
    'case1_google_finance',
    default_args=default_args,
    description='Crawl stock price from Google Finance',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

DAGS_DIR = os.path.dirname(__file__)

def crawl_stock_price():
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')

    driver = webdriver.Chrome(options=options)

    symbol = "GOOGL"
    url = f"https://www.google.com/finance/quote/{symbol}:NASDAQ"

    driver.get(url)
    time.sleep(5)  # Chờ trang tải

    try:
        price_elem = driver.find_element(By.CLASS_NAME, "YMlKec.fxKbKc")
        price_text = price_elem.text.replace('$', '').replace(',', '').strip()

        if not price_text:
            raise ValueError("Không lấy được giá cổ phiếu!")

        price = float(price_text)
    except Exception as e:
        driver.quit()
        raise RuntimeError(f"Lỗi lấy dữ liệu từ Google Finance: {e}")

    timestamp = datetime.now().isoformat()
    filepath = os.path.join(DAGS_DIR, "google_stock.csv")

    df = pd.DataFrame([[timestamp, price]], columns=["timestamp", "price"])
    
    try:
        if os.path.exists(filepath):
            df.to_csv(filepath, mode='a', header=False, index=False)
        else:
            df.to_csv(filepath, index=False)
    except Exception as e:
        raise RuntimeError(f"Lỗi khi ghi vào tệp CSV: {e}")

    driver.quit()

def visualize():
    filepath = os.path.join(DAGS_DIR, "google_stock.csv")

    try:
        df = pd.read_csv(filepath, parse_dates=["timestamp"])
        
        if df.empty:
            raise ValueError("Dữ liệu trống, không thể vẽ biểu đồ!")

        plt.figure(figsize=(10, 5))
        plt.plot(df["timestamp"], df["price"], marker='o', linestyle='-', color='b', label="GOOGL Stock Price")
        plt.xlabel("Time")
        plt.ylabel("Price (USD)")
        plt.title("GOOGL Stock Price Trend")
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.savefig(os.path.join(DAGS_DIR, "google_stock_plot.png"))
    except Exception as e:
        raise RuntimeError(f"Lỗi khi tạo biểu đồ: {e}")

crawl_task = PythonOperator(
    task_id='crawl_google_finance',
    python_callable=crawl_stock_price,
    dag=dag,
)

visualize_task = PythonOperator(
    task_id='visualize_stock_price',
    python_callable=visualize,
    dag=dag,
)

crawl_task >> visualize_task