from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import pandas as pd
import matplotlib.pyplot as plt
import yfinance as yf
import matplotlib
matplotlib.use('Agg')  

default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
}

DAGS_DIR = os.path.dirname(os.path.abspath(__file__))


dag = DAG(
    'case1_google_finance',
    default_args=default_args,
    description='Crawl GOOGL stock price and plot using yfinance',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

def crawl_stock_price():
    symbol = "GOOGL"
    stock = yf.Ticker(symbol)
    df = stock.history(period="5d", interval="1h")  # Dữ liệu theo giờ trong 5 ngày
    df.reset_index(inplace=True)
    df = df[["Datetime", "Close"]].rename(columns={"Datetime": "timestamp", "Close": "price"})
    
    filepath = os.path.join(DAGS_DIR, "google_stock.csv")
    df.to_csv(filepath, index=False)
    print(f"Đã lưu dữ liệu vào {filepath}")
    return df

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
        plt.title("GOOGL Stock Price Trend - Last 5 Days")
        plt.legend()
        plt.grid(True)
        plt.xticks(rotation=45)
        plt.tight_layout()
        plot_path = os.path.join(DAGS_DIR, "google_stock_plot.png")
        plt.savefig(plot_path)
        print(f"Đã lưu biểu đồ tại {plot_path}")
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
