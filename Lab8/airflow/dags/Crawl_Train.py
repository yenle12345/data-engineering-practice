from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
import joblib
import os
import shutil

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'case2_pima_model',
    default_args=default_args,
    description='Train model with PIMA data',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False
)

# 🗂 Định nghĩa đường dẫn thư mục dags/
DAGS_DIR = os.path.dirname(__file__)

def load_data():
    src = '/mnt/data/Lab8-pima.csv'
    dst = os.path.join(DAGS_DIR, 'pima.csv')
    shutil.copyfile(src, dst)

def train_model():
    df = pd.read_csv(os.path.join(DAGS_DIR, 'pima.csv'), header=None)

    # Đặt tên cột chính xác theo định dạng của PIMA dataset
    df.columns = ["Pregnancies", "Glucose", "BloodPressure", "SkinThickness", "Insulin", 
                  "BMI", "DiabetesPedigreeFunction", "Age", "Outcome"]

    X = df.drop("Outcome", axis=1)
    y = df["Outcome"]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    model = LogisticRegression(max_iter=200)
    model.fit(X_train, y_train)

    joblib.dump(model, os.path.join(DAGS_DIR, 'pima_model.pkl'))
    X_test.to_csv(os.path.join(DAGS_DIR, 'X_test.csv'), index=False)
    y_test.to_csv(os.path.join(DAGS_DIR, 'y_test.csv'), index=False)

def evaluate():
    model = joblib.load(os.path.join(DAGS_DIR, 'pima_model.pkl'))
    X_test = pd.read_csv(os.path.join(DAGS_DIR, 'X_test.csv'))
    y_test = pd.read_csv(os.path.join(DAGS_DIR, 'y_test.csv'))

    y_pred = model.predict(X_test)
    report = classification_report(y_test, y_pred)
    print("Classification Report:\n", report)

load_task = PythonOperator(
    task_id='load_pima_data',
    python_callable=load_data,
    dag=dag
)

train_task = PythonOperator(
    task_id='train_pima_model',
    python_callable=train_model,
    dag=dag
)

evaluate_task = PythonOperator(
    task_id='evaluate_pima_model',
    python_callable=evaluate,
    dag=dag
)

load_task >> train_task >> evaluate_task