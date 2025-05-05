from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

with DAG(
    dag_id='lab9_pipeline',
    default_args=default_args,
    schedule_interval=None, 
    catchup=False,
    description='Pipeline chạy Exercise 1 đến 5',
    tags=['exercise'],
) as dag:

    run_ex1 = BashOperator(
        task_id='ex_1',
        bash_command='python3 /opt/excercise/excercise1/main.py'
    )

    run_ex2 = BashOperator(
        task_id='ex_2',
        bash_command='python3 /opt/excercise/excercise2/main.py'
    )

    run_ex3 = BashOperator(
        task_id='ex_3',
        bash_command='python3 /opt/excercise/excercise3/main.py'
    )

    run_ex4 = BashOperator(
        task_id='ex_4',
        bash_command='python3 /opt/excercise/excercise4/main.py'
    )

    run_ex5 = BashOperator(
        task_id='ex_5',
        bash_command='cd /opt/excercise/excercise5/ && python3 main.py'
    )
    [run_ex1, run_ex2, run_ex3,run_ex4,run_ex5]