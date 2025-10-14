from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def print_hello():
    return 'Hello world!'

with DAG(
    dag_id='students_database_pipeline',
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['student_data']
) as dag:
    hello_operator = PythonOperator(
        task_id='hello_task',
        python_callable=print_hello,
    )