from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import HttpOperator
from airflow.exceptions import AirflowException
from airflow.models import Variable
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'student_database_pipeline',
    default_args=default_args,
    description='Create database, table, insert student data, and run custom query',
    schedule_interval=None,
    catchup=False,
    tags=['postgres', 'students'],
)

# # Task 1: Create database if not exists
# create_database = SQLExecuteQueryOperator(
#     task_id='create_database',
#     conn_id='postgres_students',
#     sql="CREATE DATABASE students_db;",
#     autocommit=True,
#     doc_md="""
#     Creates a PostgreSQL database named 'students_db' if it doesn't already exist.
#     This is the first task in the pipeline and ensures the required database is available.
#     Uses autocommit to execute the CREATE DATABASE command outside a transaction block.
#     Note: You may need to handle \"database already exists\" errors at the connection level.
#     """,
#     dag=dag,
# )

# Task 2: Create table if not exists
create_table = SQLExecuteQueryOperator(
    task_id='create_table',
    conn_id='postgres_students',
    sql="""
    CREATE TABLE IF NOT EXISTS students (
        student_id INTEGER PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        age INTEGER,
        grade VARCHAR(2),
        subject VARCHAR(50),
        score INTEGER,
        enrollment_date DATE
    );
    """,
    doc_md="""
    Creates the 'students' table with schema for storing student records.
    Includes fields for student_id, name, age, grade, subject, score, and enrollment_date.
    Uses IF NOT EXISTS to safely handle table creation on multiple DAG runs.
    """,
    dag=dag,
)

upload_file_task = HttpOperator(
    task_id='upload_file',
    http_conn_id='students_api',
    endpoint='/students/upload',
    method='POST',
    # Don't set 'data' parameter for file uploads
    request_kwargs={
        'files': {
            'file': open('/opt/airflow/dags/students_data.csv', 'rb')
        }
    },
    dag=dag
)

# Task 4: Run custom SQL query from Airflow Variable
run_custom_query = SQLExecuteQueryOperator(
    task_id='run_custom_query',
    conn_id='postgres_students',
    sql=Variable.get("student_query"),
    doc_md="""
    Executes a custom SQL query stored in the Airflow Variable 'student_query'.
    Allows dynamic query execution without code changes.
    Set the 'student_query' variable in Airflow UI with your desired SQL.
    """,
    dag=dag,
)

# Set task dependencies
create_table >> upload_file_task >> run_custom_query