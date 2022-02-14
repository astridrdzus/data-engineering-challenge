from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pipeline.py import extract, check_if_valid_data, load


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(0,0,0,0),
    'email': ['astrid.rguez.us@gmail.com'],
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)

}

dag = DAG(
    'credit_card_dag',
    default_args = default_args,
    description = 'Loading data to a landing stage',
    schedule_interval= timedelta(days=1),
)


extract_data = PythonOperator(
    task_id= 'extract',
    python_callable = extract('hosts/host_app/app_data.xlsx'),
    dag= dag
)

run_pipeline