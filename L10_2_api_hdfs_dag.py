from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from common.api_hd import ap_hd

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 0,
}

dag = DAG(
    'l10_2_from_api_to_hdfs',
    description='l10_2_from_api_to_hdfs',
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 14, 1, 15),
    #end_date=datetime(2022, 4, 6, 23, 59),
    default_args=default_args)

t1 = PythonOperator(
    task_id='Load_data_from_API',
    dag=dag,
    python_callable=ap_hd
)

