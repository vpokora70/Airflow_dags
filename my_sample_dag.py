from datetime import datetime

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2,
}

dag = DAG(
    'my_sample_dag',
    description='Sample DAG77',
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 14, 1, 15),
    default_args=default_args)

t1 = BashOperator(
    task_id='print_net_data',
    bash_command='ifconfig',
    dag=dag)

t2 = BashOperator(
    task_id='sleep',
    bash_command='sleep 5',
    dag=dag)

t3 = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag)

t1 >> t2 >> t3
