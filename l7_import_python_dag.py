from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from ap import ap
from l7_dump_dshop import read_pg

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2,
}

dag = DAG(
    'l7_import_python',
    description='l7_import_python_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 14, 1, 15),
    default_args=default_args)

t1 = PythonOperator(
    task_id='Load_data_from_API',
    dag=dag,
    python_callable=ap
)

t2 = PythonOperator(
    task_id='read_dshop2',
    dag=dag,
    python_callable=read_pg,

)

t1>>t2