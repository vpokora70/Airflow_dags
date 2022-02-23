from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from common.db_pagila_hdfs import ld_hadoop
from airflow.operators.dummy_operator import DummyOperator


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 1,
}

dag = DAG(
    'l13_1_pagila_hdfs_dag',
    description='l13_1_dag_import_from_pagila_to_hdfs',
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 5, 23, 11),
    # end_date=datetime(2022, 4, 6, 23, 59),
    default_args=default_args)

list_tables = ['film_actor', 'address', 'city', 'actor', 'film_category', 'inventory', 'category', 'country', 'customer', 'language', 'film',
'payment', 'payment_p2020_02', 'payment_p2020_05', 'payment_p2020_04', 'payment_p2020_03', 'payment_p2020_06', 'rental', 'staff',
'store', 'payment_p2020_01']

task_list_table = []
# It is main folder in HADOOP for data from dshop database
root_path = "/bronze/pagila"
silver_root_path = "/silver/pagila"
#HADOOP Server details.
h_server = 'http://127.0.0.1:50070'
h_user = 'user'



dummy_1 = DummyOperator(
    task_id="dummy_1",
    dag=dag
)

dummy_2 = DummyOperator(
    task_id="dummy_2",
    dag=dag
)

for table in list_tables:
    task_list_table.append(PythonOperator(
        task_id=f'Load_data_from_pagila_to_hdfs_{table}',
        dag=dag,
        python_callable=ld_hadoop,
        op_kwargs={'table': f'{table}', 'postgres_conn_id': 'my_postgres',
                   'hadoop_server': h_server,
                   'user': h_user, 'hdfs_path': root_path})
    )
dummy_1 >> task_list_table >> dummy_2