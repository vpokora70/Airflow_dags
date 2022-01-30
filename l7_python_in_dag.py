from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import json
import os
import csv
import psycopg2
from psycopg2 import Error
import requests



#Функция для загрузки данных из API
def ap(**kwargs):
    #Уставновка  даты загрузки, этот парметр ds из контекста, получили из **kwargs который
    #передал Airflow из задачи t1 посредством оператора provide_context = True.
    #аргумент **kwargs это словарь с именованными параметрами
    #process_date = kwargs['ds']
    #print(f"kwargs[ds]= {kwargs['ds']}")
    process_date = '2021-12-08'
    # store_folder = str(date.today())

    url1 = 'https://robot-dreams-de-api.herokuapp.com/auth'
    data = {'username': "rd_dreams", 'password': "djT6LasE"}
    headers1 = {'content-type': 'application/json'}
    response = requests.post(url=url1, headers=headers1, data=json.dumps(data))
    print(response.status_code)
    token = response.json()["access_token"]
    path_to_directory = os.path.join('/home/user/shared_folder/data', process_date)

    os.makedirs(path_to_directory, exist_ok=True)
    url2 = 'https://robot-dreams-de-api.herokuapp.com/out_of_stock'
    headers2 = {"content-type": "application/json", "Authorization": "JWT " + token}
    data2 = {'date': process_date}
    response2 = requests.get(url2, headers=headers2, data=json.dumps(data2))
    print(response2.status_code, process_date)
    data = response2.json()

    file_name = 'stock.json'
    with open(os.path.join(path_to_directory, file_name), 'w') as json_file:
        json.dump(data, json_file)

pg_creds = {"host": "192.168.68.108",
            "port": 5432,
            "database": "dshop",
            "user": "pguser",
            "password": "secret", }


def read_pg():
    try:
        with psycopg2.connect(**pg_creds) as pg_connection:
            cursor = pg_connection.cursor()
        # SQL-перечень таблиц базы данных схемы public
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog') AND table_schema IN('public');"
        cursor.execute(sql)
        record = cursor.fetchall()
        tables_to_load = []
        for i in range(len(record)):
            tables_to_load.append(list(record[i])[0])
        print(f"Таблицы базы данных схема public:", tables_to_load, "\n")
        for table in tables_to_load:
            with open(file=os.path.join('/home/user/shared_folder/data', f'{table}.csv'), mode='w', encoding="utf-8") as csv_file:
                cursor.copy_expert(f"COPY public.{table} TO STDOUT with header CSV", csv_file)


    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)

    finally:
        if cursor:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2,
}

dag = DAG(
    'l7_python_in_dag',
    description='l7_python_in_dag',
    schedule_interval='@daily',
    start_date=datetime(2022, 1, 14, 1, 15),
    default_args=default_args)

t1 = PythonOperator(
    task_id='data_from_api',
    dag=dag,
    python_callable=ap,
    provide_context = True
)

t2 = PythonOperator(
    task_id='read_dshop',
    dag=dag,
    python_callable=read_pg
)

t1>>t2