import os
import csv
import psycopg2
from psycopg2 import Error

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
            with open(file=os.path.join('/home/user/shared_folder/data2', f'{table}.csv'), mode='w', encoding="utf-8") as csv_file:
                cursor.copy_expert(f"COPY public.{table} TO STDOUT with header CSV", csv_file)


    except (Exception, Error) as error:
        print("Ошибка при работе с PostgreSQL", error)

    finally:
        if cursor:
            cursor.close()
            print("Соединение с PostgreSQL закрыто")


if __name__ == '__main__':
    read_pg()
