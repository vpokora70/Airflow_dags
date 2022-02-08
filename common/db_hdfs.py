from datetime import datetime
from hdfs import InsecureClient
from airflow.hooks.postgres_hook import PostgresHook

import os


def ld_hadoop(**kwargs):
    # Build folder name for  hadoop
    hdfs_directory = os.path.join(kwargs['hdfs_path'], datetime.today().strftime('%Y'),
                                  datetime.today().strftime('%Y-%m'))
    # Postgres session
    hook_db = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'])
    conn = hook_db.get_conn()
    cursor = conn.cursor()
    table = kwargs['table']
    time_file = datetime.today().strftime('%d-%H-%M')

    # hadoop session
    client = InsecureClient(url=kwargs['hadoop_server'], user='user')
    client.makedirs(hdfs_directory)
    with client.write(os.path.join(f'{hdfs_directory}', f'{table}-{time_file}.csv')) as csv_file:
        cursor.copy_expert(f"COPY public.{table} TO STDOUT with header CSV", csv_file)


if __name__ == '__main__':
    ld_hadoop()

# if __name__ == '__main__':
#     ld_hadoop(table='departments', postgres_conn_id='my_postgres',
#               hdfs_path='/my_folder/', hadoop_server='http://127.0.0.1:50070', user='user')
