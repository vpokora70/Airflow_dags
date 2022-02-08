from datetime import datetime
from airflow.hooks.postgres_hook import PostgresHook
import os


def ld_db(**kwargs):
    end_folder = datetime.today().strftime("%Y-%m-%d-%H")
    path_to_directory = os.path.join(kwargs['directory_path'], end_folder)
    os.makedirs(path_to_directory, exist_ok=True)
    hook_db = PostgresHook(postgres_conn_id=kwargs['postgres_conn_id'])
    conn = hook_db.get_conn()
    cursor = conn.cursor()
    t = kwargs['table']
    with open(os.path.join(path_to_directory, f'{t}.csv'), mode='w', encoding="UTF=8") as csv_file:
        cursor.copy_expert(f"COPY public.{kwargs['table']} TO STDOUT with header CSV", csv_file)


if __name__ == '__main__':
    ld_db()
