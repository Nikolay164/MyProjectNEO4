from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from helpers.etl_utils import get_db_connection, log_to_db
import csv
import os
import codecs

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def export_table_to_csv():
    output_path = '/opt/airflow/data/dm_f101_round_f_export.csv'
    process_name = f"EXPORT_dm.dm_f101_round_f"
    
    try:
        log_to_db(process_name, "STARTED")
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT * FROM dm.dm_f101_round_f")
                rows = cursor.fetchall()
                colnames = [desc[0] for desc in cursor.description]

                with codecs.open(output_path, 'w', encoding='utf-8-sig') as csvfile:
                    writer = csv.writer(csvfile, delimiter=';')
                    writer.writerow(colnames)
                    writer.writerows(rows)
        
        log_to_db(process_name, "SUCCESS", len(rows))
        return output_path
        
    except Exception as e:
        log_to_db(process_name, "FAILED", 0, str(e))
        raise

with DAG(
    'export_dm_f101_round_f',
    default_args=default_args,
    description='Экспорт данных из dm.dm_f101_round_f в CSV с UTF-8 BOM',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['finance', 'dm', 'export'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    export_task = PythonOperator(
        task_id='export_to_csv',
        python_callable=export_table_to_csv,
    )

    end = EmptyOperator(task_id='end')

    start >> export_task >> end