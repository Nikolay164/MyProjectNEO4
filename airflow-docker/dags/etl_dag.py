from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from helpers.etl_utils import load_csv_smart

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def load_file(csv_path, table_name):
    """Обертка для вашей функции загрузки"""
    success = load_csv_smart(csv_path=csv_path, table_name=table_name)
    if not success:
        raise ValueError(f"Ошибка при загрузке {csv_path}")

with DAG(
    'csv_to_postgres_etl',
    default_args=default_args,
    description='Загрузка CSV файлов в PostgreSQL',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['etl', 'csv'],
) as dag:

    load_balance = PythonOperator(
        task_id='load_ft_balance_f',
        python_callable=load_file,
        op_kwargs={
            'csv_path': '/opt/airflow/data/ft_balance_f.csv',
            'table_name': 'ft_balance_f'
        }
    )

    load_posting = PythonOperator(
        task_id='load_ft_posting_f',
        python_callable=load_file,
        op_kwargs={
            'csv_path': '/opt/airflow/data/ft_posting_f.csv',
            'table_name': 'ft_posting_f'
        }
    )

    load_account = PythonOperator(
        task_id='load_md_account_d',
        python_callable=load_file,
        op_kwargs={
            'csv_path': '/opt/airflow/data/md_account_d.csv',
            'table_name': 'md_account_d'
        }
    )

    load_ledger = PythonOperator(
        task_id='load_md_ledger_account_s',
        python_callable=load_file,
        op_kwargs={
            'csv_path': '/opt/airflow/data/md_ledger_account_s.csv',
            'table_name': 'md_ledger_account_s'
        }
    )

    load_balance >> load_posting  >> load_account >> load_ledger