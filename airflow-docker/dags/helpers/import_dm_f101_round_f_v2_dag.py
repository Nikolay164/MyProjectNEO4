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

def import_csv_to_table():
    """Импорт данных из CSV в dm.dm_f101_round_f_v2"""
    input_path = '/opt/airflow/data/dm_f101_round_f_export.csv'
    process_name = "IMPORT_dm.dm_f101_round_f_v2"
    
    try:
        log_to_db(process_name, "STARTED")
        
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Файл {input_path} не найден")
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f_v2 (
                        from_date DATE,
                        to_date DATE,
                        chapter CHAR(1),
                        ledger_account CHAR(5),
                        characteristic CHAR(1),
                        balance_in_rub NUMERIC(23,8),
                        r_balance_in_rub NUMERIC(23,8),
                        balance_in_val NUMERIC(23,8),
                        r_balance_in_val NUMERIC(23,8),
                        balance_in_total NUMERIC(23,8),
                        r_balance_in_total NUMERIC(23,8),
                        turn_deb_rub NUMERIC(23,8),
                        r_turn_deb_rub NUMERIC(23,8),
                        turn_deb_val NUMERIC(23,8),
                        r_turn_deb_val NUMERIC(23,8),
                        turn_deb_total NUMERIC(23,8),
                        r_turn_deb_total NUMERIC(23,8),
                        turn_cre_rub NUMERIC(23,8),
                        r_turn_cre_rub NUMERIC(23,8),
                        turn_cre_val NUMERIC(23,8),
                        r_turn_cre_val NUMERIC(23,8),
                        turn_cre_total NUMERIC(23,8),
                        r_turn_cre_total NUMERIC(23,8),
                        balance_out_rub NUMERIC(23,8),
                        r_balance_out_rub NUMERIC(23,8),
                        balance_out_val NUMERIC(23,8),
                        r_balance_out_val NUMERIC(23,8),
                        balance_out_total NUMERIC(23,8),
                        r_balance_out_total NUMERIC(23,8)
                    )
                """)
                
                cursor.execute("TRUNCATE TABLE dm.dm_f101_round_f_v2")
                
                with codecs.open(input_path, 'r', encoding='utf-8-sig') as csvfile:
                    reader = csv.reader(csvfile, delimiter=';')
                    header = next(reader)  
                    
                    sql = """
                        INSERT INTO dm.dm_f101_round_f_v2 VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                            %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """
                    
                    batch_size = 1000
                    batch = []
                    rows_processed = 0
                    
                    for row in reader:
                        
                        try:
                            processed_row = [
                                None if field.strip() == '' else field
                                for field in row
                            ]
                            
                            processed_row[0] = datetime.strptime(processed_row[0], '%Y-%m-%d').date() if processed_row[0] else None
                            processed_row[1] = datetime.strptime(processed_row[1], '%Y-%m-%d').date() if processed_row[1] else None
                            
                            batch.append(processed_row)
                            
                            if len(batch) >= batch_size:
                                cursor.executemany(sql, batch)
                                rows_processed += len(batch)
                                batch = []
                                
                        except Exception as e:
                            print(f"Ошибка обработки строки {row}: {str(e)}")
                            continue
                    
                    if batch:
                        cursor.executemany(sql, batch)
                        rows_processed += len(batch)
                
                conn.commit()
        
        log_to_db(process_name, "SUCCESS", rows_processed)
        print(f"Успешно импортировано {rows_processed} строк")
        return rows_processed
        
    except Exception as e:
        log_to_db(process_name, "FAILED", 0, str(e))
        print(f"Ошибка импорта: {str(e)}")
        raise

with DAG(
    'import_dm_f101_round_f_v2',
    default_args=default_args,
    description='Импорт данных в dm.dm_f101_round_f_v2 из CSV',
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['finance', 'dm', 'import'],
) as dag:

    start = EmptyOperator(task_id='start')
    
    import_task = PythonOperator(
        task_id='import_from_csv',
        python_callable=import_csv_to_table,
    )

    end = EmptyOperator(task_id='end')

    start >> import_task >> end