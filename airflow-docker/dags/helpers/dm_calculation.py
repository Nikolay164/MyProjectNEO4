from datetime import datetime, date
from airflow.exceptions import AirflowException
from helpers.etl_utils import get_db_connection

def log_to_db(process_name, status, rows_processed=0, error_message=None):
    """Логирование результатов выполнения в БД"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO logs.etl_logs 
                    (process_name, start_time, end_time, status, rows_processed, error_message)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, (
                    process_name,
                    datetime.now(),
                    datetime.now(),
                    status,
                    rows_processed,
                    error_message
                ))
                conn.commit()
    except Exception as e:
        raise AirflowException(f"Ошибка при логировании: {e}")

def _execute_procedure(proc_name, param=None):
    """Универсальная функция выполнения процедур с логированием"""
    start_time = datetime.now()
    rows_affected = 0
    
    try:
        log_to_db(proc_name, 'STARTED')
        
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                if param:
                    cursor.execute(f"CALL {proc_name}(%s)", (param,))
                else:
                    cursor.execute(f"CALL {proc_name}()")
                rows_affected = cursor.rowcount
                conn.commit()
        
        log_to_db(proc_name, "SUCCESS", rows_processed=rows_affected)
        return rows_affected
        
    except Exception as e:
        log_to_db(proc_name, "ERROR", error_message=str(e))
        raise AirflowException(f"Ошибка выполнения {proc_name}: {str(e)}")

def init_balances():
    """Инициализация остатков на 31.12.2017"""
    return _execute_procedure("ds.init_account_balance_20171231")

def calculate_dm_tables_for_january_2018():
    """Расчет витрин за январь 2018 по дням"""
    for day in range(1, 32):
        current_date = date(2018, 1, day)
        try:
            _execute_procedure("ds.fill_account_turnover_f", current_date)
            _execute_procedure("ds.fill_account_balance_f", current_date)
        except Exception as e:
            print(f"Пропуск даты {current_date} из-за ошибки: {str(e)}")
            continue

def calculate_f101_for_january_2018():
    """Расчет формы 101 за январь 2018"""
    try:
        # Для января передаем 1 февраля как отчетную дату
        report_date = date(2018, 2, 1)
        print(f"Запуск расчета формы 101 за январь 2018 (отчетная дата: {report_date})")
        
        rows = _execute_procedure("dm.fill_f101_round_f", report_date)
        print(f"Форма 101 успешно рассчитана. Обработано записей: {rows}")
        return rows
        
    except Exception as e:
        raise AirflowException(f"Ошибка расчета формы 101: {str(e)}")