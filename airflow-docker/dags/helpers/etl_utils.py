import pandas as pd
import psycopg2
import os
from datetime import datetime
from airflow.exceptions import AirflowException

def get_db_connection():
    """Создает подключение к PostgreSQL используя переменные окружения Airflow"""
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST", "host.docker.internal"),
            database=os.getenv("DB_NAME", "postgres"),
            user=os.getenv("DB_USER", "ds"),
            password=os.getenv("DB_PASSWORD", "13245678"),
            port=os.getenv("DB_PORT", "5432")
        )
        return conn
    except Exception as e:
        raise AirflowException(f"Ошибка подключения к БД: {e}")

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

def get_table_columns(table_name, schema="ds"):
    """Получение списка колонок таблицы"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = '{schema}' 
                    AND table_name = '{table_name}'
                    ORDER BY ordinal_position
                """)
                return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        raise AirflowException(f"Ошибка при получении колонок таблицы: {e}")

def has_primary_key(table_name, schema="ds"):
    """Проверяет наличие первичного ключа у таблицы"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(f"""
                    SELECT COUNT(*)
                    FROM pg_constraint c
                    JOIN pg_class t ON c.conrelid = t.oid
                    JOIN pg_namespace n ON t.relnamespace = n.oid
                    WHERE n.nspname = '{schema}'
                    AND t.relname = '{table_name}'
                    AND c.contype = 'p'
                """)
                return cursor.fetchone()[0] > 0
    except Exception as e:
        raise AirflowException(f"Ошибка при проверке первичного ключа: {e}")

def load_with_upsert(df, table_name, schema="ds"):
    """Загрузка данных с использованием UPSERT (INSERT ON CONFLICT UPDATE)"""
    process_name = f"LOAD_{schema}.{table_name}"
    log_to_db(process_name, "STARTED")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Получаем первичный ключ таблицы
                cursor.execute(f"""
                    SELECT a.attname
                    FROM pg_index i
                    JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
                    WHERE i.indrelid = '{schema}.{table_name}'::regclass
                    AND i.indisprimary
                """)
                primary_keys = [row[0] for row in cursor.fetchall()]
                
                if not primary_keys:
                    raise ValueError("Не найден первичный ключ таблицы")
                
                # Формируем запрос UPSERT
                columns = ", ".join([f'"{col}"' for col in df.columns])
                placeholders = ", ".join(["%s"] * len(df.columns))
                update_set = ", ".join([
                    f'"{col}" = EXCLUDED."{col}"' 
                    for col in df.columns 
                    if col not in primary_keys
                ])
                conflict_target = ", ".join([f'"{pk}"' for pk in primary_keys])
                
                query = f"""
                    INSERT INTO {schema}.{table_name} ({columns})
                    VALUES ({placeholders})
                    ON CONFLICT ({conflict_target}) 
                    DO UPDATE SET {update_set}
                """
                
                # Выполняем UPSERT
                data = [tuple(row) for row in df.values]
                cursor.executemany(query, data)
                conn.commit()
        
        log_to_db(process_name, "SUCCESS", len(df))       
        return True
        
    except Exception as e:
        error_msg = f"Ошибка при UPSERT: {str(e)}"
        log_to_db(process_name, "FAILED", 0, error_msg)
        raise AirflowException(error_msg)

def load_with_truncate(df, table_name, schema="ds"):
    """Полная перезапись таблицы (TRUNCATE + INSERT)"""
    process_name = f"LOAD_{schema}.{table_name}"
    log_to_db(process_name, "STARTED")
    
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                # Очищаем таблицу
                cursor.execute(f"TRUNCATE TABLE {schema}.{table_name}")
                
                # Вставляем данные
                columns = ", ".join([f'"{col}"' for col in df.columns])
                placeholders = ", ".join(["%s"] * len(df.columns))
                query = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({placeholders})"
                data = [tuple(row) for row in df.values]
                cursor.executemany(query, data)
                conn.commit()
        
        log_to_db(process_name, "SUCCESS", len(df))        
        return True
        
    except Exception as e:
        error_msg = f"Ошибка при полной перезаписи: {str(e)}"
        log_to_db(process_name, "FAILED", 0, error_msg)
        raise AirflowException(error_msg)

def load_csv_smart(csv_path, table_name, schema="ds"):
    """Основная функция загрузки CSV в таблицу"""
    print(f"\nЗагрузка файла: {csv_path} в таблицу {schema}.{table_name}")
    process_name = f"LOAD_{schema}.{table_name}"
    
    try:
        # 1. Проверка существования файла
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Файл {csv_path} не найден")
        
        # 2. Чтение CSV
        df = pd.read_csv(csv_path, sep=";", encoding='utf-8')
        
        # 3. Получаем колонки таблицы в БД
        db_columns = get_table_columns(table_name, schema)
        if not db_columns:
            raise ValueError(f"Не удалось получить колонки таблицы {schema}.{table_name}")
        
        # 4. Фильтруем колонки DF, оставляя только существующие в БД
        df_columns = [col for col in df.columns if col.lower() in [c.lower() for c in db_columns]]
        if not df_columns:
            raise ValueError("Нет совпадающих колонок между CSV и таблицей БД")
        
        df = df[df_columns]
        
        # 5. Приводим названия колонок к виду как в БД
        db_columns_lower = {c.lower(): c for c in db_columns}
        df.columns = [db_columns_lower[col.lower()] for col in df.columns]
        
        # 6. Выбираем стратегию загрузки
        if has_primary_key(table_name, schema):
            return load_with_upsert(df, table_name, schema)
        else:
            return load_with_truncate(df, table_name, schema)
            
    except Exception as e:
        error_msg = f"Ошибка при загрузке: {str(e)}"
        log_to_db(process_name, "FAILED", 0, error_msg)
        raise AirflowException(error_msg)