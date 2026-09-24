from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
from airflow.models import Variable
from airflow.providers.telegram.operators.telegram import TelegramOperator

# Импорты для работы с базами данных
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow_clickhouse_plugin.hooks.clickhouse import ClickHouseHook
from sqlalchemy import create_engine

# Импорты для обработки ошибок
from airflow.exceptions import AirflowException
import time
from sqlalchemy.exc import OperationalError, DatabaseError
from clickhouse_driver.errors import Error as ClickHouseError
from mysql.connector.errors import Error as MySqlError
import socket

# Импорты для работы с данными и системой
import pyarrow.parquet as pq
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import pandas as pd
import os
import gc
import logging
import numpy as np

# Импорты для типизации
from typing import List, Tuple, Optional, Any
from types import TracebackType

# Настройка логирования
logger = logging.getLogger(__name__)

# Константы для повторных попыток
MAX_RETRIES = 3
RETRY_DELAY = 30  # секунды
CONNECTION_TIMEOUT = 300  # секунды

# Идентификаторы подключений в Airflow
MARIADB_CONN_ID = 'pp'
CLICKHOUSE_CONN_ID = 'click_house_work'

# Настройки таблиц и файлов
TABLE_NAME = 'pp_Order_Sales'
EXPORT_PATH = '/tmp/pp_Order_Sales.parquet'
DAYS_WINDOW = 30 # "окно" оновления к-во дней

# SQL запрос
SQL_QUERY_ALL = f"""
WITH sales_certificate AS (
    SELECT 
        s.taviat_sale_order_id,
        s.type,
        s.company_name,
        s.sbis_created_at,
        s.sale_seller,
        CONCAT(e.last_name, ' ', e.first_name, ' ', e.patronymic) as Emp_Name,
        sotp.nomenclature_id,
        n.name,
        n.folder,
        sotp.quantity,
        sotp.unit_name,
        sotp.unit_price,
        sotp.total_price,
        s.sale_total_price,
        s.pay_certificate,
        COUNT(sotp.nomenclature_id) OVER (PARTITION BY s.taviat_sale_order_id) as count_position,
        sotp.total_discount,
        n2.name as nomenclature_group
    FROM 
        sale_orders s 
    LEFT JOIN 
        sale_order_tabular_parts sotp ON 
        s.taviat_sale_order_id = sotp.taviat_sale_order_id
    LEFT JOIN 
        nomenclatures n ON 
        sotp.nomenclature_id = n.nomenclature_id 
    LEFT JOIN 
        employees e ON 
        s.sale_seller = e.employee_id 
    LEFT JOIN 
        nomenclatures n2 ON 
        n.root_folder_id = n2.nomenclature_id
    WHERE 
        sotp.is_return = 0 
        AND sotp.is_return_sn = 0 
        AND s.pay_certificate > 0
        AND s.sbis_created_at >= NOW() - INTERVAL {DAYS_WINDOW} DAY
)
SELECT 
    s.taviat_sale_order_id,
    s.type,
    s.company_name,
    s.sbis_created_at,
    s.sale_seller,
    CONCAT(e.last_name, ' ', e.first_name, ' ', e.patronymic) as Emp_Name,
    sotp.nomenclature_id,
    n.name,
    n.folder,
    sotp.quantity,
    sotp.unit_name,
    sotp.unit_price,
    sotp.total_price,
    sotp.total_discount,
    n2.name as nomenclature_group
FROM 
    sale_orders s 
LEFT JOIN 
    sale_order_tabular_parts sotp ON 
    s.taviat_sale_order_id = sotp.taviat_sale_order_id
LEFT JOIN 
    nomenclatures n ON 
    sotp.nomenclature_id = n.nomenclature_id 
LEFT JOIN 
    employees e ON 
    s.sale_seller = e.employee_id 
LEFT JOIN 
    nomenclatures n2 ON 
    n.root_folder_id = n2.nomenclature_id
WHERE 
    sotp.is_return = 0 
    AND sotp.is_return_sn = 0 
    AND s.pay_certificate = 0 
    AND s.sbis_created_at >= NOW() - INTERVAL {DAYS_WINDOW} DAY
UNION ALL 
SELECT 
    s.taviat_sale_order_id,
    s.type,
    s.company_name,
    s.sbis_created_at,
    s.sale_seller,
    s.Emp_Name,
    s.nomenclature_id,
    s.name,
    s.folder,
    s.quantity,
    s.unit_name,
    s.unit_price,
    s.total_price - s.pay_certificate / s.count_position as total_price,
    s.total_discount,
    s.nomenclature_group
FROM 
    sales_certificate s
"""

# Декоратор для повторных попыток с экспоненциальной задержкой
def retry_with_backoff(
    max_retries: int = MAX_RETRIES,
    retry_delay: int = RETRY_DELAY,
    exceptions: tuple = (
        OperationalError, 
        DatabaseError, 
        ClickHouseError, 
        MySqlError,
        ConnectionError,
        TimeoutError,
        socket.timeout,
        socket.error
    )
):
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    if attempt < max_retries:
                        wait_time = retry_delay * (2 ** attempt)  # Экспоненциальная задержка
                        logger.warning(
                            f"Attempt {attempt + 1}/{max_retries} failed for {func.__name__}. "
                            f"Error: {str(e)}. Retrying in {wait_time} seconds..."
                        )
                        time.sleep(wait_time)
                    else:
                        logger.error(
                            f"All {max_retries} attempts failed for {func.__name__}. "
                            f"Last error: {str(e)}"
                        )
                        raise AirflowException(f"Operation failed after {max_retries} retries: {str(e)}") from e
                except Exception as e:
                    # Нет повторных попыток для непредвиденных ошибок
                    logger.error(f"Unexpected error in {func.__name__}: {str(e)}")
                    raise AirflowException(f"Unexpected error: {str(e)}") from e
            return None
        return wrapper
    return decorator

# Класс для управления подключениями с обработкой ошибок
class ConnectionManager:
    def __init__(self):
        self._mysql_hook = None
        self._ch_hook = None
        self._mysql_engine = None
        self._connection_attempts = 0
        self._max_connection_attempts = 3
    
    # Получение хука MySQL с повторными попытками
    @retry_with_backoff(max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY)
    def get_mysql_hook(self) -> MySqlHook:
        if self._mysql_hook is None:
            logger.info("Creating MySQL hook connection")
            self._mysql_hook = MySqlHook(mysql_conn_id=MARIADB_CONN_ID)
            logger.info("MySQL hook created successfully")
        return self._mysql_hook
    
    # Получение хука ClickHouse с повторными попытками
    @retry_with_backoff(max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY)
    def get_clickhouse_hook(self) -> ClickHouseHook:
        if self._ch_hook is None:
            logger.info("Creating ClickHouse hook connection")
            self._ch_hook = ClickHouseHook(clickhouse_conn_id=CLICKHOUSE_CONN_ID)
            # ПРОСТОЕ ТЕСТИРОВАНИЕ БЕЗ ПАРАМЕТРОВ
            client = self._ch_hook.get_conn()
            client.execute("SELECT 1")
            logger.info("ClickHouse connection tested successfully")
        return self._ch_hook
    
    # Получение SQLAlchemy engine с настройками для устойчивости
    @retry_with_backoff(max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY)
    def get_mysql_engine(self):
        if self._mysql_engine is None:
            mysql_hook = self.get_mysql_hook()
            conn = mysql_hook.get_connection(mysql_hook.mysql_conn_id)
            connection_string = f"mysql+mysqldb://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
            
            self._mysql_engine = create_engine(
                connection_string,
                pool_recycle=300,      # Уменьшаем до 5 минут для частого переподключения
                pool_pre_ping=True,   # ОБЯЗАТЕЛЬНО - проверять перед использованием проверять "живое" ли соединение
                pool_size=5,          # Базовая размерность пула 
                max_overflow=10,       # Максимум дополнительных соединений
                pool_timeout=30,       # Таймаут ожидания свободного соединения
                pool_reset_on_return='rollback',  # Добавляем сброс при возврате в пул
                connect_args={
                    'connect_timeout': CONNECTION_TIMEOUT,
                    'read_timeout': 300,           # Таймаут на чтение
                    'write_timeout': 300,          # Таймаут на запись
                },
                # Уменьшаем логирование для производительности
                echo_pool=False,
            )
            logger.info("SQLAlchemy engine created with improved settings")
        return self._mysql_engine
    
    # Принудительное обновление engine
    def refresh_mysql_engine(self):
        if self._mysql_engine:
            self._mysql_engine.dispose()
            self._mysql_engine = None
            logger.info("MySQL engine refreshed")
        return self.get_mysql_engine()
    
    # Закрытие всех соединений
    def close_connections(self):
        try:
            if self._mysql_engine:
                self._mysql_engine.dispose()
                logger.info("MySQL engine connections closed")
        except Exception as e:
            logger.warning(f"Error closing MySQL engine: {e}")
            
# Контекстный менеджер для безопасной работы с соединениями
class DatabaseConnectionContext:
    def __init__(self):
        self.connection_manager = ConnectionManager()
    
    def __enter__(self):
        return self.connection_manager
    
    def __exit__(self, exc_type: Optional[type], exc_val: Optional[Exception], exc_tb: Optional[TracebackType]) -> bool:
        self.connection_manager.close_connections()
        if exc_type is not None:
            logger.error(f"Database operation failed: {exc_val}")
        return False  # Пропускаем исключение дальше
            
# Оптимизированное преобразование типов с экономией памяти
def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting data type conversion for sales data")

    # Создаем копию для безопасной модификации
    df_converted = df.copy()
    
    # Обрабатываем дату
    if 'sbis_created_at' in df_converted.columns:
        df_converted['sbis_created_at'] = pd.to_datetime(df_converted['sbis_created_at'])
    
    # Преобразуем типы согласно структуре источника
    # VARCHAR/TEXT колонки
    string_cols = [
        'taviat_sale_order_id',  # varchar(100)
        'type',                  # varchar(255)
        'company_name',          # varchar(255)
        'Emp_Name',              # CONCAT результат
        'name',                  # text
        'nomenclature_group',    # text
        'unit_name'              # varchar(255)
    ]
    
    for col in string_cols:
        if col in df_converted.columns:
            # Заменяем NaN на пустую строку и конвертируем в str
            df_converted[col] = df_converted[col].fillna('').astype(str)
    
    # Целочисленные колонки
    int_cols = {
        'sale_seller': 'UInt64',        # bigint(20) unsigned
        'nomenclature_id': 'UInt64',    # bigint(20)
        'folder': 'UInt32'              # int(11)
    }
    
    for col, dtype in int_cols.items():
        if col in df_converted.columns:
            # Конвертируем в целые числа, заменяя NaN на 0
            df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce').fillna(0)
            if dtype == 'UInt64':
                df_converted[col] = df_converted[col].astype(np.uint64)
            elif dtype == 'UInt32':
                df_converted[col] = df_converted[col].astype(np.uint32)

    
    # Десятичные колонки (DECIMAL)
    decimal_cols = [
        'quantity',        # decimal(16,4)
        'unit_price',      # decimal(16,2)
        'total_price',     # decimal(16,2)
        'total_discount'   # decimal(16,2)
    ]
    
    for col in decimal_cols:
        if col in df_converted.columns:
            # Используем Decimal для точности
            try:
                df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce')
                # Округляем до 4 знаков для quantity и 2 знаков для остальных
                if col == 'quantity':
                    df_converted[col] = df_converted[col].round(4)
                else:
                    df_converted[col] = df_converted[col].round(2)
                # Преобразуем в float64 для ClickHouse
                df_converted[col] = df_converted[col].astype('float64')
            except Exception as e:
                logger.warning(f"Error converting {col} to decimal: {e}")
                df_converted[col] = 0.0
    
    logger.info("Optimized data type conversion completed")
    return df_converted

# Подготовка данных для вставки с оптимизацией памяти
def prepare_data_for_insert(df: pd.DataFrame, load_batch_id: str = '') -> List[Tuple]:
    data = []
    processed_rows = 0
    skipped_rows = 0

    for _, row in df.iterrows():
        try:
            # Проверяем наличие необходимых колонок
            required_cols = ['taviat_sale_order_id', 'type', 'company_name', 
                           'sbis_created_at', 'sale_seller', 'Emp_Name',
                           'nomenclature_id', 'name', 'folder', 'quantity',
                           'unit_name', 'unit_price', 'total_price', 
                           'total_discount', 'nomenclature_group']
            
            # Проверяем, что все необходимые колонки присутствуют
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"Missing columns in row: {missing_cols}")
                skipped_rows += 1
                continue
            
            # Проверяем ключевые поля
            if pd.isna(row['taviat_sale_order_id']) or pd.isna(row['nomenclature_id']):
                logger.warning(f"Missing key fields in row")
                skipped_rows += 1
                continue
            
            # Преобразуем данные в правильные типы с обработкой NaN
            item = (
                str(row['taviat_sale_order_id']) if pd.notna(row['taviat_sale_order_id']) else '',
                str(row['type']) if pd.notna(row['type']) else '',
                str(row['company_name']) if pd.notna(row['company_name']) else '',
                row['sbis_created_at'] if pd.notna(row['sbis_created_at']) else None,
                int(row['sale_seller']) if pd.notna(row['sale_seller']) else 0,
                str(row['Emp_Name']) if pd.notna(row['Emp_Name']) else '',
                int(row['nomenclature_id']) if pd.notna(row['nomenclature_id']) else 0,
                str(row['name']) if pd.notna(row['name']) else '',
                int(row['folder']) if pd.notna(row['folder']) else 0,
                float(row['quantity']) if pd.notna(row['quantity']) else 0.0,
                str(row['unit_name']) if pd.notna(row['unit_name']) else '',
                float(row['unit_price']) if pd.notna(row['unit_price']) else 0.0,
                float(row['total_price']) if pd.notna(row['total_price']) else 0.0,
                float(row['total_discount']) if pd.notna(row['total_discount']) else 0.0,
                str(row['nomenclature_group']) if pd.notna(row['nomenclature_group']) else '',
                load_batch_id  
            )
            data.append(item)
            processed_rows += 1

        except Exception as e:
            logger.warning(f"Error preparing row for insert: {e}")
            skipped_rows += 1
            continue
    
    if skipped_rows > 0:
        logger.warning(f"Skipped {skipped_rows} rows during preparation")
    
    logger.info(f"Prepared {processed_rows} rows for insertion")
    return data

# Проверяем объем данных перед полной синхронизацией
def check_data_volume(**kwargs):
    try:
        with DatabaseConnectionContext() as db:
            engine = db.get_mysql_engine()
            
            # Упрощенный запрос для проверки объема
            count_query = """
            SELECT COUNT(DISTINCT s.taviat_sale_order_id) as total_count 
            FROM sale_orders s
            JOIN sale_order_tabular_parts sotp ON s.taviat_sale_order_id = sotp.taviat_sale_order_id
            WHERE sotp.is_return = 0 AND sotp.is_return_sn = 0
            """
            result = pd.read_sql(count_query, con=engine)
            total_count = result.iloc[0]['total_count']
            
            logger.info(f"Total unique sale orders in source: {total_count}")
            
            if total_count > 1000000:  # 1 млн заказов
                logger.warning(f"Large dataset detected: {total_count} sale orders. Consider optimizing.")
            
            ti = kwargs['ti']
            volume_info = {
                'total_count': total_count,
                'warning_level': 'CRITICAL' if total_count > 1000000 else 'WARNING' if total_count > 500000 else 'NORMAL',
                'checked_at': datetime.now(timezone.utc).isoformat()
            }
            ti.xcom_push(key='data_volume_info', value=volume_info)
            
            return volume_info
            
    except Exception as e:
        logger.error(f"Error checking data volume: {str(e)}")
        return None
    
# Обрабатываем чанки итеративно для экономии памяти
def process_chunks_iteratively(all_chunks, export_path):
    logger.info("Processing chunks iteratively to save memory")
    
    if not all_chunks:
        return pd.DataFrame()
    
    # Записываем первый чанк
    try:
        all_chunks[0].to_parquet(export_path, index=False, engine='pyarrow')
    except Exception as e:
        # Если не поддерживается append для pyarrow, используем fastparquet
        logger.warning(f"PyArrow append failed, using alternative method: {e}")
        all_chunks[0].to_parquet(export_path, index=False)
    
    # Для остальных чанков дописываем в существующий файл
    for i, chunk in enumerate(all_chunks[1:], 2):
        try:
            # Пытаемся использовать append mode
            try:
                chunk.to_parquet(
                    export_path, 
                    index=False,
                    engine='pyarrow',
                    append=True
                )
            except Exception as append_error:
                logger.warning(f"Append mode failed: {append_error}")
                # Альтернативный подход для форматов без поддержки append
                temp_path = f"{export_path}.temp_{i}"
                chunk.to_parquet(temp_path, index=False)
                
                # Читаем оба файла и объединяем
                existing = pd.read_parquet(export_path)
                new_chunk = pd.read_parquet(temp_path)
                combined = pd.concat([existing, new_chunk], ignore_index=True)
                combined.to_parquet(export_path, index=False)
                
                # Удаляем временный файл
                if os.path.exists(temp_path):
                    os.remove(temp_path)
                del existing, new_chunk, combined
                gc.collect()

            if i % 5 == 0:
                logger.info(f"Progress: processed {i} chunks")
                
        except MemoryError as mem_error:
            logger.error(f"Memory error processing chunk {i}: {mem_error}")
            # Попробуем уменьшить размер чанка
            if len(chunk) > 10000:
                logger.info("Splitting large chunk...")
                # Разбиваем большой чанк на части
                split_size = len(chunk) // 2
                for j in range(0, len(chunk), split_size):
                    sub_chunk = chunk.iloc[j:j+split_size]
                    if not sub_chunk.empty:
                        sub_chunk.to_parquet(
                            export_path,
                            index=False,
                            engine='pyarrow',
                            append=True if j > 0 or i > 2 else False
                        )
            else:
                raise
                
        except Exception as e:
            logger.error(f"Error processing chunk {i}: {e}")
            raise
    
    return pd.read_parquet(export_path)

# Получение данных из MariaDB
@retry_with_backoff()
def extract_from_mariadb(**kwargs):
    try:
        max_retries=MAX_RETRIES
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        export_path = f"/tmp/sales_data_{execution_date.strftime('%Y%m%d_%H%M%S')}.parquet"
        
        logger.info("Starting sales data extraction from MariaDB")

        with DatabaseConnectionContext() as db:
            engine = db.get_mysql_engine()

            volume_info = ti.xcom_pull(task_ids='check_data_volume', key='data_volume_info')
        
            if volume_info:
                warning_level = volume_info.get('warning_level', 'NORMAL')
                
                # Адаптивные настройки
                if warning_level in ["CRITICAL", "WARNING"]:
                    chunk_size = 25000  # Меньше чанки для сложных запросов
                    logger.info(f"Using reduced chunk size {chunk_size} for large dataset")
                else:
                    chunk_size = 50000
            else:
                chunk_size = 50000
            
            logger.info(f"Using chunk size {chunk_size}") 
            all_chunks = []
            chunk_number = 0
            total_rows = 0
            
            # Улучшенная обработка чанков с проверкой соединения
            for chunk_df in pd.read_sql(SQL_QUERY_ALL, con=engine, chunksize=chunk_size):
                chunk_number += 1
                
                try:
                    # Проверяем соединение перед обработкой чанка
                    with engine.connect() as test_conn:
                        test_conn.execute("SELECT 1")
                    
                    if chunk_df.empty:
                        logger.info(f"Chunk {chunk_number} is empty, skipping")
                        continue
                    
                    logger.info(f"Processing chunk {chunk_number} with {len(chunk_df)} rows")
                                            
                    # Конвертируем типы данных в чанке
                    chunk_df = convert_data_types(chunk_df)
                    all_chunks.append(chunk_df)
                    total_rows += len(chunk_df)
                    
                    # Периодически сохраняем прогресс для больших датасетов
                    if chunk_number % 10 == 0:
                        logger.info(f"Progress: processed {chunk_number} chunks, {total_rows} total rows")
                        # Сохраняем промежуточные результаты
                        temp_export_path = f"{export_path}.part_{chunk_number}"
                        chunk_df.to_parquet(temp_export_path, index=False, compression='snappy')
                    
                    # Освобождаем память
                    del chunk_df
                    gc.collect()
                    
                except (OperationalError, DatabaseError) as e:
                    if chunk_number >= max_retries:  # Добавить ограничение
                        raise
                    logger.warning(f"Database error in chunk {chunk_number}: {str(e)}")
                    logger.info("Attempting to refresh database connection...")
                    
                    # Закрываем текущее соединение и создаем новое
                    db.close_connections()
                    engine = db.get_mysql_engine()
                    
                    logger.info("Database connection refreshed, continuing...")
                    continue
                    
                except Exception as e:
                    logger.error(f"Unexpected error in chunk {chunk_number}: {str(e)}")
                    raise
            
            if not all_chunks:
                logger.warning("No data found in sales tables")
                ti.xcom_push(key='mariadb_data', value=None)
                ti.xcom_push(key='has_new_data', value=False)
                return
            
            logger.info(f"Combining {len(all_chunks)} chunks")
            
            # Улучшенное объединение чанков с обработкой памяти
            try:
                df_mariadb = pd.concat(all_chunks, ignore_index=True)
                logger.info(f"Successfully combined chunks. Total rows: {len(df_mariadb)}")
            except MemoryError:
                logger.warning("Memory error during concat, using iterative processing")
                # Альтернативный подход для больших датасетов
                df_mariadb = process_chunks_iteratively(all_chunks, export_path)
                all_chunks.clear()  # Освобождаем память
            
            logger.info(f"Extracted {len(df_mariadb)} sales rows from MariaDB in {chunk_number} chunks")
            
            # Сохраняем в файл с обработкой ошибок
            try:
                df_mariadb.to_parquet(export_path, index=False, compression='snappy')
                logger.info(f"Data saved to {export_path}")
            except Exception as e:
                logger.error(f"Error saving to parquet: {str(e)}")
                # Пробуем альтернативный формат
                csv_path = export_path.replace('.parquet', '.csv')
                df_mariadb.to_csv(csv_path, index=False)
                export_path = csv_path 
                logger.info(f"Data saved to CSV instead: {export_path}")
            
            # Отправляем в xcom информацию о местоположении файлов
            ti.xcom_push(key='mariadb_data', value=export_path)
            ti.xcom_push(key='has_new_data', value=True)
            ti.xcom_push(key='total_rows_extracted', value=len(df_mariadb))
            
            logger.info(f"Sales data prepared successfully.")
            
            # Освобождаем память
            del df_mariadb
            del all_chunks
            gc.collect()
            
    except Exception as e:
        logger.error(f"Error in extract_from_mariadb: {str(e)}", exc_info=True)
        raise AirflowException(f"Extraction from MariaDB failed: {str(e)}")

# Функция для очистки логов и временных файлов ClickHouse
def cleanup_clickhouse_temp_files(**kwargs):
    try:
        logger.info("Starting ClickHouse temporary files cleanup via remote SQL")
        
        with DatabaseConnectionContext() as db:
            ch_hook = db.get_clickhouse_hook()
            client = ch_hook.get_conn()
            
            # 1. Останавливаем текущие мутации (если есть проблемы с местом)
            try:
                stop_mutations_sql = """
                KILL MUTATION 
                WHERE database = currentDatabase() 
                AND table = '{0}'
                AND command LIKE '%DELETE%'
                """.format(TABLE_NAME)
                result = client.execute(stop_mutations_sql)
                if result:
                    logger.info(f"Stopped {len(result)} running DELETE mutations")
                else:
                    logger.info("No running DELETE mutations found")
            except Exception as e:
                logger.warning(f"Could not stop mutations: {str(e)}")
            
            # 2. Очищаем системные кеши для освобождения памяти
            try:
                client.execute("SYSTEM DROP MARK CACHE")
                client.execute("SYSTEM DROP UNCOMPRESSED CACHE")
                client.execute("SYSTEM DROP COMPILED EXPRESSION CACHE")
                logger.info("Cleaned system caches")
            except Exception as e:
                logger.warning(f"Could not clean system caches: {str(e)}")
            
            # 3. Проверяем свободное место на удаленном сервере
            try:
                disk_space_sql = """
                SELECT 
                    name,
                    free_space as free_bytes,
                    free_space / 1024 / 1024 / 1024 as free_gb,
                    total_space / 1024 / 1024 / 1024 as total_gb
                FROM system.disks
                """
                disk_info = client.execute(disk_space_sql)
                
                for disk in disk_info:
                    disk_name, free_bytes, free_gb, total_gb = disk
                    logger.info(f"Disk {disk_name}: {free_gb:.2f} GB free of {total_gb:.2f} GB total")
                    
                    if free_gb < 1.0:  # Меньше 1 GB свободного места
                        logger.error(f"CRITICAL: Low disk space on {disk_name}: {free_gb:.2f} GB")
                        
            except Exception as e:
                logger.warning(f"Could not check disk space: {str(e)}")
            
            # 4. Проверяем и отменяем зависшие мутации
            try:
                check_mutations_sql = f"""
                SELECT 
                    mutation_id,
                    command,
                    create_time,
                    now() - create_time as running_time
                FROM system.mutations 
                WHERE database = currentDatabase() 
                AND table = '{TABLE_NAME}'
                AND is_done = 0
                AND (now() - create_time) > 3600
                """
                stuck_mutations = client.execute(check_mutations_sql)
                
                if stuck_mutations:
                    logger.warning(f"Found {len(stuck_mutations)} stuck mutations")
                    for mutation in stuck_mutations:
                        mutation_id, command, create_time, running_time = mutation
                        logger.warning(f"Stuck mutation: {mutation_id}, running for {running_time} seconds")
                        
                        # Пытаемся отменить зависшую мутацию
                        try:
                            kill_mutation_sql = f"KILL MUTATION WHERE mutation_id = '{mutation_id}'"
                            client.execute(kill_mutation_sql)
                            logger.info(f"Killed stuck mutation: {mutation_id}")
                        except Exception as kill_error:
                            logger.warning(f"Could not kill mutation {mutation_id}: {str(kill_error)}")
                else:
                    logger.info("No stuck mutations found")
                
            except Exception as e:
                logger.warning(f"Could not check mutations: {str(e)}")
        
        logger.info("Remote ClickHouse cleanup completed successfully")
        
    except Exception as e:
        logger.error(f"Error in cleanup_clickhouse_temp_files: {str(e)}")
        # Все равно очищаем локальные файлы
        cleanup_file(**kwargs)

# Удаление "окна" записей в 30 последних дней
def delete_window_in_clickhouse(**kwargs):

    with DatabaseConnectionContext() as db:
        ch_hook = db.get_clickhouse_hook()
        client = ch_hook.get_conn()
        sql = f"""
        ALTER TABLE {TABLE_NAME}
        DELETE WHERE sbis_created_at >= now() - INTERVAL {DAYS_WINDOW} DAY
        """
        client.execute(sql)
    
# Загрузка новых данных в Clickhouse
@retry_with_backoff()
def load_window_to_clickhouse(**kwargs):
    try:
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        mariadb_data_path = ti.xcom_pull(key='mariadb_data', task_ids='extract_from_mariadb')
        has_new_data = ti.xcom_pull(key='has_new_data', task_ids='extract_from_mariadb')
        
        if not has_new_data or not mariadb_data_path or not os.path.isfile(mariadb_data_path):
            logger.info("No data file to load")
            return
        
        logger.info(f"Loading sales data from {mariadb_data_path} into ClickHouse")

        # Определяем формат файла и загружаем данные
        if mariadb_data_path.endswith('.csv'):
            base_reader = pd.read_csv
        else:
            base_reader = None
                    
        with DatabaseConnectionContext() as db:
            ch_hook = db.get_clickhouse_hook()
            client = ch_hook.get_conn()
            logger.info("Connected to ClickHouse successfully")
                
            check_table_sql = f"""
            SELECT COUNT() as count FROM system.tables 
            WHERE database = currentDatabase() AND name = '{TABLE_NAME}'
            """
            table_exists = client.execute(check_table_sql)[0][0] > 0
            
            if not table_exists:
                logger.info(f"Table {TABLE_NAME} does not exist, creating it")
                # Создаем таблицу с правильными типами данных
                create_table_sql = f"""
                CREATE TABLE {TABLE_NAME} (
                    taviat_sale_order_id String,        -- varchar(100)
                    type String,                       -- varchar(255)
                    company_name String,               -- varchar(255)
                    sbis_created_at DateTime,
                    sale_seller UInt64,                -- bigint(20) unsigned
                    Emp_Name String,
                    nomenclature_id UInt64,            -- bigint(20)
                    name String,                       -- text
                    folder UInt32,                     -- int(11)
                    quantity Float64,                  -- decimal(16,4)
                    unit_name String,                  -- varchar(255)
                    unit_price Float64,                -- decimal(16,2)
                    total_price Float64,               -- decimal(16,2)
                    total_discount Float64,            -- decimal(16,2)
                    nomenclature_group String,         -- text
                    created_at DateTime DEFAULT now(),
                    load_batch_id String DEFAULT ''  -- Добавляем идентификатор партии
                ) ENGINE = MergeTree()  
                PARTITION BY toYYYYMM(sbis_created_at)
                ORDER BY (taviat_sale_order_id, nomenclature_id, sbis_created_at)
                SETTINGS index_granularity = 8192
                """
                client.execute(create_table_sql)
                logger.info("Table created successfully with correct data types")

            load_batch_id = f"batch_{execution_date.strftime('%Y%m%d_%H%M%S')}"
            insert_sql = f"""
            INSERT INTO {TABLE_NAME} (
                taviat_sale_order_id, type, company_name, sbis_created_at, 
                sale_seller, Emp_Name, nomenclature_id, name, folder, quantity, 
                unit_name, unit_price, total_price, total_discount, nomenclature_group, load_batch_id
            ) VALUES
            """

            # Обрабатываем данные чанками
            chunk_size = 50000
            total_inserted = 0
            
            if mariadb_data_path.endswith('.parquet'):
                # Для Parquet файлов используем итеративное чтение
                parquet_file = pq.ParquetFile(mariadb_data_path)
                
                for batch in parquet_file.iter_batches(batch_size=chunk_size):
                    df_batch = batch.to_pandas()
                    df_batch = convert_data_types(df_batch)
                    data = prepare_data_for_insert(df_batch, load_batch_id)
                    
                    if data:
                        client.execute(insert_sql, data)
                        total_inserted += len(data)
                        logger.info(f"Inserted {len(data)} records, total: {total_inserted}")
            else:
                # Для CSV файлов используем чанки Pandas
                for chunk in base_reader(mariadb_data_path, chunksize=chunk_size):
                    # Конвертируем типы данных
                    chunk = convert_data_types(chunk)
                    data = prepare_data_for_insert(chunk, load_batch_id)
                    
                    if data:
                        client.execute(insert_sql, data)
                        total_inserted += len(data)
                        logger.info(f"Inserted {len(data)} records, total: {total_inserted}")
            
            logger.info(f"Successfully loaded {total_inserted} new sales records into ClickHouse")
            
            # Проверяем итоговое количество записей
            count_sql = f"SELECT COUNT() as total_count FROM {TABLE_NAME}"
            final_count = client.execute(count_sql)[0][0]
            logger.info(f"Total records in {TABLE_NAME}: {final_count}")
    
    except Exception as e:
        logger.error(f"Error in load_window_to_clickhouse: {str(e)}", exc_info=True)
        raise AirflowException(f"Load to ClickHouse failed: {str(e)}")

# Контроль агрегатов и алерты
def quality_check(**kwargs):
    threshold = 0.005  # 0.5%
    context = kwargs

    with DatabaseConnectionContext() as db:
        # MariaDB агрегаты
        engine = db.get_mysql_engine()
        mysql_sql = f"""
        WITH sales_certificate AS (
            SELECT 
                s.taviat_sale_order_id,
                s.sbis_created_at,
                sotp.total_price,
                s.sale_total_price,
                s.pay_certificate,
                COUNT(sotp.nomenclature_id) OVER (PARTITION BY s.taviat_sale_order_id) as count_position
            FROM sale_orders s
            JOIN sale_order_tabular_parts sotp 
            ON s.taviat_sale_order_id = sotp.taviat_sale_order_id
            WHERE sotp.is_return = 0
            AND sotp.is_return_sn = 0
            AND s.pay_certificate > 0
            AND s.sbis_created_at >= NOW() - INTERVAL {DAYS_WINDOW} DAY
        ),
        all_rows AS (
            SELECT 
                s.sbis_created_at,
                sotp.total_price
            FROM sale_orders s
            JOIN sale_order_tabular_parts sotp 
            ON s.taviat_sale_order_id = sotp.taviat_sale_order_id
            WHERE sotp.is_return = 0
            AND sotp.is_return_sn = 0
            AND s.pay_certificate = 0
            AND s.sbis_created_at >= NOW() - INTERVAL {DAYS_WINDOW} DAY

            UNION ALL

            SELECT 
                sc.sbis_created_at,
                sc.total_price - sc.pay_certificate / sc.count_position AS total_price
            FROM sales_certificate sc
        )
        SELECT
            DATE(sbis_created_at) AS d,
            SUM(total_price)      AS revenue
        FROM all_rows
        GROUP BY DATE(sbis_created_at)
        """
        df_src = pd.read_sql(mysql_sql, con=engine)

        # ClickHouse агрегаты
        ch_hook = db.get_clickhouse_hook()
        client = ch_hook.get_conn()
        ch_sql = f"""
        SELECT
            toDate(sbis_created_at) as d,
            sum(total_price) as revenue
        FROM {TABLE_NAME}
        WHERE sbis_created_at >= now() - INTERVAL {DAYS_WINDOW} DAY
        GROUP BY d
        """
        df_tgt = pd.DataFrame(client.execute(ch_sql), columns=['d','revenue'])

    df = df_src.merge(df_tgt, on='d', how='outer', suffixes=('_src','_tgt')).fillna(0)
    df['diff_abs'] = df['revenue_tgt'] - df['revenue_src']
    df['diff_rel'] = df['diff_abs'] / df['revenue_src'].replace(0, 1)

    bad = df[abs(df['diff_rel']) > threshold]
    if not bad.empty:
        logger.error(f'QUALITY ALERT: mismatch in revenue: {bad}')
        # Отправляем сообщение в Telegram через оператор
        telegram_op = TelegramOperator(
            task_id='send_telegram_quality_alert',
            telegram_conn_id='Notification_telegram',
            chat_id='-5034366843',
            text=f'⚠️ Quality check failed: revenue mismatch for {len(bad)} days:\n{bad.to_string()}',
            dag=context['dag']
        )
        telegram_op.execute(context=context)
        logger.info("Quality check failed but task completed successfully after sending alert")
        return
    else:
        logger.info('Quality check passed')

# Очистка временного файла
def cleanup_file(**kwargs):
    try:
        ti = kwargs['ti']
        mariadb_data_path = ti.xcom_pull(key='mariadb_data', task_ids='extract_from_mariadb')
        
        files_to_remove = []
        
        if mariadb_data_path and os.path.isfile(mariadb_data_path):
            files_to_remove.append(mariadb_data_path)

        # Также удаляем промежуточные файлы
        temp_dir = '/tmp'
        for file_name in os.listdir(temp_dir):
            if file_name.startswith('sales_data_') and ('.parquet.part_' in file_name or '.csv.part_' in file_name):
                file_path = os.path.join(temp_dir, file_name)
                files_to_remove.append(file_path)
        
        # Удаляем все временные файлы
        for file_path in files_to_remove:
            try:
                os.remove(file_path)
                logger.info(f"Temporary file removed: {file_path}")
            except Exception as e:
                logger.warning(f"Could not remove file {file_path}: {e}")
                
    except Exception as e:
        logger.error(f"Error in cleanup_file: {str(e)}")

# Функция для обработки ошибки
def task_failure_callback(context):
     # Отправка уведомления в Telegram
    send_telegram = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='Notification_telegram',
        chat_id='-5506',
        text=f'❌ Даг {context["dag"].dag_id} Задача {context["task_instance"].task_id} упала с ошибкой: {context["exception"]}',
        dag=context['dag']
    )
    send_telegram.execute(context=context)
    
    # Отправка email-уведомления
    send_email = EmailOperator(
        task_id='send_email_failure',
        to='alerts@_.ru',
        subject=f'Airflow task failure: {context["task_instance"].task_id}',
        html_content=
                     f'<p>Даг <b>{context["dag"].dag_id}</b></p>'
                     f'<p>Задача <b>{context["task_instance"].task_id}</b> завершилась ошибкой.</p>'
                     f'<p>Ошибка: {context["exception"]}</p>'
                     f'<p>Дополнительная информация: {context}</p>',
        dag=context['dag']
    )
    send_email.execute(context=context)

# Функция для отправки сообщения об успешном выполнении
def dag_success_callback(context):
     # Отправка уведомления в Telegram
    send_telegram = TelegramOperator(
        task_id='send_telegram_success',
        telegram_conn_id='Notification_telegram',
        chat_id='-5506',
        text=f'✅ DAG {context["dag"].dag_id} успешно выполнен!',
        dag=context['dag']
    )
    send_telegram.execute(context=context)
    
    # Отправка email-уведомления
    send_email = EmailOperator(
        task_id='send_email_success',
        to='alerts@_.ru',
        subject=f'Airflow DAG success: {context["dag"].dag_id}',
        html_content=f'<p>DAG <b>{context["dag"].dag_id}</b> успешно выполнен.</p>'
                     f'<p>Время выполнения: {context["execution_date"]}</p>',
        dag=context['dag']
    )
    send_email.execute(context=context)

# Настройки DAG с улучшенной обработкой ошибок
default_args = {
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'retry_exponential_backoff': True,  # Добавляем экспоненциальную задержку
    'max_retry_delay': timedelta(minutes=30),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['dev@sbis18.ru'],
    'on_failure_callback':task_failure_callback,
}

with DAG(
    dag_id='pan_palych',
    start_date=datetime(2024, 10, 1, 0, 15),
    schedule='15 0 * * *', #None,   Запуск в 3:15 МСК
    catchup=False,
    tags=['pan_palych', 'mariadb', 'clickhouse', 'order_sales'],
    max_active_runs=1,
    concurrency=1,
    default_args=default_args,
    on_success_callback=dag_success_callback, 
    on_failure_callback=task_failure_callback  # Глобальный обработчик ошибок
) as dag:
    
    check_volume = PythonOperator(
        task_id='check_data_volume',
        python_callable=check_data_volume,
        provide_context=True
    )

    extract = PythonOperator(
        task_id='extract_from_mariadb',
        python_callable=extract_from_mariadb
    )

    # Очистка ClickHouse перед удалением данных
    cleanup_temp_files = PythonOperator(
        task_id='cleanup_temp_files',
        python_callable=cleanup_clickhouse_temp_files,
        provide_context=True
    )

    delete_window = PythonOperator(
        task_id='delete_window_in_clickhouse',
        python_callable=delete_window_in_clickhouse,
        provide_context=True,
    )

    load = PythonOperator(
        task_id='load_window_to_clickhouse',
        python_callable=load_window_to_clickhouse
    )
    
    quality = PythonOperator(
    task_id='quality_check',
    python_callable=quality_check,
    provide_context=True,
    )

    # Существующая функция очистки файлов
    cleanup = PythonOperator(
        task_id='cleanup_file',
        python_callable=cleanup_file,
        trigger_rule='none_failed_min_one_success'
    )
    
    # Логика выполнения
    check_volume >> extract >> cleanup_temp_files >> delete_window >> load >> quality >> cleanup
