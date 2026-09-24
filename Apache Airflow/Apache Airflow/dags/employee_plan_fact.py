# Импорты для работы с Apache Airflow
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
import calendar
from datetime import timezone

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
TABLE_NAME = 'pp_employee_plan_fact'
EXPORT_PATH = '/tmp/pp_employee_plan_fact.parquet'

# Базовый SQL запрос
BASE_SQL_TEMPLATE = """
WITH sales_customers as (
    SELECT 
        so.taviat_sale_order_id,
        DATE_FORMAT(so.sbis_created_at, '%%Y-%%m-01') as date,
        so.company_name,
        concat(e.last_name, ' ', e.first_name, ' ', e.patronymic) as Emp_Name,
        sotp.total_price,
        sotp.quantity,
        n.folder,
        n2.name as nomenclature_group,
        so.fiscal_number,
        so.pay_certificate
    FROM 
        sale_orders so
    left JOIN 
        sale_order_tabular_parts sotp ON 
        so.taviat_sale_order_id=sotp.taviat_sale_order_id
    left JOIN 
        nomenclatures n ON 
        sotp.nomenclature_id = n.nomenclature_id 
    left JOIN 
        employees e ON 
        so.sale_seller = e.employee_id 
    left JOIN 
        nomenclatures n2 ON 
        n.root_folder_id=n2.nomenclature_id
    WHERE 
        sotp.is_return =0 
        AND sotp.is_return_sn =0 
        {{fact_where_clause}}
),
total_revenue AS (
    SELECT 
        sc.`date`,
        sc.company_name,
        sc.Emp_Name,
        count(distinct sc.taviat_sale_order_id) as count_sales,
        SUM(sc.total_price) as revenue,
        SUM(CASE 
            when sc.fiscal_number is null
            then sc.total_price
            else 0
        END) as non_fiscal_revenue
    FROM 
        sales_customers sc
    GROUP BY 
        sc.`date` ,
        sc.company_name ,
        sc.Emp_Name 
),
sertificate as (
    SELECT
        t.`date` ,
        t.company_name ,
        t.Emp_Name,
        sum(t.pay_certificate) as pay_certificate
    FROM (
        SELECT DISTINCT 
            sc.`date`,
            sc.taviat_sale_order_id,
            sc.company_name,
            sc.Emp_Name,
            sc.pay_certificate 
        FROM 
            sales_customers sc) t
    GROUP BY 
        t.`date` ,
        t.company_name ,
        t.Emp_Name 
),
snack_revenue AS (
    SELECT 
        sc.`date`,
        sc.company_name,
        sc.Emp_Name,
        SUM(sc.total_price) as sn_revenue
    FROM 
        sales_customers sc
    WHERE 
        sc.nomenclature_group='3 СНЕКИ'
    GROUP BY 
        sc.`date` ,
        sc.company_name ,
        sc.Emp_Name 
),
packaging_revenue AS (
    SELECT 
        sc.`date`,
        sc.company_name,
        sc.Emp_Name,
        SUM(sc.total_price) as p_revenue
    FROM 
        sales_customers sc
    WHERE 
        sc.nomenclature_group='2 ФАСОВКА'
    GROUP BY 
        sc.`date` ,
        sc.company_name ,
        sc.Emp_Name 
),
liters AS (
    SELECT 
        sc.`date`,
        sc.company_name,
        sc.Emp_Name,
        SUM(sc.quantity) as liters_fact
    FROM 
        sales_customers sc
    WHERE 
        sc.folder in (49,62,565,1519,1520,1521,1522,1523,1524,1525,1526,4857,5569,5930)
    GROUP BY 
        sc.`date` ,
        sc.company_name ,
        sc.Emp_Name 
),
plan_customers AS (
    SELECT 
        ps.`date` ,
        ps.employee ,
        ps.sale_point ,
        ps.snacks ,
        ps.packaging,
        ps.liters  
    FROM 
        usertable_planovye_pokazateli_sotrudniki ps 
    {{plan_where_clause}}
)
SELECT 
    tr.`date` ,
    tr.Emp_Name,
    tr.company_name as sale_point,
    COALESCE(tr.count_sales,0) as count_sales,
    COALESCE(tr.revenue,0) as total_revenue_fact,
    COALESCE(tr.non_fiscal_revenue,0) as non_fiscal_revenue,
    COALESCE(l.liters_fact,0) as liters_fact,
    COALESCE(pc.liters,0) as liters_plan,
    COALESCE(pc.snacks,0) as snacks_percent_plan,
    COALESCE(pc.packaging,0) as packaging_percent_plan,
    COALESCE(sr.sn_revenue,0) as snack_revenue_fact,
    COALESCE(sr.sn_revenue/tr.revenue*100,0) as snacks_percent_fact,
    COALESCE(pr.p_revenue,0) as packaging_revenue_fact,
    COALESCE(pr.p_revenue/tr.revenue*100,0) as packaging_percent_fact,
    COALESCE(s.pay_certificate,0) as pay_certificate
FROM 
    total_revenue tr 
LEFT JOIN 
    plan_customers pc ON 
    pc.`date` =tr.`date` 
    AND pc.sale_point =tr.company_name 
    AND SUBSTRING_INDEX(pc.employee, ' ', 1) =SUBSTRING_INDEX(tr.Emp_Name, ' ', 1) 
LEFT JOIN 
    snack_revenue sr ON 
    tr.`date` =sr.`date` 
    AND tr.company_name =sr.company_name 
    AND tr.Emp_Name =sr.Emp_Name
LEFT JOIN 
    packaging_revenue pr ON 
    tr.`date` =pr.`date` 
    AND tr.company_name =pr.company_name 
    AND tr.Emp_Name =pr.Emp_Name
LEFT JOIN 
    liters l ON 
    tr.`date` =l.`date` 
    AND tr.company_name =l.company_name 
    AND tr.Emp_Name =l.Emp_Name
LEFT JOIN 
    sertificate s ON 
    tr.`date` =s.`date` 
    AND tr.company_name =s.company_name 
    AND tr.Emp_Name =s.Emp_Name 
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
                pool_recycle=300,
                pool_pre_ping=True,
                pool_size=5,
                max_overflow=10,
                pool_timeout=30,
                pool_reset_on_return='rollback',
                connect_args={
                    'connect_timeout': CONNECTION_TIMEOUT,
                    'read_timeout': 300,
                    'write_timeout': 300,
                },
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
        return False

# Модифицируем SQL запрос для получения данных за конкретный месяц
def get_sql_query_for_specific_month(month_date):
    month_start = month_date.replace(day=1)
    
    # Формируем условие WHERE для фактов и планов
    fact_where_clause = f"AND date(so.sbis_created_at) >= '{month_start.strftime('%Y-%m-01')}' AND date(so.sbis_created_at) <= LAST_DAY('{month_start.strftime('%Y-%m-01')}')"
    plan_where_clause = f"WHERE ps.`date` = '{month_start.strftime('%Y-%m-01')}'"
    
    # Подставляем условия в шаблон
    sql_query = BASE_SQL_TEMPLATE.replace("{{fact_where_clause}}", fact_where_clause).replace("{{plan_where_clause}}", plan_where_clause)
    
    logger.info(f"Generated SQL for month: {month_start.strftime('%Y-%m')}")
    return sql_query

# Функция для получения SQL запроса за диапазон месяцев
def get_sql_query_for_date_range(start_date, end_date):
    # Формируем условие WHERE для фактов и планов
    fact_where_clause = f"AND date(so.sbis_created_at) >= '{start_date.strftime('%Y-%m-01')}' AND date(so.sbis_created_at) <= LAST_DAY('{end_date.strftime('%Y-%m-01')}')"
    plan_where_clause = f"WHERE ps.`date` >= '{start_date.strftime('%Y-%m-01')}' AND ps.`date` <= '{end_date.strftime('%Y-%m-01')}'"
    
    # Подставляем условия в шаблон
    sql_query = BASE_SQL_TEMPLATE.replace("{{fact_where_clause}}", fact_where_clause).replace("{{plan_where_clause}}", plan_where_clause)
    
    logger.info(f"Generated SQL for date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    return sql_query

# Возвращает SQL запрос для всех данных (без фильтрации)
def get_sql_query_all_data():
    # Для всех данных используем базовое условие по дате и пустое условие для планов
    fact_where_clause = "AND date(so.sbis_created_at)>'2025-09-30'"
    plan_where_clause = ""
    
    sql_query = BASE_SQL_TEMPLATE.replace("{{fact_where_clause}}", fact_where_clause).replace("{{plan_where_clause}}", plan_where_clause)
    
    logger.info("Generated SQL for all data (from 2025-10-01)")
    return sql_query

# Функция для проверки состояния таблицы в ClickHouse
def check_table_status(**kwargs):
    try:
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        
        logger.info("Checking ClickHouse table status...")
        
        with DatabaseConnectionContext() as db:
            ch_hook = db.get_clickhouse_hook()
            client = ch_hook.get_conn()
            
            # 1. Проверяем существование таблицы
            check_table_sql = f"""
            SELECT COUNT() as count FROM system.tables 
            WHERE database = currentDatabase() AND name = '{TABLE_NAME}'
            """
            table_exists = client.execute(check_table_sql)[0][0] > 0
            
            if not table_exists:
                logger.info(f"Table {TABLE_NAME} does not exist. Will create it and load all historical data.")
                return {
                    'table_exists': False,
                    'months_to_process': 'all',
                    'months_list': []
                }

            # 2. Проверяем, есть ли данные за текущий месяц
            current_month = execution_date.replace(day=1).replace(tzinfo=None)
            
            check_current_month_sql = f"""
            SELECT COUNT(*) as count 
            FROM {TABLE_NAME} 
            WHERE toStartOfMonth(date) = toStartOfMonth(toDate('{current_month.strftime("%Y-%m-%d")}'))
            AND date >= toDate('2025-10-01')  -- Только разумные даты (из условия SQL)
            """
            has_current_month = client.execute(check_current_month_sql)[0][0] > 0
            
            if has_current_month:
                logger.info(f"Data exists for current month {current_month.strftime('%Y-%m')}. Will update.")
                return {
                    'table_exists': True,
                    'months_to_process': 'update_current',
                    'months_list': [current_month.strftime('%Y-%m-%d')]
                }
            
            # 3. Проверяем, есть ли вообще какие-то данные в таблице
            count_sql = f"""
            SELECT COUNT(*) as count 
            FROM {TABLE_NAME} 
            WHERE date >= toDate('2025-10-01')  -- Только разумные даты (из условия SQL)
            """
            has_any_data = client.execute(count_sql)[0][0] > 0
            
            if has_any_data:
                # Получаем последний месяц с данными
                last_month_sql = f"""
                SELECT MAX(date) as last_month 
                FROM {TABLE_NAME}
                WHERE date >= toDate('2025-10-01')  -- Только разумные даты
                """
                result = client.execute(last_month_sql)
                last_month_in_table = result[0][0] if result and result[0][0] else None
                
                if last_month_in_table:
                    # Преобразуем в datetime
                    if isinstance(last_month_in_table, datetime):
                        last_month_date = last_month_in_table.replace(tzinfo=None)
                    else:
                        if isinstance(last_month_in_table, str):
                                last_month_date = datetime.strptime(last_month_in_table, '%Y-%m-%d').date()
                        else:
                                last_month_date = last_month_in_table
                    
                    last_month_date = last_month_date.replace(day=1)
                    current_month_start = current_month.date()
                    
                    # Логируем типы для отладки
                    logger.info(f"Comparison types - last_month_date: {type(last_month_date)}, tzinfo: {getattr(last_month_date, 'tzinfo', None)}, current_month_start: {type(current_month_start)}, tzinfo: {getattr(current_month_start, 'tzinfo', None)}")
                    
                    # Находим месяцы для заполнения (начиная со следующего месяца после последнего)
                    months_list = []
                    temp_date = last_month_date + timedelta(days=32)
                    temp_date = temp_date.replace(day=1)
                    
                    max_months = 12  # Максимум на 12 месяцев назад
                    month_counter = 0
                    
                    while temp_date <= current_month_start and month_counter < max_months:
                        months_list.append(temp_date)
                        temp_date = temp_date + timedelta(days=32)
                        temp_date = temp_date.replace(day=1)
                        month_counter += 1
                    
                    if months_list:
                        logger.info(f"Will fill gap with {len(months_list)} months: {[m.strftime('%Y-%m') for m in months_list]}")
                        return {
                            'table_exists': True,
                            'months_to_process': 'fill_gap',
                            'months_list': [m.strftime('%Y-%m-%d') for m in months_list]
                        }
                    else:
                        # Нет месяцев для заполнения
                        logger.info("No months to fill. Loading historical data.")
                        return {
                            'table_exists': True,
                            'months_to_process': 'all',
                            'months_list': []
                        }
            
            # Таблица пустая или содержит только старые данные
            logger.info("Table exists but is empty or contains only old data. Will load historical data.")
            return {
                'table_exists': True,
                'months_to_process': 'all',
                'months_list': []
            }
            
    except Exception as e:
        logger.error(f"Error checking table status: {str(e)}", exc_info=True)
        raise AirflowException(f"Table status check failed: {str(e)}")

# Оптимизированное преобразование типов для аналитических данных
def convert_analytics_data_types(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Starting data type conversion for analytics data")

    # Создаем копию для безопасной модификации
    df_converted = df.copy()
    
    # Обрабатываем дату (она в формате 'YYYY-MM-01')
    if 'date' in df_converted.columns:
        df_converted['date'] = pd.to_datetime(df_converted['date'])
    
    # VARCHAR/TEXT колонки
    string_cols = ['Emp_Name', 'sale_point']
    
    for col in string_cols:
        if col in df_converted.columns:
            df_converted[col] = df_converted[col].fillna('').astype(str)

    # Числовые колонки - целые числа (количество продаж и литры)
    integer_cols = ['count_sales']
    
    for col in integer_cols:
        if col in df_converted.columns:
            try:
                df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce').fillna(0).astype(int)
            except Exception as e:
                logger.warning(f"Error converting {col} to integer: {e}")
                df_converted[col] = 0
    
    # Числовые колонки (проценты и суммы)
    numeric_cols = [
        'snacks_percent_plan', 'packaging_percent_plan', 'total_revenue_fact',
        'non_fiscal_revenue','snack_revenue_fact', 'snacks_percent_fact',
        'packaging_revenue_fact', 'packaging_percent_fact', 'liters_fact','liters_plan','pay_certificate'
    ]
    
    for col in numeric_cols:
        if col in df_converted.columns:
            try:
                df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce').fillna(0)
                # Для процентов округляем до 2 знаков
                if 'percent' in col.lower():
                    df_converted[col] = df_converted[col].round(2)
                # Для выручки округляем до 2 знаков
                elif 'revenue' in col.lower():
                    df_converted[col] = df_converted[col].round(2)
            except Exception as e:
                logger.warning(f"Error converting {col} to numeric: {e}")
                df_converted[col] = 0
    
    logger.info("Analytics data type conversion completed")
    return df_converted

# Подготовка данных для вставки в ClickHouse
def prepare_analytics_data_for_insert(df: pd.DataFrame, load_batch_id: str = '') -> List[Tuple]:
    data = []
    processed_rows = 0
    skipped_rows = 0

    for _, row in df.iterrows():
        try:
            # Проверяем наличие необходимых колонок
            required_cols = ['date', 'Emp_Name', 'sale_point']
            
            # Проверяем, что все необходимые колонки присутствуют
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"Missing columns in row: {missing_cols}")
                skipped_rows += 1
                continue
            
            # Проверяем ключевые поля
            if pd.isna(row['date']) or pd.isna(row['Emp_Name']):
                logger.warning(f"Missing key fields in row")
                skipped_rows += 1
                continue
            
            # Преобразуем данные в правильные типы с обработкой NaN
            item = (
                row['date'] if pd.notna(row['date']) else None,
                str(row['Emp_Name']) if pd.notna(row['Emp_Name']) else '',
                str(row['sale_point']) if pd.notna(row['sale_point']) else '',
                int(row['count_sales']) if pd.notna(row['count_sales']) else 0,
                float(row['liters_fact']) if pd.notna(row['liters_fact']) else 0.0,
                float(row['liters_plan']) if pd.notna(row['liters_plan']) else 0.0,
                float(row['snacks_percent_plan']) if pd.notna(row['snacks_percent_plan']) else 0.0,
                float(row['packaging_percent_plan']) if pd.notna(row['packaging_percent_plan']) else 0.0,
                float(row['total_revenue_fact']) if pd.notna(row['total_revenue_fact']) else 0.0,
                float(row['non_fiscal_revenue']) if pd.notna(row['non_fiscal_revenue']) else 0.0,
                float(row['snack_revenue_fact']) if pd.notna(row['snack_revenue_fact']) else 0.0,
                float(row['snacks_percent_fact']) if pd.notna(row['snacks_percent_fact']) else 0.0,
                float(row['packaging_revenue_fact']) if pd.notna(row['packaging_revenue_fact']) else 0.0,
                float(row['packaging_percent_fact']) if pd.notna(row['packaging_percent_fact']) else 0.0,
                float(row['pay_certificate']) if pd.notna(row['pay_certificate']) else 0.0,
                datetime.now(),  # calculated_at
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

# Модифицируем функцию extract_analytics_from_mariadb для обработки разных сценариев
@retry_with_backoff()
def extract_analytics_from_mariadb(**kwargs):
    try:
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        
        # Получаем информацию о том, что нужно обработать
        table_status = ti.xcom_pull(task_ids='check_table_status')

        if not table_status:
            logger.error("No table status returned from check_table_status")
            raise AirflowException("Table status check failed")

        months_to_process = table_status.get('months_to_process', 'all')
        months_list_str = table_status.get('months_list', [])
        
        # Преобразуем строки обратно в даты
        months_list = []
        for month_str in months_list_str:
            try:
                month_date = datetime.strptime(month_str, '%Y-%m-%d').replace(day=1)
                months_list.append(month_date)
            except:
                continue
        
        logger.info(f"Processing mode: {months_to_process}")
        logger.info(f"Months to process: {[m.strftime('%Y-%m') for m in months_list]}")
        
        if months_to_process == 'all':
            # Загружаем все исторические данные
            logger.info("Extracting all historical analytics data")
            export_path = f"/tmp/employee_performance_all_{execution_date.strftime('%Y%m%d_%H%M%S')}.parquet"
            
            sql_query = get_sql_query_all_data()
            
        elif months_to_process == 'update_current':
            # Обновляем только текущий месяц
            current_month = execution_date.replace(day=1)
            logger.info(f"Updating current month: {current_month.strftime('%Y-%m')}")
            export_path = f"/tmp/employee_performance_{current_month.strftime('%Y%m')}_{execution_date.strftime('%Y%m%d_%H%M%S')}.parquet"
            
            sql_query = get_sql_query_for_specific_month(current_month)
            
        elif months_to_process == 'fill_gap':
            # Загружаем несколько месяцев
            if not months_list:
                logger.warning("No months to process in fill_gap mode")
                ti.xcom_push(key='analytics_data', value=None)
                ti.xcom_push(key='has_new_analytics_data', value=False)
                ti.xcom_push(key='processing_mode', value=months_to_process)
                ti.xcom_push(key='months_to_load', value=[])
                return
            
            logger.info(f"Filling gap for {len(months_list)} months")
            export_path = f"/tmp/employee_performance_gap_{execution_date.strftime('%Y%m%d_%H%M%S')}.parquet"
            
            # Для нескольких месяцев используем общий запрос с диапазоном
            start_date = min(months_list)
            end_date = max(months_list)
            
            sql_query = get_sql_query_for_date_range(start_date, end_date)
        
        else:
            logger.error(f"Unknown processing mode: {months_to_process}")
            return
        # ДОБАВЛЯЕМ ДЛЯ ДИАГНОСТИКИ:
        logger.info(f"SQL query length: {len(sql_query)} characters")
        logger.debug(f"SQL query preview (first 500 chars):\n{sql_query[:500]}...")
        
        # Также можно добавить проверку на наличие фигурных скобок
        if '{' in sql_query or '}' in sql_query:
            # Заменяем оставшиеся одинарные фигурные скобки на двойные
            sql_query = sql_query.replace('{fact_where_clause}', '{{fact_where_clause}}')
            sql_query = sql_query.replace('{plan_where_clause}', '{{plan_where_clause}}')
            sql_query = sql_query.replace('{{fact_where_clause}}', fact_where_clause if 'fact_where_clause' in locals() else "")
            sql_query = sql_query.replace('{{plan_where_clause}}', plan_where_clause if 'plan_where_clause' in locals() else "")
            
        with DatabaseConnectionContext() as db:
            engine = db.get_mysql_engine()

            chunk_size = 50000
            all_chunks = []
            chunk_number = 0
            total_rows = 0
            
            logger.info(f"Executing SQL query for {months_to_process} mode")
            
            for chunk_df in pd.read_sql(sql_query, con=engine, chunksize=chunk_size):
                chunk_number += 1
                
                try:
                    with engine.connect() as test_conn:
                        test_conn.execute("SELECT 1")
                    
                    logger.info(f"Processing chunk {chunk_number} with {len(chunk_df)} rows")
                    
                    if chunk_df.empty:
                        logger.info(f"Chunk {chunk_number} is empty, skipping")
                        continue
                        
                    chunk_df = convert_analytics_data_types(chunk_df)
                    all_chunks.append(chunk_df)
                    total_rows += len(chunk_df)
                    
                    if chunk_number % 10 == 0:
                        logger.info(f"Progress: processed {chunk_number} chunks, {total_rows} total rows")
                    
                    del chunk_df
                    gc.collect()
                    
                except (OperationalError, DatabaseError) as e:
                    if chunk_number >= MAX_RETRIES:
                        raise
                    logger.warning(f"Database error in chunk {chunk_number}: {str(e)}")
                    logger.info("Attempting to refresh database connection...")
                    
                    db.close_connections()
                    engine = db.get_mysql_engine()
                    
                    logger.info("Database connection refreshed, continuing...")
                    continue
                    
                except Exception as e:
                    logger.error(f"Unexpected error in chunk {chunk_number}: {str(e)}")
                    raise
            
            if not all_chunks:
                logger.warning(f"No analytics data found for {months_to_process} mode")
                ti.xcom_push(key='analytics_data', value=None)
                ti.xcom_push(key='has_new_analytics_data', value=False)
                ti.xcom_push(key='processing_mode', value=months_to_process)
                ti.xcom_push(key='months_to_load', value=[m.strftime('%Y-%m') for m in months_list])
                return
            
            logger.info(f"Combining {len(all_chunks)} chunks")
            
            try:
                df_analytics = pd.concat(all_chunks, ignore_index=True)
                logger.info(f"Successfully combined chunks. Total rows: {len(df_analytics)}")
            except MemoryError:
                logger.warning("Memory error during concat, using iterative processing")
                if not all_chunks:
                    df_analytics = pd.DataFrame()
                else:
                    all_chunks[0].to_parquet(export_path, index=False, engine='pyarrow')
                    
                    for i, chunk in enumerate(all_chunks[1:], 2):
                        try:
                            chunk.to_parquet(export_path, index=False, engine='pyarrow', append=True)
                        except:
                            existing = pd.read_parquet(export_path)
                            combined = pd.concat([existing, chunk], ignore_index=True)
                            combined.to_parquet(export_path, index=False)
                            del existing, combined
                            gc.collect()
                    
                    df_analytics = pd.read_parquet(export_path)
            
            logger.info(f"Extracted {len(df_analytics)} analytics rows from MariaDB")
            
            # Сохраняем в файл
            try:
                df_analytics.to_parquet(export_path, index=False, compression='snappy')
                logger.info(f"Data saved to {export_path}")
            except Exception as e:
                logger.error(f"Error saving to parquet: {str(e)}")
                csv_path = export_path.replace('.parquet', '.csv')
                df_analytics.to_csv(csv_path, index=False)
                export_path = csv_path 
                logger.info(f"Data saved to CSV instead: {export_path}")
            
            # Сохраняем информацию в XCom
            ti.xcom_push(key='analytics_data', value=export_path)
            ti.xcom_push(key='has_new_analytics_data', value=True)
            ti.xcom_push(key='total_rows_extracted', value=len(df_analytics))
            ti.xcom_push(key='processing_mode', value=months_to_process)
            ti.xcom_push(key='months_to_load', value=[m.strftime('%Y-%m') for m in months_list])
            ti.xcom_push(key='months_list_dates', value=months_list_str)
            
            logger.info(f"Analytics data prepared successfully for {months_to_process} mode")
            
            del df_analytics
            del all_chunks
            gc.collect()
            logger.debug(f"Memory usage after cleanup: ...")
            
    except Exception as e:
        logger.error(f"Error in extract_analytics_from_mariadb: {str(e)}", exc_info=True)
        raise AirflowException(f"Analytics extraction from MariaDB failed: {str(e)}")

# Модифицируем функцию load_analytics_to_clickhouse для интеллектуальной загрузки
@retry_with_backoff()
def load_analytics_to_clickhouse(**kwargs):
    try:
        ti = kwargs['ti']
        execution_date = kwargs['execution_date']
        analytics_data_path = ti.xcom_pull(key='analytics_data', task_ids='extract_analytics_from_mariadb')
        has_new_data = ti.xcom_pull(key='has_new_analytics_data', task_ids='extract_analytics_from_mariadb')
        processing_mode = ti.xcom_pull(key='processing_mode', task_ids='extract_analytics_from_mariadb')
        months_to_load = ti.xcom_pull(key='months_to_load', task_ids='extract_analytics_from_mariadb', default=[])
        months_list_dates = ti.xcom_pull(key='months_list_dates', task_ids='extract_analytics_from_mariadb', default=[])
        
        if not has_new_data:
            logger.info("No new analytics data to load")
            return
    
        if not analytics_data_path or not os.path.isfile(analytics_data_path):
            logger.warning("Analytics data file not found or path is None")
            return
        
        logger.info(f"Processing mode: {processing_mode}")
        logger.info(f"Months to load: {months_to_load}")

        with DatabaseConnectionContext() as db:
            ch_hook = db.get_clickhouse_hook()
            client = ch_hook.get_conn()
            logger.info("Connected to ClickHouse successfully")
                
            # Проверяем существование таблицы
            check_table_sql = f"""
            SELECT COUNT() as count FROM system.tables 
            WHERE database = currentDatabase() AND name = '{TABLE_NAME}'
            """
            table_exists = client.execute(check_table_sql)[0][0] > 0
            
            if not table_exists:
                logger.info(f"Table {TABLE_NAME} does not exist, creating it")
                create_table_sql = f"""
                CREATE TABLE {TABLE_NAME} (
                    date Date,
                    Emp_Name String,
                    sale_point String,
                    count_sales UInt32,
                    liters_fact Float64,
                    liters_plan Float64,
                    snacks_percent_plan Float64,
                    packaging_percent_plan Float64,
                    total_revenue_fact Float64,
                    non_fiscal_revenue Float64,
                    snack_revenue_fact Float64,
                    snacks_percent_fact Float64,
                    packaging_revenue_fact Float64,
                    packaging_percent_fact Float64,
                    pay_certificate Float64,
                    calculated_at DateTime,
                    load_batch_id String,
                    created_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()  
                PARTITION BY toYYYYMM(date)
                ORDER BY (date, Emp_Name, sale_point)
                SETTINGS index_granularity = 8192
                """
                client.execute(create_table_sql)
                logger.info("Table created successfully")
            
            # В зависимости от режима обработки выполняем разные действия
            if processing_mode == 'update_current':
                # Удаляем данные за текущий месяц перед вставкой
                if months_list_dates:
                    # Берем первый месяц из списка (должен быть текущий)
                    target_month = datetime.strptime(months_list_dates[0], '%Y-%m-%d').replace(day=1)
                else:
                    target_month = execution_date.replace(day=1)
                
                logger.info(f"Deleting existing data for month {target_month.strftime('%Y-%m')}")
                
                delete_sql = f"""
                ALTER TABLE {TABLE_NAME} 
                DELETE WHERE toStartOfMonth(date) = toStartOfMonth(toDate('{target_month.strftime("%Y-%m-%d")}'))
                """
                client.execute(delete_sql)
                logger.info(f"Deleted old records for month {target_month.strftime('%Y-%m')}")
            
            # Подготавливаем данные для вставки
            load_batch_id = f"batch_{execution_date.strftime('%Y%m%d_%H%M%S')}_{processing_mode}"
            insert_sql = f"""
            INSERT INTO {TABLE_NAME} (
                date, Emp_Name, sale_point, count_sales, liters_fact,  liters_plan, snacks_percent_plan, packaging_percent_plan,
                total_revenue_fact, non_fiscal_revenue, snack_revenue_fact, snacks_percent_fact,
                packaging_revenue_fact, packaging_percent_fact, pay_certificate,
                calculated_at, load_batch_id
            ) VALUES
            """

            # Обрабатываем данные чанками
            chunk_size = 50000
            total_inserted = 0
            
            logger.info(f"Loading data from {analytics_data_path}")
            
            # Загружаем весь файл
            df_analytics = pd.read_parquet(analytics_data_path)
            logger.info(f"Total rows in file: {len(df_analytics)}")
            
            # Разбиваем на чанки вручную
            total_rows = len(df_analytics)
            
            for start_idx in range(0, total_rows, chunk_size):
                end_idx = min(start_idx + chunk_size, total_rows)
                chunk = df_analytics.iloc[start_idx:end_idx]
                
                # Конвертируем типы данных
                chunk = convert_analytics_data_types(chunk)
                data = prepare_analytics_data_for_insert(chunk, load_batch_id)
                
                if data:
                    client.execute(insert_sql, data)
                    inserted_rows = len(data)
                    total_inserted += inserted_rows
                    logger.info(f"Inserted {inserted_rows} records (chunk {start_idx//chunk_size + 1}), total: {total_inserted}")
                
                # Освобождаем память
                del chunk
                gc.collect()
            
            # Освобождаем память
            del df_analytics
            gc.collect()
            
            logger.info(f"Successfully loaded {total_inserted} analytics records into ClickHouse")
            
            # Проверяем итоговое состояние таблицы
            # 1. Общее количество записей
            count_sql = f"SELECT COUNT() as total_count FROM {TABLE_NAME}"
            final_count = client.execute(count_sql)[0][0]
            logger.info(f"Total records in {TABLE_NAME}: {final_count}")
            
            # 2. Минимальная и максимальная даты
            date_range_sql = f"""
            SELECT 
                MIN(date) as min_date,
                MAX(date) as max_date
            FROM {TABLE_NAME}
            WHERE date >= toDate('2025-10-01')
            """
            date_range = client.execute(date_range_sql)[0]
            logger.info(f"Date range in table: {date_range[0]} to {date_range[1]}")
            
            # 3. Количество месяцев
            months_count_sql = f"""
            SELECT COUNT(DISTINCT toStartOfMonth(date)) as month_count
            FROM {TABLE_NAME}
            WHERE date >= toDate('2025-10-01')
            """
            months_count = client.execute(months_count_sql)[0][0]
            logger.info(f"Total months in table: {months_count}")

            # 4. дополнительная проверка
            monthly_stats_sql = f"""
            SELECT 
                toStartOfMonth(date) as month,
                COUNT(DISTINCT Emp_Name) as unique_employees,
                COUNT(DISTINCT sale_point) as unique_points,
                SUM(count_sales) as total_sales,
                SUM(liters_fact) as total_liters,
                AVG(snacks_percent_fact) as avg_snacks_percent
            FROM {TABLE_NAME}
            WHERE date >= toDate('2025-10-01')
            GROUP BY month
            ORDER BY month
            """
            monthly_stats = client.execute(monthly_stats_sql)
            for stat in monthly_stats:
                logger.info(f"Month {stat[0]}: {stat[1]} employees, {stat[2]} points, {stat[3]} sales, {stat[4]} liters, snacks: {stat[5]:.2f}%")
    
    except Exception as e:
        logger.error(f"Error in load_analytics_to_clickhouse: {str(e)}", exc_info=True)
        raise AirflowException(f"Analytics load to ClickHouse failed: {str(e)}")

# Очистка временных файлов
def cleanup_analytics_files(**kwargs):
    try:
        ti = kwargs['ti']
        analytics_data_path = ti.xcom_pull(key='analytics_data', task_ids='extract_analytics_from_mariadb')
        
        files_to_remove = []
        
        if analytics_data_path and os.path.isfile(analytics_data_path):
            files_to_remove.append(analytics_data_path)
        
        # Удаляем все временные файлы
        for file_path in files_to_remove:
            try:
                os.remove(file_path)
                logger.info(f"Temporary file removed: {file_path}")
            except Exception as e:
                logger.warning(f"Could not remove file {file_path}: {e}")
        
        logger.info("Analytics cleanup completed")
            
    except Exception as e:
        logger.error(f"Error in cleanup_analytics_files: {str(e)}")

# Функция для обработки ошибки
def task_failure_callback(context):
     # Отправка уведомления в Telegram
    send_telegram = TelegramOperator(
        task_id='send_message_telegram',
        telegram_conn_id='Notification_telegram',
        chat_id='-5656',
        text=f'❌ Даг {context["dag"].dag_id} Задача {context["task_instance"].task_id} упала с ошибкой: {context["exception"]}',
        dag=context['dag']
    )
    send_telegram.execute(context=context)
    
    # Отправка email-уведомления
    send_email = EmailOperator(
        task_id='send_email_failure',
        to='alerts@_.ru',
        subject=f'Airflow task failure: {context["task_instance"].task_id}',
        html_content=f'<p>Даг <b>{context["dag"].dag_id}</b></p>'
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
        chat_id='-5656',
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

  
# Настройки DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['dev@sbis18.ru'],
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'on_failure_callback':task_failure_callback,
}

# Создание DAG
with DAG(
    dag_id='pp_employee_plan_fact',
    start_date=datetime(2024, 10, 1, 0, 30),
    schedule='22 0 * * *',  # Ежедневно в 3:22 МСК
    catchup=False,
    tags=['analytics', 'sales', 'reports', 'mariadb', 'clickhouse', 'employees'],
    max_active_runs=1,
    concurrency=1,
    default_args=default_args,
    on_success_callback=dag_success_callback, 
    on_failure_callback=task_failure_callback  # Глобальный обработчик ошибок
) as dag:
    
    # Первая задача - проверка статуса таблицы в ClickHouse
    check_table = PythonOperator(
        task_id='check_table_status',
        python_callable=check_table_status,
        provide_context=True
    )

    extract = PythonOperator(
        task_id='extract_analytics_from_mariadb',
        python_callable=extract_analytics_from_mariadb,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load_analytics_to_clickhouse',
        python_callable=load_analytics_to_clickhouse,
        provide_context=True
    )
    
    cleanup = PythonOperator(
        task_id='cleanup_analytics_files',
        python_callable=cleanup_analytics_files,
        trigger_rule='none_failed_min_one_success',
        provide_context=True
    )
    
    # Логика выполнения
    check_table >> extract >> load >> cleanup
