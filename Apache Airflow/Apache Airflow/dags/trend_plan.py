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
TABLE_NAME = 'pp_Trend_Plan'
EXPORT_PATH = '/tmp/pp_Trend_Plan.parquet'

# Базовый SQL шаблон (без фильтрации по дате в CTE plan)
BASE_SQL_TEMPLATE = """
WITH filtered_plan AS (
    SELECT 
        LAST_DAY(pm.`date`) as last_day_of_month,
        pm.sale_point as company_name,
        SUM(pm.revenue) as plan_revenue,
        SUM(pm.checks) as plan_count_orders,
        SUM(pm.liters) as plan_liters,
        SUM(pm.revenue_snacks) as plan_revenue_snacks,
        SUM(pm.revenue_packaging) as plan_revenue_packaging
    FROM 
        usertable_planovye_pokazateli_magaziny pm 
    {plan_where_clause}
    GROUP BY 1,2
),
sales_certificate as (
    SELECT 
        s.taviat_sale_order_id,
        s.company_name,
        s.sbis_created_at,
        sotp.quantity,
        sotp.total_price,
        s.pay_certificate,
        count(sotp.nomenclature_id) OVER (partition by s.taviat_sale_order_id) as count_position,
        n.root_folder_id,
        n.folder
    FROM 
        sale_orders s 
    left JOIN 
        sale_order_tabular_parts sotp ON 
        s.taviat_sale_order_id=sotp.taviat_sale_order_id
    left JOIN 
        nomenclatures n ON 
        sotp.nomenclature_id = n.nomenclature_id 
    WHERE 
        sotp.is_return =0 and sotp.is_return_sn =0 and s.pay_certificate>0 
), sales_all as (
    SELECT 
        s.taviat_sale_order_id,
        s.company_name,
        s.sbis_created_at,
        sotp.quantity,
        sotp.total_price,
        n.root_folder_id,
        n.folder
    FROM 
        sale_orders s 
    left JOIN 
        sale_order_tabular_parts sotp ON 
        s.taviat_sale_order_id=sotp.taviat_sale_order_id
    left JOIN 
        nomenclatures n ON 
        sotp.nomenclature_id = n.nomenclature_id 
    WHERE 
        sotp.is_return =0 and sotp.is_return_sn =0 and (s.pay_certificate=0 or s.pay_certificate is null)
    UNION ALL 
    SELECT 
        s.taviat_sale_order_id,
        s.company_name,
        s.sbis_created_at,
        s.quantity,
        s.total_price-s.pay_certificate/s.count_position as total_price,
        s.root_folder_id,
        s.folder
    FROM 
        sales_certificate s
),
sales AS (
    SELECT 
        s.taviat_sale_order_id,
        LAST_DAY(s.sbis_created_at) as last_day_of_month,
        s.company_name,
        s.total_price
    FROM 
        sales_all s
),
grouped_sales as(
    SELECT 
        s.last_day_of_month,
        s.company_name,
        COUNT(distinct s.taviat_sale_order_id) as count_orders,
        SUM(s.total_price) as revenue
    FROM 
        sales s
    GROUP BY 
        s.last_day_of_month,
        s.company_name
), revenue_count AS (
    SELECT 
        gs.last_day_of_month,
        gs.company_name,
        gs.revenue as fact_revenue,
        gs.count_orders as fact_count_orders,
        CASE 
            WHEN LAST_DAY(NOW())= gs.last_day_of_month
            THEN gs.count_orders/(DAY(NOW())-1)*DAY(gs.last_day_of_month)
            ELSE gs.count_orders
        END AS trend_count_orders,
        CASE 
            WHEN LAST_DAY(NOW())= gs.last_day_of_month
            THEN gs.revenue/(DAY(NOW())-1)*DAY(gs.last_day_of_month)
            ELSE gs.revenue
        END AS trend_revenue
    FROM 
        grouped_sales gs
),
sales_liters AS (
    SELECT 
        s.taviat_sale_order_id,
        LAST_DAY(s.sbis_created_at) as last_day_of_month,
        s.company_name,
        s.total_price,
        s.quantity
    FROM 
        sales_all s
    WHERE
        s.folder in (62,565,1519,1520,1521,1522,1523,1524,1525,1526,4857,5569,5930)
),
grouped_sales_liters AS (
    SELECT 
        s.last_day_of_month,
        s.company_name,
        COUNT(distinct s.taviat_sale_order_id) as count_orders,
        SUM(s.total_price) as revenue,
        SUM(s.quantity) as liters
    FROM 
        sales_liters s
    GROUP BY 
        s.last_day_of_month,
        s.company_name
), 
liters AS (
    SELECT
        gs.last_day_of_month,
        gs.company_name,
        gs.liters as fact_liters,
        CASE 
            WHEN LAST_DAY(NOW())= gs.last_day_of_month
            THEN gs.count_orders/(DAY(NOW())-1)*DAY(gs.last_day_of_month)
            ELSE gs.count_orders
        END AS trend_count_orders_liters,
        CASE 
            WHEN LAST_DAY(NOW())= gs.last_day_of_month
            THEN gs.revenue/(DAY(NOW())-1)*DAY(gs.last_day_of_month)
            ELSE gs.revenue
        END AS trend_revenue_liters,
        CASE 
            WHEN LAST_DAY(NOW())= gs.last_day_of_month
            THEN gs.liters/(DAY(NOW())-1)*DAY(gs.last_day_of_month)
            ELSE gs.liters
        END AS trend_liters
    FROM 
        grouped_sales_liters gs
),
sales_snacks AS (
    SELECT 
        s.taviat_sale_order_id,
        LAST_DAY(s.sbis_created_at) as last_day_of_month,
        s.company_name,
        s.total_price
    FROM 
        sales_all s
    WHERE 
        s.root_folder_id = 50
),
grouped_sales_snacks AS (
    SELECT 
        s.last_day_of_month,
        s.company_name,
        COUNT(distinct s.taviat_sale_order_id) as count_orders,
        SUM(s.total_price) as revenue
    FROM 
        sales_snacks s
    GROUP BY 
        s.last_day_of_month,
        s.company_name
), 
snacks AS (
    SELECT
        gs.last_day_of_month,
        gs.company_name,
        gs.revenue as fact_revenue_snacks,
        CASE 
            WHEN LAST_DAY(NOW())= gs.last_day_of_month
            THEN gs.count_orders/(DAY(NOW())-1)*DAY(gs.last_day_of_month)
            ELSE gs.count_orders
        END AS trend_count_orders_snacks,
        CASE 
            WHEN LAST_DAY(NOW())= gs.last_day_of_month
            THEN gs.revenue/(DAY(NOW())-1)*DAY(gs.last_day_of_month)
            ELSE gs.revenue
        END AS trend_revenue_snacks
    FROM 
        grouped_sales_snacks gs
),
sales_packaging AS (
    SELECT 
        s.taviat_sale_order_id,
        LAST_DAY(s.sbis_created_at) as last_day_of_month,
        s.company_name,
        s.total_price
    FROM 
        sales_all s
    WHERE 
        s.root_folder_id = 1070
),
grouped_sales_packaging AS (
    SELECT 
        s.last_day_of_month,
        s.company_name,
        COUNT(distinct s.taviat_sale_order_id) as count_orders,
        SUM(s.total_price) as revenue
    FROM 
        sales_packaging s
    GROUP BY 
        s.last_day_of_month,
        s.company_name
), 
packaging AS (
    SELECT
        gs.last_day_of_month,
        gs.company_name,
        gs.revenue as fact_revenue_packaging,
        CASE 
            WHEN LAST_DAY(NOW())= gs.last_day_of_month
            THEN gs.count_orders/(DAY(NOW())-1)*DAY(gs.last_day_of_month)
            ELSE gs.count_orders
        END AS trend_count_orders_packaging,
        CASE 
            WHEN LAST_DAY(NOW())= gs.last_day_of_month
            THEN gs.revenue/(DAY(NOW())-1)*DAY(gs.last_day_of_month)
            ELSE gs.revenue
        END AS trend_revenue_packaging
    FROM 
        grouped_sales_packaging gs
)
SELECT 
    pl.last_day_of_month,
    pl.company_name,
    COALESCE(pl.plan_count_orders,0) as plan_count_orders, 
    COALESCE(rc.trend_count_orders,0) as trend_count_orders,
    COALESCE(rc.fact_count_orders,0) as fact_count_orders,
    COALESCE(pl.plan_revenue,0) as plan_revenue,
    COALESCE(rc.trend_revenue,0) as trend_revenue,
    COALESCE(rc.fact_revenue,0) as fact_revenue,
    COALESCE(l.trend_count_orders_liters,0) as trend_count_orders_liters,
    COALESCE(l.trend_liters,0) as trend_liters,
    COALESCE(l.fact_liters,0) as fact_liters,
    COALESCE(pl.plan_liters,0) as plan_liters,
    COALESCE(l.trend_revenue_liters,0) as trend_revenue_liters,
    COALESCE(sn.trend_count_orders_snacks,0) as trend_count_orders_snacks,
    COALESCE(pl.plan_revenue_snacks,0) as plan_revenue_snacks,
    COALESCE(sn.trend_revenue_snacks,0) as trend_revenue_snacks,
    COALESCE(sn.fact_revenue_snacks,0) as fact_revenue_snacks,
    COALESCE(p.trend_count_orders_packaging,0) as trend_count_orders_packaging,
    COALESCE(pl.plan_revenue_packaging,0) as plan_revenue_packaging,
    COALESCE(p.trend_revenue_packaging,0) as trend_revenue_packaging,
    COALESCE(p.fact_revenue_packaging,0) as  fact_revenue_packaging,
    NOW() as calculated_at
FROM 
    filtered_plan pl
LEFT JOIN
    revenue_count rc ON 
    pl.last_day_of_month =rc.last_day_of_month 
    AND pl.company_name =rc.company_name 
LEFT JOIN 
    liters l ON 
    rc.last_day_of_month =l.last_day_of_month 
    AND rc.company_name =l.company_name 
LEFT JOIN 
    snacks sn ON 
    rc.last_day_of_month =sn.last_day_of_month 
    AND rc.company_name =sn.company_name 
LEFT JOIN 
    packaging p ON 
    rc.last_day_of_month =p.last_day_of_month 
    AND rc.company_name =p.company_name
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
    """Возвращает SQL запрос для конкретного месяца"""
    month_start = month_date.replace(day=1)
    month_end = month_start.replace(day=calendar.monthrange(month_start.year, month_start.month)[1])
    
    # Формируем условие WHERE для конкретного месяца
    where_clause = f"WHERE pm.`date` >= '{month_start.strftime('%Y-%m-%d')}' AND pm.`date` <= '{month_end.strftime('%Y-%m-%d')}'"
    
    # Подставляем условие в шаблон
    sql_query = BASE_SQL_TEMPLATE.replace("{plan_where_clause}", where_clause)
    
    logger.info(f"Generated SQL for month: {month_start.strftime('%Y-%m')}")
    return sql_query

# Функция для получения SQL запроса за диапазон месяцев
def get_sql_query_for_date_range(start_date, end_date):
    """Возвращает SQL запрос для диапазона дат"""
    # Формируем условие WHERE для диапазона дат
    where_clause = f"WHERE pm.`date` >= '{start_date.strftime('%Y-%m-%d')}' AND pm.`date` <= '{end_date.strftime('%Y-%m-%d')}'"
    
    # Подставляем условие в шаблон
    sql_query = BASE_SQL_TEMPLATE.replace("{plan_where_clause}", where_clause)
    
    logger.info(f"Generated SQL for date range: {start_date.strftime('%Y-%m-%d')} to {end_date.strftime('%Y-%m-%d')}")
    return sql_query

def get_sql_query_all_data():
    """Возвращает SQL запрос для всех данных (без фильтрации)"""
    # Для всех данных используем пустое условие WHERE
    sql_query = BASE_SQL_TEMPLATE.replace("{plan_where_clause}", "")
    
    logger.info("Generated SQL for all data")
    return sql_query

# Функция для проверки состояния таблицы в ClickHouse
def check_table_status(**kwargs):
    """
    Проверяет состояние таблицы в ClickHouse
    """
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
            WHERE toStartOfMonth(last_day_of_month) = toStartOfMonth(toDate('{current_month.strftime("%Y-%m-%d")}'))
            AND last_day_of_month >= toDate('2024-01-01')  -- Только разумные даты
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
            WHERE last_day_of_month >= toDate('2024-01-01')  -- Только разумные даты
            """
            has_any_data = client.execute(count_sql)[0][0] > 0
            
            if has_any_data:
                # Получаем последний месяц с данными
                last_month_sql = f"""
                SELECT MAX(last_day_of_month) as last_month 
                FROM {TABLE_NAME}
                WHERE last_day_of_month >= toDate('2024-01-01')  -- Только разумные даты
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
                    
                    # Находим месяцы для заполнения (не более 12 месяцев назад)
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
                        logger.info(f"Will fill gap with {len(months_list)} months")
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
    
    # Обрабатываем дату
    if 'last_day_of_month' in df_converted.columns:
        df_converted['last_day_of_month'] = pd.to_datetime(df_converted['last_day_of_month'])
    
    if 'calculated_at' in df_converted.columns:
        df_converted['calculated_at'] = pd.to_datetime(df_converted['calculated_at'])
    
    # VARCHAR/TEXT колонки
    string_cols = ['company_name']
    
    for col in string_cols:
        if col in df_converted.columns:
            df_converted[col] = df_converted[col].fillna('').astype(str)
    
    # Числовые колонки (целые и десятичные)
    numeric_cols = [
        'plan_count_orders', 'trend_count_orders', 'fact_count_orders',
        'plan_revenue', 'trend_revenue', 'fact_revenue',
        'trend_count_orders_liters', 'trend_liters', 'fact_liters', 'plan_liters', 'trend_revenue_liters',
        'trend_count_orders_snacks', 'plan_revenue_snacks', 'trend_revenue_snacks', 'fact_revenue_snacks',
        'trend_count_orders_packaging', 'plan_revenue_packaging', 'trend_revenue_packaging', 'fact_revenue_packaging'
    ]
    
    for col in numeric_cols:
        if col in df_converted.columns:
            try:
                df_converted[col] = pd.to_numeric(df_converted[col], errors='coerce').fillna(0)
                # Для денежных значений округляем до 2 знаков
                if 'revenue' in col.lower() or 'price' in col.lower():
                    df_converted[col] = df_converted[col].round(2)
                # Для литров округляем до 3 знаков
                elif 'liters' in col.lower():
                    df_converted[col] = df_converted[col].round(3)
                # Для количества заказов - целые числа
                elif 'count' in col.lower():
                    df_converted[col] = df_converted[col].astype(np.int64)
            except Exception as e:
                logger.warning(f"Error converting {col} to numeric: {e}")
                df_converted[col] = 0
    
    logger.info("Analytics data type conversion completed")
    return df_converted

# Подготовка данных для вставки в ClickHouse
def prepare_analytics_data_for_insert(df: pd.DataFrame, load_batch_id: str = '') -> List[Tuple]:
    """Подготавливает аналитические данные для вставки с оптимизацией памяти"""
    data = []
    processed_rows = 0
    skipped_rows = 0

    for _, row in df.iterrows():
        try:
            # Проверяем наличие необходимых колонок
            required_cols = ['last_day_of_month', 'company_name']
            
            # Проверяем, что все необходимые колонки присутствуют
            missing_cols = [col for col in required_cols if col not in df.columns]
            if missing_cols:
                logger.warning(f"Missing columns in row: {missing_cols}")
                skipped_rows += 1
                continue
            
            # Проверяем ключевые поля
            if pd.isna(row['last_day_of_month']) or pd.isna(row['company_name']):
                logger.warning(f"Missing key fields in row")
                skipped_rows += 1
                continue
            
            # Преобразуем данные в правильные типы с обработкой NaN
            item = (
                row['last_day_of_month'] if pd.notna(row['last_day_of_month']) else None,
                str(row['company_name']) if pd.notna(row['company_name']) else '',
                int(row['plan_count_orders']) if pd.notna(row['plan_count_orders']) else 0,
                int(row['trend_count_orders']) if pd.notna(row['trend_count_orders']) else 0,
                int(row['fact_count_orders']) if pd.notna(row['fact_count_orders']) else 0,
                float(row['plan_revenue']) if pd.notna(row['plan_revenue']) else 0.0,
                float(row['trend_revenue']) if pd.notna(row['trend_revenue']) else 0.0,
                float(row['fact_revenue']) if pd.notna(row['fact_revenue']) else 0.0,
                int(row['trend_count_orders_liters']) if pd.notna(row['trend_count_orders_liters']) else 0,
                float(row['trend_liters']) if pd.notna(row['trend_liters']) else 0.0,
                float(row['fact_liters']) if pd.notna(row['fact_liters']) else 0.0,
                float(row['plan_liters']) if pd.notna(row['plan_liters']) else 0.0,
                float(row['trend_revenue_liters']) if pd.notna(row['trend_revenue_liters']) else 0.0,
                int(row['trend_count_orders_snacks']) if pd.notna(row['trend_count_orders_snacks']) else 0,
                float(row['plan_revenue_snacks']) if pd.notna(row['plan_revenue_snacks']) else 0.0,
                float(row['trend_revenue_snacks']) if pd.notna(row['trend_revenue_snacks']) else 0.0,
                float(row['fact_revenue_snacks']) if pd.notna(row['fact_revenue_snacks']) else 0.0,
                int(row['trend_count_orders_packaging']) if pd.notna(row['trend_count_orders_packaging']) else 0,
                float(row['plan_revenue_packaging']) if pd.notna(row['plan_revenue_packaging']) else 0.0,
                float(row['trend_revenue_packaging']) if pd.notna(row['trend_revenue_packaging']) else 0.0,
                float(row['fact_revenue_packaging']) if pd.notna(row['fact_revenue_packaging']) else 0.0,
                row['calculated_at'] if pd.notna(row['calculated_at']) else None,
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
            export_path = f"/tmp/analytics_all_{execution_date.strftime('%Y%m%d_%H%M%S')}.parquet"
            
            sql_query = get_sql_query_all_data()
            
        elif months_to_process == 'update_current':
            # Обновляем только текущий месяц
            current_month = execution_date.replace(day=1)
            logger.info(f"Updating current month: {current_month.strftime('%Y-%m')}")
            export_path = f"/tmp/analytics_{current_month.strftime('%Y%m')}_{execution_date.strftime('%Y%m%d_%H%M%S')}.parquet"
            
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
            export_path = f"/tmp/analytics_gap_{execution_date.strftime('%Y%m%d_%H%M%S')}.parquet"
            
            # Для нескольких месяцев используем общий запрос с диапазоном
            start_date = min(months_list)
            end_date = max(months_list) + timedelta(days=32)
            end_date = end_date.replace(day=1) - timedelta(days=1)

            sql_query = get_sql_query_for_date_range(start_date, end_date)
        
        else:
            logger.error(f"Unknown processing mode: {months_to_process}")
            return
        
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
                    last_day_of_month Date,
                    company_name String,
                    plan_count_orders UInt64,
                    trend_count_orders UInt64,
                    fact_count_orders UInt64,
                    plan_revenue Float64,
                    trend_revenue Float64,
                    fact_revenue Float64,
                    trend_count_orders_liters UInt64,
                    trend_liters Float64,
                    fact_liters Float64,
                    plan_liters Float64,
                    trend_revenue_liters Float64,
                    trend_count_orders_snacks UInt64,
                    plan_revenue_snacks Float64,
                    trend_revenue_snacks Float64,
                    fact_revenue_snacks Float64,
                    trend_count_orders_packaging UInt64,
                    plan_revenue_packaging Float64,
                    trend_revenue_packaging Float64,
                    fact_revenue_packaging Float64,
                    calculated_at DateTime,
                    load_batch_id String,
                    created_at DateTime DEFAULT now()
                ) ENGINE = MergeTree()  
                PARTITION BY toYYYYMM(last_day_of_month)
                ORDER BY (last_day_of_month, company_name)
                SETTINGS index_granularity = 8192
                """
                client.execute(create_table_sql)
                logger.info("Table created successfully")
            
            # В зависимости от режима обработки выполняем разные действия
            if processing_mode == 'update_current':
                # Удаляем данные за текущий месяц перед вставкой
                current_month = execution_date.replace(day=1)
                if months_list_dates:
                    # Берем первый месяц из списка (должен быть текущий)
                    target_month = datetime.strptime(months_list_dates[0], '%Y-%m-%d').replace(day=1)
                else:
                    target_month = current_month
                
                logger.info(f"Deleting existing data for month {target_month.strftime('%Y-%m')}")
                
                delete_sql = f"""
                ALTER TABLE {TABLE_NAME} 
                DELETE WHERE toStartOfMonth(last_day_of_month) = toStartOfMonth(toDate('{target_month.strftime("%Y-%m-%d")}'))
                """
                client.execute(delete_sql)
                logger.info(f"Deleted old records for month {target_month.strftime('%Y-%m')}")
            
            # Для режима 'all' не удаляем ничего - таблица либо пустая, либо пересоздается
            
            # Подготавливаем данные для вставки
            load_batch_id = f"batch_{execution_date.strftime('%Y%m%d_%H%M%S')}_{processing_mode}"
            insert_sql = f"""
            INSERT INTO {TABLE_NAME} (
                last_day_of_month, company_name, plan_count_orders, trend_count_orders, 
                fact_count_orders, plan_revenue, trend_revenue, fact_revenue,
                trend_count_orders_liters, trend_liters, fact_liters, plan_liters, 
                trend_revenue_liters, trend_count_orders_snacks, plan_revenue_snacks, 
                trend_revenue_snacks, fact_revenue_snacks, trend_count_orders_packaging, 
                plan_revenue_packaging, trend_revenue_packaging, fact_revenue_packaging,
                calculated_at, load_batch_id
            ) VALUES
            """

            # Обрабатываем данные чанками - ИСПРАВЛЕННЫЙ КОД
            chunk_size = 50000
            total_inserted = 0
            
            # Способ 1: Чтение всего файла и разбивка на чанки
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
                MIN(last_day_of_month) as min_date,
                MAX(last_day_of_month) as max_date
            FROM {TABLE_NAME}
            """
            date_range = client.execute(date_range_sql)[0]
            logger.info(f"Date range in table: {date_range[0]} to {date_range[1]}")
            
            # 3. Количество месяцев
            months_count_sql = f"""
            SELECT COUNT(DISTINCT toStartOfMonth(last_day_of_month)) as month_count
            FROM {TABLE_NAME}
            """
            months_count = client.execute(months_count_sql)[0][0]
            logger.info(f"Total months in table: {months_count}")
    
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
        chat_id='-5034366843',
        text=f'❌ Даг {context["dag"].dag_id} Задача {context["task_instance"].task_id} упала с ошибкой: {context["exception"]}',
        dag=context['dag']
    )
    send_telegram.execute(context=context)
    
    # Отправка email-уведомления
    send_email = EmailOperator(
        task_id='send_email_failure',
        to='alerts@sbis18.ru',
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
        chat_id='-5034366843',
        text=f'✅ DAG {context["dag"].dag_id} успешно выполнен!',
        dag=context['dag']
    )
    send_telegram.execute(context=context)
    
    # Отправка email-уведомления
    send_email = EmailOperator(
        task_id='send_email_success',
        to='alerts@sbis18.ru',
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
    'email': ['alerts@sbis18.ru'],
    'retries': 3,
    'retry_delay': timedelta(minutes=3),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'on_failure_callback':task_failure_callback,
}

# В определении DAG добавляем новую задачу проверки статуса таблицы
with DAG(
    dag_id='pp_trend_plan',
    start_date=datetime(2024, 10, 1, 0, 20),
    schedule='20 0 * * *',  # Ежедневно в 3:20 МСК
    catchup=False,
    tags=['analytics', 'sales', 'reports', 'mariadb', 'clickhouse'],
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
    check_table >>  extract >> load >> cleanup
