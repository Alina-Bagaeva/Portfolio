from airflow import DAG
from airflow_clickhouse_plugin.operators.clickhouse import ClickHouseOperator
from airflow.utils.dates import days_ago

with DAG(
    dag_id='click_house_work',
    start_date=days_ago(1),
    schedule_interval=None,
) as dag:
    check = ClickHouseOperator(
        task_id='select_version',
        sql='SELECT version()',
        clickhouse_conn_id='click_house_work',  # id соединения Airflow для ClickHouse
    )
