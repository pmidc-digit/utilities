import airflow
import logging
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.utils.dates import days_ago


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2017, 1, 24)

}

dag_psql = DAG(
    dag_id = "postgresoperator_demo",
    default_args=default_args,
    schedule_interval=None
)


select_data = PostgresOperator(
    task_id="get_citizen_count",
    postgres_conn_id="postgres_qa",
    sql="select count(*) from eg_user where type = 'CITIZEN';",
    dag = dag_psql
)

select_data


