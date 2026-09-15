from datetime import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator


with DAG(
    dag_id="sw_amritsar_sql_test_dag",
    start_date=datetime(2026, 9, 7),
    schedule=None,
    catchup=False,
    tags=["testing", "postgres"],
) as dag:

    test_query = PostgresOperator(
        task_id="select_bills_summary",
        postgres_conn_id="postgres_default",
        sql="""
            SELECT *
            FROM public.mv_sw_amritsar_bills_summary;
        """,
    )