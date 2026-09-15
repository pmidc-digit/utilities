from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


with DAG(
    dag_id="sw_amritsar_sql_test_dag",
    start_date=datetime(2026, 9, 7),
    schedule=None,
    catchup=False,
    tags=["testing", "postgres"],
) as dag:

    test_query = SQLExecuteQueryOperator(
        task_id="select_bills_summary",
        conn_id="source_postgre",
        sql="""
            SELECT *
            FROM public.mv_sw_amritsar_bills_summary
            LIMIT 10;
        """,
    )
