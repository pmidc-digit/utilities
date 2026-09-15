from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sdk import dag, task

log = logging.getLogger(__name__)
SOURCE_CONNECTION_ID = "source_postgres"


@dag(
    dag_id="remote_postgres_pipeline",
    schedule="0 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["postgres", "pgbouncer"],
)
def remote_postgres_pipeline():
    @task(retries=3, retry_delay=timedelta(seconds=60))
    def verify_source() -> dict[str, str]:
        hook = PostgresHook(postgres_conn_id=SOURCE_CONNECTION_ID)
        row = hook.get_first(
            "SELECT current_database(), current_user, current_setting('server_version')"
        )
        result = {"database": row[0], "user": row[1], "server_version": row[2]}
        log.info("Source connection verified: %s", result)
        return result

    @task(retries=2, retry_delay=timedelta(seconds=60))
    def process_data(_: dict[str, str]) -> None:
        hook = PostgresHook(postgres_conn_id=SOURCE_CONNECTION_ID)
        rows = hook.get_records("SELECT now() AS observed_at LIMIT 1")
        log.info("Fetched %d row(s) from the source database", len(rows))

    process_data(verify_source())


remote_postgres_pipeline()
