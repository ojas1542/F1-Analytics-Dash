"""
Periodic dbt refresh, decoupled from how raw data actually lands.

Kafka Connect's Snowflake sink streams car_data/laps/pit/race_control
straight into the raw tables (from historic-producer's replay or
live-producer's live sim) without ever invoking dbt -- only the
f1_historical_batch DAG's own COPY INTO step triggers a dbt run, and only
for that batch path. This DAG just rebuilds every model (staging through
marts) on a fixed interval so data arriving via Kafka gets picked up too,
independent of which producer put it there. `dbt build` (not `dbt run`) so
schema tests execute as a pipeline gate, not just dead schema.yml
declarations; `dbt deps` first since packages aren't vendored.
"""

from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag
from airflow.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"


@dag(
    dag_id="f1_dbt_refresh",
    description="Rebuild dbt staging models on a fixed interval, regardless of ingestion path",
    schedule="*/15 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["f1", "dbt", "streaming"],
)
def f1_dbt_refresh():
    BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt deps --project-dir {PROJECT_DIR} --profiles-dir {PROJECT_DIR} && "
            f"dbt build --project-dir {PROJECT_DIR} --profiles-dir {PROJECT_DIR}"
        ),
    )


f1_dbt_refresh()
