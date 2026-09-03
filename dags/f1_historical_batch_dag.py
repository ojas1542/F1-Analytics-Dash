"""
Airflow DAG for the F1 historical batch pipeline.

OpenF1 API -> local disk -> Snowflake -> dbt -> Snowflake Analytics

One mapped task extracts+lands each race session on local disk (a volume
shared by every task via LocalExecutor), a second independent mapped task
does the same for year-grain dimension data (sessions, meetings), a single
task then PUTs every dataset's files into a Snowflake internal stage and
COPY INTOs its raw table, and dbt builds the analytics models on top. Task
bodies live in historical_race_batch.py so they can also be run standalone
for local backfills.
"""

from __future__ import annotations

import sys
import uuid
from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator

# Repo root is mounted as a directory (not per-file) so edits to
# historical_race_batch.py are always visible here -- see docker-compose.yml.
PROJECT_DIR = "/opt/airflow/project"
sys.path.append(PROJECT_DIR)

from historical_race_batch import (  # noqa: E402
    extract_session_locally,
    fetch_and_extract_year_dimensions,
    list_race_session_keys,
    load_all_datasets_to_snowflake,
)
from telemetryIngester import OpenF1Client  # noqa: E402


@dag(
    dag_id="f1_historical_batch",
    description="OpenF1 -> local disk -> Snowflake -> dbt historical batch load",
    schedule=None,  # manually triggered backfills only -- see `airflow dags trigger --conf`
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_tasks=4,  # LocalExecutor on a resource-capped Docker Desktop VM; keep concurrent OpenF1 fetches low
    params={"start_year": 2023, "end_year": 2025},
    tags=["f1", "batch", "openf1", "snowflake", "dbt"],
)
def f1_historical_batch():

    @task
    def new_batch_id() -> str:
        return uuid.uuid4().hex

    @task
    def list_sessions(**context) -> list[int]:
        params = context["params"]
        return list_race_session_keys(params["start_year"], params["end_year"])

    @task
    def list_years(**context) -> list[int]:
        params = context["params"]
        return list(range(params["start_year"], params["end_year"] + 1))

    @task
    def extract_locally(session_key: int, batch_id: str) -> dict[str, str]:
        return extract_session_locally(session_key, batch_id)

    @task
    def extract_dimensions(year: int, batch_id: str) -> dict[str, str]:
        return fetch_and_extract_year_dimensions(OpenF1Client(), year, batch_id)

    @task(trigger_rule="all_done")
    def load_to_snowflake(
        _extracted: list[dict[str, str]], _extracted_dims: list[dict[str, str]]
    ) -> None:
        load_all_datasets_to_snowflake()

    batch_id = new_batch_id()
    session_keys = list_sessions()
    years = list_years()
    extracted = extract_locally.partial(batch_id=batch_id).expand(session_key=session_keys)
    extracted_dims = extract_dimensions.partial(batch_id=batch_id).expand(year=years)
    loaded = load_to_snowflake(extracted, extracted_dims)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"dbt deps --project-dir {PROJECT_DIR} --profiles-dir {PROJECT_DIR} && "
            f"dbt build --project-dir {PROJECT_DIR} --profiles-dir {PROJECT_DIR}"
        ),
    )

    loaded >> dbt_run


f1_historical_batch()
