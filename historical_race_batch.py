"""
F1 historical batch pipeline: OpenF1 API -> local disk -> Snowflake.

Pure Python (no Spark). Each race session's raw records are fetched from
OpenF1, newline-delimited JSON-gzipped, and landed on local disk. Snowflake
then PUTs each dataset's files into an internal stage and COPY INTOs them
in one shot per table. Orchestration (scheduling, parallelizing session
extraction, chaining the dbt run) lives in dags/f1_historical_batch_dag.py
-- the functions here are the task bodies, importable standalone for local
backfills too.

No cloud storage required: swap in S3 later by pointing get_raw_data_dir()
and the stage at a bucket instead, without touching the extraction logic.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import uuid
from pathlib import Path
from typing import Any, Iterable

import snowflake.connector
from cryptography.hazmat.primitives import serialization

from telemetryIngester import OpenF1Client, OpenF1Error

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("f1_historical_batch")

DATASET_TO_TABLE = {
    "car_data": "raw_car_data",
    "laps": "raw_laps",
    "pit": "raw_pit",
    "race_control": "raw_race_control",
}

RACE_SESSION_NAME = "Race"


# -- OpenF1 extraction -------------------------------------------------

def list_race_session_keys(start_year: int, end_year: int) -> list[int]:
    client = OpenF1Client()
    session_keys: list[int] = []

    for year in range(start_year, end_year + 1):
        sessions = client.get_sessions(year=year, session_name=RACE_SESSION_NAME)
        keys_for_year = [s["session_key"] for s in sessions]
        logger.info("Found %d race sessions for %d", len(keys_for_year), year)
        session_keys.extend(keys_for_year)

    return session_keys


def fetch_car_data_for_session(client: OpenF1Client, session_key: int) -> list[dict[str, Any]]:
    drivers = client.get_drivers(session_key=session_key)
    all_records: list[dict[str, Any]] = []

    for driver in drivers:
        driver_number = driver["driver_number"]
        try:
            records = client.get_car_data(session_key=session_key, driver_number=driver_number)
            all_records.extend(records)
        except OpenF1Error:
            logger.exception(
                "Failed to fetch car_data for session_key=%s driver=%s; skipping",
                session_key,
                driver_number,
            )
            continue

    return all_records


def fetch_session_records(client: OpenF1Client, session_key: int) -> dict[str, list[dict[str, Any]]]:
    """Fetch every dataset for one race session. Returns {dataset_name: [records]}."""
    simple_fetchers = {
        "laps": client.get_laps,
        "pit": client.get_pit,
        "race_control": client.get_race_control,
    }

    datasets: dict[str, list[dict[str, Any]]] = {}

    try:
        datasets["car_data"] = fetch_car_data_for_session(client, session_key)
    except Exception:
        logger.exception("Failed to fetch car_data for session_key=%s", session_key)
        datasets["car_data"] = []

    for dataset_name, fetch in simple_fetchers.items():
        try:
            datasets[dataset_name] = fetch(session_key=session_key)
        except Exception:
            logger.exception("Failed to fetch %s for session_key=%s", dataset_name, session_key)
            datasets[dataset_name] = []

    return datasets


# -- local disk landing ---------------------------------------------------

def get_raw_data_dir() -> Path:
    path = Path(os.environ.get("F1_RAW_DATA_DIR", "data/raw"))
    path.mkdir(parents=True, exist_ok=True)
    return path.resolve()


def local_path_for(dataset: str, session_key: int, run_id: str) -> Path:
    return get_raw_data_dir() / dataset / f"session_key={session_key}" / f"{run_id}.ndjson.gz"


def _write_ndjson_gz(path: Path, records: Iterable[dict[str, Any]], session_key: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, mode="wt", encoding="utf-8") as gz:
        for record in records:
            gz.write(json.dumps({"session_key": str(session_key), "record_content": record}) + "\n")


def write_records_to_local(
    dataset: str,
    session_key: int,
    records: list[dict[str, Any]],
    run_id: str,
) -> Path | None:
    if not records:
        return None

    path = local_path_for(dataset, session_key, run_id)
    _write_ndjson_gz(path, records, session_key)
    logger.info("Wrote %d %s records for session_key=%s to %s", len(records), dataset, session_key, path)
    return path


def extract_session_locally(
    session_key: int,
    run_id: str,
    client: OpenF1Client = None,
) -> dict[str, str]:
    """Fetch every dataset for one session from OpenF1 and land it on local disk.

    Returns {dataset_name: file_path} for datasets that produced at least one record.
    """
    client = client or OpenF1Client()

    written: dict[str, str] = {}
    for dataset_name, records in fetch_session_records(client, session_key).items():
        path = write_records_to_local(dataset_name, session_key, records, run_id)
        if path:
            written[dataset_name] = str(path)

    return written


# -- Snowflake load (local disk -> internal stage -> COPY INTO) -----------

def get_snowflake_connection() -> snowflake.connector.SnowflakeConnection:
    account = os.environ.get("SNOWFLAKE_ACCOUNT")
    if not account:
        # Fall back to deriving the account locator from a JDBC-style URL,
        # e.g. "abc12345.us-east-1.snowflakecomputing.com" -> "abc12345.us-east-1".
        url = os.environ["SNOWFLAKE_URL"]
        account = url.replace("https://", "").replace("http://", "").split(".snowflakecomputing.com")[0]

    connect_kwargs: dict[str, Any] = {
        "account": account,
        "user": os.environ["SNOWFLAKE_USER"],
        "database": os.environ.get("SNOWFLAKE_DATABASE", "F1_ANALYTICS"),
        "schema": os.environ.get("SNOWFLAKE_SCHEMA", "RAW"),
        "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "F1_WH"),
        "login_timeout": 60,
        "network_timeout": 120,
    }

    if os.environ.get("SNOWFLAKE_ROLE"):
        connect_kwargs["role"] = os.environ["SNOWFLAKE_ROLE"]

    private_key_path = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH")
    if private_key_path:
        passphrase = os.environ.get("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
        with open(private_key_path, "rb") as f:
            private_key = serialization.load_pem_private_key(
                f.read(),
                password=passphrase.encode("utf-8") if passphrase else None,
            )
        connect_kwargs["private_key"] = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )
    elif "SNOWFLAKE_PASSWORD" in os.environ:
        connect_kwargs["password"] = os.environ["SNOWFLAKE_PASSWORD"]
    else:
        raise ValueError("Must provide either SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD")

    return snowflake.connector.connect(**connect_kwargs)


def get_stage_name() -> str:
    db = os.environ.get("SNOWFLAKE_DATABASE", "F1_ANALYTICS")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "RAW")
    return os.environ.get("SNOWFLAKE_STAGE", f"{db}.{schema}.F1_RAW_STAGE")


def ensure_stage(conn: snowflake.connector.SnowflakeConnection, stage_name: str) -> None:
    conn.cursor().execute(
        f"""
        CREATE STAGE IF NOT EXISTS {stage_name}
        FILE_FORMAT = (TYPE = JSON, COMPRESSION = 'GZIP')
        """
    )
    logger.info("Ensured internal stage %s", stage_name)


def put_dataset_files_to_stage(
    conn: snowflake.connector.SnowflakeConnection,
    dataset: str,
    stage_name: str,
) -> int:
    dataset_dir = get_raw_data_dir() / dataset
    files = list(dataset_dir.rglob("*.ndjson.gz")) if dataset_dir.exists() else []

    for path in files:
        conn.cursor().execute(
            f"PUT 'file://{path.as_posix()}' @{stage_name}/{dataset}/ AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
        )

    logger.info("Staged %d %s file(s) to @%s/%s/", len(files), dataset, stage_name, dataset)
    return len(files)


def ensure_raw_table(conn: snowflake.connector.SnowflakeConnection, qualified_table_name: str) -> None:
    conn.cursor().execute(
        f"""
        CREATE TABLE IF NOT EXISTS {qualified_table_name} (
            session_key VARCHAR,
            record_content VARCHAR
        )
        """
    )
    logger.info("Ensured table %s", qualified_table_name)


def load_dataset_to_snowflake(
    conn: snowflake.connector.SnowflakeConnection,
    dataset: str,
    stage_name: str,
) -> None:
    db = os.environ.get("SNOWFLAKE_DATABASE", "F1_ANALYTICS")
    schema = os.environ.get("SNOWFLAKE_SCHEMA", "RAW")
    table_name = DATASET_TO_TABLE[dataset]
    qualified_table_name = f"{db}.{schema}.{table_name}"

    ensure_raw_table(conn, qualified_table_name)

    conn.cursor().execute(
        f"""
        COPY INTO {qualified_table_name} (session_key, record_content)
        FROM (
            SELECT $1:session_key::string, TO_JSON($1:record_content)
            FROM @{stage_name}/{dataset}/
        )
        FILE_FORMAT = (TYPE = JSON, COMPRESSION = 'GZIP')
        ON_ERROR = 'CONTINUE'
        """
    )
    logger.info("Loaded @%s/%s/ into %s", stage_name, dataset, qualified_table_name)


def load_all_datasets_to_snowflake() -> None:
    conn = get_snowflake_connection()
    try:
        stage_name = get_stage_name()
        ensure_stage(conn, stage_name)
        for dataset in DATASET_TO_TABLE:
            if put_dataset_files_to_stage(conn, dataset, stage_name):
                load_dataset_to_snowflake(conn, dataset, stage_name)
    finally:
        conn.close()


# -- standalone orchestration (Airflow calls the pieces above directly) ---

def run(start_year: int, end_year: int, run_id: str | None = None) -> None:
    run_id = run_id or uuid.uuid4().hex

    session_keys = list_race_session_keys(start_year, end_year)
    if not session_keys:
        logger.warning("No race sessions found for %d-%d; nothing to do", start_year, end_year)
        return

    client = OpenF1Client()
    for session_key in session_keys:
        extract_session_locally(session_key, run_id, client=client)

    load_all_datasets_to_snowflake()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-year", type=int, required=True)
    parser.add_argument("--end-year", type=int, required=True)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.start_year, args.end_year)
