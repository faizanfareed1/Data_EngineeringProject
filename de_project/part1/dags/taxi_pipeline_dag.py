"""
Airflow DAG for running the Part 1: Yellow Taxi Batch Processing Pipeline.
"""

# pylint: disable=duplicate-code, trailing-newlines
import logging
import os
import sys
from datetime import datetime

import pandas as pd  # pylint: disable=import-error
from airflow import DAG  # pylint: disable=import-error
from airflow.operators.python import PythonOperator  # pylint: disable=import-error

sys.path.insert(0, "/opt/airflow/dags/part1")

from backup_validator import (
    TaxiBackupValidator,
)  # pylint: disable=import-error, wrong-import-position
from processor import (
    TaxiProcessor,
)  # pylint: disable=import-error, wrong-import-position
from reader import TaxiReader  # pylint: disable=import-error, wrong-import-position
from validator import (
    TaxiValidator,
)  # pylint: disable=import-error, wrong-import-position
from writer import TaxiWriter  # pylint: disable=import-error, wrong-import-position

# ------------------------------------------------------------------ #
#  Configuration — update these before running
# ------------------------------------------------------------------ #

INPUT_FILE = "/opt/airflow/dags/part1/input/yellow_tripdata_2025-01.parquet"
OUTPUT_DIR = "/opt/airflow/dags/part1/output"
TEMP_DIR = "/opt/airflow/dags/part1/tmp"

AZURE_CONNECTION_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", None)
AZURE_CONTAINER = "taxi-output"

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Helpers — file-based inter-task handoff (avoids XCom size limits)
# ------------------------------------------------------------------ #


def _save_df(df, prefix: str) -> str:
    """Persist DataFrame as a parquet file; return its path."""
    os.makedirs(TEMP_DIR, exist_ok=True)
    path = os.path.join(TEMP_DIR, f"{prefix}.parquet")
    df.to_parquet(path, index=False)
    return path


def _load_df(path: str):
    return pd.read_parquet(path)


def _cleanup(path: str) -> None:
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception as e:  # pylint: disable=broad-exception-caught
        logger.warning("Could not remove temp file %s: %s", path, e)


# ------------------------------------------------------------------ #
#  Task functions
# ------------------------------------------------------------------ #


def task_read(**context):
    """Airflow task to read the parquet source data."""
    reader = TaxiReader(INPUT_FILE)
    df = reader.read()
    path = _save_df(df, "after_read")
    context["ti"].xcom_push(key="df_path", value=path)
    logger.info("Read task complete. Shape: %s. Saved to: %s", df.shape, path)


def task_validate(**context):
    """Airflow task to run basic domain validation checks on the source data."""
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="read")
    df = _load_df(path_in)
    df = TaxiValidator().validate(df)
    path_out = _save_df(df, "after_validate")
    context["ti"].xcom_push(key="df_path", value=path_out)
    _cleanup(path_in)
    logger.info("Validate task complete. Shape: %s", df.shape)


def task_process(**context):
    """Airflow task to compute and add derived metrics to the data."""
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="validate")
    df = _load_df(path_in)
    df = TaxiProcessor().process(df)
    path_out = _save_df(df, "after_process")
    context["ti"].xcom_push(key="df_path", value=path_out)
    _cleanup(path_in)
    logger.info("Process task complete. Shape: %s", df.shape)


def task_backup_validate(**context):
    """Airflow task to run post-processing consistency validation checks."""
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="process")
    df = _load_df(path_in)
    passed = TaxiBackupValidator().validate(df)
    if not passed:
        _cleanup(path_in)
        raise ValueError(
            "Backup validation failed — pipeline halted. Check logs for details."
        )
    context["ti"].xcom_push(key="df_path", value=path_in)
    logger.info("Backup validation task complete.")


def task_write(**context):
    """Airflow task to write output locally and to cloud storage."""
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="backup_validate")
    df = _load_df(path_in)
    writer = TaxiWriter(
        local_output_dir=OUTPUT_DIR,
        azure_connection_str=AZURE_CONNECTION_STR,
        azure_container=AZURE_CONTAINER,
    )
    output_path = writer.write(df)
    _cleanup(path_in)
    logger.info("Write task complete. Output: %s", output_path)


# ------------------------------------------------------------------ #
#  DAG definition
# ------------------------------------------------------------------ #

with DAG(
    dag_id="yellow_taxi_batch_pipeline",
    description="Part 1: Yellow Taxi Trip Records batch processing pipeline",
    start_date=datetime(2026, 5, 5),  # Defence date
    schedule="@once",
    catchup=False,
    tags=["data-engineering", "part1", "batch"],
) as dag:

    read = PythonOperator(task_id="read", python_callable=task_read)
    validate = PythonOperator(task_id="validate", python_callable=task_validate)
    process = PythonOperator(task_id="process", python_callable=task_process)
    backup_validate = PythonOperator(
        task_id="backup_validate", python_callable=task_backup_validate
    )
    write = PythonOperator(task_id="write", python_callable=task_write)

    (
        read >> validate >> process >> backup_validate >> write
    )  # pylint: disable=pointless-statement
