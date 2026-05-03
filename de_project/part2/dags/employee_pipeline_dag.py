"""
Part 2 DAG: Real-Time Employee Records Processing Pipeline

Uses a FileSensor to watch the input folder.
When a .csv or .xlsx file appears, the pipeline activates:
  FileSensor → Read → Validate → Process → BackupValidate → Write → Cleanup
"""

# pylint: disable=duplicate-code, trailing-newlines
import glob
import logging
import os
import shutil
import sys
from datetime import datetime

sys.path.insert(0, "/opt/airflow/dags/part2")

import pandas as pd  # pylint: disable=import-error
from airflow import DAG  # pylint: disable=import-error
from airflow.operators.python import PythonOperator  # pylint: disable=import-error
from airflow.sensors.filesystem import FileSensor  # pylint: disable=import-error
from backup_validator_writer import (
    EmployeeBackupValidator,
    EmployeeWriter,
)  # pylint: disable=import-error, wrong-import-position
from processor import (
    EmployeeProcessor,
)  # pylint: disable=import-error, wrong-import-position
from reader import EmployeeReader  # pylint: disable=import-error, wrong-import-position
from validator import (
    EmployeeValidator,
)  # pylint: disable=import-error, wrong-import-position


# ------------------------------------------------------------------ #
#  Configuration
# ------------------------------------------------------------------ #

WATCH_DIR = "/opt/airflow/dags/part2/input"
OUTPUT_DIR = "/opt/airflow/dags/part2/output"
TEMP_DIR = "/opt/airflow/dags/part2/tmp"
AZURE_CONNECTION_STR = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", None)
AZURE_CONTAINER = "employee-output"

logger = logging.getLogger(__name__)


# ------------------------------------------------------------------ #
#  Helpers
# ------------------------------------------------------------------ #


def _save_df(df, prefix: str) -> str:
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


def task_find_file(**context):
    """Find the newest CSV or XLSX file in the input folder."""
    files = glob.glob(os.path.join(WATCH_DIR, "*.csv")) + glob.glob(
        os.path.join(WATCH_DIR, "*.xlsx")
    )
    # Exclude the archived subfolder
    files = [f for f in files if "archived" not in f]

    if not files:
        raise FileNotFoundError(f"No CSV/XLSX files found in {WATCH_DIR}")

    latest = max(files, key=os.path.getmtime)
    logger.info("Found file: %s", latest)
    context["ti"].xcom_push(key="input_file", value=latest)


def task_read(**context):
    """Airflow task to read employee source records."""
    file_path = context["ti"].xcom_pull(key="input_file", task_ids="find_file")
    df = EmployeeReader(file_path).read()
    path = _save_df(df, "after_read")
    context["ti"].xcom_push(key="df_path", value=path)
    logger.info("Read task complete. Shape: %s", df.shape)


def task_validate(**context):
    """Airflow task to run domain data quality checks on raw records."""
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="read")
    df = _load_df(path_in)
    df = EmployeeValidator().validate(df)
    path_out = _save_df(df, "after_validate")
    context["ti"].xcom_push(key="df_path", value=path_out)
    _cleanup(path_in)
    logger.info("Validate task complete. Shape: %s", df.shape)


def task_process(**context):
    """Airflow task to compute and enrich employee records."""
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="validate")
    df = _load_df(path_in)
    df = EmployeeProcessor().process(df)
    path_out = _save_df(df, "after_process")
    context["ti"].xcom_push(key="df_path", value=path_out)
    _cleanup(path_in)
    logger.info("Process task complete. Shape: %s", df.shape)


def task_backup_validate(**context):
    """Airflow task to run post-processing consistency validation checks."""
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="process")
    df = _load_df(path_in)
    passed = EmployeeBackupValidator().validate(df)
    if not passed:
        _cleanup(path_in)
        raise ValueError("Backup validation failed — halting pipeline.")
    context["ti"].xcom_push(key="df_path", value=path_in)
    logger.info("Backup validation task complete.")


def task_write(**context):
    """Airflow task to write the final enriched employee data to disk and cloud."""
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="backup_validate")
    df = _load_df(path_in)
    writer = EmployeeWriter(OUTPUT_DIR, AZURE_CONNECTION_STR, AZURE_CONTAINER)
    output_path = writer.write(df)
    _cleanup(path_in)
    logger.info("Write complete: %s", output_path)


def task_cleanup(**context):
    """Move processed input file to archived/ so FileSensor doesn't re-trigger."""
    file_path = context["ti"].xcom_pull(key="input_file", task_ids="find_file")
    if not file_path:
        logger.warning("No input file path found in XCom — skipping cleanup.")
        return
    archive_dir = os.path.join(WATCH_DIR, "archived")
    os.makedirs(archive_dir, exist_ok=True)
    dest = os.path.join(archive_dir, os.path.basename(file_path))
    # If a file with the same name was already archived, add a timestamp suffix
    if os.path.exists(dest):
        name, ext = os.path.splitext(os.path.basename(file_path))
        dest = os.path.join(
            archive_dir, f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}"
        )
    shutil.move(file_path, dest)
    logger.info("Archived input file to: %s", dest)


# ------------------------------------------------------------------ #
#  DAG definition
# ------------------------------------------------------------------ #

with DAG(
    dag_id="employee_realtime_pipeline",
    description="Part 2: Real-time employee data processing with FileSensor",
    start_date=datetime(2026, 5, 5),  # Defence date
    schedule="*/2 * * * *",
    catchup=False,
    tags=["data-engineering", "part2", "realtime"],
) as dag:

    sense_file = FileSensor(
        task_id="sense_file",
        filepath=os.path.join(WATCH_DIR, "*.csv"),
        poke_interval=20,
        timeout=119,  # just under 2 min so it exits before next run starts
        mode="reschedule",  # releases worker slot between pokes — no hanging
        soft_fail=True,  # no file found = skipped (not failed), run ends cleanly
        fs_conn_id="fs_default",
    )

    find_file = PythonOperator(task_id="find_file", python_callable=task_find_file)
    read = PythonOperator(task_id="read", python_callable=task_read)
    validate = PythonOperator(task_id="validate", python_callable=task_validate)
    process = PythonOperator(task_id="process", python_callable=task_process)
    backup_validate = PythonOperator(
        task_id="backup_validate", python_callable=task_backup_validate
    )
    write = PythonOperator(task_id="write", python_callable=task_write)
    cleanup = PythonOperator(task_id="cleanup", python_callable=task_cleanup)

    (  # pylint: disable=pointless-statement
        sense_file
        >> find_file
        >> read
        >> validate
        >> process
        >> backup_validate
        >> write
        >> cleanup
    )
