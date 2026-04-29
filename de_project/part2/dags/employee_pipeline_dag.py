"""
Part 2 DAG: Real-Time Employee Records Processing Pipeline

Uses a FileSensor to watch the input folder.
When a .csv or .xlsx file appears, the pipeline activates:
  FileSensor → Read → Validate → Process → BackupValidate → Write → Cleanup

NOTE: FileSensor can only watch one glob pattern at a time. We watch for *.csv
by default. To also catch .xlsx, set up a second DAG or use the find_file task
which already checks both extensions.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from datetime import datetime
import logging
import os
import sys
import glob
import shutil

sys.path.insert(0, "/opt/airflow/dags/part2")

from reader import EmployeeReader
from validator import EmployeeValidator
from processor import EmployeeProcessor
from backup_validator_writer import EmployeeBackupValidator, EmployeeWriter

# ------------------------------------------------------------------ #
#  Configuration
# ------------------------------------------------------------------ #

WATCH_DIR  = "/opt/airflow/dags/part2/input"
OUTPUT_DIR = "/opt/airflow/dags/part2/output"
TEMP_DIR   = "/opt/airflow/dags/part2/tmp"
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
    import pandas as pd
    return pd.read_parquet(path)


def _cleanup(path: str) -> None:
    try:
        if path and os.path.exists(path):
            os.remove(path)
    except Exception as e:
        logger.warning(f"Could not remove temp file {path}: {e}")


# ------------------------------------------------------------------ #
#  Task functions
# ------------------------------------------------------------------ #

def task_find_file(**context):
    """Find the newest CSV or XLSX file in the input folder."""
    files = (
        glob.glob(os.path.join(WATCH_DIR, "*.csv")) +
        glob.glob(os.path.join(WATCH_DIR, "*.xlsx"))
    )
    # Exclude the archived subfolder
    files = [f for f in files if "archived" not in f]

    if not files:
        raise FileNotFoundError(f"No CSV/XLSX files found in {WATCH_DIR}")

    latest = max(files, key=os.path.getmtime)
    logger.info(f"Found file: {latest}")
    context["ti"].xcom_push(key="input_file", value=latest)


def task_read(**context):
    file_path = context["ti"].xcom_pull(key="input_file", task_ids="find_file")
    df = EmployeeReader(file_path).read()
    path = _save_df(df, "after_read")
    context["ti"].xcom_push(key="df_path", value=path)
    logger.info(f"Read task complete. Shape: {df.shape}")


def task_validate(**context):
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="read")
    df = _load_df(path_in)
    df = EmployeeValidator().validate(df)
    path_out = _save_df(df, "after_validate")
    context["ti"].xcom_push(key="df_path", value=path_out)
    _cleanup(path_in)
    logger.info(f"Validate task complete. Shape: {df.shape}")


def task_process(**context):
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="validate")
    df = _load_df(path_in)
    df = EmployeeProcessor().process(df)
    path_out = _save_df(df, "after_process")
    context["ti"].xcom_push(key="df_path", value=path_out)
    _cleanup(path_in)
    logger.info(f"Process task complete. Shape: {df.shape}")


def task_backup_validate(**context):
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="process")
    df = _load_df(path_in)
    passed = EmployeeBackupValidator().validate(df)
    if not passed:
        _cleanup(path_in)
        raise ValueError("Backup validation failed — halting pipeline.")
    context["ti"].xcom_push(key="df_path", value=path_in)
    logger.info("Backup validation task complete.")


def task_write(**context):
    path_in = context["ti"].xcom_pull(key="df_path", task_ids="backup_validate")
    df = _load_df(path_in)
    writer = EmployeeWriter(OUTPUT_DIR, AZURE_CONNECTION_STR, AZURE_CONTAINER)
    output_path = writer.write(df)
    _cleanup(path_in)
    logger.info(f"Write complete: {output_path}")


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
        dest = os.path.join(archive_dir, f"{name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}{ext}")
    shutil.move(file_path, dest)
    logger.info(f"Archived input file to: {dest}")


# ------------------------------------------------------------------ #
#  DAG definition
# ------------------------------------------------------------------ #

with DAG(
    dag_id="employee_realtime_pipeline",
    description="Part 2: Real-time employee data processing with FileSensor",
    start_date=datetime(2026, 4, 1),
    schedule="*/2 * * * *",   # Re-check every 2 minutes for new files
    catchup=False,
    tags=["data-engineering", "part2", "realtime"],
) as dag:

    sense_file = FileSensor(
        task_id="sense_file",
        filepath=os.path.join(WATCH_DIR, "*.csv"),  # watches for CSV files
        poke_interval=30,      # check every 30 seconds
        timeout=3600,          # give up after 1 hour
        mode="poke",
        fs_conn_id="fs_default",
    )

    find_file       = PythonOperator(task_id="find_file",       python_callable=task_find_file)
    read            = PythonOperator(task_id="read",            python_callable=task_read)
    validate        = PythonOperator(task_id="validate",        python_callable=task_validate)
    process         = PythonOperator(task_id="process",         python_callable=task_process)
    backup_validate = PythonOperator(task_id="backup_validate", python_callable=task_backup_validate)
    write           = PythonOperator(task_id="write",           python_callable=task_write)
    cleanup         = PythonOperator(task_id="cleanup",         python_callable=task_cleanup)

    sense_file >> find_file >> read >> validate >> process >> backup_validate >> write >> cleanup