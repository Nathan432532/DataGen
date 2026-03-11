"""
DAG: fluvius_energy
────────────────────
ETL pipeline for Fluvius electricity consumption (parquet ingestion).

Tasks:  extract  →  load_raw  →  validate  →  transform_clean
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

TEMP_FILE = "/tmp/fluvius_energy.parquet"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── task callables ───────────────────────────────────────────────
def _extract(**ctx):
    from src.fluvius.extract import extract_fluvius_data

    df = extract_fluvius_data()
    df.to_parquet(TEMP_FILE, index=False)
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _load_raw(**ctx):
    import pandas as pd
    from src.fluvius.load import load_raw_fluvius

    df = pd.read_parquet(TEMP_FILE)
    run_id = ctx["run_id"]
    load_raw_fluvius(df, run_id=run_id)


def _validate(**ctx):
    from src.common.validation import (
        validate_table_exists,
        validate_row_count,
        validate_columns,
    )

    validate_table_exists("raw", "raw_fluvius_energy")
    validate_row_count("raw", "raw_fluvius_energy")
    validate_columns("raw", "raw_fluvius_energy", ["ingested_at", "source", "run_id"])


def _transform_clean(**ctx):
    from src.fluvius.transform import transform_fluvius_to_clean

    transform_fluvius_to_clean()


# ── DAG ──────────────────────────────────────────────────────────
with DAG(
    dag_id="fluvius_energy",
    default_args=default_args,
    description="Fluvius energy consumption (parquet) → raw → clean",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["energy", "fluvius"],
) as dag:

    t_extract = PythonOperator(task_id="extract", python_callable=_extract)
    t_load    = PythonOperator(task_id="load_raw", python_callable=_load_raw)
    t_val     = PythonOperator(task_id="validate", python_callable=_validate)
    t_clean   = PythonOperator(task_id="transform_clean", python_callable=_transform_clean)

    t_extract >> t_load >> t_val >> t_clean
