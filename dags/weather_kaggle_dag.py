"""
DAG: weather_kaggle_antwerp
────────────────────────────
ETL pipeline for the Kaggle *Weather in Antwerp* dataset.

Tasks:  extract  →  load_raw  →  validate  →  transform_clean
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

TEMP_FILE = "/tmp/weather_antwerp.parquet"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── task callables ───────────────────────────────────────────────
def _extract(**ctx):
    from src.weather_kaggle.extract import extract_weather_data

    df = extract_weather_data()
    df.to_parquet(TEMP_FILE, index=False)
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _load_raw(**ctx):
    import pandas as pd
    from src.weather_kaggle.load import load_raw_weather

    df = pd.read_parquet(TEMP_FILE)
    run_id = ctx["run_id"]
    load_raw_weather(df, run_id=run_id)


def _validate(**ctx):
    from src.common.validation import (
        validate_table_exists,
        validate_row_count,
        validate_columns,
    )

    validate_table_exists("raw", "raw_weather_antwerp")
    validate_row_count("raw", "raw_weather_antwerp")
    validate_columns("raw", "raw_weather_antwerp", ["ingested_at", "source", "run_id"])


def _transform_clean(**ctx):
    from src.weather_kaggle.transform import transform_weather_to_clean

    transform_weather_to_clean()


# ── DAG ──────────────────────────────────────────────────────────
with DAG(
    dag_id="weather_kaggle_antwerp",
    default_args=default_args,
    description="Kaggle Weather Antwerp → raw → clean",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["weather", "kaggle"],
) as dag:

    t_extract = PythonOperator(task_id="extract", python_callable=_extract)
    t_load    = PythonOperator(task_id="load_raw", python_callable=_load_raw)
    t_val     = PythonOperator(task_id="validate", python_callable=_validate)
    t_clean   = PythonOperator(task_id="transform_clean", python_callable=_transform_clean)

    t_extract >> t_load >> t_val >> t_clean
