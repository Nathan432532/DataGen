"""
DAG: elia_generation
─────────────────────
ETL pipeline for ELIA wind and solar forecast data (REST API).

Tasks:
Wind:  extract_wind  →  load_raw_wind  →  validate_wind  →  transform_clean_wind
Solar: extract_solar →  load_raw_solar →  validate_solar →  transform_clean_solar
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

TEMP_WIND_FILE = "/tmp/elia_wind.parquet"
TEMP_SOLAR_FILE = "/tmp/elia_solar.parquet"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── WIND task callables ──────────────────────────────────────────
def _extract_wind(**ctx):
    from src.elia.extract import extract_elia_wind_data

    df = extract_elia_wind_data()
    df.to_parquet(TEMP_WIND_FILE, index=False)
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _load_raw_wind(**ctx):
    import pandas as pd
    from src.elia.load import load_raw_elia_wind

    df = pd.read_parquet(TEMP_WIND_FILE)
    run_id = ctx["run_id"]
    load_raw_elia_wind(df, run_id=run_id)


def _validate_wind(**ctx):
    from src.common.validation import (
        validate_table_exists,
        validate_row_count,
        validate_columns,
    )

    validate_table_exists("raw", "raw_elia_wind")
    validate_row_count("raw", "raw_elia_wind")
    validate_columns("raw", "raw_elia_wind", ["ingested_at", "source", "run_id"])


def _transform_clean_wind(**ctx):
    from src.elia.transform import transform_elia_wind_to_clean

    transform_elia_wind_to_clean()


# ── SOLAR task callables ─────────────────────────────────────────
def _extract_solar(**ctx):
    from src.elia.extract import extract_elia_solar_data

    df = extract_elia_solar_data()
    df.to_parquet(TEMP_SOLAR_FILE, index=False)
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _load_raw_solar(**ctx):
    import pandas as pd
    from src.elia.load import load_raw_elia_solar

    df = pd.read_parquet(TEMP_SOLAR_FILE)
    run_id = ctx["run_id"]
    load_raw_elia_solar(df, run_id=run_id)


def _validate_solar(**ctx):
    from src.common.validation import (
        validate_table_exists,
        validate_row_count,
        validate_columns,
    )

    validate_table_exists("raw", "raw_elia_solar")
    validate_row_count("raw", "raw_elia_solar")
    validate_columns("raw", "raw_elia_solar", ["ingested_at", "source", "run_id"])


def _transform_clean_solar(**ctx):
    from src.elia.transform import transform_elia_solar_to_clean

    transform_elia_solar_to_clean()


# ── DAG ──────────────────────────────────────────────────────────
with DAG(
    dag_id="elia_generation",
    default_args=default_args,
    description="ELIA wind & solar forecast data (API) → raw → clean",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["energy", "elia", "wind", "solar"],
) as dag:

    # Wind pipeline
    t_extract_wind = PythonOperator(task_id="extract_wind", python_callable=_extract_wind)
    t_load_wind    = PythonOperator(task_id="load_raw_wind", python_callable=_load_raw_wind)
    t_val_wind     = PythonOperator(task_id="validate_wind", python_callable=_validate_wind)
    t_clean_wind   = PythonOperator(task_id="transform_clean_wind", python_callable=_transform_clean_wind)

    # Solar pipeline
    t_extract_solar = PythonOperator(task_id="extract_solar", python_callable=_extract_solar)
    t_load_solar    = PythonOperator(task_id="load_raw_solar", python_callable=_load_raw_solar)
    t_val_solar     = PythonOperator(task_id="validate_solar", python_callable=_validate_solar)
    t_clean_solar   = PythonOperator(task_id="transform_clean_solar", python_callable=_transform_clean_solar)

    # Define task dependencies
    t_extract_wind >> t_load_wind >> t_val_wind >> t_clean_wind
    t_extract_solar >> t_load_solar >> t_val_solar >> t_clean_solar
