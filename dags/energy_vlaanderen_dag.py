"""
DAG: energy_vlaanderen
───────────────────────
ETL pipeline for Vlaanderen solar + wind CSV data.

This DAG ingests BOTH solar and wind in one run, each going through
its own extract → load → validate → transform path.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

SOLAR_TEMP = "/tmp/vlaanderen_solar.parquet"
WIND_TEMP = "/tmp/vlaanderen_wind.parquet"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ── SOLAR task callables ─────────────────────────────────────────
def _extract_solar(**ctx):
    from src.vlaanderen_energy.extract import extract_solar_data

    df = extract_solar_data()
    df.to_parquet(SOLAR_TEMP, index=False)
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _load_raw_solar(**ctx):
    import pandas as pd
    from src.vlaanderen_energy.load import load_raw_solar

    df = pd.read_parquet(SOLAR_TEMP)
    load_raw_solar(df, run_id=ctx["run_id"])


def _validate_solar(**ctx):
    from src.common.validation import (
        validate_table_exists,
        validate_row_count,
        validate_columns,
    )

    validate_table_exists("raw", "raw_vlaanderen_solar")
    validate_row_count("raw", "raw_vlaanderen_solar")
    validate_columns("raw", "raw_vlaanderen_solar", ["ingested_at", "source", "run_id"])


def _transform_solar(**ctx):
    from src.vlaanderen_energy.transform import transform_solar_to_clean

    transform_solar_to_clean()


# ── WIND task callables ──────────────────────────────────────────
def _extract_wind(**ctx):
    from src.vlaanderen_energy.extract import extract_wind_data

    df = extract_wind_data()
    df.to_parquet(WIND_TEMP, index=False)
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _load_raw_wind(**ctx):
    import pandas as pd
    from src.vlaanderen_energy.load import load_raw_wind

    df = pd.read_parquet(WIND_TEMP)
    load_raw_wind(df, run_id=ctx["run_id"])


def _validate_wind(**ctx):
    from src.common.validation import (
        validate_table_exists,
        validate_row_count,
        validate_columns,
    )

    validate_table_exists("raw", "raw_vlaanderen_wind")
    validate_row_count("raw", "raw_vlaanderen_wind")
    validate_columns("raw", "raw_vlaanderen_wind", ["ingested_at", "source", "run_id"])


def _transform_wind(**ctx):
    from src.vlaanderen_energy.transform import transform_wind_to_clean

    transform_wind_to_clean()


# ── DAG ──────────────────────────────────────────────────────────
with DAG(
    dag_id="energy_vlaanderen",
    default_args=default_args,
    description="Vlaanderen solar + wind CSV → raw → clean",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["energy", "vlaanderen", "solar", "wind"],
) as dag:

    # Solar branch
    s_extract   = PythonOperator(task_id="extract_solar",        python_callable=_extract_solar)
    s_load      = PythonOperator(task_id="load_raw_solar",       python_callable=_load_raw_solar)
    s_validate  = PythonOperator(task_id="validate_solar",       python_callable=_validate_solar)
    s_transform = PythonOperator(task_id="transform_clean_solar", python_callable=_transform_solar)

    # Wind branch
    w_extract   = PythonOperator(task_id="extract_wind",         python_callable=_extract_wind)
    w_load      = PythonOperator(task_id="load_raw_wind",        python_callable=_load_raw_wind)
    w_validate  = PythonOperator(task_id="validate_wind",        python_callable=_validate_wind)
    w_transform = PythonOperator(task_id="transform_clean_wind", python_callable=_transform_wind)

    # Solar and wind run in PARALLEL inside the same DAG
    s_extract >> s_load >> s_validate >> s_transform
    w_extract >> w_load >> w_validate >> w_transform
