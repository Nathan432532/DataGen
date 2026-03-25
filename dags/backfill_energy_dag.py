"""
DAG: backfill_energy
────────────────────
Backfill historical energy data — fetch all available records from APIs.

This DAG fetches all available historical data respecting API limits.
Run this once to populate your database with maximum available data,
then the daily DAG handles ongoing updates.

Usage:
  1. Trigger manually from Airflow UI
  2. Or: airflow dags trigger backfill_energy
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from src.common.logging_config import setup_logging

# Temp file paths
VLAANDEREN_SOLAR_TEMP = "/tmp/backfill_vl_solar.parquet"
VLAANDEREN_WIND_TEMP = "/tmp/backfill_vl_wind.parquet"
ELIA_SOLAR_TEMP = "/tmp/backfill_elia_solar.parquet"
ELIA_WIND_TEMP = "/tmp/backfill_elia_wind.parquet"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

logger = setup_logging("backfill.energy")

BACKFILL_START_DATE = "2025-03-01"
BACKFILL_REGION = "Flanders"


def _backfill_vlaanderen_solar(**ctx):
    """Backfill Vlaanderen solar — fetch all available records from start date to today."""
    from src.vlaanderen_energy.extract import extract_solar_data

    logger.info("Fetching Vlaanderen solar from %s...", BACKFILL_START_DATE)
    df = extract_solar_data(start_date=BACKFILL_START_DATE)
    df.to_parquet(VLAANDEREN_SOLAR_TEMP, index=False)
    logger.info("Backfill solar: %d rows", len(df))
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _backfill_vlaanderen_wind(**ctx):
    """Backfill Vlaanderen wind — fetch all available records from start date to today."""
    from src.vlaanderen_energy.extract import extract_wind_data

    logger.info("Fetching Vlaanderen wind from %s...", BACKFILL_START_DATE)
    df = extract_wind_data(start_date=BACKFILL_START_DATE)
    df.to_parquet(VLAANDEREN_WIND_TEMP, index=False)
    logger.info("Backfill wind: %d rows", len(df))
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _backfill_elia_solar(**ctx):
    """Backfill ELIA solar — fetch all available records."""
    from src.elia.extract import extract_elia_solar_data

    logger.info(
        "Fetching ELIA solar from %s (%s)...", BACKFILL_START_DATE, BACKFILL_REGION
    )
    df = extract_elia_solar_data(start_date=BACKFILL_START_DATE, region=BACKFILL_REGION)
    df.to_parquet(ELIA_SOLAR_TEMP, index=False)
    logger.info(f"✅ Backfill ELIA solar: {len(df)} rows")
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _backfill_elia_wind(**ctx):
    """Backfill ELIA wind — fetch all available records."""
    from src.elia.extract import extract_elia_wind_data

    logger.info(
        "Fetching ELIA wind from %s (%s)...", BACKFILL_START_DATE, BACKFILL_REGION
    )
    df = extract_elia_wind_data(start_date=BACKFILL_START_DATE, region=BACKFILL_REGION)
    df.to_parquet(ELIA_WIND_TEMP, index=False)
    logger.info(f"✅ Backfill ELIA wind: {len(df)} rows")
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _load_backfill_vlaanderen_solar(**ctx):
    """Load backfilled Vlaanderen solar."""
    import pandas as pd
    from src.vlaanderen_energy.load import load_raw_solar

    df = pd.read_parquet(VLAANDEREN_SOLAR_TEMP)
    load_raw_solar(df, run_id=ctx["run_id"])


def _load_backfill_vlaanderen_wind(**ctx):
    """Load backfilled Vlaanderen wind."""
    import pandas as pd
    from src.vlaanderen_energy.load import load_raw_wind

    df = pd.read_parquet(VLAANDEREN_WIND_TEMP)
    load_raw_wind(df, run_id=ctx["run_id"])


def _load_backfill_elia_solar(**ctx):
    """Load backfilled ELIA solar."""
    import pandas as pd
    from src.elia.load import load_raw_elia_solar

    df = pd.read_parquet(ELIA_SOLAR_TEMP)
    load_raw_elia_solar(df, run_id=ctx["run_id"])


def _load_backfill_elia_wind(**ctx):
    """Load backfilled ELIA wind."""
    import pandas as pd
    from src.elia.load import load_raw_elia_wind

    df = pd.read_parquet(ELIA_WIND_TEMP)
    load_raw_elia_wind(df, run_id=ctx["run_id"])


def _transform_and_combine(**ctx):
    """Transform all sources and combine into final table."""
    from src.vlaanderen_energy.transform import (
        transform_solar_to_clean,
        transform_wind_to_clean,
    )
    from src.elia.transform import (
        transform_elia_wind_to_clean,
        transform_elia_solar_to_clean,
    )
    from src.combined.transform import transform_combined_energy

    logger.info("Transforming Vlaanderen solar...")
    transform_solar_to_clean()
    logger.info("Transforming Vlaanderen wind...")
    transform_wind_to_clean()
    logger.info("Transforming ELIA solar...")
    transform_elia_solar_to_clean()
    logger.info("Transforming ELIA wind...")
    transform_elia_wind_to_clean()
    logger.info("Combining all sources...")
    transform_combined_energy()
    logger.info("✅ Backfill complete!")


# ═══════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════════
with DAG(
    dag_id="backfill_energy",
    default_args=default_args,
    description="Backfill all available historical energy data",
    schedule_interval=None,  # Manual trigger only
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["energy", "backfill", "historical"],
) as dag:

    # Extract in parallel (all records, respecting API limits)
    extract_vl_solar = PythonOperator(
        task_id="extract_backfill_vlaanderen_solar",
        python_callable=_backfill_vlaanderen_solar,
        provide_context=True,
    )

    extract_vl_wind = PythonOperator(
        task_id="extract_backfill_vlaanderen_wind",
        python_callable=_backfill_vlaanderen_wind,
        provide_context=True,
    )

    extract_elia_solar = PythonOperator(
        task_id="extract_backfill_elia_solar",
        python_callable=_backfill_elia_solar,
        provide_context=True,
    )

    extract_elia_wind = PythonOperator(
        task_id="extract_backfill_elia_wind",
        python_callable=_backfill_elia_wind,
        provide_context=True,
    )

    # Load all extracted data
    load_vl_solar = PythonOperator(
        task_id="load_backfill_vlaanderen_solar",
        python_callable=_load_backfill_vlaanderen_solar,
        provide_context=True,
    )

    load_vl_wind = PythonOperator(
        task_id="load_backfill_vlaanderen_wind",
        python_callable=_load_backfill_vlaanderen_wind,
        provide_context=True,
    )

    load_elia_solar = PythonOperator(
        task_id="load_backfill_elia_solar",
        python_callable=_load_backfill_elia_solar,
        provide_context=True,
    )

    load_elia_wind = PythonOperator(
        task_id="load_backfill_elia_wind",
        python_callable=_load_backfill_elia_wind,
        provide_context=True,
    )

    # Transform & combine
    combine = PythonOperator(
        task_id="transform_and_combine",
        python_callable=_transform_and_combine,
        provide_context=True,
    )

    # Dependencies
    [
        extract_vl_solar,
        extract_vl_wind,
        extract_elia_solar,
        extract_elia_wind,
    ] >> combine
    [load_vl_solar, load_vl_wind, load_elia_solar, load_elia_wind] >> combine

    extract_vl_solar >> load_vl_solar
    extract_vl_wind >> load_vl_wind
    extract_elia_solar >> load_elia_solar
    extract_elia_wind >> load_elia_wind
