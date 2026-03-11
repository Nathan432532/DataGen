"""
DAG: combined_energy
────────────────────
Complete end-to-end pipeline that extracts, transforms, and combines energy data.

This DAG orchestrates:
1. Vlaanderen solar & wind data extraction and transformation
2. ELIA solar & wind data extraction and transformation
3. Combination into a single hourly aggregated table

Pipeline stages:
  [Vlaanderen Solar: extract → load → validate → transform]
  [Vlaanderen Wind:  extract → load → validate → transform]
  [ELIA Solar:       extract → load → validate → transform]
  [ELIA Wind:        extract → load → validate → transform]
         ↓
  [Combine all sources into hourly table → validate]

Output: clean.clean_combined_energy
Columns: tijd, energie_vlaanderen_zon_megawatt, energie_vlaanderen_wind_megawatt,
         elia_zon_megawatt, elia_wind_megawatt
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# Temp file paths
VLAANDEREN_SOLAR_TEMP = "/tmp/combined_vl_solar.parquet"
VLAANDEREN_WIND_TEMP = "/tmp/combined_vl_wind.parquet"
ELIA_SOLAR_TEMP = "/tmp/combined_elia_solar.parquet"
ELIA_WIND_TEMP = "/tmp/combined_elia_wind.parquet"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


# ═══════════════════════════════════════════════════════════════════
# VLAANDEREN SOLAR — Task callables
# ═══════════════════════════════════════════════════════════════════
def _extract_vlaanderen_solar(**ctx):
    """Extract Vlaanderen solar data from ELIA API."""
    from src.vlaanderen_energy.extract import extract_solar_data

    df = extract_solar_data()
    df.to_parquet(VLAANDEREN_SOLAR_TEMP, index=False)
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _load_raw_vlaanderen_solar(**ctx):
    """Load raw Vlaanderen solar data."""
    import pandas as pd
    from src.vlaanderen_energy.load import load_raw_solar

    df = pd.read_parquet(VLAANDEREN_SOLAR_TEMP)
    load_raw_solar(df, run_id=ctx["run_id"])


def _validate_vlaanderen_solar(**ctx):
    """Validate raw Vlaanderen solar table."""
    from src.common.validation import (
        validate_table_exists,
        validate_row_count,
        validate_columns,
    )

    validate_table_exists("raw", "raw_vlaanderen_solar")
    validate_row_count("raw", "raw_vlaanderen_solar")
    validate_columns("raw", "raw_vlaanderen_solar", ["ingested_at", "source", "run_id"])


def _transform_vlaanderen_solar(**ctx):
    """Transform Vlaanderen solar to clean layer."""
    from src.vlaanderen_energy.transform import transform_solar_to_clean

    transform_solar_to_clean()


# ═══════════════════════════════════════════════════════════════════
# VLAANDEREN WIND — Task callables
# ═══════════════════════════════════════════════════════════════════
def _extract_vlaanderen_wind(**ctx):
    """Extract Vlaanderen wind data from ELIA API."""
    from src.vlaanderen_energy.extract import extract_wind_data

    df = extract_wind_data()
    df.to_parquet(VLAANDEREN_WIND_TEMP, index=False)
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _load_raw_vlaanderen_wind(**ctx):
    """Load raw Vlaanderen wind data."""
    import pandas as pd
    from src.vlaanderen_energy.load import load_raw_wind

    df = pd.read_parquet(VLAANDEREN_WIND_TEMP)
    load_raw_wind(df, run_id=ctx["run_id"])


def _validate_vlaanderen_wind(**ctx):
    """Validate raw Vlaanderen wind table."""
    from src.common.validation import (
        validate_table_exists,
        validate_row_count,
        validate_columns,
    )

    validate_table_exists("raw", "raw_vlaanderen_wind")
    validate_row_count("raw", "raw_vlaanderen_wind")
    validate_columns("raw", "raw_vlaanderen_wind", ["ingested_at", "source", "run_id"])


def _transform_vlaanderen_wind(**ctx):
    """Transform Vlaanderen wind to clean layer."""
    from src.vlaanderen_energy.transform import transform_wind_to_clean

    transform_wind_to_clean()


# ═══════════════════════════════════════════════════════════════════
# ELIA SOLAR — Task callables
# ═══════════════════════════════════════════════════════════════════
def _extract_elia_solar(**ctx):
    """Extract ELIA solar forecast data from API."""
    from src.elia.extract import extract_elia_solar_data

    df = extract_elia_solar_data()
    df.to_parquet(ELIA_SOLAR_TEMP, index=False)
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _load_raw_elia_solar(**ctx):
    """Load raw ELIA solar data."""
    import pandas as pd
    from src.elia.load import load_raw_elia_solar

    df = pd.read_parquet(ELIA_SOLAR_TEMP)
    load_raw_elia_solar(df, run_id=ctx["run_id"])


def _validate_elia_solar(**ctx):
    """Validate raw ELIA solar table."""
    from src.common.validation import (
        validate_table_exists,
        validate_row_count,
        validate_columns,
    )

    validate_table_exists("raw", "raw_elia_solar")
    validate_row_count("raw", "raw_elia_solar")
    validate_columns("raw", "raw_elia_solar", ["ingested_at", "source", "run_id"])


def _transform_elia_solar(**ctx):
    """Transform ELIA solar to clean layer."""
    from src.elia.transform import transform_elia_solar_to_clean

    transform_elia_solar_to_clean()


# ═══════════════════════════════════════════════════════════════════
# ELIA WIND — Task callables
# ═══════════════════════════════════════════════════════════════════
def _extract_elia_wind(**ctx):
    """Extract ELIA wind forecast data from API."""
    from src.elia.extract import extract_elia_wind_data

    df = extract_elia_wind_data()
    df.to_parquet(ELIA_WIND_TEMP, index=False)
    ctx["ti"].xcom_push(key="rows", value=len(df))


def _load_raw_elia_wind(**ctx):
    """Load raw ELIA wind data."""
    import pandas as pd
    from src.elia.load import load_raw_elia_wind

    df = pd.read_parquet(ELIA_WIND_TEMP)
    load_raw_elia_wind(df, run_id=ctx["run_id"])


def _validate_elia_wind(**ctx):
    """Validate raw ELIA wind table."""
    from src.common.validation import (
        validate_table_exists,
        validate_row_count,
        validate_columns,
    )

    validate_table_exists("raw", "raw_elia_wind")
    validate_row_count("raw", "raw_elia_wind")
    validate_columns("raw", "raw_elia_wind", ["ingested_at", "source", "run_id"])


def _transform_elia_wind(**ctx):
    """Transform ELIA wind to clean layer."""
    from src.elia.transform import transform_elia_wind_to_clean

    transform_elia_wind_to_clean()


# ═══════════════════════════════════════════════════════════════════
# COMBINED — Task callables
# ═══════════════════════════════════════════════════════════════════
def _transform_combined(**ctx):
    """Combine all sources into a single hourly aggregated table."""
    from src.combined.transform import transform_combined_energy

    transform_combined_energy()


def _validate_combined(**ctx):
    """Validate the final combined table."""
    from src.common.validation import (
        validate_table_exists,
        validate_row_count,
        validate_columns,
    )

    validate_table_exists("clean", "clean_combined_energy")
    validate_row_count("clean", "clean_combined_energy")
    validate_columns("clean", "clean_combined_energy", [
        "tijd",
        "energie_vlaanderen_zon_megawatt",
        "energie_vlaanderen_wind_megawatt",
        "elia_zon_megawatt",
        "elia_wind_megawatt"
    ])


# ═══════════════════════════════════════════════════════════════════
# DAG DEFINITION
# ═══════════════════════════════════════════════════════════════════
with DAG(
    dag_id="combined_energy",
    default_args=default_args,
    description="Complete energy pipeline: extract → transform → combine (Vlaanderen + ELIA)",
    schedule="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["energy", "combined", "pipeline", "end-to-end"],
) as dag:

    # ── Vlaanderen Solar pipeline ────────────────────────────────
    vl_solar_extract = PythonOperator(
        task_id="extract_vlaanderen_solar",
        python_callable=_extract_vlaanderen_solar,
        provide_context=True,
    )

    vl_solar_load = PythonOperator(
        task_id="load_raw_vlaanderen_solar",
        python_callable=_load_raw_vlaanderen_solar,
        provide_context=True,
    )

    vl_solar_validate = PythonOperator(
        task_id="validate_vlaanderen_solar",
        python_callable=_validate_vlaanderen_solar,
        provide_context=True,
    )

    vl_solar_transform = PythonOperator(
        task_id="transform_clean_vlaanderen_solar",
        python_callable=_transform_vlaanderen_solar,
        provide_context=True,
    )

    # ── Vlaanderen Wind pipeline ─────────────────────────────────
    vl_wind_extract = PythonOperator(
        task_id="extract_vlaanderen_wind",
        python_callable=_extract_vlaanderen_wind,
        provide_context=True,
    )

    vl_wind_load = PythonOperator(
        task_id="load_raw_vlaanderen_wind",
        python_callable=_load_raw_vlaanderen_wind,
        provide_context=True,
    )

    vl_wind_validate = PythonOperator(
        task_id="validate_vlaanderen_wind",
        python_callable=_validate_vlaanderen_wind,
        provide_context=True,
    )

    vl_wind_transform = PythonOperator(
        task_id="transform_clean_vlaanderen_wind",
        python_callable=_transform_vlaanderen_wind,
        provide_context=True,
    )

    # ── ELIA Solar pipeline ──────────────────────────────────────
    elia_solar_extract = PythonOperator(
        task_id="extract_elia_solar",
        python_callable=_extract_elia_solar,
        provide_context=True,
    )

    elia_solar_load = PythonOperator(
        task_id="load_raw_elia_solar",
        python_callable=_load_raw_elia_solar,
        provide_context=True,
    )

    elia_solar_validate = PythonOperator(
        task_id="validate_elia_solar",
        python_callable=_validate_elia_solar,
        provide_context=True,
    )

    elia_solar_transform = PythonOperator(
        task_id="transform_clean_elia_solar",
        python_callable=_transform_elia_solar,
        provide_context=True,
    )

    # ── ELIA Wind pipeline ───────────────────────────────────────
    elia_wind_extract = PythonOperator(
        task_id="extract_elia_wind",
        python_callable=_extract_elia_wind,
        provide_context=True,
    )

    elia_wind_load = PythonOperator(
        task_id="load_raw_elia_wind",
        python_callable=_load_raw_elia_wind,
        provide_context=True,
    )

    elia_wind_validate = PythonOperator(
        task_id="validate_elia_wind",
        python_callable=_validate_elia_wind,
        provide_context=True,
    )

    elia_wind_transform = PythonOperator(
        task_id="transform_clean_elia_wind",
        python_callable=_transform_elia_wind,
        provide_context=True,
    )

    # ── Combined transformation ──────────────────────────────────
    transform_combined = PythonOperator(
        task_id="transform_combined",
        python_callable=_transform_combined,
        provide_context=True,
    )

    validate_combined = PythonOperator(
        task_id="validate_combined",
        python_callable=_validate_combined,
        provide_context=True,
    )

    # ═══════════════════════════════════════════════════════════════
    # TASK DEPENDENCIES
    # ═══════════════════════════════════════════════════════════════
    # Four parallel pipelines, then combine
    vl_solar_extract >> vl_solar_load >> vl_solar_validate >> vl_solar_transform
    vl_wind_extract >> vl_wind_load >> vl_wind_validate >> vl_wind_transform
    elia_solar_extract >> elia_solar_load >> elia_solar_validate >> elia_solar_transform
    elia_wind_extract >> elia_wind_load >> elia_wind_validate >> elia_wind_transform

    # All four transforms must complete before combining
    [vl_solar_transform, vl_wind_transform, elia_solar_transform, elia_wind_transform] >> transform_combined >> validate_combined
