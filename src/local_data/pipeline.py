"""ETL helpers for local CSV files stored in /opt/airflow/Data."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("local_data.pipeline")

DATA_DIR = Path("/opt/airflow/Data")
SOURCE_FILES = {
    "solar_radiation": "sun_combined.csv",
    "energy_consumption": "consumptie.csv",
    "wind_history": "v_wind_alles_compleet.csv",
}

RAW_TABLES = {
    "solar_radiation": "raw_local_solar_radiation",
    "energy_consumption": "raw_local_energy_consumption",
    "wind_history": "raw_local_wind_history",
}

CLEAN_TABLES = {
    "solar_radiation": "clean_local_solar_radiation",
    "energy_consumption": "clean_local_energy_consumption",
    "wind_history": "clean_local_wind_history",
}


def extract_local_csv(dataset_key: str) -> pd.DataFrame:
    """Read one local CSV dataset from the mounted Data directory."""
    path = DATA_DIR / SOURCE_FILES[dataset_key]
    if not path.exists():
        raise FileNotFoundError(f"Dataset not found: {path}")

    logger.info("Reading local dataset %s from %s", dataset_key, path)

    if dataset_key == "wind_history":
        df = pd.read_csv(path, na_values=["NULL", "null", ""])
    else:
        df = pd.read_csv(path)

    logger.info("Loaded %s: %d rows, columns=%s", dataset_key, len(df), list(df.columns))
    return df


def load_raw_dataset(df: pd.DataFrame, dataset_key: str, run_id: str) -> None:
    """Write the raw dataset to Postgres with metadata columns."""
    raw_df = df.copy()
    raw_df["ingested_at"] = pd.Timestamp.utcnow()
    raw_df["source"] = SOURCE_FILES[dataset_key]
    raw_df["run_id"] = run_id

    engine = get_engine()
    raw_table = RAW_TABLES[dataset_key]
    raw_df.to_sql(raw_table, engine, schema="raw", if_exists="replace", index=False)
    logger.info("Wrote %d rows to raw.%s", len(raw_df), raw_table)


def transform_dataset(dataset_key: str) -> None:
    """Normalize one raw dataset into its clean analytical table."""
    engine = get_engine()
    ensure_schema_exists("clean")
    raw_table = RAW_TABLES[dataset_key]
    clean_table = CLEAN_TABLES[dataset_key]

    df = pd.read_sql(f'SELECT * FROM raw."{raw_table}"', engine)
    df = df.drop(columns=["ingested_at", "source", "run_id"], errors="ignore")

    if dataset_key == "solar_radiation":
        clean_df = _transform_solar_radiation(df)
    elif dataset_key == "energy_consumption":
        clean_df = _transform_energy_consumption(df)
    elif dataset_key == "wind_history":
        clean_df = _transform_wind_history(df)
    else:
        raise ValueError(f"Unknown dataset key: {dataset_key}")

    clean_df.to_sql(clean_table, engine, schema="clean", if_exists="replace", index=False)
    logger.info("Wrote %d rows to clean.%s", len(clean_df), clean_table)


def validate_table(schema: str, table: str, min_rows: int = 1) -> None:
    """Basic table-level validation."""
    engine = get_engine()
    with engine.connect() as conn:
        count = conn.execute(text(f'SELECT COUNT(*) FROM "{schema}"."{table}"')).scalar()
    if count is None or count < min_rows:
        raise ValueError(f"Table {schema}.{table} has insufficient rows: {count}")
    logger.info("Validated %s.%s with %d rows", schema, table, count)


def ensure_schema_exists(schema: str) -> None:
    """Create a schema on demand to avoid first-run volume initialization issues."""
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
    logger.info("Ensured schema exists: %s", schema)


def _transform_solar_radiation(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"datum": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    numeric_cols = [
        "open_meteo_radiation",
        "kmi_radiation_avg",
        "kaggle_radiation_avg",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    clean_df = (
        df[["timestamp", *numeric_cols]]
        .dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    return clean_df


def _transform_energy_consumption(df: pd.DataFrame) -> pd.DataFrame:
    rename_map = {
        "tijd": "timestamp",
        "Energie vlaanderen zon": "energie_vlaanderen_zon",
        "Energie vlaanderen wind": "energie_vlaanderen_wind",
        "Elia totaal": "elia_totaal",
        "kaggle prive": "kaggle_prive",
        "kaggle openbaar": "kaggle_openbaar",
    }
    df = df.rename(columns=rename_map)
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce")

    numeric_cols = [
        "energie_vlaanderen_zon",
        "energie_vlaanderen_wind",
        "elia_totaal",
        "kaggle_prive",
        "kaggle_openbaar",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    clean_df = (
        df[["timestamp", *numeric_cols]]
        .dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    return clean_df


def _transform_wind_history(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"tijdstip": "timestamp"})
    df["timestamp"] = pd.to_datetime(df["timestamp"], errors="coerce", utc=True).dt.tz_localize(None)

    numeric_cols = [
        "wind_ecmwf_2026",
        "wind_kmi_2002",
        "wind_ukkel_2024",
        "wind_antwerpen_archive",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    clean_df = (
        df[["timestamp", *numeric_cols]]
        .dropna(subset=["timestamp"])
        .sort_values("timestamp")
        .reset_index(drop=True)
    )
    return clean_df
