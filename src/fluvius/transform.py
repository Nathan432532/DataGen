"""Transform — clean Fluvius energy data into the *clean* layer."""

import pandas as pd

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("fluvius.transform")

CLEAN_TABLE = "clean_energy_hourly"


def transform_fluvius_to_clean() -> None:
    """Read from ``raw.raw_fluvius_energy``, normalise, write to *clean*."""
    engine = get_engine()

    df = pd.read_sql("SELECT * FROM raw.raw_fluvius_energy", engine)
    logger.info("Read %d rows from raw.raw_fluvius_energy", len(df))
    logger.info("Columns: %s", list(df.columns))

    # Drop metadata columns
    meta = {"ingested_at", "source", "run_id"}
    data_cols = [c for c in df.columns if c not in meta]
    df_clean = df[data_cols].copy()

    # Detect time/year column
    ts_col = _detect_timestamp_col(df_clean)
    if ts_col:
        df_clean[ts_col] = pd.to_datetime(df_clean[ts_col], errors="coerce")
        df_clean = df_clean.rename(columns={ts_col: "timestamp"})
        df_clean = df_clean.dropna(subset=["timestamp"])
        df_clean = df_clean.sort_values("timestamp").reset_index(drop=True)
    else:
        logger.warning("No timestamp column detected in Fluvius data")
        df_clean["timestamp"] = pd.NaT

    df_clean = df_clean.dropna(how="all").reset_index(drop=True)

    df_clean.to_sql(CLEAN_TABLE, engine, schema="clean", if_exists="replace", index=False)
    logger.info("Wrote %d rows → clean.%s", len(df_clean), CLEAN_TABLE)


def _detect_timestamp_col(df: pd.DataFrame) -> str | None:
    keywords = ("datetime", "timestamp", "date", "time", "jaar", "year")
    for col in df.columns:
        if any(kw in col.lower() for kw in keywords):
            return col
    return None
