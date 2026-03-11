"""Transform — clean ELIA generation data into the *clean* layer."""

import pandas as pd

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("elia.transform")

CLEAN_TABLE = "clean_elia_generation"


def transform_elia_to_clean() -> None:
    """Read from ``raw.raw_elia_generation``, normalise, write to *clean*."""
    engine = get_engine()

    df = pd.read_sql("SELECT * FROM raw.raw_elia_generation", engine)
    logger.info("Read %d rows from raw.raw_elia_generation", len(df))
    logger.info("Columns: %s", list(df.columns))

    # Drop metadata
    meta = {"ingested_at", "source", "run_id"}
    data_cols = [c for c in df.columns if c not in meta]
    df_clean = df[data_cols].copy()

    # Normalise timestamp
    ts_col = _detect_timestamp_col(df_clean)
    if ts_col:
        df_clean[ts_col] = pd.to_datetime(df_clean[ts_col], errors="coerce")
        df_clean = df_clean.rename(columns={ts_col: "timestamp"})
        df_clean = df_clean.dropna(subset=["timestamp"])
        df_clean = df_clean.sort_values("timestamp")
        df_clean = df_clean.drop_duplicates(subset=["timestamp"], keep="last")
    else:
        logger.warning("No timestamp column detected — storing as-is")
        df_clean["timestamp"] = pd.NaT

    df_clean = df_clean.dropna(how="all").reset_index(drop=True)

    df_clean.to_sql(CLEAN_TABLE, engine, schema="clean", if_exists="replace", index=False)
    logger.info("Wrote %d rows → clean.%s", len(df_clean), CLEAN_TABLE)


def _detect_timestamp_col(df: pd.DataFrame) -> str | None:
    keywords = ("datetime", "timestamp", "date", "time")
    for col in df.columns:
        if any(kw in col.lower() for kw in keywords):
            return col
    return None
