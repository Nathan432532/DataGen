"""Transform — move weather data from *raw* to *clean* layer.

The clean table normalises timestamps and removes duplicates so that
downstream consumers always get one row per timestamp.
"""

import pandas as pd

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("weather_kaggle.transform")

CLEAN_TABLE = "clean_weather_hourly"


def transform_weather_to_clean() -> None:
    """Read ``raw.raw_weather_antwerp``, clean, write ``clean.clean_weather_hourly``."""
    engine = get_engine()

    df = pd.read_sql("SELECT * FROM raw.raw_weather_antwerp", engine)
    logger.info("Read %d rows from raw.raw_weather_antwerp", len(df))
    logger.info("Columns: %s", list(df.columns))

    # Drop metadata columns
    meta = {"ingested_at", "source", "run_id"}
    data_cols = [c for c in df.columns if c not in meta]
    df_clean = df[data_cols].copy()

    # Detect the timestamp / datetime column
    ts_col = _detect_timestamp_col(df_clean)

    if ts_col:
        df_clean[ts_col] = pd.to_datetime(df_clean[ts_col], errors="coerce")
        df_clean = df_clean.rename(columns={ts_col: "timestamp"})

        before = len(df_clean)
        df_clean = df_clean.dropna(subset=["timestamp"])
        if (dropped := before - len(df_clean)) > 0:
            logger.warning("Dropped %d rows with unparseable timestamps", dropped)

        df_clean = df_clean.sort_values("timestamp")
        df_clean = df_clean.drop_duplicates(subset=["timestamp"], keep="last")
    else:
        logger.warning("No timestamp column detected — storing as-is with NaT")
        df_clean["timestamp"] = pd.NaT

    df_clean = df_clean.dropna(how="all").reset_index(drop=True)

    df_clean.to_sql(CLEAN_TABLE, engine, schema="public", if_exists="replace", index=False)
    logger.info("Wrote %d rows → %s", len(df_clean), CLEAN_TABLE)


def _detect_timestamp_col(df: pd.DataFrame) -> str | None:
    """Heuristic: return the first column whose name hints at a timestamp."""
    keywords = ("datetime", "timestamp", "date", "time")
    for col in df.columns:
        if any(kw in col.lower() for kw in keywords):
            return col
    return None
