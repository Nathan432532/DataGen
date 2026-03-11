"""Transform — clean Vlaanderen solar/wind data into the *clean* layer."""

import pandas as pd

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("vlaanderen_energy.transform")


def transform_solar_to_clean() -> None:
    """Normalise ``raw.raw_vlaanderen_solar`` → ``clean.clean_solar_hourly``."""
    _transform("raw_vlaanderen_solar", "clean_solar_hourly")


def transform_wind_to_clean() -> None:
    """Normalise ``raw.raw_vlaanderen_wind`` → ``clean.clean_wind_hourly``."""
    _transform("raw_vlaanderen_wind", "clean_wind_hourly")


# ------------------------------------------------------------------
# Shared logic
# ------------------------------------------------------------------
def _transform(raw_table: str, clean_table: str) -> None:
    engine = get_engine()

    df = pd.read_sql(f"SELECT * FROM raw.{raw_table}", engine)
    logger.info("Read %d rows from raw.%s", len(df), raw_table)
    logger.info("Columns: %s", list(df.columns))

    meta = {"ingested_at", "source", "run_id"}
    data_cols = [c for c in df.columns if c not in meta]
    df_clean = df[data_cols].copy()

    ts_col = _detect_timestamp_col(df_clean)
    if ts_col:
        df_clean[ts_col] = pd.to_datetime(df_clean[ts_col], errors="coerce")
        df_clean = df_clean.rename(columns={ts_col: "timestamp"})
        df_clean = df_clean.dropna(subset=["timestamp"])
        df_clean = df_clean.sort_values("timestamp")
        df_clean = df_clean.drop_duplicates(subset=["timestamp"], keep="last")
    else:
        logger.warning("No timestamp column detected for %s", raw_table)
        df_clean["timestamp"] = pd.NaT

    df_clean = df_clean.dropna(how="all").reset_index(drop=True)

    df_clean.to_sql(clean_table, engine, schema="clean", if_exists="replace", index=False)
    logger.info("Wrote %d rows → clean.%s", len(df_clean), clean_table)


def _detect_timestamp_col(df: pd.DataFrame) -> str | None:
    keywords = ("datetime", "timestamp", "date", "time")
    for col in df.columns:
        if any(kw in col.lower() for kw in keywords):
            return col
    return None
