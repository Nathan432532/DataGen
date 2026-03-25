"""Transform — clean ELIA wind and solar forecast data into the *clean* layer."""

import pandas as pd

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("elia.transform")

CLEAN_WIND_TABLE = "clean_elia_wind"
CLEAN_SOLAR_TABLE = "clean_elia_solar"


def _transform_elia_data(raw_table: str, clean_table: str, data_type: str) -> None:
    """Generic transformation function for ELIA data.

    Aggregates 15-minute raw data to hourly averages.
    """
    engine = get_engine()

    df = pd.read_sql(f"SELECT * FROM raw.{raw_table}", engine)
    logger.info("Read %d rows from raw.%s", len(df), raw_table)
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
        # Drop duplicates based on timestamp AND region (not just timestamp)
        # to preserve data for different regions at the same time
        if "region" in df_clean.columns:
            df_clean = df_clean.drop_duplicates(
                subset=["timestamp", "region"], keep="last"
            )
        else:
            df_clean = df_clean.drop_duplicates(subset=["timestamp"], keep="last")
    else:
        logger.warning("No timestamp column detected for %s — storing as-is", data_type)
        df_clean["timestamp"] = pd.NaT

    df_clean = df_clean.dropna(how="all").reset_index(drop=True)

    # Aggregate 15-minute data to hourly averages
    # Find numeric columns (forecast/measured values)
    numeric_cols = df_clean.select_dtypes(include=["number"]).columns.tolist()

    if "timestamp" in df_clean.columns and numeric_cols:
        logger.info(
            "Aggregating 15-minute data to hourly (%d numeric columns)...",
            len(numeric_cols),
        )
        df_clean["hour"] = df_clean["timestamp"].dt.floor("H")

        group_cols = ["hour"]
        if "region" in df_clean.columns:
            group_cols.append("region")

        agg_dict = {col: "mean" for col in numeric_cols}
        hourly = df_clean.groupby(group_cols)[numeric_cols].agg(agg_dict).reset_index()
        hourly = hourly.rename(columns={"hour": "timestamp"})

        logger.info(
            "Aggregated %d 15-min records → %d hourly records",
            len(df_clean),
            len(hourly),
        )
        df_clean = hourly

    df_clean.to_sql(
        clean_table, engine, schema="public", if_exists="replace", index=False
    )
    logger.info("Wrote %d rows → %s (%s)", len(df_clean), clean_table, data_type)


def transform_elia_wind_to_clean() -> None:
    """Read from ``raw.raw_elia_wind``, normalise, write to *clean*."""
    _transform_elia_data("raw_elia_wind", CLEAN_WIND_TABLE, "wind")


def transform_elia_solar_to_clean() -> None:
    """Read from ``raw.raw_elia_solar``, normalise, write to *clean*."""
    _transform_elia_data("raw_elia_solar", CLEAN_SOLAR_TABLE, "solar")


def _detect_timestamp_col(df: pd.DataFrame) -> str | None:
    keywords = ("datetime", "timestamp", "date", "time")
    for col in df.columns:
        if any(kw in col.lower() for kw in keywords):
            return col
    return None
