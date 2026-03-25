"""Transform — clean Vlaanderen solar/wind data into the *clean* layer.

The raw table contains a `datetime` column plus NIS-code columns (5-digit
municipality codes) with MW values. This transform sums all NIS columns per
timestamp and writes a slim two-column table to the clean layer.
"""

import pandas as pd

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("vlaanderen_energy.transform")


def transform_solar_to_clean() -> None:
    """Normalise ``raw.raw_vlaanderen_solar`` → ``clean.clean_solar_hourly``."""
    _transform("raw_vlaanderen_solar", "clean_solar_hourly", "vlaanderen_zon_mw")


def transform_wind_to_clean() -> None:
    """Normalise ``raw.raw_vlaanderen_wind`` → ``clean.clean_wind_hourly``."""
    _transform("raw_vlaanderen_wind", "clean_wind_hourly", "vlaanderen_wind_mw")


# ---------------------------------------------------------------------------
# Shared logic
# ---------------------------------------------------------------------------

def _transform(raw_table: str, clean_table: str, value_col: str) -> None:
    engine = get_engine()

    df = pd.read_sql(f"SELECT * FROM raw.{raw_table}", engine)
    logger.info("Read %d rows from raw.%s", len(df), raw_table)

    # Drop pipeline metadata columns
    meta = {"ingested_at", "source", "run_id", "source_file_date"}
    data_cols = [c for c in df.columns if c not in meta]
    df = df[data_cols].copy()

    # Identify the timestamp column
    ts_col = next(
        (c for c in df.columns if any(kw in c.lower() for kw in ("datetime", "timestamp", "date", "time"))),
        None,
    )
    if ts_col is None:
        raise ValueError(f"No timestamp column found in raw.{raw_table}")

    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df = df.dropna(subset=[ts_col])
    df = df.rename(columns={ts_col: "datetime"})

    # NIS-code columns are 5-digit numeric strings
    nis_cols = [c for c in df.columns if c != "datetime" and c.isdigit() and len(c) == 5]
    if not nis_cols:
        raise ValueError(f"No NIS-code columns found in raw.{raw_table}")

    logger.info("Summing %d NIS columns for %s", len(nis_cols), raw_table)

    df[nis_cols] = df[nis_cols].apply(pd.to_numeric, errors="coerce").fillna(0)
    df[value_col] = df[nis_cols].sum(axis=1)

    clean = df[["datetime", value_col]].copy()
    clean = clean.sort_values("datetime").drop_duplicates(subset=["datetime"], keep="last")
    clean = clean.reset_index(drop=True)

    clean.to_sql(clean_table, engine, schema="public", if_exists="replace", index=False)
    logger.info("Wrote %d rows → %s", len(clean), clean_table)
