"""Transform — combine Vlaanderen + ELIA energy data into a single hourly table.

Reads from the four clean tables and joins them on hourly timestamp.
Values are converted from MW to kWh (MW * 1000) before writing.

Output table: clean.clean_combined_energy
Columns:
  - tijd               : hourly timestamp
  - vlaanderen_zon_kwh : Vlaanderen solar in kWh
  - vlaanderen_wind_kwh: Vlaanderen wind in kWh
  - elia_zon_kwh       : ELIA solar forecast (Flanders) in kWh
  - elia_wind_kwh      : ELIA wind forecast (Flanders) in kWh
"""

import pandas as pd

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("combined.transform")

CLEAN_TABLE = "clean_combined_energy"
ELIA_REGION = "Flanders"


def transform_combined_energy() -> None:
    engine = get_engine()

    # --- Vlaanderen solar ---------------------------------------------------
    logger.info("Reading clean.clean_solar_hourly...")
    df_vl_solar = pd.read_sql("SELECT datetime, vlaanderen_zon_mw FROM clean.clean_solar_hourly", engine)
    df_vl_solar["datetime"] = pd.to_datetime(df_vl_solar["datetime"], errors="coerce", utc=True)
    df_vl_solar = df_vl_solar.dropna(subset=["datetime"])
    df_vl_solar["vlaanderen_zon_kwh"] = pd.to_numeric(df_vl_solar["vlaanderen_zon_mw"], errors="coerce") * 1000.0
    df_vl_solar = df_vl_solar[["datetime", "vlaanderen_zon_kwh"]]
    logger.info("Vlaanderen solar: %d rows", len(df_vl_solar))

    # --- Vlaanderen wind ----------------------------------------------------
    logger.info("Reading clean.clean_wind_hourly...")
    df_vl_wind = pd.read_sql("SELECT datetime, vlaanderen_wind_mw FROM clean.clean_wind_hourly", engine)
    df_vl_wind["datetime"] = pd.to_datetime(df_vl_wind["datetime"], errors="coerce", utc=True)
    df_vl_wind = df_vl_wind.dropna(subset=["datetime"])
    df_vl_wind["vlaanderen_wind_kwh"] = pd.to_numeric(df_vl_wind["vlaanderen_wind_mw"], errors="coerce") * 1000.0
    df_vl_wind = df_vl_wind[["datetime", "vlaanderen_wind_kwh"]]
    logger.info("Vlaanderen wind: %d rows", len(df_vl_wind))

    # --- ELIA solar (Flanders, hourly avg from clean table) -----------------
    logger.info("Reading clean.clean_elia_solar...")
    df_elia_solar = _read_elia_clean("clean_elia_solar", "elia_zon_kwh", engine)

    # --- ELIA wind (Flanders, hourly avg from clean table) ------------------
    logger.info("Reading clean.clean_elia_wind...")
    df_elia_wind = _read_elia_clean("clean_elia_wind", "elia_wind_kwh", engine)

    # --- Join all sources on timestamp --------------------------------------
    df = (
        df_vl_solar
        .merge(df_vl_wind, on="datetime", how="outer")
        .merge(df_elia_solar, on="datetime", how="outer")
        .merge(df_elia_wind, on="datetime", how="outer")
        .sort_values("datetime")
        .reset_index(drop=True)
    )

    # Rename datetime → tijd and round values
    df = df.rename(columns={"datetime": "tijd"})
    df["vlaanderen_zon_kwh"] = pd.to_numeric(df["vlaanderen_zon_kwh"], errors="coerce").fillna(0).round(3)
    df["vlaanderen_wind_kwh"] = pd.to_numeric(df["vlaanderen_wind_kwh"], errors="coerce").fillna(0).round(3)
    df["elia_zon_kwh"] = pd.to_numeric(df["elia_zon_kwh"], errors="coerce").round(3)
    df["elia_wind_kwh"] = pd.to_numeric(df["elia_wind_kwh"], errors="coerce").round(3)

    df = df[["tijd", "vlaanderen_zon_kwh", "vlaanderen_wind_kwh", "elia_zon_kwh", "elia_wind_kwh"]]

    logger.info("Combined table: %d rows", len(df))
    logger.info("Date range: %s → %s", df["tijd"].min(), df["tijd"].max())

    df.to_sql(CLEAN_TABLE, engine, schema="clean", if_exists="replace", index=False)
    logger.info("Wrote %d rows → clean.%s", len(df), CLEAN_TABLE)


def _read_elia_clean(table: str, out_col: str, engine) -> pd.DataFrame:
    """Read an ELIA clean table, filter to Flanders, and return datetime + kWh column."""
    df = pd.read_sql(f"SELECT * FROM clean.{table}", engine)

    # Normalise timestamp column
    ts_col = next(
        (c for c in df.columns if any(kw in c.lower() for kw in ("datetime", "timestamp"))),
        None,
    )
    if ts_col is None:
        raise ValueError(f"No timestamp column in clean.{table}")

    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df = df.dropna(subset=[ts_col])
    df = df.rename(columns={ts_col: "datetime"})

    # Filter to Flanders if region column exists
    if "region" in df.columns and (df["region"] == ELIA_REGION).any():
        df = df[df["region"] == ELIA_REGION]

    # Find the value column (mostrecentforecast preferred, else measured or realtime)
    value_col = next(
        (c for c in ("mostrecentforecast", "measured", "realtime") if c in df.columns),
        None,
    )
    if value_col is None:
        raise ValueError(f"No value column found in clean.{table}")

    df[value_col] = pd.to_numeric(df[value_col], errors="coerce")
    df = df.dropna(subset=[value_col])

    # Convert MW → kWh
    df[out_col] = df[value_col] * 1000.0

    return df[["datetime", out_col]]
