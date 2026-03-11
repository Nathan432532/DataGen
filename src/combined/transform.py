"""Transform — combine energy production data from multiple sources into one table.

This script creates a unified hourly view of energy production combining:
- Vlaanderen solar (Flanders)
- Vlaanderen wind (Flanders)
- ELIA solar (Flanders)
- ELIA wind (Flanders)

All sources provide 15-minute resolution data, which is aggregated to hourly averages.
Note: Using Flanders region for consistent data availability across all sources.
"""

import pandas as pd

from src.common.db import get_engine
from src.common.logging_config import setup_logging

logger = setup_logging("combined.transform")

CLEAN_TABLE = "clean_combined_energy"


def transform_combined_energy() -> None:
    """
    Combine energy production data from multiple sources into a single hourly table.

    Output columns:
    - tijd (timestamp): Hourly timestamp
    - energie_vlaanderen_zon_megawatt: Vlaanderen solar production (Flanders)
    - energie_vlaanderen_wind_megawatt: Vlaanderen wind production (Flanders)
    - elia_zon_megawatt: ELIA solar forecast (Flanders)
    - elia_wind_megawatt: ELIA wind forecast (Flanders)
    """
    engine = get_engine()

    # Use Flanders region for all sources (most consistent data availability)
    region = 'Flanders'

    # Fetch Vlaanderen solar (Flanders, measured)
    logger.info(f"Fetching Vlaanderen solar data ({region})...")
    df_vl_solar = pd.read_sql(f"""
        SELECT
            timestamp,
            measured as value
        FROM clean.clean_solar_hourly
        WHERE region = '{region}'
        AND measured IS NOT NULL
    """, engine)

    # Fetch Vlaanderen wind (Flanders, measured)
    logger.info(f"Fetching Vlaanderen wind data ({region})...")
    df_vl_wind = pd.read_sql(f"""
        SELECT
            timestamp,
            measured as value
        FROM clean.clean_wind_hourly
        WHERE region = '{region}'
        AND measured IS NOT NULL
    """, engine)

    # Fetch ELIA solar (Flanders, forecast)
    logger.info(f"Fetching ELIA solar data ({region})...")
    df_elia_solar = pd.read_sql(f"""
        SELECT
            timestamp,
            mostrecentforecast as value
        FROM clean.clean_elia_solar
        WHERE region = '{region}'
        AND mostrecentforecast IS NOT NULL
    """, engine)

    # Fetch ELIA wind (Flanders, forecast)
    logger.info(f"Fetching ELIA wind data ({region})...")
    df_elia_wind = pd.read_sql(f"""
        SELECT
            timestamp,
            mostrecentforecast as value
        FROM clean.clean_elia_wind
        WHERE region = '{region}'
        AND mostrecentforecast IS NOT NULL
    """, engine)

    # Convert timestamps to datetime
    for df in [df_vl_solar, df_vl_wind, df_elia_solar, df_elia_wind]:
        if not df.empty:
            df['timestamp'] = pd.to_datetime(df['timestamp'])

    # Aggregate to hourly (average of 15-minute intervals)
    logger.info("Aggregating to hourly resolution...")

    def aggregate_to_hourly(df: pd.DataFrame, name: str) -> pd.DataFrame:
        if df.empty:
            logger.warning(f"No data for {name}")
            return pd.DataFrame(columns=['timestamp', name])

        df['hour'] = df['timestamp'].dt.floor('H')
        hourly = df.groupby('hour')['value'].mean().reset_index()
        hourly.columns = ['timestamp', name]
        logger.info(f"{name}: {len(df)} 15-min records → {len(hourly)} hourly records")
        return hourly

    df_vl_solar_hourly = aggregate_to_hourly(df_vl_solar, 'energie_vlaanderen_zon_megawatt')
    df_vl_wind_hourly = aggregate_to_hourly(df_vl_wind, 'energie_vlaanderen_wind_megawatt')
    df_elia_solar_hourly = aggregate_to_hourly(df_elia_solar, 'elia_zon_megawatt')
    df_elia_wind_hourly = aggregate_to_hourly(df_elia_wind, 'elia_wind_megawatt')

    # Get all unique timestamps
    all_timestamps = pd.concat([
        df_vl_solar_hourly[['timestamp']],
        df_vl_wind_hourly[['timestamp']],
        df_elia_solar_hourly[['timestamp']],
        df_elia_wind_hourly[['timestamp']]
    ]).drop_duplicates().sort_values('timestamp')

    logger.info(f"Total unique hourly timestamps: {len(all_timestamps)}")

    # Merge all data sources
    df_combined = all_timestamps.copy()
    df_combined = df_combined.merge(df_vl_solar_hourly, on='timestamp', how='left')
    df_combined = df_combined.merge(df_vl_wind_hourly, on='timestamp', how='left')
    df_combined = df_combined.merge(df_elia_solar_hourly, on='timestamp', how='left')
    df_combined = df_combined.merge(df_elia_wind_hourly, on='timestamp', how='left')

    # Rename timestamp column to 'tijd' (Dutch for 'time')
    df_combined = df_combined.rename(columns={'timestamp': 'tijd'})

    # Sort by time
    df_combined = df_combined.sort_values('tijd').reset_index(drop=True)

    # Count nulls before filtering
    total_before = len(df_combined)
    has_data = df_combined[['energie_vlaanderen_zon_megawatt', 'energie_vlaanderen_wind_megawatt',
                             'elia_zon_megawatt', 'elia_wind_megawatt']].notna().any(axis=1)
    df_combined = df_combined[has_data].reset_index(drop=True)

    logger.info(f"Filtered out {total_before - len(df_combined)} empty rows (kept {len(df_combined)} rows with data)")

    # Log summary
    logger.info(f"Combined table: {len(df_combined)} rows")
    logger.info(f"Columns: {list(df_combined.columns)}")
    logger.info(f"Date range: {df_combined['tijd'].min()} to {df_combined['tijd'].max()}")

    # Log data availability
    vl_solar_count = df_combined['energie_vlaanderen_zon_megawatt'].notna().sum()
    vl_wind_count = df_combined['energie_vlaanderen_wind_megawatt'].notna().sum()
    elia_solar_count = df_combined['elia_zon_megawatt'].notna().sum()
    elia_wind_count = df_combined['elia_wind_megawatt'].notna().sum()

    logger.info(f"Data availability:")
    logger.info(f"  Vlaanderen Solar: {vl_solar_count}/{len(df_combined)} rows ({100*vl_solar_count/len(df_combined):.1f}%)")
    logger.info(f"  Vlaanderen Wind:  {vl_wind_count}/{len(df_combined)} rows ({100*vl_wind_count/len(df_combined):.1f}%)")
    logger.info(f"  ELIA Solar:       {elia_solar_count}/{len(df_combined)} rows ({100*elia_solar_count/len(df_combined):.1f}%)")
    logger.info(f"  ELIA Wind:        {elia_wind_count}/{len(df_combined)} rows ({100*elia_wind_count/len(df_combined):.1f}%)")

    logger.info(f"Sample data:\n{df_combined.head(10)}")

    # Write to database
    df_combined.to_sql(
        CLEAN_TABLE,
        engine,
        schema='clean',
        if_exists='replace',
        index=False
    )

    logger.info(f"Successfully wrote {len(df_combined)} rows to clean.{CLEAN_TABLE}")
