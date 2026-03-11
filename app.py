"""Standalone pipeline runner — for testing outside Airflow.

In production the Airflow DAGs orchestrate these same modules.

Usage (inside the Docker network or with correct env vars):
    python app.py
"""

import sys
import os

# Ensure project root is on the path so ``src.*`` imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.weather_kaggle.extract import extract_weather_data
from src.weather_kaggle.load import load_raw_weather
from src.weather_kaggle.transform import transform_weather_to_clean
from src.common.validation import validate_table_exists, validate_row_count


def main() -> None:
    print("=== Weather Antwerp Pipeline (standalone) ===")

    df = extract_weather_data()

    run_id = load_raw_weather(df)

    validate_table_exists("raw", "raw_weather_antwerp")
    count = validate_row_count("raw", "raw_weather_antwerp")
    print(f"raw.raw_weather_antwerp → {count} rows")

    transform_weather_to_clean()

    print("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
