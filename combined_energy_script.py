#!/usr/bin/env python3
"""
Standalone script to generate combined energy data table.

This script combines energy production data from multiple sources:
- Vlaanderen solar (Antwerp)
- Vlaanderen wind (Flanders)
- ELIA solar (Antwerp)
- ELIA wind (Flanders)

All data is aggregated to hourly resolution and stored in clean.clean_combined_energy.

Usage:
    python combined_energy_script.py
"""

import sys
import os

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.combined.transform import transform_combined_energy
from src.common.logging_config import setup_logging

logger = setup_logging("combined_energy_script")


def main():
    """Run the combined energy transformation."""
    try:
        logger.info("=" * 60)
        logger.info("Starting combined energy data transformation")
        logger.info("=" * 60)

        transform_combined_energy()

        logger.info("=" * 60)
        logger.info("✓ Combined energy data transformation completed successfully")
        logger.info("=" * 60)
        logger.info("\nTo view the data, run:")
        logger.info("  docker compose exec -T postgres psql -U user -d bank -c \"SELECT * FROM clean.clean_combined_energy ORDER BY tijd LIMIT 10;\"")

        return 0

    except Exception as e:
        logger.error("=" * 60)
        logger.error(f"✗ Error during transformation: {e}")
        logger.error("=" * 60)
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
