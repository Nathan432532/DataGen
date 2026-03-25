# DataGen — Energy Data Pipeline

Airflow + PostgreSQL pipeline that ingests Flemish energy production data, normalises it to hourly resolution, and combines it into a single analysis-ready table.

---

## Architecture

```
project-root
│
├── docker-compose.yml      # PostgreSQL + Airflow (webserver, scheduler)
├── Dockerfile              # Extends official Airflow image
├── requirements.txt        # Python dependencies
├── .env / .env.example     # Credentials and config
│
├── dags/                   # Airflow DAG definitions
│   ├── backfill_energy_dag.py      # One-shot historical backfill (manual trigger)
│   ├── combined_energy_dag.py      # Daily end-to-end pipeline (main DAG)
│   ├── energy_vlaanderen_dag.py    # Vlaanderen solar + wind only
│   ├── elia_dag.py                 # ELIA solar + wind only
│   ├── fluvius_dag.py              # Fluvius energy consumption
│   └── weather_kaggle_dag.py       # Antwerp weather
│
├── src/                    # ETL modules (used by DAGs)
│   ├── common/             # Database connection, logging, validation
│   ├── vlaanderen_energy/  # Realtime solar + wind (Netlify CSV per-day files)
│   ├── elia/               # Solar + wind measured data (ELIA Open Data)
│   ├── fluvius/            # Energy consumption (Fluvius Open Data)
│   ├── weather_kaggle/     # Antwerp weather (Kaggle)
│   └── combined/           # Merges Vlaanderen + ELIA into the output table
│
└── sql/
    └── init.sql            # Creates raw and clean schemas on first boot
```

---

## Data pipelines

Each pipeline follows: **extract → load_raw → validate → transform_clean**

| DAG | Source | Raw table | Clean table |
|---|---|---|---|
| `energy_vlaanderen` | Netlify realtime CSVs (NIS per-municipality MW) | `raw.raw_vlaanderen_solar` / `raw.raw_vlaanderen_wind` | `clean.clean_solar_hourly` / `clean.clean_wind_hourly` |
| `elia_generation` | ELIA Open Data ods031 (wind) / ods032 (solar) | `raw.raw_elia_wind` / `raw.raw_elia_solar` | `clean.clean_elia_wind` / `clean.clean_elia_solar` |
| `combined_energy` | Combines Vlaanderen + ELIA | — | `clean.clean_combined_energy` |
| `fluvius_energy` | Fluvius Open Data | `raw.raw_fluvius_energy` | `clean.clean_energy_hourly` |
| `weather_kaggle_antwerp` | Kaggle (kagglehub) | `raw.raw_weather_antwerp` | `clean.clean_weather_hourly` |

### Output table: `clean.clean_combined_energy`

| Column | Description |
|---|---|
| `tijd` | Hourly timestamp |
| `vlaanderen_zon_kwh` | Vlaanderen solar production (sum of NIS municipalities, kWh) |
| `vlaanderen_wind_kwh` | Vlaanderen wind production (sum of NIS municipalities, kWh) |
| `elia_zon_kwh` | ELIA solar measured (Flanders, kWh) |
| `elia_wind_kwh` | ELIA wind measured (Flanders, kWh) |

Values are converted from MW to kWh (MW * 1000). ELIA 15-minute data is aggregated to hourly averages before conversion.

### Database layers

| Schema | Purpose |
|---|---|
| `raw` | As-is ingestion with metadata columns (`ingested_at`, `source`, `run_id`) |
| `clean` | Normalised timestamps, deduped, hourly resolution, ready for analysis |

---

## Quick start

### 1. Create `.env`

```bash
cp .env.example .env
# fill in your credentials
```

```dotenv
POSTGRES_USER=user
POSTGRES_PASSWORD=supersecret
POSTGRES_DB=bank
KAGGLE_API_TOKEN=KGAT_xxxxxxxxxxxxxxxxxxxx
```

### 2. Start the stack

```bash
docker compose up --build
```

| Service | URL |
|---|---|
| Airflow UI | http://localhost:8080 (admin / admin) |
| pgAdmin | http://localhost:5050 (admin@admin.com / admin) |
| PostgreSQL | localhost:5332 |

### 3. Populate historical data

Trigger `backfill_energy` manually from the Airflow UI. This fetches all available data from 2025-03-01 to today across all sources and writes the combined output table. Run this once before enabling the daily DAG.

### 4. Enable daily updates

Unpause `combined_energy`. It runs daily, fetching that day's data from both sources and appending to the combined table.

---

## Verification queries

```sql
-- Row counts per table
SELECT 'raw.raw_vlaanderen_solar'  AS tbl, COUNT(*) FROM raw.raw_vlaanderen_solar
UNION ALL
SELECT 'raw.raw_vlaanderen_wind',         COUNT(*) FROM raw.raw_vlaanderen_wind
UNION ALL
SELECT 'raw.raw_elia_solar',              COUNT(*) FROM raw.raw_elia_solar
UNION ALL
SELECT 'raw.raw_elia_wind',               COUNT(*) FROM raw.raw_elia_wind;

-- Combined output (most recent 10 hours)
SELECT * FROM clean.clean_combined_energy ORDER BY tijd DESC LIMIT 10;

-- Date range covered
SELECT MIN(tijd), MAX(tijd), COUNT(*) FROM clean.clean_combined_energy;
```

---

## Stopping

```bash
docker compose down       # stop containers, keep data
docker compose down -v    # stop containers and delete all data
```

---

## Tech stack

- **Apache Airflow 2.8** — orchestration
- **PostgreSQL 15** — storage
- **pgAdmin 4** — database UI
- **Python 3.11** — ETL logic
- **Docker Compose** — local deployment
- **pandas / SQLAlchemy / requests / pyarrow** — data processing
