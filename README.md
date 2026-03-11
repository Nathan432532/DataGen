# Data Engineering Lab — Airflow + PostgreSQL

Multi-pipeline data engineering project orchestrated by **Apache Airflow** and backed by **PostgreSQL**.

---

## Architecture

```
project-root
│
├── docker-compose.yml          # Postgres + Airflow (webserver, scheduler)
├── Dockerfile                  # Extends official Airflow image
├── requirements.txt            # Python dependencies
├── .env / .env.example         # Environment variables
│
├── dags/                       # Airflow DAG definitions
│   ├── weather_kaggle_dag.py
│   ├── fluvius_dag.py
│   ├── elia_dag.py
│   └── energy_vlaanderen_dag.py
│
├── src/                        # Reusable ETL modules
│   ├── common/                 #   shared DB, validation, logging
│   ├── weather_kaggle/         #   Kaggle weather Antwerp
│   ├── fluvius/                #   Fluvius energy consumption
│   ├── elia/                   #   ELIA generation data
│   └── vlaanderen_energy/      #   Vlaanderen solar + wind
│
├── sql/
│   └── init.sql                # Creates raw / clean schemas on first boot
│
├── app.py                      # Standalone runner (for testing outside Airflow)
└── README.md
```

## Data Pipelines

| DAG ID | Source | Format | Raw Table | Clean Table |
|---|---|---|---|---|
| `weather_kaggle_antwerp` | Kaggle (kagglehub) | CSV (`;`) | `raw.raw_weather_antwerp` | `clean.clean_weather_hourly` |
| `fluvius_energy` | Fluvius Open Data | Parquet / JSON | `raw.raw_fluvius_energy` | `clean.clean_energy_hourly` |
| `elia_generation` | ELIA Open Data API | JSON | `raw.raw_elia_generation` | `clean.clean_elia_generation` |
| `energy_vlaanderen` | ELIA (solar ods032, wind ods031) | CSV | `raw.raw_vlaanderen_solar` / `raw.raw_vlaanderen_wind` | `clean.clean_solar_hourly` / `clean.clean_wind_hourly` |

Every pipeline follows: **extract → load_raw → validate → transform_clean**.

### Database layers

| Schema | Purpose |
|---|---|
| `raw` | As-is ingestion + metadata (`ingested_at`, `source`, `run_id`) |
| `clean` | Normalised timestamps, deduped, ready for analysis |

---

## Quick start

### 1. Create `.env`

```bash
cp .env.example .env
# edit .env with your credentials
```

Your `.env` needs:

```dotenv
POSTGRES_USER=user
POSTGRES_PASSWORD=supersecret
POSTGRES_DB=bank
KAGGLE_API_TOKEN={"username":"your_kaggle_username","key":"your_kaggle_api_key"}
```

### 2. Start everything

```bash
docker compose up --build
```

This starts:

| Service | Port |
|---|---|
| PostgreSQL | `localhost:5332` |
| Airflow Webserver | `localhost:8080` |
| Airflow Scheduler | (internal) |

### 3. Open Airflow UI

Navigate to **http://localhost:8080** and log in:

- **Username:** `admin`
- **Password:** `admin`

### 4. Trigger DAGs

1. In the Airflow UI, toggle each DAG **ON** (unpause).
2. Click the **play** button (▶) on a DAG to trigger it manually.
3. Watch tasks turn green in the Graph view.

---

## Verification queries

Connect to PostgreSQL (`localhost:5332`, database `bank`):

```sql
-- Check schemas exist
SELECT schema_name FROM information_schema.schemata
WHERE schema_name IN ('raw', 'clean');

-- Row counts for raw tables
SELECT 'raw.raw_weather_antwerp'   AS tbl, COUNT(*) FROM raw.raw_weather_antwerp
UNION ALL
SELECT 'raw.raw_fluvius_energy',           COUNT(*) FROM raw.raw_fluvius_energy
UNION ALL
SELECT 'raw.raw_elia_generation',          COUNT(*) FROM raw.raw_elia_generation
UNION ALL
SELECT 'raw.raw_vlaanderen_solar',         COUNT(*) FROM raw.raw_vlaanderen_solar
UNION ALL
SELECT 'raw.raw_vlaanderen_wind',          COUNT(*) FROM raw.raw_vlaanderen_wind;

-- Row counts for clean tables
SELECT 'clean.clean_weather_hourly'  AS tbl, COUNT(*) FROM clean.clean_weather_hourly
UNION ALL
SELECT 'clean.clean_energy_hourly',          COUNT(*) FROM clean.clean_energy_hourly
UNION ALL
SELECT 'clean.clean_elia_generation',        COUNT(*) FROM clean.clean_elia_generation
UNION ALL
SELECT 'clean.clean_solar_hourly',           COUNT(*) FROM clean.clean_solar_hourly
UNION ALL
SELECT 'clean.clean_wind_hourly',            COUNT(*) FROM clean.clean_wind_hourly;

-- Duplicate timestamp check (clean tables)
SELECT timestamp, COUNT(*) FROM clean.clean_weather_hourly
GROUP BY timestamp HAVING COUNT(*) > 1;
```

---

## Stopping

```bash
docker compose down          # stop containers
docker compose down -v       # stop + delete volumes (fresh start)
```

---

## Validation rules (built into every DAG)

- Table exists in the expected schema
- Row count > 0
- Metadata columns (`ingested_at`, `source`, `run_id`) are present
- No duplicate timestamps in clean tables

If any check fails the Airflow task is marked **FAILED**.

---

## Tech stack

- **Apache Airflow 2.8** — orchestration
- **PostgreSQL 15** — storage
- **Python 3.11** — ETL logic
- **Docker Compose** — deployment
- **pandas / SQLAlchemy / requests / kagglehub / pyarrow** — data processing
