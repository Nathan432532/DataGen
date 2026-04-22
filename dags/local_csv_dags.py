"""Dedicated DAGs for the three local CSV datasets in Data/."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.local_data.pipeline import (
    CLEAN_TABLES,
    RAW_TABLES,
    extract_local_csv,
    load_raw_dataset,
    transform_dataset,
    validate_table,
)

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

DATASETS = {
    "local_solar_radiation": {
        "dataset_key": "solar_radiation",
        "description": "Local CSV pipeline for sun_combined.csv",
        "tags": ["local", "csv", "solar", "graphs"],
    },
    "local_energy_consumption": {
        "dataset_key": "energy_consumption",
        "description": "Local CSV pipeline for consumptie.csv",
        "tags": ["local", "csv", "energy", "graphs"],
    },
    "local_wind_history": {
        "dataset_key": "wind_history",
        "description": "Local CSV pipeline for v_wind_alles_compleet.csv",
        "tags": ["local", "csv", "wind", "graphs"],
    },
}


def _extract(dataset_key: str, **ctx) -> None:
    df = extract_local_csv(dataset_key)
    ctx["ti"].xcom_push(key=f"{dataset_key}_rows", value=len(df))


def _load_raw(dataset_key: str, **ctx) -> None:
    df = extract_local_csv(dataset_key)
    load_raw_dataset(df, dataset_key, run_id=ctx["run_id"])


def _validate_raw(dataset_key: str, **ctx) -> None:
    validate_table("raw", RAW_TABLES[dataset_key])


def _transform_clean(dataset_key: str, **ctx) -> None:
    transform_dataset(dataset_key)


def _validate_clean(dataset_key: str, **ctx) -> None:
    validate_table("clean", CLEAN_TABLES[dataset_key])


def _build_dag(dag_id: str, dataset_key: str, description: str, tags: list[str]) -> DAG:
    with DAG(
        dag_id=dag_id,
        default_args=default_args,
        description=description,
        schedule=None,
        start_date=datetime(2024, 1, 1),
        catchup=False,
        tags=tags,
    ) as dag:
        t_extract = PythonOperator(
            task_id="extract",
            python_callable=_extract,
            op_kwargs={"dataset_key": dataset_key},
        )
        t_load = PythonOperator(
            task_id="load_raw",
            python_callable=_load_raw,
            op_kwargs={"dataset_key": dataset_key},
        )
        t_validate_raw = PythonOperator(
            task_id="validate_raw",
            python_callable=_validate_raw,
            op_kwargs={"dataset_key": dataset_key},
        )
        t_transform = PythonOperator(
            task_id="transform_clean",
            python_callable=_transform_clean,
            op_kwargs={"dataset_key": dataset_key},
        )
        t_validate_clean = PythonOperator(
            task_id="validate_clean",
            python_callable=_validate_clean,
            op_kwargs={"dataset_key": dataset_key},
        )

        t_extract >> t_load >> t_validate_raw >> t_transform >> t_validate_clean

    return dag


for dag_name, config in DATASETS.items():
    globals()[dag_name] = _build_dag(
        dag_id=dag_name,
        dataset_key=config["dataset_key"],
        description=config["description"],
        tags=config["tags"],
    )
