"""
DP1 — raw source files -> Bronze (Delta Lake on MinIO).

Two stages: ingest_bronze (BashOperator, runs ingest_bronze.py unmodified in the
project's own Python 3.13 venv) -> validate_bronze (ExternalPythonOperator, runs
b_schema_pipelines.dq.validation_runner.validate_bronze_tables in that same venv).
See dags/plan.md §4/§6 for why two Python environments are involved and §9 for the
validation runner's design.

One-time setup (per dags/plan.md §10 — never hardcoded below). Host is the
docker-compose service name (minio), not localhost — this container reaches it
over the compose network, not host networking (confirmed live that
network_mode: host doesn't work under Docker Desktop without an opt-in feature):
    airflow variables set repo_root /opt/project
    airflow connections add fsds_minio --conn-type http \
        --conn-host minio --conn-port 9000 \
        --conn-login minio_access_key --conn-password minio_secret_key
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ExternalPythonOperator

REPO_ROOT_VAR = "{{ var.value.repo_root }}"
PROJECT_PYTHON = "/opt/project/.venv/bin/python3"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(seconds=120),
}


def _validate_bronze(minio_cfg: dict) -> None:
    # Deferred import: Airflow's own environment doesn't have great_expectations,
    # deltalake, or psycopg2 installed (dags/plan.md §4) — only this callable's
    # target interpreter (/opt/project/.venv) does. Importing at DAG module scope
    # would break DAG parsing in Airflow's own process.
    from b_schema_pipelines.dq.validation_runner import validate_bronze_tables

    validate_bronze_tables(minio_cfg)


with DAG(
    dag_id="dp1_bronze",
    description="Raw source files -> Bronze Delta Lake, then validate.",
    schedule="0 0 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:
    ingest_bronze = BashOperator(
        task_id="ingest_bronze",
        bash_command=(
            f"cd {REPO_ROOT_VAR} && "
            "uv run python3 b_schema_pipelines/pipelines/bronze/ingest_bronze.py"
        ),
    )

    validate_bronze = ExternalPythonOperator(
        task_id="validate_bronze",
        python=PROJECT_PYTHON,
        python_callable=_validate_bronze,
        op_kwargs={
            "minio_cfg": {
                "endpoint": "http://{{ conn.fsds_minio.host }}:{{ conn.fsds_minio.port }}",
                "access_key": "{{ conn.fsds_minio.login }}",
                "secret_key": "{{ conn.fsds_minio.password }}",
            },
        },
    )

    ingest_bronze >> validate_bronze
