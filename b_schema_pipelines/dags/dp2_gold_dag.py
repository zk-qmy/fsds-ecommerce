"""
DP2 — Bronze -> Silver -> Gold.

wait_for_bronze (ExternalTaskSensor on dp1_bronze) -> transform_silver -> validate_silver
-> build_gold -> validate_gold. One DAG for Silver+Gold (not two) — the rubric allows
"bronze -> silver and gold zone (or bronze -> gold only)" as a single DP2 line; splitting
would only add a second cross-DAG sensor hop for no rubric benefit. See dags/plan.md §7.

One-time setup (per dags/plan.md §10 — never hardcoded below). Hosts are the
docker-compose service names (postgres, minio), not localhost — this container
reaches them over the compose network, not host networking (confirmed live that
network_mode: host doesn't work under Docker Desktop without an opt-in feature):
    airflow variables set repo_root /opt/project
    airflow connections add fsds_minio --conn-type http \
        --conn-host minio --conn-port 9000 \
        --conn-login minio_access_key --conn-password minio_secret_key
    airflow connections add fsds_postgres --conn-type postgres \
        --conn-host postgres --conn-port 5432 --conn-schema fsds \
        --conn-login fsds --conn-password fsds
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import ExternalPythonOperator
from airflow.sensors.external_task import ExternalTaskSensor

REPO_ROOT_VAR = "{{ var.value.repo_root }}"
PROJECT_PYTHON = "/opt/project/.venv/bin/python3"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/fsds"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(seconds=120),
}


def _validate_silver(minio_cfg: dict) -> None:
    from b_schema_pipelines.dq.validation_runner import validate_silver_tables

    validate_silver_tables(minio_cfg)


def _validate_gold(postgres_cfg: dict) -> None:
    from b_schema_pipelines.dq.validation_runner import validate_gold_tables

    validate_gold_tables(postgres_cfg)


with DAG(
    dag_id="dp2_gold",
    description="Bronze -> Silver -> Gold, validated at each stage.",
    schedule="0 1 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:
    wait_for_bronze = ExternalTaskSensor(
        task_id="wait_for_bronze",
        external_dag_id="dp1_bronze",
        external_task_id="validate_bronze",
        execution_delta=timedelta(hours=1),
        mode="reschedule",
        timeout=1800,
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command=(
            f"cd {REPO_ROOT_VAR} && "
            "uv run python3 b_schema_pipelines/pipelines/silver/transform_silver.py --mode optimized"
        ),
    )

    validate_silver = ExternalPythonOperator(
        task_id="validate_silver",
        python=PROJECT_PYTHON,
        python_callable=_validate_silver,
        op_kwargs={
            "minio_cfg": {
                "endpoint": "http://{{ conn.fsds_minio.host }}:{{ conn.fsds_minio.port }}",
                "access_key": "{{ conn.fsds_minio.login }}",
                "secret_key": "{{ conn.fsds_minio.password }}",
            },
        },
    )

    build_gold = BashOperator(
        task_id="build_gold",
        bash_command=(
            f"cd {REPO_ROOT_VAR} && "
            "uv run python3 b_schema_pipelines/pipelines/gold/build_gold.py --mode optimized "
            f"--postgres-url {POSTGRES_URL}"
        ),
    )

    validate_gold = ExternalPythonOperator(
        task_id="validate_gold",
        python=PROJECT_PYTHON,
        python_callable=_validate_gold,
        op_kwargs={
            "postgres_cfg": {
                "host": "{{ conn.fsds_postgres.host }}",
                "port": "{{ conn.fsds_postgres.port }}",
                "db": "{{ conn.fsds_postgres.schema }}",
                "user": "{{ conn.fsds_postgres.login }}",
                "password": "{{ conn.fsds_postgres.password }}",
            },
        },
    )

    wait_for_bronze >> transform_silver >> validate_silver >> build_gold >> validate_gold
