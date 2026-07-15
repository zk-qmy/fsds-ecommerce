"""
DP3 — Gold + Flink stream -> feature tables.

wait_for_gold (ExternalTaskSensor on dp2_gold) -> run_flink -> {feat_customer_90d,
feat_stream_60m} in parallel -> feat_customer_unified -> validate_features.
feat_customer_90d/feat_stream_60m only share `run_flink` + `wait_for_gold` as upstream,
not each other — feat_customer_unified needs both populated first for its as-of join.
See dags/plan.md §8.

One-time setup (per dags/plan.md §10 — never hardcoded below). Host is the
docker-compose service name (postgres), not localhost — this container reaches
it over the compose network, not host networking (confirmed live that
network_mode: host doesn't work under Docker Desktop without an opt-in feature):
    airflow variables set repo_root /opt/project
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
PROJECT_PYTHON = "/opt/venvs/project/bin/python3"
FLINK_CLEAN_EVENTS_DIR = "b_schema_pipelines/streaming_data/flink_clean_events/optimized"
POSTGRES_URL = "jdbc:postgresql://postgres:5432/fsds"

default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(seconds=120),
}


def _validate_features(postgres_cfg: dict) -> None:
    from b_schema_pipelines.dq.validation_runner import validate_feature_tables

    validate_feature_tables(postgres_cfg)


with DAG(
    dag_id="dp3_feature",
    description="Gold + Flink stream -> feature tables, validated at the end.",
    schedule="30 2 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
) as dag:
    wait_for_gold = ExternalTaskSensor(
        task_id="wait_for_gold",
        external_dag_id="dp2_gold",
        external_task_id="validate_gold",
        execution_delta=timedelta(hours=1, minutes=30),
        mode="reschedule",
        timeout=1800,
    )

    run_flink = BashOperator(
        task_id="run_flink",
        bash_command=(
            f"cd {REPO_ROOT_VAR} && "
            "uv run --no-project --python 3.12 --with apache-flink python3 "
            "b_schema_pipelines/pipelines/streaming/flink_stream_pipeline.py --mode optimized"
        ),
    )

    feat_customer_90d = BashOperator(
        task_id="feat_customer_90d",
        bash_command=(
            f"cd {REPO_ROOT_VAR} && "
            "uv run python3 b_schema_pipelines/pipelines/features/feat_customer_90d.py "
            "--snapshot-date {{ ds }} "
            f"--postgres-url {POSTGRES_URL}"
        ),
    )

    feat_stream_60m = BashOperator(
        task_id="feat_stream_60m",
        bash_command=(
            f"cd {REPO_ROOT_VAR} && "
            "uv run python3 b_schema_pipelines/pipelines/features/feat_stream_60m.py "
            f"--events-source {FLINK_CLEAN_EVENTS_DIR} "
            f"--postgres-url {POSTGRES_URL}"
        ),
    )

    feat_customer_unified = BashOperator(
        task_id="feat_customer_unified",
        bash_command=(
            f"cd {REPO_ROOT_VAR} && "
            "uv run python3 b_schema_pipelines/pipelines/features/feat_customer_unified.py "
            f"--postgres-url {POSTGRES_URL}"
        ),
    )

    validate_features = ExternalPythonOperator(
        task_id="validate_features",
        python=PROJECT_PYTHON,
        python_callable=_validate_features,
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

    wait_for_gold >> run_flink
    run_flink >> [feat_customer_90d, feat_stream_60m] >> feat_customer_unified >> validate_features
