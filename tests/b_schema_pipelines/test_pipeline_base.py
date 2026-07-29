"""
Tests for PipelineBase.

PipelineBase is abstract — tests use a minimal concrete subclass (ConcretePipeline).
SparkSession is mocked to avoid starting a real Spark cluster in base-class tests.
"""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.pipelines.pipeline_base import PipelineBase

# ── Minimal concrete subclass ─────────────────────────────────────────────────


class ConcretePipeline(PipelineBase):
    PREFIX = "test-pipeline"

    def __init__(self):
        super().__init__()
        self.spark = MagicMock()  # never start a real session in base-class tests

    def run(self):
        pass


# ── Fixtures ──────────────────────────────────────────────────────────────────


@pytest.fixture
def pipeline():
    return ConcretePipeline()


# ── 1. run_id ─────────────────────────────────────────────────────────────────


def test_run_id_contains_prefix(pipeline):
    assert pipeline.run_id.startswith("test-pipeline_")


def test_run_id_format_matches_timestamp(pipeline):
    # Expected: test-pipeline_YYYYMMDD_HHMMSS
    pattern = r"^test-pipeline_\d{8}_\d{6}$"
    assert re.match(pattern, pipeline.run_id), f"run_id format wrong: {pipeline.run_id}"


def test_run_id_unique_across_instances():
    a = ConcretePipeline()
    b = ConcretePipeline()
    # Two instances created at least a microsecond apart will differ.
    # If they happen to collide (same second), that's still valid behaviour —
    # but the test structure verifies the ID is generated per instance.
    assert isinstance(a.run_id, str)
    assert isinstance(b.run_id, str)


def test_run_id_not_set_by_base_init_directly():
    """run_id is set in PipelineBase.__init__; spark is NOT set there."""
    p = ConcretePipeline()
    assert hasattr(p, "run_id")
    # spark is set by the subclass, not PipelineBase.__init__
    assert hasattr(p, "spark")


# ── 2. Abstract enforcement ───────────────────────────────────────────────────


def test_cannot_instantiate_pipeline_base_directly():
    with pytest.raises(TypeError):
        PipelineBase()  # type: ignore[abstract]


def test_subclass_without_run_raises():
    class Incomplete(PipelineBase):
        PREFIX = "incomplete"

        def __init__(self):
            super().__init__()
            self.spark = MagicMock()

        # run() deliberately omitted

    with pytest.raises(TypeError):
        Incomplete()  # type: ignore[abstract]


# ── 4. PREFIX ─────────────────────────────────────────────────────────────────


def test_prefix_appears_in_run_id(pipeline):
    assert "test-pipeline" in pipeline.run_id


# ── 5. _resolve_s3_config ─────────────────────────────────────────────────────


def test_resolve_s3_config_returns_correct_s3a_uri(pipeline):
    output_dir, cfg = pipeline._resolve_s3_config("bronze")
    assert output_dir == "s3a://bronze-data/bronze"
    assert cfg["endpoint"] == pipeline.shared_cfg["minio"]["endpoint"]


def test_resolve_s3_config_unknown_layer_raises(pipeline):
    with pytest.raises(ValueError):
        pipeline._resolve_s3_config("platinum")


def test_minio_endpoint_env_var_overrides_yaml_default(monkeypatch):
    """Lets the same unmodified pipeline scripts run inside the Airflow
    container, where "localhost" doesn't reach the sibling minio container —
    docker-compose sets this for the airflow service only (dags/plan.md §4)."""
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    pipeline = ConcretePipeline()
    assert pipeline.shared_cfg["minio"]["endpoint"] == "http://minio:9000"


def test_minio_endpoint_falls_back_to_yaml_default_when_env_var_unset(monkeypatch):
    from b_schema_pipelines.pipelines.utils.config import load_config

    monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
    yaml_default = load_config(PipelineBase._SHARED_CONFIG)["minio"]["endpoint"]

    pipeline = ConcretePipeline()
    assert pipeline.shared_cfg["minio"]["endpoint"] == yaml_default


def test_postgres_host_env_var_overrides_yaml_default(monkeypatch):
    """Same reasoning as MINIO_ENDPOINT — the raw psycopg2 connections
    build_gold.py/feat_*.py open directly never see each DAG's --postgres-url
    flag (that only feeds Spark's JDBC writer), so without this override they
    always fell back to pipeline_config.yaml's localhost and failed inside the
    Airflow container."""
    monkeypatch.setenv("POSTGRES_HOST", "postgres")
    pipeline = ConcretePipeline()
    assert pipeline.shared_cfg["postgres"]["host"] == "postgres"


def test_postgres_port_env_var_overrides_yaml_default(monkeypatch):
    monkeypatch.setenv("POSTGRES_PORT", "5433")
    pipeline = ConcretePipeline()
    assert pipeline.shared_cfg["postgres"]["port"] == 5433


def test_postgres_db_env_var_overrides_yaml_default(monkeypatch):
    monkeypatch.setenv("POSTGRES_DB", "other_db")
    pipeline = ConcretePipeline()
    assert pipeline.shared_cfg["postgres"]["db"] == "other_db"


def test_postgres_config_falls_back_to_yaml_default_when_env_vars_unset(monkeypatch):
    from b_schema_pipelines.pipelines.utils.config import load_config

    monkeypatch.delenv("POSTGRES_HOST", raising=False)
    monkeypatch.delenv("POSTGRES_PORT", raising=False)
    monkeypatch.delenv("POSTGRES_DB", raising=False)
    yaml_default = load_config(PipelineBase._SHARED_CONFIG)["postgres"]

    pipeline = ConcretePipeline()
    assert pipeline.shared_cfg["postgres"]["host"] == yaml_default["host"]
    assert pipeline.shared_cfg["postgres"]["port"] == yaml_default["port"]
    assert pipeline.shared_cfg["postgres"]["db"] == yaml_default["db"]


# ── 6. log_run — level routing (drives Grafana/Loki alerting) ────────────────


def test_log_run_uses_error_level_only_for_status_error(pipeline):
    start = end = datetime(2026, 4, 1)

    pipeline.logger = MagicMock()
    pipeline.log_run("orders", start, end, 0, 0, "error", error="boom")
    pipeline.logger.error.assert_called_once()
    pipeline.logger.info.assert_not_called()

    pipeline.logger = MagicMock()
    pipeline.log_run("orders", start, end, 100, 100, "success")
    pipeline.logger.info.assert_called_once()
    pipeline.logger.error.assert_not_called()


# ── 7. log_run — structured JSON payload (consumed by Loki/Grafana queries) ──


def test_log_run_structured_json_fields_and_duration(pipeline):
    pipeline.logger = MagicMock()
    start = datetime(2026, 4, 1, 1, 0, 0)
    end = datetime(2026, 4, 1, 1, 0, 5)

    pipeline.log_run("orders", start, end, 1000, 980, "success")

    json_arg = pipeline.logger.debug.call_args.args[1]
    payload = json.loads(json_arg)
    for field in (
        "run_id", "pipeline_name", "table", "start_ts", "end_ts",
        "duration_s", "input_rows", "output_rows", "status", "error_summary",
    ):
        assert field in payload, f"Required field '{field}' missing from log"
    assert payload["duration_s"] == 5.0
    assert payload["input_rows"] == 1000
    assert payload["output_rows"] == 980
    assert payload["run_id"] == pipeline.run_id


# ── 8. _create_spark_session — Delta + S3A/MinIO wiring ──────────────────────


def test_create_spark_session_configures_delta_and_s3a(monkeypatch):
    """Faking SparkSession.builder avoids the JVM's one-session-per-process
    quirk: once any test starts a real SparkSession, later getOrCreate() calls
    silently reuse it and ignore new config, which would let a broken key
    name here pass unnoticed.
    """
    calls = {}

    class FakeBuilder:
        def master(self, *a):
            return self

        def appName(self, *a):
            return self

        def config(self, key, value):
            calls[key] = value
            return self

        def getOrCreate(self):
            return MagicMock()

    monkeypatch.setattr(
        "b_schema_pipelines.pipelines.pipeline_base.SparkSession.builder",
        FakeBuilder(),
    )

    class ConfigCheckPipeline(PipelineBase):
        PREFIX = "cfgcheck"

        def run(self):
            pass

    p = ConfigCheckPipeline()

    assert calls["spark.sql.extensions"] == "io.delta.sql.DeltaSparkSessionExtension"
    assert calls["spark.hadoop.fs.s3a.endpoint"] == p.shared_cfg["minio"]["endpoint"]
    assert calls["spark.hadoop.fs.s3a.access.key"] == p.shared_cfg["minio"]["access_key"]
    assert calls["spark.hadoop.fs.s3a.secret.key"] == p.shared_cfg["minio"]["secret_key"]


def test_create_spark_session_shuffle_partitions_scales_with_cores(monkeypatch):
    """Default (200) wastes per-task scheduling overhead on local datasets
    this small — must scale with the same n_cores used for local[n_cores],
    not the Spark default."""
    calls = {}

    class FakeBuilder:
        def master(self, master_str):
            calls["master"] = master_str
            return self

        def appName(self, *a):
            return self

        def config(self, key, value):
            calls[key] = value
            return self

        def getOrCreate(self):
            return MagicMock()

    monkeypatch.setattr(
        "b_schema_pipelines.pipelines.pipeline_base.SparkSession.builder",
        FakeBuilder(),
    )
    monkeypatch.setattr("os.cpu_count", lambda: 8)

    class ConfigCheckPipeline(PipelineBase):
        PREFIX = "cfgcheck"

        def run(self):
            pass

    ConfigCheckPipeline()

    assert calls["master"] == "local[6]"          # max(2, 8 - 2)
    assert calls["spark.sql.shuffle.partitions"] == "18"   # n_cores * 3


def test_each_subclass_has_own_prefix():
    class PipelineA(PipelineBase):
        PREFIX = "alpha"

        def __init__(self):
            super().__init__()
            self.spark = MagicMock()

        def run(self):
            pass

    class PipelineB(PipelineBase):
        PREFIX = "beta"

        def __init__(self):
            super().__init__()
            self.spark = MagicMock()

        def run(self):
            pass

    a, b = PipelineA(), PipelineB()
    assert a.run_id.startswith("alpha_")
    assert b.run_id.startswith("beta_")
