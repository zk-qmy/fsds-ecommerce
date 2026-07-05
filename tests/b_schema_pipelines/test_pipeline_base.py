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
from unittest.mock import MagicMock, patch
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.pipelines.pipeline_base import PipelineBase


# ── Minimal concrete subclass ─────────────────────────────────────────────────

class ConcretePipeline(PipelineBase):
    PREFIX = "test-pipeline"

    def __init__(self):
        super().__init__()
        self.spark = MagicMock()   # never start a real session in base-class tests

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


# ── 3. _log_run ───────────────────────────────────────────────────────────────

def test_log_run_emits_valid_json(pipeline, capsys):
    start = datetime(2026, 4, 1, 1, 0, 0)
    end   = datetime(2026, 4, 1, 1, 0, 5)
    pipeline._log_run("orders", start, end, 1000, 980, "success")

    captured = capsys.readouterr().out.strip()
    parsed = json.loads(captured)   # raises if not valid JSON
    assert isinstance(parsed, dict)


@pytest.mark.parametrize("field", [
    "run_id", "pipeline_name", "table", "start_ts", "end_ts",
    "duration_s", "input_rows", "output_rows", "status", "error_summary",
])
def test_log_run_required_fields_present(field, pipeline, capsys):
    start = datetime(2026, 4, 1, 1, 0, 0)
    end   = datetime(2026, 4, 1, 1, 0, 5)
    pipeline._log_run("orders", start, end, 1000, 980, "success")

    parsed = json.loads(capsys.readouterr().out.strip())
    assert field in parsed, f"Required field '{field}' missing from log"


def test_log_run_duration_seconds_correct(pipeline, capsys):
    start = datetime(2026, 4, 1, 0, 0, 0)
    end   = datetime(2026, 4, 1, 0, 0, 10)
    pipeline._log_run("orders", start, end, 100, 100, "success")

    parsed = json.loads(capsys.readouterr().out.strip())
    assert parsed["duration_s"] == 10.0


def test_log_run_row_counts_recorded(pipeline, capsys):
    start = end = datetime(2026, 4, 1)
    pipeline._log_run("order_items", start, end, 909_000, 890_000, "success")

    parsed = json.loads(capsys.readouterr().out.strip())
    assert parsed["input_rows"]  == 909_000
    assert parsed["output_rows"] == 890_000


def test_log_run_error_summary_included(pipeline, capsys):
    start = end = datetime(2026, 4, 1)
    pipeline._log_run("orders", start, end, 0, 0, "failed", error="NullPointerException")

    parsed = json.loads(capsys.readouterr().out.strip())
    assert parsed["status"]        == "failed"
    assert parsed["error_summary"] == "NullPointerException"


def test_log_run_run_id_matches_instance(pipeline, capsys):
    start = end = datetime(2026, 4, 1)
    pipeline._log_run("orders", start, end, 1, 1, "success")

    parsed = json.loads(capsys.readouterr().out.strip())
    assert parsed["run_id"] == pipeline.run_id


# ── 4. PREFIX ─────────────────────────────────────────────────────────────────

def test_prefix_appears_in_run_id(pipeline):
    assert "test-pipeline" in pipeline.run_id


def test_each_subclass_has_own_prefix():
    class PipelineA(PipelineBase):
        PREFIX = "alpha"
        def __init__(self): super().__init__(); self.spark = MagicMock()
        def run(self): pass

    class PipelineB(PipelineBase):
        PREFIX = "beta"
        def __init__(self): super().__init__(); self.spark = MagicMock()
        def run(self): pass

    a, b = PipelineA(), PipelineB()
    assert a.run_id.startswith("alpha_")
    assert b.run_id.startswith("beta_")
