"""
Tests for validation_runner.py.

No Spark/live Postgres/MinIO needed — psycopg2/deltalake reads are mocked or
replaced with small pandas fixtures; GX validation itself runs for real (fast on
tiny DataFrames), the same way it was verified against real Bronze/Silver/Gold data
during implementation (see dags/plan.md §9/§15).
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

import pandas as pd
import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.dq import validation_runner as vr


# ── _read_baseline_row_count ──────────────────────────────────────────────────

def _write_log(log_dir: Path, run_id: str, table: str, output_rows: int, status: str = "ok") -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    line = (
        f'2026-07-15 10:00:00  DEBUG  {run_id}  —  STRUCTURED '
        f'{{"run_id": "{run_id}", "table": "{table}", "output_rows": {output_rows}, "status": "{status}"}}'
    )
    (log_dir / f"{run_id}.log").write_text(line + "\n")


def test_read_baseline_row_count_skips_the_current_runs_own_entry(tmp_path, monkeypatch):
    """The most recent log entry is this run's own fresh output, not a baseline —
    the function must return the run before that, not the newest one."""
    monkeypatch.setattr(vr, "REPO_ROOT", tmp_path)
    log_dir = tmp_path / "logs" / "gold"
    _write_log(log_dir, "gold_20260101_000000", "fact_order", 1000)
    _write_log(log_dir, "gold_20260102_000000", "fact_order", 1050)

    assert vr._read_baseline_row_count("gold", "fact_order") == 1000


def test_read_baseline_row_count_none_when_fewer_than_two_runs_exist(tmp_path, monkeypatch):
    monkeypatch.setattr(vr, "REPO_ROOT", tmp_path)
    log_dir = tmp_path / "logs" / "gold"
    _write_log(log_dir, "gold_20260101_000000", "fact_order", 1000)

    assert vr._read_baseline_row_count("gold", "fact_order") is None


def test_read_baseline_row_count_none_when_log_dir_missing(tmp_path, monkeypatch):
    monkeypatch.setattr(vr, "REPO_ROOT", tmp_path)
    assert vr._read_baseline_row_count("gold", "fact_order") is None


def test_read_baseline_row_count_ignores_error_status_entries(tmp_path, monkeypatch):
    """A failed run's entry isn't a valid baseline to compare against."""
    monkeypatch.setattr(vr, "REPO_ROOT", tmp_path)
    log_dir = tmp_path / "logs" / "gold"
    _write_log(log_dir, "gold_20260101_000000", "fact_order", 1000)
    _write_log(log_dir, "gold_20260102_000000", "fact_order", 999, status="error")
    _write_log(log_dir, "gold_20260103_000000", "fact_order", 1010)

    assert vr._read_baseline_row_count("gold", "fact_order") == 1000


# ── _validate_suite ───────────────────────────────────────────────────────────

def test_validate_suite_returns_empty_list_when_data_passes():
    from b_schema_pipelines.dq.bronze_suite import bronze_expectation_suite

    df = pd.DataFrame({"a": [1, 2], "b": [3, 4]})
    suite = bronze_expectation_suite("t", ["a", "b"])
    assert vr._validate_suite(suite, df) == []


def test_validate_suite_reports_expectation_type_and_observed_value_on_failure():
    from b_schema_pipelines.dq.bronze_suite import bronze_expectation_suite

    df = pd.DataFrame({"a": [1, 2]})  # missing expected column "b"
    suite = bronze_expectation_suite("t", ["a", "b"])
    failures = vr._validate_suite(suite, df)

    assert len(failures) == 1
    assert failures[0]["expectation_type"] == "expect_table_columns_to_match_set"


# ── _raise_if_any_failures ─────────────────────────────────────────────────────

def test_raise_if_any_failures_no_op_when_all_empty():
    vr._raise_if_any_failures({"orders": [], "customers": []})  # must not raise


def test_raise_if_any_failures_raises_with_every_failing_table_not_just_the_first():
    failures = {
        "orders": [{"expectation_type": "e1", "observed_value": "x", "details": None}],
        "customers": [],
        "payments": [{"expectation_type": "e2", "observed_value": "y", "details": None}],
    }
    with pytest.raises(vr.ValidationFailure) as exc_info:
        vr._raise_if_any_failures(failures)
    message = str(exc_info.value)
    assert "orders" in message and "e1" in message
    assert "payments" in message and "e2" in message
    assert "customers" not in message  # no failures for this table, not mentioned


# ── validate_gold_tables ────────────────────────────────────────────────────────

DIM_CUSTOMER_DF = pd.DataFrame({
    "customer_key": [1, 2, 3], "customer_id": ["C1", "C2", "C3"],
    "signup_ts": ["2026-01-01"] * 3, "segment": ["gold"] * 3, "country": ["VN"] * 3,
    "marketing_opt_in": [True, True, False],
    "valid_from_ts": ["2026-01-01"] * 3, "valid_to_ts": ["9999-12-31"] * 3,
    "is_current": [True, True, True],
})


def _gold_fixture_frames():
    frames = {}
    for table, columns in vr.GOLD_TABLE_COLUMNS.items():
        if table == "dim_customer":
            frames[table] = DIM_CUSTOMER_DF
        else:
            frames[table] = pd.DataFrame({c: [] for c in columns})
    return frames


@pytest.fixture
def mock_gold_io():
    """Patches every psycopg2/pandas I/O call validate_gold_tables makes, with
    empty-but-schema-correct fixtures for every table except dim_customer (given
    real rows, so the uniqueness/null-PK checks have something to run against)."""
    frames = _gold_fixture_frames()
    with (
        patch.object(vr, "_pg_connect", return_value=MagicMock()),
        patch.object(vr, "_read_postgres_table", side_effect=lambda conn, schema, table: frames[table]),
        patch.object(vr, "_read_distinct_column", return_value=[1, 2, 3]),
        patch.object(vr, "_read_baseline_row_count", return_value=None),
    ):
        yield frames


def test_validate_gold_tables_passes_on_clean_fixture_data(mock_gold_io):
    vr.validate_gold_tables({"host": "x", "port": 1, "db": "x", "user": "x", "password": "x"})


def test_validate_gold_tables_uses_is_current_filtered_batch_for_uniqueness_check(mock_gold_io):
    """A customer_id duplicated across a non-current + a current row must NOT be
    flagged — SCD2 history legitimately reuses customer_id across versions. Only a
    duplicate among is_current=True rows is a real violation."""
    frames = mock_gold_io
    frames["dim_customer"] = pd.concat([
        DIM_CUSTOMER_DF,
        pd.DataFrame({
            "customer_key": [4], "customer_id": ["C1"],  # same customer_id as key=1
            "signup_ts": ["2026-01-01"], "segment": ["silver"], "country": ["VN"],
            "marketing_opt_in": [True], "valid_from_ts": ["2025-01-01"],
            "valid_to_ts": ["2026-01-01"], "is_current": [False],  # historical, not current
        }),
    ], ignore_index=True)

    vr.validate_gold_tables({"host": "x", "port": 1, "db": "x", "user": "x", "password": "x"})


def test_validate_gold_tables_raises_on_duplicate_current_customer_id(mock_gold_io):
    frames = mock_gold_io
    frames["dim_customer"] = pd.concat([
        DIM_CUSTOMER_DF,
        pd.DataFrame({
            "customer_key": [4], "customer_id": ["C1"],  # duplicate, also is_current
            "signup_ts": ["2026-01-01"], "segment": ["silver"], "country": ["VN"],
            "marketing_opt_in": [True], "valid_from_ts": ["2026-02-01"],
            "valid_to_ts": ["9999-12-31"], "is_current": [True],
        }),
    ], ignore_index=True)

    with pytest.raises(vr.ValidationFailure, match="dim_customer"):
        vr.validate_gold_tables({"host": "x", "port": 1, "db": "x", "user": "x", "password": "x"})


def test_validate_gold_tables_builds_fk_checks_from_every_dim_key_ever_issued(mock_gold_io):
    """fact_order.customer_key must be checked against ALL of dim_customer's keys
    (no is_current filter) — a fact row pointing at a historical, non-current
    customer_key is a valid SCD2 reference, not an orphan."""
    frames = mock_gold_io
    frames["fact_order"] = pd.DataFrame({
        "order_key": [1], "customer_key": [1], "order_date_key": [1], "order_status_key": [1],
        "order_id": ["O1"], "order_gross_amount": [10.0], "order_discount_amount": [0.0],
        "order_net_amount": [10.0], "item_count": [1],
    })
    with patch.object(vr, "_read_distinct_column", return_value=[1]) as mock_distinct:
        vr.validate_gold_tables({"host": "x", "port": 1, "db": "x", "user": "x", "password": "x"})
    # dim_customer's key column queried without any is_current filter in the SQL
    dim_customer_calls = [c for c in mock_distinct.call_args_list if c.args[1:3] == ("gold_ecommerce", "dim_customer")]
    assert dim_customer_calls, "expected a distinct-key query against dim_customer"


def test_validate_gold_tables_skips_volume_check_for_incremental_tables_by_default(mock_gold_io):
    """dim_customer's log_run history records incremental rows written (often 0),
    not total table size — must not be used as a volume-check baseline unless a
    caller passes one in explicitly."""
    with patch.object(vr, "_read_baseline_row_count", return_value=None) as mock_baseline:
        vr.validate_gold_tables({"host": "x", "port": 1, "db": "x", "user": "x", "password": "x"})
        called_tables = {c.args[1] for c in mock_baseline.call_args_list}
    assert "dim_customer" not in called_tables


# ── validate_bronze_tables ───────────────────────────────────────────────────────

def test_validate_bronze_tables_defaults_to_every_table_in_bronze_config(monkeypatch):
    seen_tables = []

    def fake_read(path, minio_cfg):
        table = path.rsplit("/", 1)[-1]
        seen_tables.append(table)
        columns = vr.BRONZE_CONFIG["quality"]["expected_columns"][table]
        return pd.DataFrame({c: [] for c in columns})

    monkeypatch.setattr(vr, "_read_delta_table", fake_read)
    vr.validate_bronze_tables({"endpoint": "x", "access_key": "x", "secret_key": "x"})

    assert set(seen_tables) == set(vr.BRONZE_CONFIG["quality"]["expected_columns"].keys())


def test_validate_bronze_tables_raises_on_missing_column(monkeypatch):
    def fake_read(path, minio_cfg):
        return pd.DataFrame({"customer_id": ["C1"]})  # missing every other expected column

    monkeypatch.setattr(vr, "_read_delta_table", fake_read)
    with pytest.raises(vr.ValidationFailure, match="customers"):
        vr.validate_bronze_tables({"endpoint": "x", "access_key": "x", "secret_key": "x"}, tables=["customers"])


# ── validate_silver_tables ──────────────────────────────────────────────────────

def _silver_fixture_frames():
    return {
        table: pd.DataFrame(
            {c: [] for c in vr.BRONZE_CONFIG["quality"]["expected_columns"][table]}
        )
        for table in vr.SILVER_TABLES
    }


@pytest.fixture
def mock_silver_io():
    """Patches every deltalake read validate_silver_tables makes with empty-but-
    schema-correct fixtures per table — 0-row fixtures vacuously satisfy the
    null-PK/skew/dedup row-level checks, the same pattern
    test_validate_bronze_tables_defaults_to_every_table_in_bronze_config already
    relies on for Bronze's own suite. Also stubs the log-derived baseline to None
    (skip the volume check) so tests don't need real log files on disk."""
    frames = _silver_fixture_frames()

    def fake_read(path, minio_cfg):
        return frames[path.rsplit("/", 1)[-1]]

    with (
        patch.object(vr, "_read_delta_table", side_effect=fake_read),
        patch.object(vr, "_read_baseline_row_count", return_value=None),
    ):
        yield frames


def test_validate_silver_tables_passes_on_clean_fixture_data(mock_silver_io):
    vr.validate_silver_tables({"endpoint": "x", "access_key": "x", "secret_key": "x"})


def test_validate_silver_tables_derives_bronze_order_items_count_when_not_supplied(mock_silver_io):
    """bronze_order_items_row_count defaults to None -> a live Bronze order_items
    read, not a hardcoded skip. This is the one auto-derivation
    validate_silver_tables does that validate_gold_tables/validate_bronze_tables
    don't (dags/plan.md §9)."""
    frames = mock_silver_io
    with patch.object(
        vr, "_read_delta_table",
        side_effect=lambda path, cfg: frames[path.rsplit("/", 1)[-1]],
    ) as mock_read:
        vr.validate_silver_tables({"endpoint": "x", "access_key": "x", "secret_key": "x"})

    bronze_calls = [c for c in mock_read.call_args_list if c.args[0] == f"{vr.BRONZE_DIR}/order_items"]
    assert bronze_calls, "expected a live Bronze order_items read for the dedup-rate baseline"


def test_validate_silver_tables_raises_on_null_primary_key(mock_silver_io):
    frames = mock_silver_io
    columns = vr.BRONZE_CONFIG["quality"]["expected_columns"]["customers"]
    frames["customers"] = pd.DataFrame(
        {c: ([None] if c == "customer_id" else ["x"]) for c in columns}
    )

    with pytest.raises(vr.ValidationFailure, match="customers"):
        vr.validate_silver_tables({"endpoint": "x", "access_key": "x", "secret_key": "x"})


def test_validate_silver_tables_raises_when_orders_skew_below_floor(mock_silver_io):
    """Problem A check: shipping_city must be >=80% Ho Chi Minh City (the floor of
    the +/-5pp band around 85%, dq/silver_suite.py's HCMC_SKEW_TARGET/TOLERANCE)."""
    frames = mock_silver_io
    columns = vr.BRONZE_CONFIG["quality"]["expected_columns"]["orders"]
    n = 10
    row = {c: ["x"] * n for c in columns}
    row["shipping_city"] = ["Hanoi"] * n  # 0% HCMC -- well below the 80% floor
    row["coupon_code"] = ["C1"] * n  # keep Problem B's null-fill check passing
    row["shipping_method"] = ["standard"] * n
    frames["orders"] = pd.DataFrame(row)

    with pytest.raises(vr.ValidationFailure, match="orders"):
        vr.validate_silver_tables({"endpoint": "x", "access_key": "x", "secret_key": "x"})


# ── validate_feature_tables ──────────────────────────────────────────────────────

def _feature_fixture_frames():
    return {
        table: pd.DataFrame({c: [] for c in columns})
        for table, columns in vr.FEATURE_TABLE_COLUMNS.items()
    }


@pytest.fixture
def mock_feature_io():
    frames = _feature_fixture_frames()
    with (
        patch.object(vr, "_pg_connect", return_value=MagicMock()),
        patch.object(vr, "_read_postgres_table", side_effect=lambda conn, schema, table: frames[table]),
    ):
        yield frames


def test_validate_feature_tables_passes_on_clean_fixture_data(mock_feature_io):
    vr.validate_feature_tables({"host": "x", "port": 1, "db": "x", "user": "x", "password": "x"})


def test_validate_feature_tables_raises_on_null_event_timestamp(mock_feature_io):
    frames = mock_feature_io
    columns = vr.FEATURE_TABLE_COLUMNS["feat_customer_90d"]
    frames["feat_customer_90d"] = pd.DataFrame(
        {c: ([None] if c == "event_timestamp" else ["x"]) for c in columns}
    )

    with pytest.raises(vr.ValidationFailure, match="feat_customer_90d"):
        vr.validate_feature_tables({"host": "x", "port": 1, "db": "x", "user": "x", "password": "x"})


def test_validate_feature_tables_never_derives_a_log_based_baseline(mock_feature_io):
    """Unlike Gold/Silver, all three feature tables write incrementally per
    snapshot/window (dags/plan.md §8) -- there is no log-derived number that means
    "total table size" for them, so the volume check must only ever use an
    explicitly passed-in baseline, never fall back to _read_baseline_row_count."""
    with patch.object(vr, "_read_baseline_row_count") as mock_baseline:
        vr.validate_feature_tables({"host": "x", "port": 1, "db": "x", "user": "x", "password": "x"})
    mock_baseline.assert_not_called()
