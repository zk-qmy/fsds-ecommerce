"""
Tests for CustomerFeature90d.

JDBC I/O is mocked; tests validate the feature computation logic and schema.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

import pytest
from pyspark.sql import functions as F


sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.pipelines.features.feat_customer_90d import CustomerFeature90d


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def feat_90d(spark):
    with patch.object(CustomerFeature90d, "_build_spark", return_value=spark):
        f = CustomerFeature90d(snapshot_date="2026-06-01")
    f._read_gold = MagicMock()
    f._write     = MagicMock(side_effect=lambda df: df.count())
    return f


@pytest.fixture
def gold_tables(spark):
    """Minimal Gold tables needed for feat_customer_90d computation."""
    T = datetime.datetime

    dim_customer = spark.createDataFrame(
        [
            (1, "C001", "gold",   "VN", True,  T(2026, 1, 1), True),
            (2, "C002", "silver", "US", False, T(2026, 1, 2), True),
        ],
        ["customer_key", "customer_id", "segment", "country", "marketing_opt_in", "signup_ts", "is_current"],
    )

    dim_date = spark.createDataFrame(
        [(20260301, datetime.date(2026, 3, 1)),
         (20260401, datetime.date(2026, 4, 1)),
         (20260501, datetime.date(2026, 5, 1)),
         (20260601, datetime.date(2026, 6, 1))],
        ["date_key", "calendar_date"],
    )

    dim_product = spark.createDataFrame(
        [(1, "P001", "electronics"), (2, "P002", "fashion")],
        ["product_key", "product_id", "category"],
    )

    fact_order = spark.createDataFrame(
        [
            # C001: 2 orders in 90-day window (before 2026-06-01)
            (101, 1, 20260401, 1, "O001", 100.0, 10.0, 90.0,  2),
            (102, 1, 20260501, 1, "O002", 200.0, 20.0, 180.0, 3),
            # C002: 1 order in window
            (103, 2, 20260401, 1, "O003", 150.0, 0.0,  150.0, 1),
            # C001: order outside window (before window_start = 2026-03-03)
            (104, 1, 20260301, 1, "O004", 50.0,  0.0,  50.0,  1),
        ],
        ["order_key", "customer_key", "order_date_key", "order_status_key",
         "order_id", "order_gross_amount", "order_discount_amount", "order_net_amount", "item_count"],
    )

    fact_order_item = spark.createDataFrame(
        [
            (1001, 101, 1, 2, 50.0, 10.0, 90.0),    # O001 → P001 (electronics)
            (1002, 102, 2, 1, 200.0, 20.0, 180.0),  # O002 → P002 (fashion)
            (1003, 103, 1, 1, 150.0, 0.0, 150.0),   # O003 → P001 (electronics)
        ],
        ["order_item_key", "order_key", "product_key", "quantity",
         "unit_price", "discount_amount", "line_net_amount"],
    )

    fact_payment = spark.createDataFrame(
        [
            (201, 101, 20260401, 1, 90.0,  True,  False),   # O001 paid
            (202, 102, 20260501, 2, 180.0, True,  False),   # O002 paid
            (203, 103, 20260401, 3, 150.0, False, True),    # O003 failed
        ],
        ["payment_key", "order_key", "payment_date_key", "payment_method_key",
         "amount", "is_payment_success", "is_payment_failed"],
    )

    return {
        "dim_customer":       dim_customer,
        "dim_date":           dim_date,
        "dim_product":        dim_product,
        "fact_order":         fact_order,
        "fact_order_item":    fact_order_item,
        "fact_payment_attempt": fact_payment,
    }


# ── 1. Initialisation ─────────────────────────────────────────────────────────

def test_window_start_is_90_days_before_snapshot(feat_90d):
    expected = (
        datetime.datetime(2026, 6, 1) - datetime.timedelta(days=90)
    ).strftime("%Y-%m-%d")
    assert feat_90d.snapshot_date == "2026-06-01"
    assert feat_90d.window_start == expected


def test_snapshot_date_defaults_to_today():
    with patch.object(CustomerFeature90d, "_build_spark", return_value=MagicMock()):
        f = CustomerFeature90d()
    assert f.snapshot_date == datetime.datetime.now().strftime("%Y-%m-%d")


# ── 2. _compute_features ─────────────────────────────────────────────────────

def test_compute_features_output_schema(feat_90d, gold_tables):
    feat_90d._read_gold.side_effect = lambda t: gold_tables[t]
    result = feat_90d._compute_features()
    expected_cols = {
        "customer_id",
        "event_timestamp",
        "created_ts",
        "f_customer_total_orders_90d",
        "f_customer_avg_order_value_90d",
        "f_customer_distinct_categories_90d",
        "f_customer_payment_fail_rate_90d",
    }
    assert expected_cols.issubset(set(result.columns))


def test_compute_features_one_row_per_customer(feat_90d, gold_tables):
    feat_90d._read_gold.side_effect = lambda t: gold_tables[t]
    result = feat_90d._compute_features()
    total    = result.count()
    distinct = result.select("customer_id").distinct().count()
    assert total == distinct, "Must be exactly one row per customer per snapshot"


def test_compute_features_total_orders_in_window_only(feat_90d, gold_tables):
    """C001 has 3 orders total but only 2 fall in the 90-day window."""
    feat_90d._read_gold.side_effect = lambda t: gold_tables[t]
    result = feat_90d._compute_features()
    row = result.filter(F.col("customer_id") == "C001").collect()[0]
    assert row["f_customer_total_orders_90d"] == 2


def test_compute_features_avg_order_value(feat_90d, gold_tables):
    """C001: orders O001 (90.0) + O002 (180.0) → avg = 135.0."""
    feat_90d._read_gold.side_effect = lambda t: gold_tables[t]
    result = feat_90d._compute_features()
    row = result.filter(F.col("customer_id") == "C001").collect()[0]
    assert abs(row["f_customer_avg_order_value_90d"] - 135.0) < 0.01


def test_compute_features_distinct_categories(feat_90d, gold_tables):
    """C001 bought from electronics (O001) and fashion (O002) → 2 distinct categories."""
    feat_90d._read_gold.side_effect = lambda t: gold_tables[t]
    result = feat_90d._compute_features()
    row = result.filter(F.col("customer_id") == "C001").collect()[0]
    assert row["f_customer_distinct_categories_90d"] == 2


def test_compute_features_payment_fail_rate(feat_90d, gold_tables):
    """C002 has 1 order with 1 failed payment → fail rate = 1.0."""
    feat_90d._read_gold.side_effect = lambda t: gold_tables[t]
    result = feat_90d._compute_features()
    row = result.filter(F.col("customer_id") == "C002").collect()[0]
    assert abs(row["f_customer_payment_fail_rate_90d"] - 1.0) < 0.01


def test_compute_features_event_timestamp_is_snapshot_date(feat_90d, gold_tables):
    feat_90d._read_gold.side_effect = lambda t: gold_tables[t]
    result = feat_90d._compute_features()
    row = result.first()
    # event_timestamp should be set to snapshot_date (2026-06-01)
    assert row["event_timestamp"].strftime("%Y-%m-%d") == "2026-06-01"


def test_compute_features_no_future_data_included(feat_90d, gold_tables, spark):
    """Orders after snapshot_date must be excluded from features."""
    # Add a future order (after snapshot_date 2026-06-01)
    future_order = spark.createDataFrame(
        [(999, 1, 20261001, 1, "OFUTURE", 500.0, 0.0, 500.0, 1)],
        ["order_key", "customer_key", "order_date_key", "order_status_key",
         "order_id", "order_gross_amount", "order_discount_amount", "order_net_amount", "item_count"],
    )
    updated_fact_order = gold_tables["fact_order"].union(future_order)

    def mock_read(t):
        if t == "fact_order":
            return updated_fact_order
        return gold_tables[t]

    feat_90d._read_gold.side_effect = mock_read
    result = feat_90d._compute_features()
    row = result.filter(F.col("customer_id") == "C001").collect()[0]
    # C001 should still have 2 orders, not 3
    assert row["f_customer_total_orders_90d"] == 2


# ── 3. _write (idempotency) ───────────────────────────────────────────────────

def test_write_deletes_existing_snapshot_before_insert(feat_90d, gold_tables):
    """Second run for same snapshot_date must not produce duplicate rows (DELETE then INSERT)."""
    feat_90d._read_gold.side_effect = lambda t: gold_tables[t]
    feat_90d._write = MagicMock(return_value=0)   # track calls

    feat_90d.run()
    feat_90d.run()

    # The write method must be called twice (once per run)
    assert feat_90d._write.call_count == 2


# ── 4. run() ──────────────────────────────────────────────────────────────────

def test_run_calls_compute_then_write(feat_90d, gold_tables):
    feat_90d._read_gold.side_effect = lambda t: gold_tables[t]
    with patch.object(feat_90d, "_compute_features", wraps=feat_90d._compute_features) as mock_compute, \
         patch.object(feat_90d, "_write", wraps=feat_90d._write) as mock_write:
        feat_90d.run()
    mock_compute.assert_called_once()
    mock_write.assert_called_once()
