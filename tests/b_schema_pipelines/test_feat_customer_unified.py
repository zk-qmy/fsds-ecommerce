"""
Tests for CustomerFeatureUnified.

JDBC I/O is mocked; tests validate the as-of join logic and null-fill rules.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

import pytest
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.pipelines.features.feat_customer_unified import CustomerFeatureUnified


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def feat_unified(spark):
    with patch.object(CustomerFeatureUnified, "_build_spark", return_value=spark):
        f = CustomerFeatureUnified()
    f._read_feature_table = MagicMock()
    f._write = MagicMock(side_effect=lambda df: df.count())
    return f


@pytest.fixture
def feature_tables(spark):
    """feat_customer_90d (daily snapshots) + feat_stream_60m (hourly windows).

    C001 has two offline snapshots (2026-03-01, 2026-04-01) to exercise
    "pick the latest one at or before the stream timestamp, not just any".
    C002 has one snapshot with zero orders (avg stays NULL).
    C003 has no offline snapshot at all (as-of join finds nothing).
    """
    T = datetime.datetime

    offline = spark.createDataFrame(
        [
            # customer_id, event_timestamp,     created_ts,          total_orders, avg_value, categories, fail_rate
            ("C001", T(2026, 3, 1),  T(2026, 3, 1),  1, 50.0,  1, 0.0),
            ("C001", T(2026, 4, 1),  T(2026, 4, 1),  3, 100.0, 2, 0.1),
            ("C002", T(2026, 3, 15), T(2026, 3, 15), 0, None,  0, 0.0),
        ],
        ["customer_id", "event_timestamp", "created_ts",
         "f_customer_total_orders_90d", "f_customer_avg_order_value_90d",
         "f_customer_distinct_categories_90d", "f_customer_payment_fail_rate_90d"],
    )

    stream = spark.createDataFrame(
        [
            # C001: window after the 04-01 snapshot -> must pick 04-01, not 03-01
            ("C001", T(2026, 4, 1, 9, 0),  T(2026, 4, 1, 9, 0),  5, 2, 1.0, 0),
            # C001: window between the two snapshots -> must pick 03-01, not 04-01 (no future leakage)
            ("C001", T(2026, 3, 15, 10, 0), T(2026, 3, 15, 10, 0), 1, 0, 0.0, 0),
            # C002: window after its only snapshot
            ("C002", T(2026, 3, 20, 8, 0), T(2026, 3, 20, 8, 0), 3, 1, 0.5, 1),
            # C003: no offline snapshot exists for this customer at all
            ("C003", T(2026, 5, 1, 12, 0), T(2026, 5, 1, 12, 0), 2, 1, 1.0, 0),
        ],
        ["customer_id", "event_timestamp", "created_ts",
         "f_stream_views_30m", "f_stream_add_to_cart_30m",
         "f_stream_cart_to_purchase_ratio_60m", "f_stream_burst_activity_flag"],
    )

    return {"feat_customer_90d": offline, "feat_stream_60m": stream}


def _mock_read(feat_unified, feature_tables):
    feat_unified._read_feature_table.side_effect = lambda t: feature_tables[t]


# ── 1. _compute_features — grain and schema ─────────────────────────────────

def test_compute_features_output_schema(feat_unified, feature_tables):
    _mock_read(feat_unified, feature_tables)
    result = feat_unified._compute_features()
    expected_cols = {
        "customer_id", "event_timestamp", "created_ts",
        "f_customer_total_orders_90d", "f_customer_avg_order_value_90d",
        "f_customer_distinct_categories_90d", "f_customer_payment_fail_rate_90d",
        "f_stream_views_30m", "f_stream_add_to_cart_30m",
        "f_stream_cart_to_purchase_ratio_60m", "f_stream_burst_activity_flag",
    }
    assert expected_cols.issubset(set(result.columns))


def test_compute_features_one_row_per_stream_window(feat_unified, feature_tables):
    """Grain follows feat_stream_60m (the finer-grained table) — the as-of
    join must not fan out rows even though C001 has two offline snapshots."""
    _mock_read(feat_unified, feature_tables)
    result = feat_unified._compute_features()
    assert result.count() == feature_tables["feat_stream_60m"].count()


# ── 2. As-of join correctness ────────────────────────────────────────────────

def test_asof_join_picks_latest_snapshot_at_or_before(feat_unified, feature_tables):
    """C001's window at 04-01 09:00 must attach the 04-01 snapshot (3 orders),
    not the older 03-01 snapshot (1 order)."""
    _mock_read(feat_unified, feature_tables)
    result = feat_unified._compute_features()
    row = result.filter(
        (F.col("customer_id") == "C001") &
        (F.col("event_timestamp") == datetime.datetime(2026, 4, 1, 9, 0))
    ).collect()[0]
    assert row["f_customer_total_orders_90d"] == 3


def test_asof_join_excludes_future_snapshots(feat_unified, feature_tables):
    """C001's window at 03-15 10:00 falls between the two snapshots — it must
    attach the 03-01 snapshot (1 order), never the later 04-01 one (data leak)."""
    _mock_read(feat_unified, feature_tables)
    result = feat_unified._compute_features()
    row = result.filter(
        (F.col("customer_id") == "C001") &
        (F.col("event_timestamp") == datetime.datetime(2026, 3, 15, 10, 0))
    ).collect()[0]
    assert row["f_customer_total_orders_90d"] == 1


def test_asof_join_missing_offline_snapshot_fills_counts_with_zero(feat_unified, feature_tables):
    """C003 has no offline snapshot at all -> counts/rate default to 0."""
    _mock_read(feat_unified, feature_tables)
    result = feat_unified._compute_features()
    row = result.filter(F.col("customer_id") == "C003").collect()[0]
    assert row["f_customer_total_orders_90d"] == 0
    assert row["f_customer_distinct_categories_90d"] == 0
    assert row["f_customer_payment_fail_rate_90d"] == 0.0


def test_asof_join_missing_offline_snapshot_avg_order_value_stays_null(feat_unified, feature_tables):
    """Average of zero orders is undefined, not 0 — same rule as
    feat_customer_90d.py's own zero-order customers, just reached via a
    missing as-of match instead of a zero-row aggregation."""
    _mock_read(feat_unified, feature_tables)
    result = feat_unified._compute_features()
    row = result.filter(F.col("customer_id") == "C003").collect()[0]
    assert row["f_customer_avg_order_value_90d"] is None


def test_stream_features_pass_through_unchanged(feat_unified, feature_tables):
    _mock_read(feat_unified, feature_tables)
    result = feat_unified._compute_features()
    row = result.filter(
        (F.col("customer_id") == "C002") &
        (F.col("event_timestamp") == datetime.datetime(2026, 3, 20, 8, 0))
    ).collect()[0]
    assert row["f_stream_views_30m"] == 3
    assert row["f_stream_add_to_cart_30m"] == 1
    assert row["f_stream_cart_to_purchase_ratio_60m"] == 0.5
    assert row["f_stream_burst_activity_flag"] == 1


# ── 3. run() ──────────────────────────────────────────────────────────────────

def test_run_calls_compute_then_write(feat_unified, feature_tables):
    _mock_read(feat_unified, feature_tables)
    with patch.object(feat_unified, "_compute_features", wraps=feat_unified._compute_features) as mock_compute, \
         patch.object(feat_unified, "_write", wraps=feat_unified._write) as mock_write:
        feat_unified.run()
    mock_compute.assert_called_once()
    mock_write.assert_called_once()
