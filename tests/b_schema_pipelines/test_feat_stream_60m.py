"""
Tests for StreamFeature60m.

Events are provided as in-memory DataFrames via _read_events mock (except the
two _read_events tests themselves, which read a real tmp_path NDJSON file).
JDBC writes are mocked.
"""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

import pytest
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.pipelines.features.feat_stream_60m import StreamFeature60m


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def feat_60m(spark):
    with patch.object(StreamFeature60m, "_build_spark", return_value=spark):
        f = StreamFeature60m()
    f._read_events = MagicMock()
    f._write       = MagicMock(side_effect=lambda df: df.count())
    return f


@pytest.fixture
def parsed_events(spark):
    """Events DataFrame with timestamps already cast to TimestampType (post _read_events)."""

    schema = StructType([
        StructField("event_id",        StringType(),  False),
        StructField("event_type",      StringType(),  False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("created_ts",      TimestampType(), False),
        StructField("customer_id",     StringType(),  False),
        StructField("session_id",      StringType(),  False),
        StructField("product_id",      StringType(),  True),
        StructField("order_id",        StringType(),  True),
        StructField("quantity",        LongType(),    True),
        StructField("price",           DoubleType(),  True),
    ])

    from datetime import datetime as DT

    rows = [
        # ── Window 1: 09:00–10:00 ────────────────────────────────────────────
        # C001: 2 views in first 30min, 1 add_to_cart in first 30min, 1 purchase in 60min
        ("E001", "view",        DT(2026, 4, 1, 9, 5),  DT(2026, 4, 1, 9, 5),  "C001", "S1", "P001", None,   None, None),
        ("E002", "view",        DT(2026, 4, 1, 9, 20), DT(2026, 4, 1, 9, 20), "C001", "S1", "P001", None,   None, None),
        ("E003", "add_to_cart", DT(2026, 4, 1, 9, 25), DT(2026, 4, 1, 9, 25), "C001", "S1", "P001", None,   1,    10.0),
        ("E004", "purchase",    DT(2026, 4, 1, 9, 45), DT(2026, 4, 1, 9, 45), "C001", "S1", "P001", "O001", 1,    10.0),
        # C002: 1 add_to_cart (30min), 0 purchases → ratio = 0.0
        ("E005", "add_to_cart", DT(2026, 4, 1, 9, 10), DT(2026, 4, 1, 9, 10), "C002", "S2", "P002", None,   2,    25.0),
        # ── Window 2 (burst): 12:00–13:00 ────────────────────────────────────
        # C001: burst window event at 12:05 and 12:15
        ("E006", "view",        DT(2026, 4, 1, 12, 5),  DT(2026, 4, 1, 12, 5),  "C001", "S3", "P003", None, None, None),
        ("E007", "add_to_cart", DT(2026, 4, 1, 12, 15), DT(2026, 4, 1, 12, 15), "C001", "S3", "P003", None, 1,    30.0),
        # ── Window 3 (burst): 20:00–21:00 ────────────────────────────────────
        # C002: burst window event at 20:10
        ("E008", "view",        DT(2026, 4, 1, 20, 10), DT(2026, 4, 1, 20, 10), "C002", "S4", "P001", None, None, None),
        ("E009", "purchase",    DT(2026, 4, 1, 20, 50), DT(2026, 4, 1, 20, 50), "C002", "S4", "P002", "O002", 1, 25.0),
    ]
    return spark.createDataFrame(rows, schema=schema)


@pytest.fixture
def flagged_events(feat_60m, parsed_events):
    """parsed_events with f_stream_burst_activity_flag added — the real
    pipeline always runs _add_burst_flag before _compute_features (see
    run()'s call order), so _compute_features expects that column to exist."""
    return feat_60m._add_burst_flag(parsed_events)


# ── 1. _read_events ───────────────────────────────────────────────────────────

def test_read_events_casts_timestamps(feat_60m, tmp_path):
    """event_timestamp and created_ts must come back as TimestampType, not the
    raw NDJSON strings — downstream windowing (F.window) requires it."""
    import json
    events = [
        {"event_id": "E001", "event_type": "view", "customer_id": "C001",
         "event_timestamp": "2026-04-01 09:00:00", "created_ts": "2026-04-01 09:00:05",
         "session_id": "S1"},
    ]
    events_path = tmp_path / "events.json"
    with open(events_path, "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    feat_60m.events_source = str(events_path)
    feat_60m._read_events = StreamFeature60m._read_events.__get__(feat_60m)  # use real method
    result = feat_60m._read_events()

    dtypes = dict(result.dtypes)
    assert dtypes["event_timestamp"] == "timestamp"
    assert dtypes["created_ts"] == "timestamp"


# ── 2. _add_burst_flag (Problem D) ───────────────────────────────────────────

@pytest.mark.parametrize("event_time,expected_flag", [
    ("2026-04-01 12:00:00", 1),   # window 1 start — inclusive
    ("2026-04-01 12:19:00", 1),   # window 1 — just inside
    ("2026-04-01 12:20:00", 0),   # window 1 end — exclusive
    ("2026-04-01 20:00:00", 1),   # window 2 start — inclusive
    ("2026-04-01 20:20:00", 0),   # window 2 end — exclusive
    ("2026-04-01 09:00:00", 0),   # off-peak
])
def test_burst_flag_value(event_time, expected_flag, feat_60m, spark):
    schema = StructType([
        StructField("event_id",        StringType(),    False),
        StructField("event_timestamp", StringType(),    False),
        StructField("customer_id",     StringType(),    False),
    ])
    df = spark.createDataFrame(
        [("E_TEST", event_time, "C001")],
        schema=schema,
    ).withColumn("event_timestamp", F.to_timestamp("event_timestamp"))

    feat_60m._add_burst_flag = StreamFeature60m._add_burst_flag.__get__(feat_60m)
    result = feat_60m._add_burst_flag(df)
    row = result.collect()[0]
    assert row["f_stream_burst_activity_flag"] == expected_flag, (
        f"event at {event_time} → expected burst flag {expected_flag}, "
        f"got {row['f_stream_burst_activity_flag']}"
    )


# ── 3. _compute_features ─────────────────────────────────────────────────────

def test_compute_features_output_schema(feat_60m, flagged_events):
    result = feat_60m._compute_features(flagged_events)
    expected_cols = {
        "customer_id",
        "event_timestamp",
        "created_ts",
        "f_stream_views_30m",
        "f_stream_add_to_cart_30m",
        "f_stream_cart_to_purchase_ratio_60m",
        "f_stream_burst_activity_flag",
    }
    assert expected_cols.issubset(set(result.columns))


def test_compute_features_views_30m_first_half_only(feat_60m, flagged_events):
    """C001 in window 09:00–10:00: 2 views at 09:05 and 09:20 (both within first 30min)."""
    result = feat_60m._compute_features(flagged_events)
    row = (
        result
        .filter(
            (F.col("customer_id") == "C001") &
            (F.col("event_timestamp").cast("string").startswith("2026-04-01 09"))
        )
        .collect()[0]
    )
    assert row["f_stream_views_30m"] == 2


def test_compute_features_add_to_cart_30m(feat_60m, flagged_events):
    """C001 in window 09:00–10:00: 1 add_to_cart at 09:25 (within first 30min)."""
    result = feat_60m._compute_features(flagged_events)
    row = (
        result
        .filter(
            (F.col("customer_id") == "C001") &
            (F.col("event_timestamp").cast("string").startswith("2026-04-01 09"))
        )
        .collect()[0]
    )
    assert row["f_stream_add_to_cart_30m"] == 1


def test_compute_features_cart_to_purchase_ratio_with_purchase(feat_60m, flagged_events):
    """C001 in window 09:00–10:00: 1 add_to_cart, 1 purchase → ratio = 1.0."""
    result = feat_60m._compute_features(flagged_events)
    row = (
        result
        .filter(
            (F.col("customer_id") == "C001") &
            (F.col("event_timestamp").cast("string").startswith("2026-04-01 09"))
        )
        .collect()[0]
    )
    assert abs(row["f_stream_cart_to_purchase_ratio_60m"] - 1.0) < 0.001


def test_compute_features_cart_to_purchase_ratio_no_purchase(feat_60m, flagged_events):
    """C002 in window 09:00–10:00: 1 add_to_cart, 0 purchases → ratio = 0.0."""
    result = feat_60m._compute_features(flagged_events)
    row = (
        result
        .filter(
            (F.col("customer_id") == "C002") &
            (F.col("event_timestamp").cast("string").startswith("2026-04-01 09"))
        )
        .collect()[0]
    )
    assert row["f_stream_cart_to_purchase_ratio_60m"] == 0.0


def test_compute_features_burst_flag_set_in_burst_window(feat_60m, flagged_events):
    """C001 in window 12:00–13:00 has events at 12:05 and 12:15 → burst_flag = 1."""
    result = feat_60m._compute_features(flagged_events)
    row = (
        result
        .filter(
            (F.col("customer_id") == "C001") &
            (F.col("event_timestamp").cast("string").startswith("2026-04-01 12"))
        )
        .collect()[0]
    )
    assert row["f_stream_burst_activity_flag"] == 1


def test_compute_features_burst_flag_clear_off_peak(feat_60m, flagged_events):
    """C001 in window 09:00–10:00 has no burst events → burst_flag = 0."""
    result = feat_60m._compute_features(flagged_events)
    row = (
        result
        .filter(
            (F.col("customer_id") == "C001") &
            (F.col("event_timestamp").cast("string").startswith("2026-04-01 09"))
        )
        .collect()[0]
    )
    assert row["f_stream_burst_activity_flag"] == 0


def test_compute_features_event_timestamp_is_window_start(feat_60m, flagged_events):
    """event_timestamp in output must be the window start (floor to 60 minutes)."""
    result = feat_60m._compute_features(flagged_events)
    row = (
        result
        .filter(
            (F.col("customer_id") == "C001") &
            (F.col("event_timestamp").cast("string").startswith("2026-04-01 09"))
        )
        .collect()[0]
    )
    ts = row["event_timestamp"]
    # Window start for 09:05 events should be 09:00:00
    assert ts.hour == 9
    assert ts.minute == 0
    assert ts.second == 0


# ── 4. run() ──────────────────────────────────────────────────────────────────

def test_run_pipeline_order(feat_60m, parsed_events):
    """run() must call: _read_events → _add_burst_flag → _compute_features → _write."""
    call_order = []

    feat_60m._read_events = MagicMock(return_value=parsed_events)

    with patch.object(feat_60m, "_add_burst_flag",    side_effect=lambda df: (call_order.append("burst"), df)[1]), \
         patch.object(feat_60m, "_compute_features",  side_effect=lambda df: (call_order.append("compute"), MagicMock())[1]), \
         patch.object(feat_60m, "_write",             side_effect=lambda df: (call_order.append("write"), 0)[1]):
        feat_60m.run()

    assert call_order == ["burst", "compute", "write"]
