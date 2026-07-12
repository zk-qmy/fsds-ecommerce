"""
Shared fixtures for Section 02 pipeline tests.

SparkSession is session-scoped (expensive to start — reused across all test files).
Sample DataFrames are function-scoped so each test gets a clean copy.
"""

from __future__ import annotations

import datetime
import socketserver
import sys

# PySpark 4.x unconditionally references UnixStreamServer, which doesn't exist
# on Windows. Stub it out before any pyspark import so the module loads.
if sys.platform == "win32" and not hasattr(socketserver, "UnixStreamServer"):
    socketserver.UnixStreamServer = socketserver.TCPServer  # type: ignore[attr-defined]
from pathlib import Path
import sys

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

sys.path.append(str(Path(__file__).resolve().parents[2]))

from delta import configure_spark_with_delta_pip


# ── SparkSession ──────────────────────────────────────────────────────────────

@pytest.fixture(scope="session")
def spark():
    """Session-scoped SparkSession with Delta Lake support."""
    builder = (
        SparkSession.builder
        .master("local[2]")
        .appName("test-section02")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
    )
    session = configure_spark_with_delta_pip(builder).getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


# ── Schemas ───────────────────────────────────────────────────────────────────

ORDERS_SCHEMA = StructType([
    StructField("order_id",          StringType(),    False),
    StructField("customer_id",       StringType(),    False),
    StructField("order_timestamp",   TimestampType(), False),
    StructField("status",            StringType(),    False),
    StructField("shipping_city",     StringType(),    False),
    StructField("shipping_method",   StringType(),    True),   # NULL before schema_change_date
    StructField("coupon_code",       StringType(),    True),   # NULL before schema_change_date
    StructField("ingest_ts",         TimestampType(), False),  # Bronze lineage
    StructField("source_file",       StringType(),    False),  # Bronze lineage
    StructField("pipeline_run_id",   StringType(),    False),  # Bronze lineage
])

ORDER_ITEMS_SCHEMA = StructType([
    StructField("order_item_id",     StringType(),    False),
    StructField("order_id",          StringType(),    False),
    StructField("product_id",        StringType(),    False),
    StructField("quantity",          LongType(),      False),
    StructField("unit_price",        DoubleType(),    False),
    StructField("discount",          DoubleType(),    False),  # generator column name
    StructField("line_total",        DoubleType(),    False),
    StructField("ingest_ts",         TimestampType(), False),  # Bronze lineage
    StructField("source_file",       StringType(),    False),  # Bronze lineage
    StructField("pipeline_run_id",   StringType(),    False),  # Bronze lineage
])

EVENTS_SCHEMA = StructType([
    StructField("event_id",         StringType(),    False),
    StructField("event_type",       StringType(),    False),
    StructField("event_timestamp",  StringType(),    False),
    StructField("created_ts",       StringType(),    False),
    StructField("customer_id",      StringType(),    False),
    StructField("session_id",       StringType(),    False),
    StructField("product_id",       StringType(),    True),
    StructField("order_id",         StringType(),    True),
    StructField("quantity",         LongType(),      True),
    StructField("price",            DoubleType(),    True),
])


# ── Sample DataFrames ─────────────────────────────────────────────────────────

_SRC = "a_data_generator/outputs/offline/orders.parquet"
_RUN = "bronze_test_run"


@pytest.fixture
def sample_orders(spark):
    """Orders with Problem B (NULLs before schema_change_date) and Problem A (city skew)."""
    T = datetime.datetime
    rows = [
        # order_id, customer_id, order_timestamp, status, shipping_city,
        # shipping_method, coupon_code, ingest_ts, source_file, pipeline_run_id
        ("O001", "C001", T(2026, 2, 1),  "completed", "Ho Chi Minh City", None,       None,      T(2026, 2, 1),  _SRC, _RUN),
        ("O002", "C002", T(2026, 2, 15), "pending",   "Ho Chi Minh City", None,       None,      T(2026, 2, 15), _SRC, _RUN),
        ("O003", "C003", T(2026, 3, 1),  "cancelled", "Ho Chi Minh City", None,       None,      T(2026, 3, 1),  _SRC, _RUN),
        ("O004", "C001", T(2026, 4, 1),  "completed", "Ho Chi Minh City", "express",  "PROMO10", T(2026, 4, 1),  _SRC, _RUN),
        ("O005", "C004", T(2026, 4, 10), "completed", "Hanoi",            "standard", None,      T(2026, 4, 10), _SRC, _RUN),
        ("O006", "C005", T(2026, 5, 1),  "completed", "Da Nang",          "same_day", "SALE20",  T(2026, 5, 1),  _SRC, _RUN),
    ]
    return spark.createDataFrame(rows, schema=ORDERS_SCHEMA)


_SRC_OI = "a_data_generator/outputs/offline/order_items.parquet"


@pytest.fixture
def sample_order_items(spark):
    """Order items with Problem C (2 duplicate rows on O001/P001/10.0).

    Duplicates are exact copies; ingest_ts differs so the earlier one is kept by _fix_duplicates.
    line_total = quantity * unit_price - discount.
    """
    T = datetime.datetime
    rows = [
        # order_item_id, order_id, product_id, qty, unit_price, discount,
        # line_total, ingest_ts, source_file, pipeline_run_id
        ("OI001", "O001", "P001", 2, 10.0, 1.0,  19.0, T(2026, 2, 1, 10, 0), _SRC_OI, _RUN),
        ("OI001", "O001", "P001", 2, 10.0, 1.0,  19.0, T(2026, 2, 1, 10, 5), _SRC_OI, _RUN),
        ("OI002", "O001", "P002", 1, 25.0, 0.0,  25.0, T(2026, 2, 1, 10, 0), _SRC_OI, _RUN),
        ("OI003", "O002", "P003", 3, 15.0, 2.5,  42.5, T(2026, 2, 15, 9, 0), _SRC_OI, _RUN),
        ("OI004", "O003", "P001", 1, 10.0, 0.0,  10.0, T(2026, 3, 1, 8,  0), _SRC_OI, _RUN),
    ]
    return spark.createDataFrame(rows, schema=ORDER_ITEMS_SCHEMA)


@pytest.fixture
def sample_events(spark):
    """Events with burst-window rows (12:00 and 20:00) and off-peak rows."""
    rows = [
        # Burst window 1 — 12:00–12:20
        ("E001", "view",        "2026-04-01 12:05:00", "2026-04-01 12:05:00", "C001", "S001", "P001", None, None, None),
        ("E002", "add_to_cart", "2026-04-01 12:10:00", "2026-04-01 12:10:00", "C001", "S001", "P001", None, 1,    10.0),
        ("E003", "purchase",    "2026-04-01 12:15:00", "2026-04-01 12:15:00", "C001", "S001", "P001", "O004", 1, 10.0),
        # Burst window 2 — 20:00–20:20
        ("E004", "view",        "2026-04-01 20:02:00", "2026-04-01 20:02:00", "C002", "S002", "P002", None, None, None),
        ("E005", "add_to_cart", "2026-04-01 20:18:00", "2026-04-01 20:18:00", "C002", "S002", "P002", None, 2,    25.0),
        # Off-peak
        ("E006", "view",        "2026-04-01 09:00:00", "2026-04-01 09:00:00", "C003", "S003", "P003", None, None, None),
        ("E007", "add_to_cart", "2026-04-01 09:05:00", "2026-04-01 09:05:00", "C003", "S003", "P003", None, 1,    15.0),
        ("E008", "purchase",    "2026-04-01 09:30:00", "2026-04-01 09:30:00", "C003", "S003", "P003", "O005", 1, 15.0),
    ]
    return spark.createDataFrame(rows, schema=EVENTS_SCHEMA)
