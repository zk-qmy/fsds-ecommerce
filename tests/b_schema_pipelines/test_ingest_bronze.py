"""
Tests for BronzeIngester.

BronzeIngester is fully implemented — these are real tests (not xfail stubs).
Writes go to tmp_path so no state leaks between tests.
The session-scoped SparkSession from conftest is injected via _build_spark patch.
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from unittest.mock import patch
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.pipelines.bronze.ingest_bronze import BronzeIngester, OFFLINE_TABLES


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def source_dir(spark, tmp_path):
    """Write minimal Parquet + NDJSON source files for each offline table."""
    offline = tmp_path / "offline"
    offline.mkdir(parents=True)

    T = datetime.datetime

    # customers
    spark.createDataFrame(
        [("C001", T(2026, 1, 1), "VN", "gold", True),
         ("C002", T(2026, 1, 2), "US", "silver", False)],
        ["customer_id", "signup_ts", "country", "segment", "marketing_opt_in"],
    ).write.parquet(str(offline / "customers.parquet"))

    # products
    spark.createDataFrame(
        [("P001", "electronics", "Samsung", 999.0, True, T(2026, 1, 1))],
        ["product_id", "category", "brand", "base_price", "is_active", "created_ts"],
    ).write.parquet(str(offline / "products.parquet"))

    # orders
    spark.createDataFrame(
        [("O001", "C001", T(2026, 2, 1), "completed", "Ho Chi Minh City", None, None, T(2026, 2, 1)),
         ("O002", "C002", T(2026, 4, 1), "completed", "Hanoi", "express", "PROMO", T(2026, 4, 1))],
        ["order_id", "customer_id", "order_timestamp", "status",
         "shipping_city", "shipping_method", "coupon_code", "created_ts"],
    ).write.parquet(str(offline / "orders.parquet"))

    # order_items
    spark.createDataFrame(
        [("OI001", "O001", "P001", 2, 999.0, 10.0, T(2026, 2, 1))],
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount", "created_ts"],
    ).write.parquet(str(offline / "order_items.parquet"))

    # payments
    spark.createDataFrame(
        [("PAY001", "O001", T(2026, 2, 1), "credit_card", 989.0, "paid")],
        ["payment_id", "order_id", "payment_timestamp", "payment_method", "amount", "payment_status"],
    ).write.parquet(str(offline / "payments.parquet"))

    # events (NDJSON)
    streaming = tmp_path / "streaming"
    streaming.mkdir()
    events = [
        {"event_id": "E001", "event_type": "view", "customer_id": "C001",
         "event_timestamp": "2026-04-01 12:00:00", "created_ts": "2026-04-01 12:00:00"},
        {"event_id": "E002", "event_type": "purchase", "customer_id": "C002",
         "event_timestamp": "2026-04-01 20:00:00", "created_ts": "2026-04-01 20:00:00"},
    ]
    with open(streaming / "events.json", "w") as f:
        for e in events:
            f.write(json.dumps(e) + "\n")

    return tmp_path


@pytest.fixture
def ingester(spark, source_dir, tmp_path):
    """BronzeIngester with test SparkSession injected and output routed to tmp_path."""
    with patch.object(BronzeIngester, "_build_spark", return_value=spark):
        return BronzeIngester(
            source_dir=source_dir,
            output_dir=tmp_path / "bronze",
        )


# ── 1. Initialisation ─────────────────────────────────────────────────────────

def test_prefix_is_bronze(ingester):
    assert ingester.PREFIX == "bronze"


def test_run_id_starts_with_bronze(ingester):
    assert ingester.run_id.startswith("bronze_")


def test_source_dir_stored_as_path(ingester, source_dir):
    assert ingester.source_dir == source_dir


def test_output_dir_stored_as_path(ingester, tmp_path):
    assert ingester.output_dir == tmp_path / "bronze"


# ── 2. _add_ingest_metadata ───────────────────────────────────────────────────

def test_add_ingest_metadata_adds_three_columns(ingester, spark):
    df = spark.createDataFrame([("C001",)], ["customer_id"])
    result = ingester._add_ingest_metadata(df, "test/source.parquet")
    assert "ingest_ts"        in result.columns
    assert "source_file"      in result.columns
    assert "pipeline_run_id"  in result.columns


def test_add_ingest_metadata_preserves_original_columns(ingester, spark):
    df = spark.createDataFrame([("C001", "VN")], ["customer_id", "country"])
    result = ingester._add_ingest_metadata(df, "test/source.parquet")
    assert "customer_id" in result.columns
    assert "country"     in result.columns


def test_add_ingest_metadata_source_file_value(ingester, spark):
    df = spark.createDataFrame([("C001",)], ["customer_id"])
    result = ingester._add_ingest_metadata(df, "my/source/path.parquet")
    row = result.collect()[0]
    assert row["source_file"] == "my/source/path.parquet"


def test_add_ingest_metadata_run_id_value(ingester, spark):
    df = spark.createDataFrame([("C001",)], ["customer_id"])
    result = ingester._add_ingest_metadata(df, "path")
    row = result.collect()[0]
    assert row["pipeline_run_id"] == ingester.run_id


def test_add_ingest_metadata_ingest_ts_is_not_null(ingester, spark):
    df = spark.createDataFrame([("C001",)], ["customer_id"])
    result = ingester._add_ingest_metadata(df, "path")
    row = result.collect()[0]
    assert row["ingest_ts"] is not None


def test_add_ingest_metadata_row_count_unchanged(ingester, spark):
    df = spark.createDataFrame([("C001",), ("C002",), ("C003",)], ["customer_id"])
    result = ingester._add_ingest_metadata(df, "path")
    assert result.count() == 3


# ── 3. _ingest_offline_table ──────────────────────────────────────────────────

def test_ingest_offline_table_creates_delta_directory(ingester, tmp_path):
    ingester._ingest_offline_table("customers")
    assert (tmp_path / "bronze" / "customers").exists()


@pytest.mark.parametrize("table", OFFLINE_TABLES)
def test_ingest_offline_table_all_tables(table, ingester, tmp_path):
    ingester._ingest_offline_table(table)
    assert (tmp_path / "bronze" / table).exists()


def test_ingest_offline_table_metadata_columns_present(ingester, spark, tmp_path):
    ingester._ingest_offline_table("customers")
    result = spark.read.format("delta").load(str(tmp_path / "bronze" / "customers"))
    assert "ingest_ts"       in result.columns
    assert "source_file"     in result.columns
    assert "pipeline_run_id" in result.columns


def test_ingest_offline_table_row_count_matches_source(ingester, spark, source_dir, tmp_path):
    source_count = spark.read.parquet(str(source_dir / "offline" / "customers.parquet")).count()
    ingester._ingest_offline_table("customers")
    delta_count = spark.read.format("delta").load(str(tmp_path / "bronze" / "customers")).count()
    assert delta_count == source_count


def test_ingest_offline_table_append_mode(ingester, spark, tmp_path):
    """Running twice appends — Bronze is append-only."""
    ingester._ingest_offline_table("customers")
    first_count = spark.read.format("delta").load(str(tmp_path / "bronze" / "customers")).count()

    ingester._ingest_offline_table("customers")
    second_count = spark.read.format("delta").load(str(tmp_path / "bronze" / "customers")).count()

    assert second_count == first_count * 2


def test_ingest_offline_table_logs_success(ingester, capsys):
    ingester._ingest_offline_table("customers")
    output = capsys.readouterr().out.strip()
    log = json.loads(output)
    assert log["status"] == "success"
    assert log["table"]  == "customers"


# ── 4. _ingest_events ─────────────────────────────────────────────────────────

def test_ingest_events_creates_delta_directory(ingester, tmp_path):
    ingester._ingest_events()
    assert (tmp_path / "bronze" / "events").exists()


def test_ingest_events_metadata_columns_present(ingester, spark, tmp_path):
    ingester._ingest_events()
    result = spark.read.format("delta").load(str(tmp_path / "bronze" / "events"))
    assert "ingest_ts"       in result.columns
    assert "source_file"     in result.columns
    assert "pipeline_run_id" in result.columns


def test_ingest_events_row_count_matches_source(ingester, spark, source_dir, tmp_path):
    source_lines = (source_dir / "streaming" / "events.json").read_text().strip().splitlines()
    ingester._ingest_events()
    delta_count = spark.read.format("delta").load(str(tmp_path / "bronze" / "events")).count()
    assert delta_count == len(source_lines)


def test_ingest_events_logs_success(ingester, capsys):
    ingester._ingest_events()
    # Last JSON line in stdout
    output = [json.loads(l) for l in capsys.readouterr().out.strip().splitlines()]
    events_log = next(l for l in output if l["table"] == "events")
    assert events_log["status"] == "success"


# ── 5. run() ─────────────────────────────────────────────────────────────────

def test_run_produces_all_offline_tables(ingester, tmp_path):
    with patch.object(ingester.spark, "stop"):   # avoid stopping the session-level spark
        ingester.run()
    for table in OFFLINE_TABLES:
        assert (tmp_path / "bronze" / table).exists(), f"Missing Delta dir for {table}"


def test_run_produces_events_table(ingester, tmp_path):
    with patch.object(ingester.spark, "stop"):
        ingester.run()
    assert (tmp_path / "bronze" / "events").exists()


def test_run_logs_one_entry_per_table(ingester, capsys):
    with patch.object(ingester.spark, "stop"):
        ingester.run()
    lines = [l for l in capsys.readouterr().out.strip().splitlines() if l]
    logs = [json.loads(l) for l in lines]
    tables_logged = {l["table"] for l in logs}
    expected = set(OFFLINE_TABLES) | {"events"}
    assert tables_logged == expected
