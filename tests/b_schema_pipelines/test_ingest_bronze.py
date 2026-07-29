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
        [("OI001", "O001", "P001", 2, 999.0, 10.0, 1988.0, T(2026, 2, 1))],
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount", "line_total", "created_ts"],
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
         "session_id": "S001", "product_id": "P001", "order_id": None,
         "quantity": None, "price": None,
         "event_timestamp": "2026-04-01 12:00:00", "created_ts": "2026-04-01 12:00:00"},
        {"event_id": "E002", "event_type": "purchase", "customer_id": "C002",
         "session_id": "S002", "product_id": "P001", "order_id": "O001",
         "quantity": 1, "price": 999.0,
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


# ── 2b. _relative_source ──────────────────────────────────────────────────────

def test_relative_source_strips_source_dir_prefix(ingester, source_dir):
    absolute = str(source_dir / "offline" / "order_items.parquet")
    assert ingester._relative_source(absolute) == "offline/order_items.parquet"


def test_relative_source_stable_across_offline_and_streaming(ingester, source_dir):
    """The value stored/compared for idempotency must depend only on the
    path's position under source_dir, not on source_dir's own absolute
    prefix -- this is what makes it stable whether the script runs on the
    host (/mnt/d/fsds-ecommerce/...) or inside the Airflow container
    (/opt/project/...), unlike the raw absolute path."""
    offline_path = str(source_dir / "offline" / "customers.parquet")
    stream_path = str(source_dir / "streaming" / "events.json")
    assert ingester._relative_source(offline_path) == "offline/customers.parquet"
    assert ingester._relative_source(stream_path) == "streaming/events.json"


def test_ingest_stamps_relative_not_absolute_source_file(ingester, spark, tmp_path):
    """Regression test for the real bug: two BronzeIngester instances
    pointed at the same file through different absolute source_dir prefixes
    (simulating host vs. container execution) must stamp the *same*
    source_file value, or append_if_new_source's idempotency check silently
    fails to recognize them as the same file -- confirmed live: order_items
    and events each ended up double-ingested this way, once from each
    context."""
    ingester._ingest_offline_table("customers")
    result = spark.read.format("delta").load(str(tmp_path / "bronze" / "customers"))
    row = result.collect()[0]
    assert row["source_file"] == "offline/customers.parquet"
    assert not row["source_file"].startswith("/"), (
        "source_file must be relative -- an absolute path differs between "
        "host and container execution even for the identical physical file"
    )


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


def test_ingest_offline_table_rerun_is_idempotent(ingester, spark, tmp_path):
    """Running twice against an unchanged source must not duplicate rows —
    CLAUDE.md: "Re-running a job must not produce duplicate rows." Bronze
    upserts (MERGE) keyed on each table's primary key instead of blindly
    appending, specifically because the source files here are static
    snapshots (a_data_generator's output), not a new incremental batch per
    run — re-ingesting the same snapshot must be a no-op on row count."""
    ingester._ingest_offline_table("customers")
    first_count = spark.read.format("delta").load(str(tmp_path / "bronze" / "customers")).count()

    ingester._ingest_offline_table("customers")
    second_count = spark.read.format("delta").load(str(tmp_path / "bronze" / "customers")).count()

    assert second_count == first_count


def test_ingest_offline_table_rerun_updates_changed_row_in_place(ingester, spark, source_dir, tmp_path):
    """A source row whose data changed between runs (same primary key)
    updates its existing Bronze row instead of sitting alongside a stale
    duplicate."""
    ingester._ingest_offline_table("customers")

    # Same customer_id (C001), different segment — simulates the source
    # snapshot being regenerated with updated data for an existing entity.
    T = datetime.datetime
    spark.createDataFrame(
        [("C001", T(2026, 1, 1), "VN", "platinum", True),
         ("C002", T(2026, 1, 2), "US", "silver", False)],
        ["customer_id", "signup_ts", "country", "segment", "marketing_opt_in"],
    ).write.mode("overwrite").parquet(str(source_dir / "offline" / "customers.parquet"))

    ingester._ingest_offline_table("customers")

    result = spark.read.format("delta").load(str(tmp_path / "bronze" / "customers"))
    assert result.count() == 2, "must still be 2 rows — an update, not a duplicate insert"
    c001_segments = [r["segment"] for r in result.filter("customer_id = 'C001'").collect()]
    assert c001_segments == ["platinum"], "the existing C001 row must reflect the updated value"


def test_ingest_events_rerun_is_idempotent(ingester, spark, tmp_path):
    """A second run against the same source_file must be a no-op — events,
    like order_items, uses append_if_new_source rather than merge() (Problem
    F's duplicates can legitimately share the same (event_id,
    event_timestamp) pair, confirmed live against the real generated data —
    a per-row MERGE can't be used here for the same reason as order_items)."""
    ingester._ingest_events()
    first_count = spark.read.format("delta").load(str(tmp_path / "bronze" / "events")).count()

    ingester._ingest_events()
    second_count = spark.read.format("delta").load(str(tmp_path / "bronze" / "events")).count()

    assert second_count == first_count


def test_ingest_order_items_preserves_problem_c_duplicate_order_item_id(ingester, spark, source_dir, tmp_path):
    """order_items' injected Problem C duplicates copy the original row's
    order_item_id too — they must survive ingestion into Bronze unchanged
    (Bronze is a faithful copy; Silver's dedup fix is what's supposed to
    remove them, not Bronze). A primary-key-keyed MERGE can't be used for
    this table at all (Delta forbids multiple source rows matching one
    target row) — confirms append_if_new_source is used instead."""
    T = datetime.datetime
    spark.createDataFrame(
        [("OI001", "O001", "P001", 2, 999.0, 10.0, 1988.0, T(2026, 2, 1)),
         ("OI001", "O001", "P001", 2, 999.0, 10.0, 1988.0, T(2026, 2, 1))],  # Problem C duplicate
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount", "line_total", "created_ts"],
    ).write.mode("overwrite").parquet(str(source_dir / "offline" / "order_items.parquet"))

    ingester._ingest_offline_table("order_items")

    result = spark.read.format("delta").load(str(tmp_path / "bronze" / "order_items"))
    assert result.filter("order_item_id = 'OI001'").count() == 2


def test_ingest_order_items_rerun_is_idempotent(ingester, spark, tmp_path):
    """A second run against the same source_file must be a no-op (skipped
    entirely, not re-appended) — see APPEND_IF_NEW_SOURCE_TABLES."""
    ingester._ingest_offline_table("order_items")
    first_count = spark.read.format("delta").load(str(tmp_path / "bronze" / "order_items")).count()

    ingester._ingest_offline_table("order_items")
    second_count = spark.read.format("delta").load(str(tmp_path / "bronze" / "order_items")).count()

    assert second_count == first_count


def test_ingest_events_preserves_same_event_id_different_timestamp(ingester, spark, source_dir, tmp_path):
    """Problem F's intentionally-injected near-duplicate event_ids (same
    event_id, shifted event_timestamp) must survive ingestion unchanged —
    that duplicate is meant to reach Silver/Flink so their own dedup fix has
    something real to demonstrate, not be silently absorbed in Bronze."""
    extra_event = {
        "event_id": "E001", "event_type": "view", "customer_id": "C001",
        "session_id": "S001", "product_id": "P001", "order_id": None,
        "quantity": None, "price": None,
        "event_timestamp": "2026-04-01 12:05:00",  # same event_id as E001, shifted timestamp
        "created_ts": "2026-04-01 12:05:00",
    }
    with open(source_dir / "streaming" / "events.json", "a") as f:
        f.write(json.dumps(extra_event) + "\n")

    ingester._ingest_events()

    result = spark.read.format("delta").load(str(tmp_path / "bronze" / "events"))
    e001_rows = result.filter("event_id = 'E001'").collect()
    assert len(e001_rows) == 2, "both the original and the shifted-timestamp E001 row must survive"


def test_ingest_events_preserves_exact_duplicate_event_id_and_timestamp(ingester, spark, source_dir, tmp_path):
    """Confirmed live against the real generated dataset: Problem F's
    duplicates can land on the exact same (event_id, event_timestamp) pair,
    not just a shifted one — this is why events uses append_if_new_source
    rather than a (event_id, event_timestamp)-keyed merge(), which would
    raise DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE on this
    exact case."""
    extra_event = {
        "event_id": "E001", "event_type": "view", "customer_id": "C001",
        "session_id": "S001", "product_id": "P001", "order_id": None,
        "quantity": None, "price": None,
        "event_timestamp": "2026-04-01 12:00:00",  # exact duplicate of E001
        "created_ts": "2026-04-01 12:00:00",
    }
    with open(source_dir / "streaming" / "events.json", "a") as f:
        f.write(json.dumps(extra_event) + "\n")

    ingester._ingest_events()

    result = spark.read.format("delta").load(str(tmp_path / "bronze" / "events"))
    assert result.filter("event_id = 'E001'").count() == 2


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
    output = [json.loads(line) for line in capsys.readouterr().out.strip().splitlines()]
    events_log = next(line for line in output if line["table"] == "events")
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
    lines = [line for line in capsys.readouterr().out.strip().splitlines() if line]
    logs = [json.loads(line) for line in lines]
    tables_logged = {line["table"] for line in logs}
    expected = set(OFFLINE_TABLES) | {"events"}
    assert tables_logged == expected
