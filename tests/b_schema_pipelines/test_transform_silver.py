"""
Tests for SilverTransformer.

Transformation logic is tested on in-memory DataFrames — no Delta I/O.
Delta reads/writes and log_run are mocked so tests run without MinIO/Spark cluster.
"""

from __future__ import annotations

import datetime
import sys
from unittest.mock import MagicMock, patch

import pytest

from b_schema_pipelines.pipelines.silver.transform_silver import (
    SilverTransformer,
    main,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def transformer(spark):
    """SilverTransformer with all I/O bypassed — only transform logic is live.

    Uses object.__new__ to skip __init__ entirely (avoids MinIO/Spark startup).
    Sets only the attributes that the transform methods actually read.
    """
    t = object.__new__(SilverTransformer)
    t.spark = spark
    t.silver_dir = "s3a://test-silver"
    t.run_id = "test_run"
    t.logger = MagicMock()
    t.log_run = MagicMock()  # avoids json.dumps(entry) with MagicMock row counts
    t.writer = MagicMock()
    t._read_bronze = MagicMock()
    t._write_silver = MagicMock(side_effect=lambda df, *a, **kw: df.count())
    return t


@pytest.fixture
def sample_products(spark):
    """Products used to test the Fix 4 broadcast join.

    Only P001/P002 are present — P003 (referenced by sample_order_items)
    is deliberately missing so left-join behaviour can be verified.
    """
    rows = [
        ("P001", "electronics", "Samsung", 999.0, True),
        ("P002", "fashion",     "Zara",    59.9,  True),
    ]
    return spark.createDataFrame(
        rows, ["product_id", "category", "brand", "base_price", "is_active"]
    )


# ── 1. _fix_schema_evolution (Problem B) ─────────────────────────────────────

def test_fix_schema_evolution_fills_null_coupon_code(transformer, sample_orders):
    result = transformer._fix_schema_evolution(sample_orders)
    null_count = result.filter(result["coupon_code"].isNull()).count()
    assert null_count == 0, "coupon_code must have no NULLs after Silver fix"


def test_fix_schema_evolution_fills_null_shipping_method(transformer, sample_orders):
    result = transformer._fix_schema_evolution(sample_orders)
    null_count = result.filter(result["shipping_method"].isNull()).count()
    assert null_count == 0, "shipping_method must have no NULLs after Silver fix"


@pytest.mark.parametrize("col,expected_fill", [
    ("coupon_code",     "LEGACY"),
    ("shipping_method", "UNKNOWN"),
])
def test_fix_schema_evolution_fill_values(col, expected_fill, transformer, sample_orders):
    result = transformer._fix_schema_evolution(sample_orders)
    rows = result.collect()
    # O001–O003 had NULL before schema_change_date — verify fill value
    pre_change = [r for r in rows if r["order_id"] in ("O001", "O002", "O003")]
    for row in pre_change:
        assert row[col] == expected_fill, (
            f"Expected '{expected_fill}' for NULL {col}, got {row[col]!r}"
        )


def test_fix_schema_evolution_preserves_non_null_values(transformer, sample_orders):
    result = transformer._fix_schema_evolution(sample_orders)
    rows = {r["order_id"]: r for r in result.collect()}
    # O004 already had real values — must not be overwritten
    assert rows["O004"]["coupon_code"]     == "PROMO10"
    assert rows["O004"]["shipping_method"] == "express"


def test_fix_schema_evolution_row_count_unchanged(transformer, sample_orders):
    result = transformer._fix_schema_evolution(sample_orders)
    assert result.count() == sample_orders.count()


# ── 2. _fix_duplicates (Problem C) ───────────────────────────────────────────

def test_fix_duplicates_reduces_row_count(transformer, sample_order_items):
    result = transformer._fix_duplicates(sample_order_items)
    # sample_order_items has 5 rows with 1 duplicate → expect 4 rows
    assert result.count() == 4


def test_fix_duplicates_keeps_earliest_ingest_ts(transformer, sample_order_items):
    result = transformer._fix_duplicates(sample_order_items)
    rows = {r["order_item_id"]: r for r in result.collect()}
    # OI001 exists twice — earliest ingest_ts is 10:00, not 10:05
    assert rows["OI001"]["ingest_ts"] == datetime.datetime(2026, 2, 1, 10, 0)


def test_fix_duplicates_no_duplicate_natural_keys(transformer, sample_order_items):
    from pyspark.sql import functions as F
    result = transformer._fix_duplicates(sample_order_items)
    dupes = (
        result
        .groupBy("order_id", "product_id", "unit_price", "quantity")
        .count()
        .filter(F.col("count") > 1)
    )
    assert dupes.count() == 0, "Duplicate natural keys remain after dedup"


def test_fix_duplicates_keeps_distinct_quantity_rows(transformer, spark):
    """Same (order_id, product_id, unit_price) but different quantity is a real,
    distinct row — not an injected duplicate — and must survive dedup.

    Without `quantity` in the partition key, this collapses to 1 row and
    silently destroys a genuine order_items row (found live against the real
    generated dataset: 28 such false-positive collisions)."""
    T = datetime.datetime
    rows = [
        ("OI101", "O101", "P001", 1, 10.0, 0.0, 10.0, T(2026, 2, 1, 10, 0), "src", "run"),
        ("OI102", "O101", "P001", 2, 10.0, 0.0, 20.0, T(2026, 2, 1, 10, 0), "src", "run"),
    ]
    df = spark.createDataFrame(rows, schema=[
        "order_item_id", "order_id", "product_id", "quantity", "unit_price",
        "discount", "line_total", "ingest_ts", "source_file", "pipeline_run_id",
    ])
    result = transformer._fix_duplicates(df)
    assert result.count() == 2, "Distinct-quantity rows sharing order/product/price must both survive"


@pytest.mark.parametrize("order_id,product_id,unit_price,expected_rows", [
    ("O001", "P001", 10.0, 1),   # duplicated — deduped to 1
    ("O001", "P002", 25.0, 1),   # unique — unchanged
    ("O002", "P003", 15.0, 1),   # unique — unchanged
    ("O003", "P001", 10.0, 1),   # same product as OI001 but different order_id — kept
])
def test_fix_duplicates_per_natural_key(
    order_id, product_id, unit_price, expected_rows, transformer, sample_order_items
):
    from pyspark.sql import functions as F
    result = transformer._fix_duplicates(sample_order_items)
    count = (
        result
        .filter(
            (F.col("order_id")   == order_id)   &
            (F.col("product_id") == product_id) &
            (F.col("unit_price") == unit_price)
        )
        .count()
    )
    assert count == expected_rows


# ── 3. _run_baseline ─────────────────────────────────────────────────────────

def test_run_baseline_calls_read_for_all_tables(transformer):
    transformer.run(mode="baseline")
    call_args = [c.args[0] for c in transformer._read_bronze.call_args_list]
    for table in ("orders", "order_items", "products", "customers", "payments"):
        assert table in call_args, f"_read_bronze not called for {table}"


def test_run_baseline_no_schema_evolution_fix(transformer, sample_orders):
    """Baseline mode must NOT apply NULL fills — raw data passes through."""
    transformer._read_bronze.side_effect = (
        lambda t: sample_orders if t == "orders" else MagicMock()
    )
    transformer.run(mode="baseline")

    written_df = transformer._write_silver.call_args_list[0].args[0]
    null_count = written_df.filter(written_df["coupon_code"].isNull()).count()
    assert null_count > 0, "Baseline must preserve NULLs (no transformation applied)"


# ── 4. _run_optimized ────────────────────────────────────────────────────────

def test_run_optimized_applies_schema_fix(transformer, spark, sample_orders, sample_order_items):
    def mock_read(table):
        if table == "orders":
            return sample_orders
        if table == "order_items":
            return sample_order_items
        return spark.createDataFrame([("x",)], ["id"])

    transformer._read_bronze.side_effect = mock_read
    transformer.run(mode="optimized")

    orders_written = next(
        c.args[0] for c in transformer._write_silver.call_args_list
        if "coupon_code" in c.args[0].columns
    )
    null_count = orders_written.filter(orders_written["coupon_code"].isNull()).count()
    assert null_count == 0


def test_run_optimized_applies_dedup(transformer, spark, sample_orders, sample_order_items):
    def mock_read(table):
        if table == "orders":
            return sample_orders
        if table == "order_items":
            return sample_order_items
        return spark.createDataFrame([("x",)], ["id"])

    transformer._read_bronze.side_effect = mock_read
    transformer.run(mode="optimized")

    items_written = next(
        c.args[0] for c in transformer._write_silver.call_args_list
        if "order_item_id" in c.args[0].columns
    )
    assert items_written.count() < sample_order_items.count()


def test_run_invalid_mode_raises(transformer):
    with pytest.raises(ValueError):
        transformer.run(mode="invalid_mode")


# ── 5. _build_spark (Silver) ─────────────────────────────────────────────────

def test_silver_build_spark_returns_spark_session(transformer):
    """_build_spark is a passthrough — AQE config is applied via SparkConf, not here."""
    assert transformer._build_spark() is transformer.spark


# ── 6. _fix_broadcast_join (Fix 4 — Problem A cardinality) ───────────────────

def test_fix_broadcast_join_adds_product_columns(
    transformer, sample_order_items, sample_products
):
    result = transformer._fix_broadcast_join(sample_order_items, sample_products)
    assert "category" in result.columns
    assert "brand" in result.columns


def test_fix_broadcast_join_is_left_join_preserves_row_count(
    transformer, sample_order_items, sample_products
):
    result = transformer._fix_broadcast_join(sample_order_items, sample_products)
    assert result.count() == sample_order_items.count()


def test_fix_broadcast_join_keeps_unmatched_rows_with_null_product_cols(
    transformer, sample_order_items, sample_products
):
    """P003 isn't in sample_products — a left join must keep the row with NULLs."""
    result = transformer._fix_broadcast_join(sample_order_items, sample_products)
    unmatched = result.filter(result["product_id"] == "P003").collect()
    assert len(unmatched) == 1
    assert unmatched[0]["category"] is None


def test_fix_broadcast_join_matched_rows_get_product_attrs(
    transformer, sample_order_items, sample_products
):
    result = transformer._fix_broadcast_join(sample_order_items, sample_products)
    matched = result.filter(result["product_id"] == "P002").collect()
    assert matched[0]["category"] == "fashion"
    assert matched[0]["brand"] == "Zara"


def test_fix_broadcast_join_uses_broadcast_hash_join_physical_plan(
    transformer, sample_order_items, sample_products, capsys
):
    """Confirms the broadcast() hint actually changes the physical join strategy."""
    result = transformer._fix_broadcast_join(sample_order_items, sample_products)
    result.explain(mode="simple")
    plan = capsys.readouterr().out
    assert "BroadcastHashJoin" in plan


# ── 7. _read_bronze ───────────────────────────────────────────────────────────

def test_read_bronze_reads_delta_format_from_correct_path():
    t = object.__new__(SilverTransformer)
    t.bronze_dir = "s3a://bronze-data/bronze"
    t.spark = MagicMock()

    t._read_bronze("orders")

    t.spark.read.format.assert_called_once_with("delta")
    t.spark.read.format.return_value.load.assert_called_once_with(
        "s3a://bronze-data/bronze/orders"
    )


# ── 8. _write_silver ──────────────────────────────────────────────────────────

def test_write_silver_returns_row_count(spark, sample_orders):
    t = object.__new__(SilverTransformer)
    t.silver_dir = "s3a://silver-data/silver"
    t.writer = MagicMock()

    count = t._write_silver(sample_orders, "orders")
    assert count == sample_orders.count()


def test_write_silver_calls_writer_with_correct_path_and_defaults(spark, sample_orders):
    t = object.__new__(SilverTransformer)
    t.silver_dir = "s3a://silver-data/silver"
    t.writer = MagicMock()

    t._write_silver(sample_orders, "orders")

    args, kwargs = t.writer.write.call_args
    assert args[1] == "s3a://silver-data/silver/orders"
    assert args[2] == "orders"
    assert kwargs["mode"] == "overwrite"
    assert kwargs["row_count"] == sample_orders.count()
    assert kwargs["merge_schema"] is False


def test_write_silver_passes_through_merge_schema_true(spark, sample_orders):
    t = object.__new__(SilverTransformer)
    t.silver_dir = "s3a://silver-data/silver"
    t.writer = MagicMock()

    t._write_silver(sample_orders, "orders", merge_schema=True)

    _, kwargs = t.writer.write.call_args
    assert kwargs["merge_schema"] is True


def test_write_silver_unpersists_after_write(spark, sample_orders):
    from pyspark import StorageLevel

    t = object.__new__(SilverTransformer)
    t.silver_dir = "s3a://silver-data/silver"
    t.writer = MagicMock()

    t._write_silver(sample_orders, "orders")

    assert sample_orders.storageLevel == StorageLevel.NONE


# ── 9. __init__ ────────────────────────────────────────────────────────────────

def test_init_resolves_default_bronze_and_silver_dirs(spark):
    with patch.object(SilverTransformer, "_build_spark", return_value=spark):
        t = SilverTransformer()
    assert t.bronze_dir == "s3a://bronze-data/bronze"
    assert t.silver_dir == "s3a://silver-data/silver"


def test_init_accepts_explicit_dir_overrides(spark):
    with patch.object(SilverTransformer, "_build_spark", return_value=spark):
        t = SilverTransformer(bronze_dir="custom/bronze", silver_dir="custom/silver")
    assert t.bronze_dir == "custom/bronze"
    assert t.silver_dir == "custom/silver"


# ── 10. _run_optimized — row-count telemetry passed to log_run ───────────────

def test_run_optimized_logs_rows_in_and_out_for_orders(
    transformer, spark, sample_orders, sample_order_items
):
    def mock_read(table):
        if table == "orders":
            return sample_orders
        if table == "order_items":
            return sample_order_items
        return spark.createDataFrame([("x",)], ["id"])

    transformer._read_bronze.side_effect = mock_read
    transformer.run(mode="optimized")

    call = next(
        c for c in transformer.log_run.call_args_list if c.args[0] == "orders"
    )
    rows_in, rows_out = call.args[3], call.args[4]
    assert rows_in == sample_orders.count()
    assert rows_out == sample_orders.count()  # schema fix doesn't drop rows


def test_run_optimized_logs_row_count_drop_for_deduped_order_items(
    transformer, spark, sample_orders, sample_order_items
):
    def mock_read(table):
        if table == "orders":
            return sample_orders
        if table == "order_items":
            return sample_order_items
        return spark.createDataFrame([("x",)], ["id"])

    transformer._read_bronze.side_effect = mock_read
    transformer.run(mode="optimized")

    call = next(
        c for c in transformer.log_run.call_args_list if c.args[0] == "order_items"
    )
    rows_in, rows_out = call.args[3], call.args[4]
    assert rows_in == sample_order_items.count()
    assert rows_out < rows_in


# ── 11. main() entrypoint ─────────────────────────────────────────────────────

def test_main_default_mode_is_optimized(monkeypatch):
    captured = {}

    class FakeTransformer:
        def run(self, mode):
            captured["mode"] = mode

    monkeypatch.setattr(
        "b_schema_pipelines.pipelines.silver.transform_silver.SilverTransformer",
        FakeTransformer,
    )
    monkeypatch.setattr(sys, "argv", ["transform_silver.py"])

    main()

    assert captured["mode"] == "optimized"


def test_main_passes_custom_mode(monkeypatch):
    captured = {}

    class FakeTransformer:
        def run(self, mode):
            captured["mode"] = mode

    monkeypatch.setattr(
        "b_schema_pipelines.pipelines.silver.transform_silver.SilverTransformer",
        FakeTransformer,
    )
    monkeypatch.setattr(
        sys, "argv", ["transform_silver.py", "--mode", "baseline"],
    )

    main()

    assert captured["mode"] == "baseline"


def test_main_rejects_invalid_mode(monkeypatch):
    monkeypatch.setattr(
        sys, "argv", ["transform_silver.py", "--mode", "nonsense"]
    )
    with pytest.raises(SystemExit):
        main()
