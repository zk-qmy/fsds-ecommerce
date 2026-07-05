"""
Tests for SilverTransformer.

All pipeline methods currently raise NotImplementedError — tests are marked
xfail(strict=True). Remove the xfail marker once the method is implemented;
the test body and assertions are ready.

Transformation logic is tested on in-memory DataFrames — no Delta I/O.
Delta reads/writes are mocked.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.pipelines.silver.transform_silver import SilverTransformer


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def transformer(spark):
    """SilverTransformer with Spark injected and I/O methods mocked."""
    with patch.object(SilverTransformer, "_build_spark", return_value=spark):
        t = SilverTransformer()
    # Stub out Delta I/O so tests focus on transform logic
    t._read_bronze  = MagicMock()
    t._write_silver = MagicMock(side_effect=lambda df, *a, **kw: df.count())
    return t


# ── 1. _fix_schema_evolution (Problem B) ─────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fix_schema_evolution_fills_null_coupon_code(transformer, sample_orders):
    result = transformer._fix_schema_evolution(sample_orders)
    null_count = result.filter(result["coupon_code"].isNull()).count()
    assert null_count == 0, "coupon_code must have no NULLs after Silver fix"


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fix_schema_evolution_fills_null_shipping_method(transformer, sample_orders):
    result = transformer._fix_schema_evolution(sample_orders)
    null_count = result.filter(result["shipping_method"].isNull()).count()
    assert null_count == 0, "shipping_method must have no NULLs after Silver fix"


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
@pytest.mark.parametrize("col,expected_fill", [
    ("coupon_code",     "LEGACY"),
    ("shipping_method", "UNKNOWN"),
])
def test_fix_schema_evolution_fill_values(col, expected_fill, transformer, sample_orders):
    result = transformer._fix_schema_evolution(sample_orders)
    rows = result.collect()
    # Pre-schema-change orders (O001, O002, O003) had NULL — verify fill value
    pre_change = [r for r in rows if r["order_id"] in ("O001", "O002", "O003")]
    for row in pre_change:
        assert row[col] == expected_fill, f"Expected '{expected_fill}' for NULL {col}, got {row[col]!r}"


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fix_schema_evolution_preserves_non_null_values(transformer, sample_orders):
    result = transformer._fix_schema_evolution(sample_orders)
    rows = {r["order_id"]: r for r in result.collect()}
    # O004 had coupon_code='PROMO10' and shipping_method='express' — must not be overwritten
    assert rows["O004"]["coupon_code"]    == "PROMO10"
    assert rows["O004"]["shipping_method"] == "express"


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fix_schema_evolution_row_count_unchanged(transformer, sample_orders):
    result = transformer._fix_schema_evolution(sample_orders)
    assert result.count() == sample_orders.count()


# ── 2. _fix_duplicates (Problem C) ───────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fix_duplicates_reduces_row_count(transformer, sample_order_items):
    result = transformer._fix_duplicates(sample_order_items)
    # sample_order_items has 5 rows with 1 duplicate → expect 4 rows
    assert result.count() == 4


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fix_duplicates_keeps_earliest_created_ts(transformer, sample_order_items):
    result = transformer._fix_duplicates(sample_order_items)
    rows = {r["order_item_id"]: r for r in result.collect()}
    # OI001 exists twice — earliest created_ts is 10:00, not 10:05
    assert rows["OI001"]["created_ts"] == datetime.datetime(2026, 2, 1, 10, 0)


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fix_duplicates_no_duplicate_natural_keys(transformer, sample_order_items):
    result = transformer._fix_duplicates(sample_order_items)
    from pyspark.sql import functions as F
    dupes = (
        result
        .groupBy("order_id", "product_id", "unit_price")
        .count()
        .filter(F.col("count") > 1)
    )
    assert dupes.count() == 0, "Duplicate natural keys remain after dedup"


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
@pytest.mark.parametrize("order_id,product_id,unit_price,expected_rows", [
    ("O001", "P001", 10.0, 1),   # duplicated — deduped to 1
    ("O001", "P002", 25.0, 1),   # unique — unchanged
    ("O002", "P003", 15.0, 1),   # unique — unchanged
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

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_run_baseline_calls_read_for_all_tables(transformer):
    transformer.run(mode="baseline")
    call_args = [c.args[0] for c in transformer._read_bronze.call_args_list]
    for table in ("orders", "order_items", "products", "customers", "payments"):
        assert table in call_args, f"_read_bronze not called for {table}"


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_run_baseline_no_schema_evolution_fix(transformer, sample_orders):
    """Baseline mode must NOT apply NULL fills — raw data passes through."""
    transformer._read_bronze.side_effect = lambda t: sample_orders if t == "orders" else MagicMock()
    transformer.run(mode="baseline")

    written_df = transformer._write_silver.call_args_list[0].args[0]
    null_count = written_df.filter(written_df["coupon_code"].isNull()).count()
    assert null_count > 0, "Baseline must preserve NULLs (no transformation applied)"


# ── 4. _run_optimised ────────────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_run_optimised_applies_schema_fix(transformer, spark, sample_orders, sample_order_items):
    from pyspark.sql import functions as F

    def mock_read(table):
        if table == "orders":      return sample_orders
        if table == "order_items": return sample_order_items
        return spark.createDataFrame([("x",)], ["id"])

    transformer._read_bronze.side_effect = mock_read
    transformer.run(mode="optimised")

    orders_written = next(
        c.args[0] for c in transformer._write_silver.call_args_list
        if "coupon_code" in c.args[0].columns
    )
    null_count = orders_written.filter(orders_written["coupon_code"].isNull()).count()
    assert null_count == 0


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_run_optimised_applies_dedup(transformer, spark, sample_orders, sample_order_items):
    def mock_read(table):
        if table == "orders":      return sample_orders
        if table == "order_items": return sample_order_items
        return spark.createDataFrame([("x",)], ["id"])

    transformer._read_bronze.side_effect = mock_read
    transformer.run(mode="optimised")

    items_written = next(
        c.args[0] for c in transformer._write_silver.call_args_list
        if "order_item_id" in c.args[0].columns
    )
    assert items_written.count() < sample_order_items.count()


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_run_invalid_mode_raises(transformer):
    with pytest.raises((ValueError, NotImplementedError)):
        transformer.run(mode="invalid_mode")


# ── 5. _build_spark (Silver) ─────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_silver_build_spark_returns_session():
    with patch("b_schema_pipelines.pipelines.silver.transform_silver.configure_spark_with_delta_pip"):
        t = SilverTransformer.__new__(SilverTransformer)
        PipelineBase = t.__class__.__bases__[0]
        super_spark = MagicMock()
        with patch.object(PipelineBase, "_build_spark", return_value=super_spark):
            result = t._build_spark()
    assert result is not None
