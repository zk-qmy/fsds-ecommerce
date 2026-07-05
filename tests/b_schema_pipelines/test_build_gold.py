"""
Tests for GoldBuilder.

All methods raise NotImplementedError — marked xfail(strict=True).
JDBC writes are mocked; tests validate the DataFrame shape and logic
that will be produced by the Gold builder once implemented.
"""

from __future__ import annotations

import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch
import sys

import pytest
from pyspark.sql import functions as F

sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.pipelines.gold.build_gold import GoldBuilder


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def builder(spark):
    """GoldBuilder with Spark and all I/O methods mocked."""
    with patch.object(GoldBuilder, "_build_spark", return_value=spark):
        b = GoldBuilder()
    b._read_silver   = MagicMock()
    b._read_postgres = MagicMock(return_value=spark.createDataFrame([], schema="customer_key INT"))
    b._write_postgres = MagicMock(side_effect=lambda df, *a, **kw: df.count())
    return b


@pytest.fixture
def silver_customers(spark):
    T = datetime.datetime
    rows = [
        ("C001", T(2026, 1, 1), "gold",   "VN", True),
        ("C002", T(2026, 1, 2), "silver", "US", False),
        ("C003", T(2026, 1, 3), "bronze", "JP", True),
    ]
    return spark.createDataFrame(rows, ["customer_id", "signup_ts", "segment", "country", "marketing_opt_in"])


@pytest.fixture
def silver_products(spark):
    T = datetime.datetime
    rows = [
        ("P001", "electronics", "Samsung", 999.0, True,  T(2026, 1, 1)),
        ("P002", "fashion",     "Zara",    59.9,  True,  T(2026, 1, 1)),
        ("P003", "books",       "Penguin", 12.0,  False, T(2026, 1, 1)),
    ]
    return spark.createDataFrame(rows, ["product_id", "category", "brand", "base_price", "is_active", "created_ts"])


@pytest.fixture
def silver_orders(spark):
    T = datetime.datetime
    rows = [
        ("O001", "C001", T(2026, 2, 1), "completed",  "Ho Chi Minh City", "standard", "LEGACY"),
        ("O002", "C002", T(2026, 4, 5), "completed",  "Hanoi",             "express",  "PROMO10"),
        ("O003", "C003", T(2026, 4, 10), "cancelled", "Da Nang",           "same_day", "UNKNOWN"),
    ]
    return spark.createDataFrame(
        rows,
        ["order_id", "customer_id", "order_timestamp", "status",
         "shipping_city", "shipping_method", "coupon_code"],
    )


@pytest.fixture
def silver_order_items(spark):
    rows = [
        ("OI001", "O001", "P001", 2, 999.0, 10.0),
        ("OI002", "O001", "P002", 1, 59.9,   0.0),
        ("OI003", "O002", "P002", 3, 59.9,   5.0),
        ("OI004", "O003", "P003", 1, 12.0,   0.0),
    ]
    return spark.createDataFrame(
        rows,
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount_amount"],
    )


@pytest.fixture
def silver_payments(spark):
    T = datetime.datetime
    rows = [
        ("PAY001", "O001", T(2026, 2, 1), "credit_card", 989.0, "paid"),
        ("PAY002", "O002", T(2026, 4, 5), "e_wallet",    174.7, "paid"),
        ("PAY003", "O003", T(2026, 4, 10), "cod",        12.0,  "failed"),
    ]
    return spark.createDataFrame(
        rows,
        ["payment_id", "order_id", "payment_timestamp", "payment_method", "amount", "payment_status"],
    )


# ── 1. dim_date ───────────────────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_date_has_180_rows(builder):
    builder._build_dim_date()
    written_df = builder._write_postgres.call_args.args[0]
    assert written_df.count() == 180


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_date_required_columns(builder):
    builder._build_dim_date()
    df = builder._write_postgres.call_args.args[0]
    expected_cols = {"date_key", "calendar_date", "day_of_week", "month", "year", "is_weekend"}
    assert expected_cols.issubset(set(df.columns))


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_date_key_is_yyyymmdd_format(builder):
    builder._build_dim_date()
    df = builder._write_postgres.call_args.args[0]
    sample = df.orderBy("date_key").first()
    # date_key should be integer with 8 digits (YYYYMMDD)
    assert len(str(sample["date_key"])) == 8


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_date_weekend_flag(builder):
    builder._build_dim_date()
    df = builder._write_postgres.call_args.args[0]
    # Verify is_weekend is boolean-like (0/1 or True/False)
    weekend_rows = df.filter(F.col("is_weekend") == True).count()
    assert 0 < weekend_rows < 180, "Expected some but not all dates to be weekends"


# ── 2. dim_payment_method ────────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_payment_method_has_4_rows(builder):
    builder._build_dim_payment_method()
    df = builder._write_postgres.call_args.args[0]
    assert df.count() == 4


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
@pytest.mark.parametrize("method", ["credit_card", "bank_transfer", "e_wallet", "cod"])
def test_dim_payment_method_values(method, builder):
    builder._build_dim_payment_method()
    df = builder._write_postgres.call_args.args[0]
    assert df.filter(F.col("payment_method") == method).count() == 1


# ── 3. dim_order_status ───────────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_order_status_has_4_rows(builder):
    builder._build_dim_order_status()
    df = builder._write_postgres.call_args.args[0]
    assert df.count() == 4


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
@pytest.mark.parametrize("status", ["completed", "pending", "cancelled", "returned"])
def test_dim_order_status_values(status, builder):
    builder._build_dim_order_status()
    df = builder._write_postgres.call_args.args[0]
    assert df.filter(F.col("order_status") == status).count() == 1


# ── 4. dim_product ────────────────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_product_row_count_matches_silver(builder, silver_products):
    builder._read_silver.return_value = silver_products
    builder._build_dim_product()
    df = builder._write_postgres.call_args.args[0]
    assert df.count() == silver_products.count()


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_product_has_surrogate_key(builder, silver_products):
    builder._read_silver.return_value = silver_products
    builder._build_dim_product()
    df = builder._write_postgres.call_args.args[0]
    assert "product_key" in df.columns


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_product_surrogate_key_unique(builder, silver_products):
    builder._read_silver.return_value = silver_products
    builder._build_dim_product()
    df = builder._write_postgres.call_args.args[0]
    total = df.count()
    distinct = df.select("product_key").distinct().count()
    assert total == distinct, "product_key values must be unique"


# ── 5. dim_customer (SCD Type 2) ─────────────────────────────────────────────

@pytest.fixture
def existing_dim_customer(spark):
    """Existing dim_customer state: C001 has segment='bronze' (will change to 'gold')."""
    T = datetime.datetime
    rows = [
        (1, "C001", T(2026, 1, 1), "bronze", "VN", True,
         T(2026, 1, 1), datetime.datetime(9999, 12, 31), True),
    ]
    return spark.createDataFrame(
        rows,
        ["customer_key", "customer_id", "signup_ts", "segment", "country",
         "marketing_opt_in", "valid_from_ts", "valid_to_ts", "is_current"],
    )


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_customer_scd2_changed_row_adds_new_record(builder, silver_customers, existing_dim_customer):
    """When a customer's segment changes, a new is_current=True row is inserted."""
    builder._read_silver.return_value = silver_customers
    builder._read_postgres.return_value = existing_dim_customer
    builder._build_dim_customer()

    written_df = builder._write_postgres.call_args.args[0]
    c001_rows = written_df.filter(F.col("customer_id") == "C001").collect()
    assert len(c001_rows) == 2, "SCD2: expect 2 rows for changed customer"


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_customer_scd2_old_record_closed(builder, silver_customers, existing_dim_customer):
    """Old record for changed customer must have is_current=False and valid_to_ts != 9999."""
    builder._read_silver.return_value = silver_customers
    builder._read_postgres.return_value = existing_dim_customer
    builder._build_dim_customer()

    written_df = builder._write_postgres.call_args.args[0]
    closed = (
        written_df
        .filter((F.col("customer_id") == "C001") & (F.col("is_current") == False))
        .collect()
    )
    assert len(closed) == 1
    assert closed[0]["valid_to_ts"].year != 9999


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_customer_scd2_unchanged_customer_stays_current(builder, silver_customers, existing_dim_customer):
    """Customers not in existing dim (C002, C003) are new — inserted as is_current=True."""
    builder._read_silver.return_value = silver_customers
    builder._read_postgres.return_value = existing_dim_customer
    builder._build_dim_customer()

    written_df = builder._write_postgres.call_args.args[0]
    for cid in ("C002", "C003"):
        current = written_df.filter(
            (F.col("customer_id") == cid) & (F.col("is_current") == True)
        ).count()
        assert current == 1, f"New customer {cid} must have exactly one is_current=True row"


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_dim_customer_scd2_valid_to_ts_sentinel_for_current(builder, silver_customers, existing_dim_customer):
    """Current rows (is_current=True) must have valid_to_ts = 9999-12-31."""
    builder._read_silver.return_value = silver_customers
    builder._read_postgres.return_value = existing_dim_customer
    builder._build_dim_customer()

    written_df = builder._write_postgres.call_args.args[0]
    bad = (
        written_df
        .filter(F.col("is_current") == True)
        .filter(F.year("valid_to_ts") != 9999)
        .count()
    )
    assert bad == 0, "All is_current=True rows must have valid_to_ts=9999-12-31"


# ── 6. fact_order ─────────────────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fact_order_row_count_equals_orders(builder, silver_orders, silver_order_items):
    builder._read_silver.side_effect = lambda t: (
        silver_orders if t == "orders" else silver_order_items
    )
    builder._build_fact_order()
    df = builder._write_postgres.call_args.args[0]
    assert df.count() == silver_orders.count()


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fact_order_net_amount_equals_gross_minus_discount(builder, silver_orders, silver_order_items):
    builder._read_silver.side_effect = lambda t: (
        silver_orders if t == "orders" else silver_order_items
    )
    builder._build_fact_order()
    df = builder._write_postgres.call_args.args[0]

    # Verify: net = gross - discount for every order
    bad = df.filter(
        F.round(F.col("order_net_amount"), 2) !=
        F.round(F.col("order_gross_amount") - F.col("order_discount_amount"), 2)
    ).count()
    assert bad == 0, "order_net_amount must equal order_gross_amount - order_discount_amount"


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fact_order_required_columns(builder, silver_orders, silver_order_items):
    builder._read_silver.side_effect = lambda t: (
        silver_orders if t == "orders" else silver_order_items
    )
    builder._build_fact_order()
    df = builder._write_postgres.call_args.args[0]
    expected = {
        "order_key", "customer_key", "order_date_key", "order_status_key",
        "order_id", "order_gross_amount", "order_discount_amount", "order_net_amount", "item_count",
    }
    assert expected.issubset(set(df.columns))


# ── 7. fact_order_item ────────────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fact_order_item_line_net_amount(builder, silver_order_items, silver_orders):
    builder._read_silver.side_effect = lambda t: (
        silver_order_items if t == "order_items" else silver_orders
    )
    builder._build_fact_order_item()
    df = builder._write_postgres.call_args.args[0]

    # line_net_amount = (unit_price * quantity) - discount_amount
    bad = df.filter(
        F.round(F.col("line_net_amount"), 2) !=
        F.round(F.col("unit_price") * F.col("quantity") - F.col("discount_amount"), 2)
    ).count()
    assert bad == 0, "line_net_amount must equal (unit_price * quantity) - discount_amount"


# ── 8. fact_payment_attempt ───────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
@pytest.mark.parametrize("payment_status,success,failed", [
    ("paid",    True,  False),
    ("failed",  False, True),
    ("refunded",False, False),
])
def test_fact_payment_boolean_flags(payment_status, success, failed, builder, spark):
    from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

    schema = StructType([
        StructField("payment_id",        StringType(),    False),
        StructField("order_id",          StringType(),    False),
        StructField("payment_timestamp", TimestampType(), False),
        StructField("payment_method",    StringType(),    False),
        StructField("amount",            DoubleType(),    False),
        StructField("payment_status",    StringType(),    False),
    ])
    T = datetime.datetime
    payments = spark.createDataFrame(
        [("PAY999", "O001", T(2026, 4, 1), "credit_card", 100.0, payment_status)],
        schema=schema,
    )
    builder._read_silver.return_value = payments
    builder._build_fact_payment()
    df = builder._write_postgres.call_args.args[0]
    row = df.collect()[0]
    assert row["is_payment_success"] == success
    assert row["is_payment_failed"]  == failed


# ── 9. obt_order_performance ─────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_obt_required_columns(builder, silver_orders, silver_order_items, silver_customers, silver_payments):
    def mock_read(table):
        return {
            "orders":       silver_orders,
            "order_items":  silver_order_items,
            "customers":    silver_customers,
            "payments":     silver_payments,
        }.get(table, MagicMock())

    builder._read_silver.side_effect = mock_read
    builder._build_obt_order_performance()
    df = builder._write_postgres.call_args.args[0]
    expected = {
        "order_id", "customer_id", "order_timestamp", "country", "segment",
        "total_quantity", "order_net_amount", "payment_status_last",
        "shipping_city", "coupon_code",
    }
    assert expected.issubset(set(df.columns))


@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_obt_no_surrogate_keys(builder, silver_orders, silver_order_items, silver_customers, silver_payments):
    """OBT uses business keys only — no _key columns."""
    def mock_read(table):
        return {
            "orders":      silver_orders,
            "order_items": silver_order_items,
            "customers":   silver_customers,
            "payments":    silver_payments,
        }.get(table, MagicMock())

    builder._read_silver.side_effect = mock_read
    builder._build_obt_order_performance()
    df = builder._write_postgres.call_args.args[0]
    surrogate_cols = [c for c in df.columns if c.endswith("_key")]
    assert surrogate_cols == [], f"OBT must not contain surrogate keys: {surrogate_cols}"


# ── 10. Referential integrity ────────────────────────────────────────────────

@pytest.mark.xfail(strict=True, raises=NotImplementedError, reason="Not implemented yet")
def test_fact_order_all_customer_keys_exist_in_dim(builder, spark, silver_orders, silver_customers):
    """Every customer_key in fact_order must have a matching row in dim_customer."""
    builder._read_silver.side_effect = lambda t: (
        silver_orders if t == "orders" else silver_customers
    )
    builder._build_dim_customer()
    dim_keys = {r["customer_key"] for r in builder._write_postgres.call_args.args[0].collect()}

    builder._build_fact_order()
    fact_keys = {r["customer_key"] for r in builder._write_postgres.call_args.args[0].collect()}

    assert fact_keys.issubset(dim_keys), "Orphaned customer_key values in fact_order"
