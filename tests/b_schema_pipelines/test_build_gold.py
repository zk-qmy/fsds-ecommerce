"""
Tests for GoldBuilder.

JDBC writes are mocked; tests validate the DataFrame shape and logic
produced by the Gold builder.
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
    """Column is `discount`, not `discount_amount` — matches the real Silver
    schema (b_schema_pipelines/pipelines/bronze/bronze_config.yaml). Gold
    renames it to `discount_amount` internally via `_read_order_items`."""
    rows = [
        ("OI001", "O001", "P001", 2, 999.0, 10.0),
        ("OI002", "O001", "P002", 1, 59.9,   0.0),
        ("OI003", "O002", "P002", 3, 59.9,   5.0),
        ("OI004", "O003", "P003", 1, 12.0,   0.0),
    ]
    return spark.createDataFrame(
        rows,
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount"],
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

def test_dim_date_has_180_rows(builder):
    builder._build_dim_date()
    written_df = builder._write_postgres.call_args.args[0]
    assert written_df.count() == 180


def test_dim_date_required_columns(builder):
    builder._build_dim_date()
    df = builder._write_postgres.call_args.args[0]
    expected_cols = {"date_key", "calendar_date", "day_of_week", "month", "year", "is_weekend"}
    assert expected_cols.issubset(set(df.columns))


def test_dim_date_key_is_yyyymmdd_format(builder):
    builder._build_dim_date()
    df = builder._write_postgres.call_args.args[0]
    sample = df.orderBy("date_key").first()
    # date_key should be integer with 8 digits (YYYYMMDD)
    assert len(str(sample["date_key"])) == 8


def test_dim_date_weekend_flag(builder):
    builder._build_dim_date()
    df = builder._write_postgres.call_args.args[0]
    # Verify is_weekend is boolean-like (0/1 or True/False)
    weekend_rows = df.filter(F.col("is_weekend")).count()
    assert 0 < weekend_rows < 180, "Expected some but not all dates to be weekends"


# ── 2. dim_payment_method ────────────────────────────────────────────────────

def test_dim_payment_method_matches_expected_set(builder):
    builder._build_dim_payment_method()
    df = builder._write_postgres.call_args.args[0]
    assert df.count() == 4
    values = {r["payment_method"] for r in df.collect()}
    assert values == {"credit_card", "bank_transfer", "e_wallet", "cod"}


# ── 3. dim_order_status ───────────────────────────────────────────────────────

def test_dim_order_status_matches_expected_set(builder):
    builder._build_dim_order_status()
    df = builder._write_postgres.call_args.args[0]
    assert df.count() == 4
    values = {r["order_status"] for r in df.collect()}
    assert values == {"completed", "pending", "cancelled", "returned"}


# ── 4. dim_product ────────────────────────────────────────────────────────────

def test_dim_product_row_count_matches_silver(builder, silver_products):
    builder._read_silver.return_value = silver_products
    builder._build_dim_product()
    df = builder._write_postgres.call_args.args[0]
    assert df.count() == silver_products.count()


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


def test_dim_customer_scd2_changed_row_adds_new_record(builder, silver_customers, existing_dim_customer):
    """When a customer's segment changes, a new is_current=True row is inserted."""
    builder._read_silver.return_value = silver_customers
    builder._read_postgres.return_value = existing_dim_customer
    builder._build_dim_customer()

    written_df = builder._write_postgres.call_args.args[0]
    c001_rows = written_df.filter(F.col("customer_id") == "C001").collect()
    assert len(c001_rows) == 2, "SCD2: expect 2 rows for changed customer"


def test_dim_customer_scd2_old_record_closed(builder, silver_customers, existing_dim_customer):
    """Old record for changed customer must have is_current=False and valid_to_ts != 9999."""
    builder._read_silver.return_value = silver_customers
    builder._read_postgres.return_value = existing_dim_customer
    builder._build_dim_customer()

    written_df = builder._write_postgres.call_args.args[0]
    closed = (
        written_df
        .filter((F.col("customer_id") == "C001") & ~F.col("is_current"))
        .collect()
    )
    assert len(closed) == 1
    assert closed[0]["valid_to_ts"].year != 9999


def test_dim_customer_scd2_unchanged_customer_stays_current(builder, silver_customers, existing_dim_customer):
    """Customers not in existing dim (C002, C003) are new — inserted as is_current=True."""
    builder._read_silver.return_value = silver_customers
    builder._read_postgres.return_value = existing_dim_customer
    builder._build_dim_customer()

    written_df = builder._write_postgres.call_args.args[0]
    for cid in ("C002", "C003"):
        current = written_df.filter(
            (F.col("customer_id") == cid) & (F.col("is_current"))
        ).count()
        assert current == 1, f"New customer {cid} must have exactly one is_current=True row"


def test_dim_customer_scd2_valid_to_ts_sentinel_for_current(builder, silver_customers, existing_dim_customer):
    """Current rows (is_current=True) must have valid_to_ts = 9999-12-31."""
    builder._read_silver.return_value = silver_customers
    builder._read_postgres.return_value = existing_dim_customer
    builder._build_dim_customer()

    written_df = builder._write_postgres.call_args.args[0]
    bad = (
        written_df
        .filter(F.col("is_current"))
        .filter(F.year("valid_to_ts") != 9999)
        .count()
    )
    assert bad == 0, "All is_current=True rows must have valid_to_ts=9999-12-31"


# ── 6. fact_order ─────────────────────────────────────────────────────────────

def test_fact_order_row_count_equals_orders(builder, silver_orders, silver_order_items):
    builder._read_silver.side_effect = lambda t: (
        silver_orders if t == "orders" else silver_order_items
    )
    builder._build_fact_order()
    df = builder._write_postgres.call_args.args[0]
    assert df.count() == silver_orders.count()


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

def test_fact_order_item_line_net_amount(builder, silver_order_items, silver_orders, silver_products):
    builder._read_silver.side_effect = lambda t: {
        "order_items": silver_order_items,
        "orders": silver_orders,
        "products": silver_products,
    }[t]
    builder._build_fact_order_item()
    df = builder._write_postgres.call_args.args[0]

    # line_net_amount = (unit_price * quantity) - discount_amount
    bad = df.filter(
        F.round(F.col("line_net_amount"), 2) !=
        F.round(F.col("unit_price") * F.col("quantity") - F.col("discount_amount"), 2)
    ).count()
    assert bad == 0, "line_net_amount must equal (unit_price * quantity) - discount_amount"


# ── 8. fact_payment_attempt ───────────────────────────────────────────────────

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

def test_fact_order_all_customer_keys_exist_in_dim(
    builder, spark, silver_orders, silver_order_items, silver_customers
):
    """Every customer_key in fact_order must have a matching row in dim_customer."""
    def mock_read(table):
        return {
            "orders": silver_orders,
            "order_items": silver_order_items,
            "customers": silver_customers,
        }.get(table, MagicMock())

    builder._read_silver.side_effect = mock_read
    builder._build_dim_customer()
    dim_keys = {r["customer_key"] for r in builder._write_postgres.call_args.args[0].collect()}

    builder._build_fact_order()
    fact_keys = {r["customer_key"] for r in builder._write_postgres.call_args.args[0].collect()}

    assert fact_keys.issubset(dim_keys), "Orphaned customer_key values in fact_order"


# ══════════════════════════════════════════════════════════════════════════════
# High-impact MLOps regression tests
#
# These target production-risk classes the row/column-shape tests above don't
# cover: idempotency, cross-table key consistency, NULL-safety in SCD2 change
# detection, financial-aggregate correctness, and failure handling in run().
# Several are written to the *required* behavior (per CLAUDE.md's idempotency
# and point-in-time correctness rules) rather than to whatever the code
# currently does — where the two diverge, the test fails and the docstring
# says so.
# ══════════════════════════════════════════════════════════════════════════════

# ── 11. dim_customer SCD2 idempotency ────────────────────────────────────────

def test_dim_customer_scd2_unchanged_data_writes_zero_rows(builder, silver_customers):
    """Re-running Gold with unchanged Silver data must not churn dim_customer.

    CLAUDE.md: 'Re-running a job must not produce duplicate rows.' If this
    ever writes >0 rows for identical input, every scheduled re-run grows
    dim_customer forever with no actual history to show for it.
    """
    T = datetime.datetime
    existing = builder.spark.createDataFrame(
        [
            (1, "C001", T(2026, 1, 1), "gold",   "VN", True,
             T(2026, 1, 1), datetime.datetime(9999, 12, 31), True),
            (2, "C002", T(2026, 1, 2), "silver", "US", False,
             T(2026, 1, 2), datetime.datetime(9999, 12, 31), True),
            (3, "C003", T(2026, 1, 3), "bronze", "JP", True,
             T(2026, 1, 3), datetime.datetime(9999, 12, 31), True),
        ],
        ["customer_key", "customer_id", "signup_ts", "segment", "country",
         "marketing_opt_in", "valid_from_ts", "valid_to_ts", "is_current"],
    )
    builder._read_silver.return_value = silver_customers
    builder._read_postgres.return_value = existing
    builder._build_dim_customer()

    written_df = builder._write_postgres.call_args.args[0]
    assert written_df.count() == 0, "No customer changed — dim_customer must write zero rows"


# ── 12. dim_customer SCD2 NULL-safety ────────────────────────────────────────

def test_dim_customer_scd2_detects_change_from_null_segment(builder, silver_customers):
    """A customer's segment going from NULL (unknown) to a real value must be
    detected as a change.

    Spark's `!=`/`==` return NULL (not True) when either side is NULL, and
    `.filter()` drops NULL rows — so `changed_cond` built with plain `!=`
    silently ignores any field that starts out NULL. A customer stuck with
    segment=NULL in Gold would never get corrected even after Silver has a
    real value, because the SCD2 diff can't see the change.
    """
    T = datetime.datetime
    existing = builder.spark.createDataFrame(
        [
            (1, "C001", T(2026, 1, 1), None,     "VN", True,
             T(2026, 1, 1), datetime.datetime(9999, 12, 31), True),
            (2, "C002", T(2026, 1, 2), "silver", "US", False,
             T(2026, 1, 2), datetime.datetime(9999, 12, 31), True),
            (3, "C003", T(2026, 1, 3), "bronze", "JP", True,
             T(2026, 1, 3), datetime.datetime(9999, 12, 31), True),
        ],
        ["customer_key", "customer_id", "signup_ts", "segment", "country",
         "marketing_opt_in", "valid_from_ts", "valid_to_ts", "is_current"],
    )
    builder._read_silver.return_value = silver_customers  # C001.segment == "gold"
    builder._read_postgres.return_value = existing
    builder._build_dim_customer()

    written_df = builder._write_postgres.call_args.args[0]
    c001_rows = written_df.filter(F.col("customer_id") == "C001").collect()
    assert len(c001_rows) == 2, (
        "NULL -> 'gold' must be treated as a change (expected 1 closed + 1 new row); "
        "got a plain != comparison that likely swallowed the NULL-vs-value diff"
    )


# ── 13. Cross-table surrogate key consistency ────────────────────────────────

@pytest.fixture
def orders_with_gap(spark):
    """O003 has no order_items yet (e.g. a pending order) — a routine case,
    not a data-quality defect."""
    T = datetime.datetime
    rows = [
        ("O001", "C001", T(2026, 2, 1),  "completed", "Ho Chi Minh City", "standard", "LEGACY"),
        ("O002", "C002", T(2026, 4, 5),  "completed", "Hanoi",            "express",  "PROMO10"),
        ("O003", "C003", T(2026, 4, 10), "pending",   "Da Nang",          "same_day", "UNKNOWN"),
        ("O004", "C004", T(2026, 4, 12), "completed", "Hanoi",            "standard", "UNKNOWN"),
    ]
    return spark.createDataFrame(
        rows,
        ["order_id", "customer_id", "order_timestamp", "status",
         "shipping_city", "shipping_method", "coupon_code"],
    )


@pytest.fixture
def order_items_missing_one_order(spark):
    rows = [
        ("OI001", "O001", "P001", 2, 999.0, 10.0),
        ("OI002", "O002", "P002", 1, 59.9,   0.0),
        ("OI003", "O004", "P003", 1, 12.0,   0.0),
        # O003 intentionally absent
    ]
    return spark.createDataFrame(
        rows,
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount"],
    )


def test_order_key_matches_between_fact_order_and_fact_order_item(
    builder, orders_with_gap, order_items_missing_one_order, silver_products
):
    """order_key for a given order_id must be identical in fact_order and
    fact_order_item.

    Both facts must derive order_key from the *same* canonical mapping
    (`GoldBuilder._order_key_map`, ranked once over the full orders table
    and cached) rather than each independently ranking over its own
    distinct order_id set. O003 here has no items yet — with independent
    ranking, that gap used to shift every order_key ranked after it in one
    fact but not the other, silently breaking joins between facts. This
    test pins the fix: the same cached mapping object must be reused.
    """
    builder._read_silver.side_effect = lambda t: {
        "orders": orders_with_gap,
        "order_items": order_items_missing_one_order,
        "products": silver_products,
    }[t]

    builder._build_fact_order()
    fact_order_map = {
        r["order_id"]: r["order_key"]
        for r in builder._write_postgres.call_args.args[0].select("order_id", "order_key").collect()
    }
    order_key_map_v1 = builder._order_key_map()
    assert {r["order_id"]: r["order_key"] for r in order_key_map_v1.collect()} == fact_order_map, (
        "fact_order's order_key must come straight from the canonical order_key_map"
    )

    builder._build_fact_order_item()
    order_key_map_v2 = builder._order_key_map()
    assert order_key_map_v2 is order_key_map_v1, (
        "fact_order_item must reuse the cached order_key_map from fact_order, "
        "not recompute a new one from order_items' own (smaller) order_id set"
    )


# ── 14. fact_order NULL-vs-zero handling ─────────────────────────────────────

def test_fact_order_with_no_items_defaults_to_zero_not_null(
    builder, orders_with_gap, order_items_missing_one_order
):
    """An order with zero matching order_items (O003) must get 0 for
    gross/discount/net/item_count, not NULL — a NULL here would silently
    poison any downstream SUM/AVG feature (e.g. f_customer_avg_order_value_90d)."""
    builder._read_silver.side_effect = lambda t: (
        orders_with_gap if t == "orders" else order_items_missing_one_order
    )
    builder._build_fact_order()
    df = builder._write_postgres.call_args.args[0]
    row = df.filter(F.col("order_id") == "O003").collect()[0]
    assert row["order_gross_amount"] == 0.0
    assert row["order_discount_amount"] == 0.0
    assert row["order_net_amount"] == 0.0
    assert row["item_count"] == 0


# ── 15. Financial-aggregate regression guard ─────────────────────────────────

@pytest.fixture
def order_items_with_leaked_duplicate(spark):
    """Simulates a duplicate slipping past Silver's Problem-C dedup guarantee."""
    rows = [
        ("OI001", "O001", "P001", 2, 999.0, 10.0),
        ("OI001", "O001", "P001", 2, 999.0, 10.0),  # exact duplicate
        ("OI002", "O001", "P002", 1, 59.9,   0.0),
    ]
    return spark.createDataFrame(
        rows,
        ["order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount"],
    )


def test_fact_order_has_no_defense_against_duplicate_items_from_silver(
    builder, silver_orders, order_items_with_leaked_duplicate
):
    """Gold applies no dedup of its own on order_items — it fully trusts
    Silver's Problem-C fix. This pins that (risky) contract: a leaked
    duplicate line item silently doubles order_gross_amount with no error.
    A future change to this number is a signal that either Gold gained
    defense-in-depth dedup, or Silver's dedup guarantee regressed — either
    way, worth an explicit look before accepting the diff.
    """
    builder._read_silver.side_effect = lambda t: (
        silver_orders if t == "orders" else order_items_with_leaked_duplicate
    )
    builder._build_fact_order()
    df = builder._write_postgres.call_args.args[0]
    row = df.filter(F.col("order_id") == "O001").collect()[0]
    assert row["order_gross_amount"] == pytest.approx(4055.9), (
        "order_gross_amount drifted from the known duplicate-inflated value — "
        "re-verify whether Gold or Silver's dedup behavior changed"
    )


# ── 16. obt_order_performance payment-retry correctness ─────────────────────

@pytest.fixture
def silver_payments_with_retry(spark):
    """O001 was declined once, then succeeded on retry a few minutes later."""
    T = datetime.datetime
    rows = [
        ("PAY001a", "O001", T(2026, 2, 1, 10, 0), "credit_card", 989.0, "failed"),
        ("PAY001b", "O001", T(2026, 2, 1, 10, 5), "credit_card", 989.0, "paid"),
        ("PAY002",  "O002", T(2026, 4, 5),        "e_wallet",    174.7, "paid"),
        ("PAY003",  "O003", T(2026, 4, 10),       "cod",         12.0,  "failed"),
    ]
    return spark.createDataFrame(
        rows,
        ["payment_id", "order_id", "payment_timestamp", "payment_method", "amount", "payment_status"],
    )


def test_obt_reflects_latest_payment_attempt_not_first(
    builder, silver_orders, silver_order_items, silver_customers, silver_payments_with_retry
):
    """A retried payment must resolve to the most recent attempt (paid), not
    whichever attempt happens to be first — this feeds obt_order_performance
    for BI and is exactly the kind of retry pattern real checkout flows hit."""
    def mock_read(table):
        return {
            "orders": silver_orders,
            "order_items": silver_order_items,
            "customers": silver_customers,
            "payments": silver_payments_with_retry,
        }[table]

    builder._read_silver.side_effect = mock_read
    builder._build_obt_order_performance()
    df = builder._write_postgres.call_args.args[0]
    row = df.filter(F.col("order_id") == "O001").collect()[0]
    assert row["payment_status_last"] == "paid"


# ── 17. Idempotent write-mode guardrail ──────────────────────────────────────

def test_write_mode_is_overwrite_everywhere_except_scd2_dim_customer(
    builder, silver_customers, silver_products, silver_orders, silver_order_items, silver_payments
):
    """Every table except dim_customer must write with mode='overwrite'.
    dim_customer alone uses 'append' because it's SCD2 history by design.
    If any fact/dim/OBT build ever switches to 'append', re-running the
    pipeline duplicates every row on every run — a direct violation of
    CLAUDE.md's idempotency requirement.
    """
    def mock_read(table):
        return {
            "customers": silver_customers,
            "products": silver_products,
            "orders": silver_orders,
            "order_items": silver_order_items,
            "payments": silver_payments,
        }[table]

    builder._read_silver.side_effect = mock_read

    for build_fn in (
        builder._build_dim_date,
        builder._build_dim_payment_method,
        builder._build_dim_order_status,
        builder._build_dim_product,
        builder._build_dim_customer,
        builder._build_fact_order,
        builder._build_fact_order_item,
        builder._build_fact_payment,
        builder._build_obt_order_performance,
    ):
        build_fn()

    modes_by_table = {
        call.args[1]: call.kwargs.get("mode") for call in builder._write_postgres.call_args_list
    }
    assert modes_by_table.pop("dim_customer") == "append"
    for table, mode in modes_by_table.items():
        assert mode == "overwrite", f"{table} must write with mode='overwrite', got {mode!r}"


# ── 18/19. run() failure handling ────────────────────────────────────────────

def test_run_stops_spark_session_even_when_a_build_step_fails(builder):
    """A failed table build must still stop the Spark session. Airflow
    retries this task on failure (per CLAUDE.md's 3-retry policy) — a
    leaked session on every failed attempt compounds until the executor
    runs out of memory."""
    builder.spark = MagicMock()
    builder._build_dim_date = MagicMock(side_effect=RuntimeError("boom"))

    with pytest.raises(RuntimeError):
        builder.run()

    assert builder.spark.stop.called, "Spark session must be stopped even when a build step raises"


def test_run_reraises_the_original_exception_after_logging(builder):
    """A broken Gold table must fail the Airflow task loudly, not disappear
    into a log line — swallowing the exception would mark dp2_gold_dag green
    while Gold is actually stale or incomplete."""
    builder.spark = MagicMock()
    builder._build_dim_date = MagicMock(side_effect=ValueError("schema drift"))

    with pytest.raises(ValueError, match="schema drift"):
        builder.run()
