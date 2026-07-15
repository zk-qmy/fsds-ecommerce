"""
Tests for gold_expectation_suite.

No Spark/DB needed — fk_checks/unique_column/baseline_row_count are passed
in as plain values, not computed here.
"""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.dq.gold_suite import gold_expectation_suite

DIM_PRODUCT_COLUMNS = ["product_key", "product_id", "category", "brand", "base_price", "is_active", "created_ts"]
FACT_ORDER_COLUMNS = [
    "order_key", "customer_key", "order_date_key", "order_status_key", "order_id",
    "order_gross_amount", "order_discount_amount", "order_net_amount", "item_count",
]


def _types(suite):
    return [e.expectation_type for e in suite.expectations]


def test_returns_suite_named_after_the_table():
    suite = gold_expectation_suite("dim_product", DIM_PRODUCT_COLUMNS, ["product_key"])
    assert suite.name == "gold_dim_product"


def test_schema_check_is_exact_match_in_gold():
    """Unlike Bronze/Silver, Gold tables don't carry extra lineage columns —
    the expected column set should match exactly."""
    suite = gold_expectation_suite("dim_product", DIM_PRODUCT_COLUMNS, ["product_key"])
    schema_check = suite.expectations[0]
    assert schema_check.expectation_type == "expect_table_columns_to_match_set"
    assert schema_check.column_set == DIM_PRODUCT_COLUMNS
    assert schema_check.exact_match is True


def test_minimal_call_gets_schema_and_null_pk_checks_only():
    suite = gold_expectation_suite("dim_product", DIM_PRODUCT_COLUMNS, ["product_key"])
    assert _types(suite) == [
        "expect_table_columns_to_match_set",
        "expect_column_values_to_not_be_null",
    ]


def test_null_check_added_once_per_pk_column():
    suite = gold_expectation_suite(
        "fact_order_item",
        ["order_item_key", "order_key", "product_key"],
        ["order_item_key", "order_key", "product_key"],
    )
    null_checks = [e for e in suite.expectations if e.expectation_type == "expect_column_values_to_not_be_null"]
    assert {e.column for e in null_checks} == {"order_item_key", "order_key", "product_key"}


def test_unique_column_adds_uniqueness_check():
    suite = gold_expectation_suite(
        "dim_product", DIM_PRODUCT_COLUMNS, ["product_key"], unique_column="product_id"
    )
    unique_check = next(e for e in suite.expectations if e.expectation_type == "expect_column_values_to_be_unique")
    assert unique_check.column == "product_id"


def test_unique_column_omitted_by_default():
    suite = gold_expectation_suite("dim_product", DIM_PRODUCT_COLUMNS, ["product_key"])
    assert "expect_column_values_to_be_unique" not in _types(suite)


def test_fk_checks_add_one_in_set_expectation_per_foreign_key():
    suite = gold_expectation_suite(
        "fact_order",
        FACT_ORDER_COLUMNS,
        ["order_key"],
        fk_checks={
            "customer_key": [1, 2, 3],
            "order_date_key": [20260101, 20260102],
        },
    )
    fk_checks = {
        e.column: e.value_set for e in suite.expectations if e.expectation_type == "expect_column_values_to_be_in_set"
    }
    assert fk_checks == {
        "customer_key": [1, 2, 3],
        "order_date_key": [20260101, 20260102],
    }


def test_fk_checks_omitted_by_default():
    suite = gold_expectation_suite("fact_order", FACT_ORDER_COLUMNS, ["order_key"])
    assert "expect_column_values_to_be_in_set" not in _types(suite)


def test_volume_check_uses_thirty_percent_band_around_baseline():
    suite = gold_expectation_suite(
        "fact_order", FACT_ORDER_COLUMNS, ["order_key"], baseline_row_count=360_000
    )
    volume = next(e for e in suite.expectations if e.expectation_type == "expect_table_row_count_to_be_between")
    assert volume.min_value == 252_000
    assert volume.max_value == 468_000


def test_volume_check_omitted_when_no_baseline_given():
    suite = gold_expectation_suite("fact_order", FACT_ORDER_COLUMNS, ["order_key"])
    assert "expect_table_row_count_to_be_between" not in _types(suite)


def test_all_checks_can_be_combined_for_a_scd2_dim():
    """dim_customer: null-PK on both keys, uniqueness on the business key
    (caller validates against the is_current-filtered batch), volume check."""
    columns = [
        "customer_key", "customer_id", "signup_ts", "segment", "country",
        "marketing_opt_in", "valid_from_ts", "valid_to_ts", "is_current",
    ]
    suite = gold_expectation_suite(
        "dim_customer",
        columns,
        ["customer_key", "customer_id"],
        unique_column="customer_id",
        baseline_row_count=120_000,
    )
    assert _types(suite) == [
        "expect_table_columns_to_match_set",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_not_be_null",
        "expect_column_values_to_be_unique",
        "expect_table_row_count_to_be_between",
    ]
