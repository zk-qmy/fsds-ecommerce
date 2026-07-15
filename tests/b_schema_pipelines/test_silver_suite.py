"""
Tests for silver_expectation_suite.

No Spark/DB needed — the factory only builds an in-memory ExpectationSuite;
row-count bounds are passed in as plain ints, not computed here.
"""

from __future__ import annotations

from pathlib import Path
import sys

import pytest

sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.dq.silver_suite import silver_expectation_suite

ORDERS_COLUMNS = [
    "order_id", "customer_id", "order_timestamp", "status",
    "shipping_city", "shipping_method", "coupon_code",
]
ORDER_ITEMS_COLUMNS = [
    "order_item_id", "order_id", "product_id", "quantity", "unit_price", "discount", "line_total",
]
CUSTOMERS_COLUMNS = ["customer_id", "signup_ts", "country", "segment", "marketing_opt_in"]


def _types(suite):
    return [e.expectation_type for e in suite.expectations]


# ── generic checks (every table) ─────────────────────────────────────────────

def test_returns_suite_named_after_the_table():
    suite = silver_expectation_suite("customers", CUSTOMERS_COLUMNS, ["customer_id"])
    assert suite.name == "silver_customers"


def test_plain_table_gets_schema_and_null_pk_checks_only():
    suite = silver_expectation_suite("customers", CUSTOMERS_COLUMNS, ["customer_id"])
    assert _types(suite) == [
        "expect_table_columns_to_match_set",
        "expect_column_values_to_not_be_null",
    ]
    assert suite.expectations[1].column == "customer_id"


def test_null_check_added_once_per_pk_column():
    suite = silver_expectation_suite(
        "payments", ["payment_id", "order_id"], ["payment_id", "order_id"]
    )
    null_checks = [e for e in suite.expectations if e.expectation_type == "expect_column_values_to_not_be_null"]
    assert {e.column for e in null_checks} == {"payment_id", "order_id"}


def test_volume_check_omitted_when_no_baseline_given():
    suite = silver_expectation_suite("customers", CUSTOMERS_COLUMNS, ["customer_id"])
    assert "expect_table_row_count_to_be_between" not in _types(suite)


def test_volume_check_uses_thirty_percent_band_around_baseline():
    suite = silver_expectation_suite(
        "customers", CUSTOMERS_COLUMNS, ["customer_id"], baseline_row_count=1000
    )
    volume = next(e for e in suite.expectations if e.expectation_type == "expect_table_row_count_to_be_between")
    assert volume.min_value == 700
    assert volume.max_value == 1300


# ── orders: Problem A skew + Problem B NULL-fill ─────────────────────────────

def test_orders_gets_skew_and_null_fill_checks():
    suite = silver_expectation_suite("orders", ORDERS_COLUMNS, ["order_id"])
    types = _types(suite)
    assert types.count("expect_column_values_to_be_in_set") == 1
    assert types.count("expect_column_values_to_not_be_in_set") == 1
    assert types.count("expect_column_values_to_not_be_null") == 3  # order_id PK + 2 fill columns


def test_skew_check_floor_is_eighty_percent_hcmc():
    suite = silver_expectation_suite("orders", ORDERS_COLUMNS, ["order_id"])
    floor_check = next(e for e in suite.expectations if e.expectation_type == "expect_column_values_to_be_in_set")
    assert floor_check.column == "shipping_city"
    assert floor_check.value_set == ["Ho Chi Minh City"]
    assert floor_check.mostly == pytest.approx(0.80)


def test_skew_check_ceiling_requires_at_least_ten_percent_non_hcmc():
    suite = silver_expectation_suite("orders", ORDERS_COLUMNS, ["order_id"])
    ceiling_check = next(
        e for e in suite.expectations if e.expectation_type == "expect_column_values_to_not_be_in_set"
    )
    assert ceiling_check.column == "shipping_city"
    assert ceiling_check.mostly == pytest.approx(0.10)


def test_null_fill_check_covers_coupon_code_and_shipping_method():
    suite = silver_expectation_suite("orders", ORDERS_COLUMNS, ["order_id"])
    null_checked_columns = {
        e.column for e in suite.expectations if e.expectation_type == "expect_column_values_to_not_be_null"
    }
    assert {"coupon_code", "shipping_method"} <= null_checked_columns


def test_non_orders_table_does_not_get_skew_or_null_fill_checks():
    suite = silver_expectation_suite("customers", CUSTOMERS_COLUMNS, ["customer_id"])
    types = _types(suite)
    assert "expect_column_values_to_be_in_set" not in types
    assert "expect_column_values_to_not_be_in_set" not in types


# ── order_items: Problem C dedup ─────────────────────────────────────────────

def test_order_items_dedup_check_omitted_without_bronze_row_count():
    suite = silver_expectation_suite("order_items", ORDER_ITEMS_COLUMNS, ["order_item_id"])
    assert "expect_table_row_count_to_be_between" not in _types(suite)


def test_order_items_dedup_check_targets_roughly_two_percent_reduction():
    suite = silver_expectation_suite(
        "order_items", ORDER_ITEMS_COLUMNS, ["order_item_id"], bronze_row_count=909_000
    )
    dedup_check = next(e for e in suite.expectations if e.expectation_type == "expect_table_row_count_to_be_between")
    # (1 - 0.02 - 0.01) .. (1 - 0.02 + 0.01) of 909,000
    assert dedup_check.min_value == round(909_000 * 0.97)
    assert dedup_check.max_value == round(909_000 * 0.99)


def test_other_tables_never_get_the_order_items_dedup_check_even_with_bronze_row_count():
    suite = silver_expectation_suite(
        "customers", CUSTOMERS_COLUMNS, ["customer_id"], bronze_row_count=120_000
    )
    assert "expect_table_row_count_to_be_between" not in _types(suite)
