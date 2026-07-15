"""
Tests for bronze_expectation_suite.

No Spark/DB needed — the factory only builds an in-memory ExpectationSuite.
"""

from __future__ import annotations

from pathlib import Path
import sys

sys.path.append(str(Path(__file__).resolve().parents[2]))

from b_schema_pipelines.dq.bronze_suite import bronze_expectation_suite

CUSTOMERS_COLUMNS = ["customer_id", "signup_ts", "country", "segment", "marketing_opt_in"]


def test_returns_suite_named_after_the_table():
    suite = bronze_expectation_suite("customers", CUSTOMERS_COLUMNS)
    assert suite.name == "bronze_customers"


def test_builds_exactly_one_schema_check():
    suite = bronze_expectation_suite("customers", CUSTOMERS_COLUMNS)
    assert len(suite.expectations) == 1
    assert suite.expectations[0].expectation_type == "expect_table_columns_to_match_set"


def test_schema_check_uses_the_given_columns_and_is_not_exact_match():
    """exact_match=False — Bronze appends ingest_ts/source_file/pipeline_run_id
    on top of the source schema, so the check must allow extra columns."""
    suite = bronze_expectation_suite("customers", CUSTOMERS_COLUMNS)
    expectation = suite.expectations[0]
    assert expectation.column_set == CUSTOMERS_COLUMNS
    assert expectation.exact_match is False


def test_different_tables_get_their_own_column_set():
    orders_columns = ["order_id", "customer_id", "order_timestamp", "status"]
    suite = bronze_expectation_suite("orders", orders_columns)
    assert suite.expectations[0].column_set == orders_columns
