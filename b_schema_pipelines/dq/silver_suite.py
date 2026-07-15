"""
Great Expectations suite factory for the Silver layer.

Adds the checks that only make sense once Bronze's raw copy has been cleaned
(docs/02_schema_piplines.md §5): null-PK, a volume check against the
previous run's baseline, and three table-specific checks tied to the
injected data problems Silver fixes (CLAUDE.md's Problem A/B/C table):

  * orders       — Problem A skew check (`shipping_city` HCMC rate) +
                    Problem B NULL-fill check (`coupon_code`/`shipping_method`)
  * order_items  — Problem C dedup check (row count vs. Bronze)

Row-count bounds are computed by the caller (the `dp2_gold_dag` validate
task, per pipelines/features/README.md §10) from the previous run's
`log_run` entry / a live Bronze read — this module has no Spark/DB access of
its own, so it stays unit-testable without a cluster.
"""

from __future__ import annotations

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToNotBeInSet,
    ExpectColumnValuesToNotBeNull,
    ExpectTableColumnsToMatchSet,
    ExpectTableRowCountToBeBetween,
)

from b_schema_pipelines.dq.common import new_suite

HCMC_CITY = "Ho Chi Minh City"
HCMC_SKEW_TARGET = 0.85
HCMC_SKEW_TOLERANCE = 0.05

VOLUME_TOLERANCE = 0.30

ORDER_ITEMS_DEDUP_RATE = 0.02
ORDER_ITEMS_DEDUP_TOLERANCE = 0.01


def silver_expectation_suite(
    table: str,
    expected_columns: list[str],
    pk_columns: list[str],
    baseline_row_count: int | None = None,
    bronze_row_count: int | None = None,
) -> ExpectationSuite:
    """Build the Silver suite for one table.

    Args:
        table: Silver table name (one of SILVER_TABLES in transform_silver.py).
        expected_columns: source columns the table must still carry (schema check).
        pk_columns: business-key columns that must never be NULL post-clean.
        baseline_row_count: previous run's row count for this table, if known —
            enables the ±30% volume check.
        bronze_row_count: Bronze's row count for this run, `order_items` only —
            enables the ~2% dedup-rate check (Problem C).
    """
    suite = new_suite(f"silver_{table}")

    # exact_match=False — Silver still carries the ingest_ts/source_file/
    # pipeline_run_id lineage columns Bronze stamped; only the fix columns
    # change, never the presence of the original + lineage columns.
    suite.add_expectation(
        ExpectTableColumnsToMatchSet(column_set=expected_columns, exact_match=False)
    )

    for pk in pk_columns:
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=pk))

    if baseline_row_count is not None:
        lo = round(baseline_row_count * (1 - VOLUME_TOLERANCE))
        hi = round(baseline_row_count * (1 + VOLUME_TOLERANCE))
        suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=lo, max_value=hi))

    if table == "orders":
        # Problem A — HCMC skew within ±5pp of 85%, expressed as two native
        # `mostly`-bounded expectations: a floor (>= 80% are HCMC) and, via
        # `not_in_set`, a ceiling (>= 10% are NOT HCMC, i.e. <= 90% are).
        floor = HCMC_SKEW_TARGET - HCMC_SKEW_TOLERANCE
        ceiling_complement = 1 - (HCMC_SKEW_TARGET + HCMC_SKEW_TOLERANCE)
        suite.add_expectation(
            ExpectColumnValuesToBeInSet(
                column="shipping_city", value_set=[HCMC_CITY], mostly=floor
            )
        )
        suite.add_expectation(
            ExpectColumnValuesToNotBeInSet(
                column="shipping_city", value_set=[HCMC_CITY], mostly=ceiling_complement
            )
        )
        # Problem B — fixed NULLs must be zero post-Silver.
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="coupon_code"))
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column="shipping_method"))

    if table == "order_items" and bronze_row_count is not None:
        lo = round(bronze_row_count * (1 - ORDER_ITEMS_DEDUP_RATE - ORDER_ITEMS_DEDUP_TOLERANCE))
        hi = round(bronze_row_count * (1 - ORDER_ITEMS_DEDUP_RATE + ORDER_ITEMS_DEDUP_TOLERANCE))
        suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=lo, max_value=hi))

    return suite
