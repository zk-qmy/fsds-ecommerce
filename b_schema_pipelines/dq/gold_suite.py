"""
Great Expectations suite factory for the Gold layer.

Adds the two checks that only apply once Gold's dim/fact/OBT tables exist
(docs/02_schema_piplines.md §5): uniqueness of a dim's business key, and
referential integrity of a fact's foreign keys against their dimension's
surrogate keys — on top of the schema/null-PK/volume checks shared with
Silver.

Both extra checks need data this module can't compute itself (a dimension's
full distinct key set, or which subset of `dim_customer` is the "current"
SCD2 version) — the caller (the `dp2_gold_dag` validate task, per
pipelines/features/README.md §10) is expected to read that from a live
Spark/Postgres session and pass it in. This keeps suite construction pure
and unit-testable without a cluster or database.
"""

from __future__ import annotations

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import (
    ExpectColumnValuesToBeInSet,
    ExpectColumnValuesToBeUnique,
    ExpectColumnValuesToNotBeNull,
    ExpectTableColumnsToMatchSet,
    ExpectTableRowCountToBeBetween,
)

from b_schema_pipelines.dq.common import new_suite

VOLUME_TOLERANCE = 0.30

# dim_date covers a rolling `days_history`-day window (_build_dim_date); fact_order/
# fact_payment_attempt can carry a small number of rows whose date falls just
# outside it — the same reason _create_foreign_keys() adds the real Postgres FK
# constraints as NOT VALID rather than validated. This check needs the same
# tolerance, or it's stricter than the design it's supposed to validate. Confirmed
# live against real data: order_date_key ~2.3% outside dim_date, payment_date_key
# ~0.2% — 0.95 covers both with room to spare, every other FK column stays strict
# (mostly=1.0, GE's implicit default) since those don't have this known mismatch.
DATE_FK_MOSTLY = 0.95
DATE_REFERENCING_FK_COLUMNS = frozenset({"order_date_key", "payment_date_key"})


def gold_expectation_suite(
    table: str,
    expected_columns: list[str],
    pk_columns: list[str],
    unique_column: str | None = None,
    fk_checks: dict[str, list] | None = None,
    baseline_row_count: int | None = None,
) -> ExpectationSuite:
    """Build the Gold suite for one table.

    Args:
        table: Gold table name (a `dim_*`, `fact_*`, or `obt_*` table).
        expected_columns: full column list this table must carry (exact schema
            in Gold — no lineage columns get added past Silver).
        pk_columns: key columns (surrogate and/or business) that must never
            be NULL — e.g. `["order_key", "customer_key", ...]` for a fact.
        unique_column: business-key column that must be unique in this batch.
            For `dim_customer`'s SCD2 uniqueness invariant ("`is_current`
            uniqueness per `customer_id`"), the caller validates this suite
            against the `is_current`-filtered batch, not the full history.
        fk_checks: maps a fact's FK column to the full list of valid keys
            from the dimension it references, e.g.
            `{"customer_key": [1, 2, 3, ...]}` — the caller collects this
            from the dimension table before validating. `order_date_key`/
            `payment_date_key` get a `mostly=` tolerance (DATE_FK_MOSTLY) for
            dim_date's known rolling-window boundary mismatch; every other
            FK column is checked strictly.
        baseline_row_count: previous run's row count, enabling the ±30%
            volume check.
    """
    suite = new_suite(f"gold_{table}")

    suite.add_expectation(
        ExpectTableColumnsToMatchSet(column_set=expected_columns, exact_match=True)
    )

    for pk in pk_columns:
        suite.add_expectation(ExpectColumnValuesToNotBeNull(column=pk))

    if unique_column is not None:
        suite.add_expectation(ExpectColumnValuesToBeUnique(column=unique_column))

    for fk_column, valid_keys in (fk_checks or {}).items():
        kwargs = {"column": fk_column, "value_set": valid_keys}
        if fk_column in DATE_REFERENCING_FK_COLUMNS:
            kwargs["mostly"] = DATE_FK_MOSTLY
        suite.add_expectation(ExpectColumnValuesToBeInSet(**kwargs))

    if baseline_row_count is not None:
        lo = round(baseline_row_count * (1 - VOLUME_TOLERANCE))
        hi = round(baseline_row_count * (1 + VOLUME_TOLERANCE))
        suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=lo, max_value=hi))

    return suite
