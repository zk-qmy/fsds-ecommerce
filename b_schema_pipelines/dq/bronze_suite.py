"""
Great Expectations suite factory for the Bronze layer.

Bronze validates schema presence only (docs/02_schema_piplines.md §5, "Applied
at" column) — Bronze is a faithful, untransformed copy of the source, so
null-PK / uniqueness / volume checks with meaningful thresholds only make
sense once Silver/Gold have applied their fixes. `ingest_bronze.py`'s own
`_check_quality` already blocks a bad ingest before the Delta commit; this
suite is the data contract `dp1_bronze_dag`'s `GreatExpectationsOperator`
validates against (see pipelines/features/README.md §9/§10).
"""

from __future__ import annotations

from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.expectations import ExpectTableColumnsToMatchSet

from b_schema_pipelines.dq.common import new_suite


def bronze_expectation_suite(table: str, expected_columns: list[str]) -> ExpectationSuite:
    """Schema-presence check for one Bronze table.

    `exact_match=False` — `MetadataManager.add_processing_metadata` stamps
    `ingest_ts`/`source_file`/`pipeline_run_id` onto every row on top of the
    source schema, so the expected set is a subset of the actual columns,
    not an exact match.
    """
    suite = new_suite(f"bronze_{table}")
    suite.add_expectation(
        ExpectTableColumnsToMatchSet(column_set=expected_columns, exact_match=False)
    )
    return suite
