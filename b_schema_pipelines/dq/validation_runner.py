"""
Validation runner — the DAG-facing entry point for the `dq/` suite factories.

Per `dags/plan.md` §9: this is the one module in `dq/` that touches Postgres/Delta
directly. `bronze_suite.py`/`silver_suite.py`/`gold_suite.py`/`common.py` stay pure
(no I/O of their own) — this module collects the runtime inputs they need (row
counts, FK key sets, table contents), calls them unmodified, and validates.

Called by the three DAGs' `validate_*` tasks via `ExternalPythonOperator`, running in
the project's own Python 3.13 venv (see `dags/plan.md` §4) — never imported at DAG
module scope, since Airflow's own environment doesn't have great_expectations,
deltalake, or psycopg2 installed. Each DAG's task callable imports from here inside
its own function body.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import psycopg2

import great_expectations as gx

from b_schema_pipelines.dq.bronze_suite import bronze_expectation_suite
from b_schema_pipelines.dq.silver_suite import silver_expectation_suite
from b_schema_pipelines.dq.gold_suite import gold_expectation_suite
from b_schema_pipelines.pipelines.utils.config import load_config

REPO_ROOT = Path(__file__).resolve().parents[2]
BRONZE_CONFIG = load_config(REPO_ROOT / "b_schema_pipelines/pipelines/bronze/bronze_config.yaml")

BRONZE_DIR = "s3a://bronze-data/bronze"
SILVER_DIR = "s3a://silver-data/silver"
SILVER_TABLES = ("orders", "order_items", "products", "customers", "payments")
GOLD_SCHEMA = "gold_ecommerce"

# Manual-sync point: mirrors build_gold.py's `_build_*` `.select(...)` column lists.
# There is no config file build_gold.py itself reads its column lists from (they're
# inline Python) — if a `_build_*` method's `.select(...)` changes, update this dict
# in the same PR. See dags/plan.md §9's "Known limitation" for why this can't be
# derived automatically without making the schema check tautological.
GOLD_TABLE_COLUMNS = {
    "dim_date": ["date_key", "calendar_date", "day_of_week", "month", "year", "is_weekend"],
    "dim_payment_method": ["payment_method_key", "payment_method"],
    "dim_order_status": ["order_status_key", "order_status"],
    "dim_product": [
        "product_key", "product_id", "category", "brand", "base_price", "is_active", "created_ts",
    ],
    "dim_customer": [
        "customer_key", "customer_id", "signup_ts", "segment", "country", "marketing_opt_in",
        "valid_from_ts", "valid_to_ts", "is_current",
    ],
    "fact_order": [
        "order_key", "customer_key", "order_date_key", "order_status_key", "order_id",
        "order_gross_amount", "order_discount_amount", "order_net_amount", "item_count",
    ],
    "fact_order_item": [
        "order_item_key", "order_key", "product_key", "quantity", "unit_price",
        "discount_amount", "line_net_amount",
    ],
    "fact_payment_attempt": [
        "payment_key", "order_key", "payment_date_key", "payment_method_key",
        "amount", "is_payment_success", "is_payment_failed",
    ],
    "obt_order_performance": [
        "order_id", "customer_id", "order_timestamp", "country", "segment", "total_quantity",
        "order_net_amount", "payment_status_last", "shipping_city", "coupon_code",
    ],
}

# table -> (pk_columns, unique_column, fk_checks spec as {fk_column: dim_table.dim_key_column})
GOLD_TABLE_KEYS = {
    "dim_date": (["date_key"], None, {}),
    "dim_payment_method": (["payment_method_key"], None, {}),
    "dim_order_status": (["order_status_key"], None, {}),
    "dim_product": (["product_key"], "product_id", {}),
    "dim_customer": (["customer_key", "customer_id"], "customer_id", {}),
    "fact_order": (
        ["order_key"], None,
        {"customer_key": "dim_customer", "order_date_key": "dim_date", "order_status_key": "dim_order_status"},
    ),
    "fact_order_item": (["order_item_key"], None, {"product_key": "dim_product"}),
    "fact_payment_attempt": (
        ["payment_key"], None,
        {"payment_date_key": "dim_date", "payment_method_key": "dim_payment_method"},
    ),
    "obt_order_performance": (["order_id"], None, {}),
}

# dim_customer is the one Gold table build_gold.py writes with mode="append" (true
# SCD2 incremental writes, not a full overwrite) — its log_run "output_rows" means
# "rows changed this run" (often 0 when no customer attributes changed), not "total
# table size" the way every other Gold table's output_rows is. Comparing the full
# current table against a baseline derived from that number is comparing two
# different things, not a real volume check — skip the log-derived baseline for
# these tables; the volume check only runs if a caller passes one in explicitly.
GOLD_INCREMENTAL_TABLES = frozenset({"dim_customer"})

FEATURE_TABLES = ("feat_customer_90d", "feat_stream_60m", "feat_customer_unified")
FEATURE_TABLE_COLUMNS = {
    "feat_customer_90d": [
        "customer_id", "event_timestamp", "created_ts", "f_customer_total_orders_90d",
        "f_customer_avg_order_value_90d", "f_customer_distinct_categories_90d",
        "f_customer_payment_fail_rate_90d",
    ],
    "feat_stream_60m": [
        "customer_id", "event_timestamp", "created_ts", "f_stream_views_30m",
        "f_stream_add_to_cart_30m", "f_stream_cart_to_purchase_ratio_60m",
        "f_stream_burst_activity_flag",
    ],
    "feat_customer_unified": [
        "customer_id", "event_timestamp", "created_ts", "f_customer_total_orders_90d",
        "f_customer_avg_order_value_90d", "f_customer_distinct_categories_90d",
        "f_customer_payment_fail_rate_90d", "f_stream_views_30m", "f_stream_add_to_cart_30m",
        "f_stream_cart_to_purchase_ratio_60m", "f_stream_burst_activity_flag",
    ],
}


class ValidationFailure(AssertionError):
    """Raised when one or more tables fail their suite. Airflow fails the task on
    this (ExternalPythonOperator propagates the exception), which blocks downstream
    tasks via the normal `>>` dependency graph — no extra branching needed."""


# ── suite execution ─────────────────────────────────────────────────────────────

def _validate_suite(suite, df: pd.DataFrame) -> list[dict]:
    """Validate `df` against `suite` and return a list of failure detail dicts
    (empty if `df` passes every expectation).

    Uses a context created ONLY for the pandas datasource/batch — never calls
    `context.suites.add(suite)` on the suite passed in. `suite` was already
    registered by its own factory's internal context (dq/common.py::new_suite);
    re-adding it to a second context corrupts its ability to resolve the
    datasource (see dags/plan.md §9's GX investigation note).
    """
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_pandas(f"src_{suite.name}")
    asset = data_source.add_dataframe_asset(name=suite.name)
    batch_def = asset.add_batch_definition_whole_dataframe("whole")
    batch = batch_def.get_batch(batch_parameters={"dataframe": df})
    result = batch.validate(suite)

    if result.success:
        return []
    return [
        {
            "expectation_type": r.expectation_config.type,
            "observed_value": r.result.get("observed_value"),
            "details": r.result.get("details"),
        }
        for r in result.results
        if not r.success
    ]


def _raise_if_any_failures(failures_by_table: dict[str, list[dict]]) -> None:
    """Collects every failing table's failures before raising, so one Airflow task
    failure reports everything wrong in this run, not one problem at a time across
    N re-runs."""
    failures_by_table = {table: f for table, f in failures_by_table.items() if f}
    if not failures_by_table:
        return
    lines = [f"{len(failures_by_table)} table(s) failed validation:"]
    for table, failures in failures_by_table.items():
        for f in failures:
            lines.append(
                f"  [{table}] {f['expectation_type']} — observed={f['observed_value']!r} "
                f"details={f['details']!r}"
            )
    raise ValidationFailure("\n".join(lines))


# ── baseline row counts (from PipelineBase's structured logs) ───────────────────

def _last_structured_entry(log_file: Path, table: str) -> dict | None:
    """Most recent STRUCTURED JSON log_run entry for `table` within one log file,
    or None if the file has no matching, successful entry."""
    marker = "STRUCTURED "
    for line in reversed(log_file.read_text(errors="ignore").splitlines()):
        idx = line.find(marker)
        if idx == -1:
            continue
        try:
            entry = json.loads(line[idx + len(marker):])
        except json.JSONDecodeError:
            continue
        if entry.get("table") == table and entry.get("status") == "ok":
            return entry
    return None


def _read_baseline_row_count(pipeline_prefix: str, table: str) -> int | None:
    """Row count from the run *before* the one that just finished.

    By the time a validate task runs, the pipeline job it's checking (e.g.
    build_gold.py) has already written its own fresh log entry for `table` — that
    entry is this run's own output, not a baseline to compare it against. Skips the
    newest matching entry and returns the one before it. None if fewer than two
    runs have logged `table` yet (first-ever run has nothing to compare against;
    dq/'s suite factories already treat `baseline_row_count=None` as "skip the
    volume check", not "fail closed").
    """
    log_dir = REPO_ROOT / "logs" / pipeline_prefix
    if not log_dir.is_dir():
        return None
    matches: list[int] = []
    for log_file in sorted(log_dir.glob(f"{pipeline_prefix}_*.log"), reverse=True):
        entry = _last_structured_entry(log_file, table)
        if entry is not None:
            matches.append(entry["output_rows"])
            if len(matches) == 2:
                return matches[1]
    return None


# ── Delta (Bronze/Silver) readers ────────────────────────────────────────────────

def _delta_storage_options(minio_cfg: dict) -> dict:
    return {
        "AWS_ENDPOINT_URL": minio_cfg["endpoint"],
        "AWS_ACCESS_KEY_ID": minio_cfg["access_key"],
        "AWS_SECRET_ACCESS_KEY": minio_cfg["secret_key"],
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }


def _read_delta_table(path: str, minio_cfg: dict) -> pd.DataFrame:
    from deltalake import DeltaTable

    return DeltaTable(path, storage_options=_delta_storage_options(minio_cfg)).to_pandas()


# ── Postgres (Gold/feature) readers ──────────────────────────────────────────────

def _pg_connect(postgres_cfg: dict):
    return psycopg2.connect(
        host=postgres_cfg["host"],
        port=postgres_cfg["port"],
        dbname=postgres_cfg["db"],
        user=postgres_cfg["user"],
        password=postgres_cfg["password"],
    )


def _read_postgres_table(conn, schema: str, table: str) -> pd.DataFrame:
    return pd.read_sql(f"SELECT * FROM {schema}.{table}", conn)


def _read_distinct_column(conn, schema: str, table: str, column: str) -> list:
    with conn.cursor() as cur:
        cur.execute(f"SELECT DISTINCT {column} FROM {schema}.{table}")
        return [row[0] for row in cur.fetchall()]


# ── public: Bronze ───────────────────────────────────────────────────────────────

def validate_bronze_tables(minio_cfg: dict, tables: list[str] | None = None) -> None:
    """Validate every Bronze Delta table's schema (§dq/README: Bronze gets a schema
    check only — null-PK/volume/skew/dedup only mean something once Silver/Gold
    have applied their fixes).

    `tables` defaults to every table in bronze_config.yaml's `quality.expected_columns`
    — the same file ingest_bronze.py itself reads — so there's one source of truth
    for "what tables/columns exist," not a second hardcoded list here.
    """
    expected_columns_by_table = BRONZE_CONFIG["quality"]["expected_columns"]
    tables = tables or list(expected_columns_by_table.keys())

    failures_by_table: dict[str, list[dict]] = {}
    for table in tables:
        df = _read_delta_table(f"{BRONZE_DIR}/{table}", minio_cfg)
        suite = bronze_expectation_suite(table, expected_columns_by_table[table])
        failures_by_table[table] = _validate_suite(suite, df)

    _raise_if_any_failures(failures_by_table)


# ── public: Silver ────────────────────────────────────────────────────────────────

def validate_silver_tables(
    minio_cfg: dict,
    baseline_row_counts: dict[str, int] | None = None,
    bronze_order_items_row_count: int | None = None,
) -> None:
    """Validate every Silver Delta table: schema, null-PK, volume, plus orders'
    Problem A/B checks and order_items' Problem C dedup check (dq/silver_suite.py).

    `baseline_row_counts`/`bronze_order_items_row_count` default to None, meaning
    "derive automatically" — read from the previous run's structured log
    (baseline) or a live Bronze read (dedup check's comparison point). Passing
    them in explicitly is for tests, so a unit test doesn't need real log files
    on disk to exercise the volume/dedup-check code paths.
    """
    expected_columns_by_table = BRONZE_CONFIG["quality"]["expected_columns"]
    pk_columns_by_table = BRONZE_CONFIG["quality"]["primary_keys"]
    baseline_row_counts = baseline_row_counts or {}

    if bronze_order_items_row_count is None:
        bronze_order_items_row_count = len(
            _read_delta_table(f"{BRONZE_DIR}/order_items", minio_cfg)
        )

    failures_by_table: dict[str, list[dict]] = {}
    for table in SILVER_TABLES:
        df = _read_delta_table(f"{SILVER_DIR}/{table}", minio_cfg)
        baseline = baseline_row_counts.get(table, _read_baseline_row_count("silver", table))
        suite = silver_expectation_suite(
            table,
            expected_columns_by_table[table],
            pk_columns_by_table[table],
            baseline_row_count=baseline,
            bronze_row_count=bronze_order_items_row_count if table == "order_items" else None,
        )
        failures_by_table[table] = _validate_suite(suite, df)

    _raise_if_any_failures(failures_by_table)


# ── public: Gold ──────────────────────────────────────────────────────────────────

def validate_gold_tables(postgres_cfg: dict, baseline_row_counts: dict[str, int] | None = None) -> None:
    """Validate every Gold table: schema, null-PK, uniqueness (dims), referential
    integrity (facts' FKs against their dimension's full key set), volume
    (dq/gold_suite.py).

    Volume check is skipped for GOLD_INCREMENTAL_TABLES (dim_customer) unless a
    caller passes a baseline explicitly — its log_run history records incremental
    rows written, not total table size, so there's no log-derived number that's a
    valid comparison point for "is the whole table roughly the expected size."

    fk_checks use every key a dimension has EVER issued (no `is_current` filter) —
    SCD2 means a fact row can legitimately point at a non-current dim_customer
    version, so filtering to current-only would flag valid historical references
    as broken. The `unique_column` check on dim_customer, by contrast, DOES use the
    is_current-filtered slice — the SCD2 invariant it checks is "at most one
    current row per customer_id", not "customer_id is globally unique across all
    history" (multiple historical rows per customer_id are expected and fine).
    """
    baseline_row_counts = baseline_row_counts or {}
    conn = _pg_connect(postgres_cfg)
    try:
        dim_key_cache: dict[str, list] = {}

        def dim_keys(dim_table: str, key_column: str) -> list:
            cache_key = f"{dim_table}.{key_column}"
            if cache_key not in dim_key_cache:
                dim_key_cache[cache_key] = _read_distinct_column(conn, GOLD_SCHEMA, dim_table, key_column)
            return dim_key_cache[cache_key]

        failures_by_table: dict[str, list[dict]] = {}
        for table, columns in GOLD_TABLE_COLUMNS.items():
            df = _read_postgres_table(conn, GOLD_SCHEMA, table)
            pk_columns, unique_column, fk_spec = GOLD_TABLE_KEYS[table]

            fk_checks = None
            if fk_spec:
                fk_checks = {
                    fk_column: dim_keys(dim_table, GOLD_TABLE_KEYS[dim_table][0][0])
                    for fk_column, dim_table in fk_spec.items()
                }

            if table in GOLD_INCREMENTAL_TABLES:
                baseline = baseline_row_counts.get(table)
            else:
                baseline = baseline_row_counts.get(table, _read_baseline_row_count("gold", table))

            # A single suite validates against a single batch, but dim_customer's
            # two checks need two different row subsets: null-PK/volume against
            # every historical row, uniqueness against only the is_current slice
            # (multiple historical rows sharing a customer_id is expected under
            # SCD2, not a violation). Split into two suite/batch pairs for that
            # one table rather than passing unique_column here at all.
            suite = gold_expectation_suite(
                table, columns, pk_columns, fk_checks=fk_checks, baseline_row_count=baseline,
            )
            failures = _validate_suite(suite, df)

            if unique_column is not None:
                current_df = df[df["is_current"]] if table == "dim_customer" else df
                uniqueness_suite = gold_expectation_suite(
                    f"{table}_uniqueness", columns, pk_columns=[], unique_column=unique_column,
                )
                failures += _validate_suite(uniqueness_suite, current_df)

            failures_by_table[table] = failures

        _raise_if_any_failures(failures_by_table)
    finally:
        conn.close()


# ── public: feature tables ────────────────────────────────────────────────────────

def validate_feature_tables(postgres_cfg: dict, baseline_row_counts: dict[str, int] | None = None) -> None:
    """Validate feat_customer_90d/feat_stream_60m/feat_customer_unified: schema,
    null-PK on (customer_id, event_timestamp), volume. Reuses gold_expectation_suite
    directly (no unique_column/fk_checks — feature tables have neither) rather than
    adding a fourth dq/*_suite.py file for two checks that already exist
    (dags/plan.md §8).

    Unlike Gold's dims/facts, all three feature tables write incrementally
    (psycopg2 DELETE-by-snapshot/window, then Spark JDBC append for just that
    batch — pipelines/features/README.md §5/§6) — every feature table's log_run
    "output_rows" means "rows written for one snapshot/window," not "total table
    size," so none of them have a log-derived baseline that's a valid comparison
    point (same reasoning as GOLD_INCREMENTAL_TABLES above). The volume check only
    runs here if a caller passes a baseline in explicitly.
    """
    baseline_row_counts = baseline_row_counts or {}
    conn = _pg_connect(postgres_cfg)
    try:
        failures_by_table: dict[str, list[dict]] = {}
        for table in FEATURE_TABLES:
            df = _read_postgres_table(conn, GOLD_SCHEMA, table)
            baseline = baseline_row_counts.get(table)
            suite = gold_expectation_suite(
                table,
                FEATURE_TABLE_COLUMNS[table],
                pk_columns=["customer_id", "event_timestamp"],
                baseline_row_count=baseline,
            )
            failures_by_table[table] = _validate_suite(suite, df)

        _raise_if_any_failures(failures_by_table)
    finally:
        conn.close()
