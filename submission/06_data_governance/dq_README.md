# Data Quality Suites — `b_schema_pipelines/dq/`

## Rubric proof checklist (`coursework/rubrics.md` — Data Governance, 12 pts)

| Requirement (rubric wording) | Pts | Status |
|---|---|---|
| DP1 lineage — "Capture màn hình pipeline trên DataHub UI thể hiện lineage, validation và data contract" | 2 | ❌ not started |
| DP1 data validation & contract | 2 | 🟡 validation is real (this module), but not linked to a DataHub data contract |
| DP2 lineage | 2 | ❌ not started |
| DP2 data validation & contract | 2 | 🟡 same as DP1 |
| DP3 lineage | 2 | ❌ not started |
| DP3 data validation & contract | 2 | 🟡 same as DP1 |

**Honest status**: no `emit_lineage()` call or DataHub integration exists anywhere in this
codebase — confirmed by repo-wide search. This entire section (12 pts) requires standing up
a DataHub instance (GMS + frontend), adding a lineage-emission call to the end of every
Bronze/Silver/Gold/Feature job's `run()`, and linking each GX suite here as a DataHub dataset
assertion — none of which exists yet. The GX validation logic itself (schema, null-PK,
uniqueness, referential integrity, volume — see the table below) is real, tested, and running,
which covers the "data validation" half in spirit, but not the DataHub-linked "data contract"
half specifically, and not lineage at all.

---



Great Expectations suite **factories** for the checks documented in
[`docs/02_schema_piplines.md` §5](../docs/02_schema_piplines.md), one per layer:
`bronze_suite.py`, `silver_suite.py`, `gold_suite.py`. Each factory builds and returns an
in-memory `ExpectationSuite` for one table — it does not read Spark/Delta/Postgres itself,
and it does not run the checks. That's deliberate: the same factory is reused by unit tests
(no cluster needed) and by `validation_runner.py`, which supplies the real row counts /
dimension keys and actually validates a batch.

**Built:** `validation_runner.py` (this directory) is the DAG-facing entry point that calls
these factories — **not** an Airflow `GreatExpectationsOperator`, which this repo doesn't use
(see `dags/plan.md` §3/§12 for why: the ephemeral, parameter-injected suite factories here
don't map onto that provider's persisted-checkpoint model without standing up a full GX
project first). Instead, each DAG's `validate_*` task is an `ExternalPythonOperator` running
`validation_runner.validate_{bronze,silver,gold,feature}_tables(...)` in the project's own
Python 3.13 venv. Full design in [`dags/plan.md`](../dags/plan.md) §9/§10.

---

## How it's implemented

```
table name + expected columns + primary keys              (caller-supplied, static)
baseline row count / FK valid-key sets / dim batch          (caller-supplied, from a live read)
        │
        ▼
bronze_suite.py / silver_suite.py / gold_suite.py    ← one *_expectation_suite(...) function per layer
        │
        ▼  common.py: new_suite(name) — wraps ExpectationSuite in an ephemeral GX context
        │
ExpectationSuite  (in memory — not persisted, not validated)
        │
        ▼  validation_runner.py's `_validate_suite` — builds a batch-only ephemeral GX
        │  context and validates it for real (see dags/plan.md §15 for the resolved
        │  GX API friction this hit during implementation)
   pass → next DAG stage runs
   fail → task fails, downstream stages blocked
```

`common.py` exists because GX 1.x's `ExpectationSuite.add_expectation()` checks whether the
suite has been persisted, which throws `DataContextRequiredError` without an active data
context. An ephemeral, in-memory context (`gx.get_context(mode="ephemeral")`) satisfies that
check without writing anything to disk — the three suite files would otherwise each
duplicate this three-line dance.

### Files

| File | Exports | Table scope |
|---|---|---|
| `common.py` | `new_suite(name) -> ExpectationSuite` | shared by all three factories |
| `bronze_suite.py` | `bronze_expectation_suite(table, expected_columns)` | `customers`, `products`, `orders`, `order_items`, `payments`, `events` |
| `silver_suite.py` | `silver_expectation_suite(table, expected_columns, pk_columns, baseline_row_count=None, bronze_row_count=None)` | `SILVER_TABLES` in `transform_silver.py`: `orders`, `order_items`, `products`, `customers`, `payments` |
| `gold_suite.py` | `gold_expectation_suite(table, expected_columns, pk_columns, unique_column=None, fk_checks=None, baseline_row_count=None)` | any `dim_*` / `fact_*` / `obt_*` table `build_gold.py` writes |

### Checks per layer

Matches `docs/02_schema_piplines.md` §5's "Applied at" column exactly — Bronze doesn't get
null-PK/volume/skew/dedup checks because they only mean something once Silver/Gold have
applied their fixes (and Bronze's own `ingest_bronze.py::_check_quality` already blocks a
bad ingest inline, before the Delta commit — this suite is the separate, DataHub/Airflow-
facing data contract, not a replacement for that inline gate).

| Check | Bronze | Silver | Gold | GX expectation used |
|---|---|---|---|---|
| Schema check | ✅ (`exact_match=False`) | ✅ (`exact_match=False`) | ✅ (`exact_match=True`) | `ExpectTableColumnsToMatchSet` |
| Null check on PKs | — | ✅ | ✅ | `ExpectColumnValuesToNotBeNull`, one per PK column |
| Uniqueness | — | — | ✅ (`unique_column`) | `ExpectColumnValuesToBeUnique` |
| Referential integrity | — | — | ✅ (`fk_checks`) | `ExpectColumnValuesToBeInSet`, one per FK column |
| Volume check (±30%) | — | ✅ (`baseline_row_count`) | ✅ (`baseline_row_count`) | `ExpectTableRowCountToBeBetween` |
| Skew check (Problem A, `orders` only) | — | ✅ | — | `ExpectColumnValuesToBeInSet` + `ExpectColumnValuesToNotBeInSet` (see below) |
| NULL-fill check (Problem B, `orders` only) | — | ✅ | — | `ExpectColumnValuesToNotBeNull` on `coupon_code`/`shipping_method` |
| Dedup check (Problem C, `order_items` only) | — | ✅ (`bronze_row_count`) | — | `ExpectTableRowCountToBeBetween` |

`exact_match=False` in Bronze/Silver because both still carry the `ingest_ts`/`source_file`/
`pipeline_run_id` lineage columns `MetadataManager` stamped on ingest — the expected set is a
subset of the real columns, not an exact match. Gold uses `exact_match=True` because
`build_gold.py`'s `_write_postgres` writes exactly the columns each `_build_*` method
selects — no incidental extra columns.

**The skew check is two expectations, not one** — GX has no native "proportion of rows equal
to X" expectation, so the ±5pp band around 85% HCMC is built from two `mostly`-bounded checks
that each express one side of the band:

```python
# floor: at least 80% of rows are Ho Chi Minh City
ExpectColumnValuesToBeInSet(column="shipping_city", value_set=["Ho Chi Minh City"], mostly=0.80)
# ceiling: at least 10% of rows are NOT Ho Chi Minh City (⇔ at most 90% are)
ExpectColumnValuesToNotBeInSet(column="shipping_city", value_set=["Ho Chi Minh City"], mostly=0.10)
```

**The uniqueness/FK/volume checks all need data the factory doesn't have.** `unique_column`
on `dim_customer` must be validated against the `is_current`-filtered batch, not full SCD2
history — filtering happens on the caller's side. `fk_checks` needs a dimension's full
distinct key set (e.g. every valid `customer_key`) collected by the caller before building
the suite. `baseline_row_count`/`bronze_row_count` come from the previous run's `log_run`
entry or a live Bronze read. None of dq/'s three files touch Spark or Postgres themselves —
that division of labor is what keeps them unit-testable without a running cluster.

### Constants (`silver_suite.py`)

| Name | Value | Meaning |
|---|---|---|
| `HCMC_SKEW_TARGET` | `0.85` | CLAUDE.md's documented Problem A injection rate |
| `HCMC_SKEW_TOLERANCE` | `0.05` | ±5pp band, per `docs/02_schema_piplines.md` §5 |
| `VOLUME_TOLERANCE` | `0.30` | ±30% of baseline, shared with `gold_suite.py` |
| `ORDER_ITEMS_DEDUP_RATE` | `0.01` | actual fraction of Bronze rows a correct dedup removes — half of `generator_config.yaml`'s `duplicate_rate_offline` (`0.02`), because the generator injects `dup_rate/2` extra-copy rows; that config value instead matches `quality_report.txt`'s `duplicated(keep=False)` measurement, which double-counts each duplicate pair. Confirmed against live Bronze data: 909,000 → 899,999 true-unique rows (~0.99%) |
| `ORDER_ITEMS_DEDUP_TOLERANCE` | `0.01` | ±1pp band around the dedup rate |

---

## Prerequisites

- `great-expectations>=1.19.0` (in `pyproject.toml` — `uv sync` from the repo root pulls it in; confirmed to resolve cleanly on Python 3.13)
- No Spark, MinIO, or Postgres needed to use this package directly — those are only needed by whatever caller supplies the runtime parameters (row counts, FK key sets), which today is nothing yet (see "What's still open")

---

## Running

There's no CLI entrypoint — these are library functions, called with values the caller
already has. Typical usage once wired into a DAG task (§10) will look like:

```python
from b_schema_pipelines.dq.silver_suite import silver_expectation_suite

suite = silver_expectation_suite(
    table="orders",
    expected_columns=["order_id", "customer_id", "order_timestamp", "status",
                       "shipping_city", "shipping_method", "coupon_code"],
    pk_columns=["order_id"],
    baseline_row_count=360_000,   # from the previous run's log_run entry
)
# suite now holds 7 expectations: schema, null-PK, volume, 2x skew, 2x null-fill
```

### Tests

```bash
uv run pytest tests/b_schema_pipelines/test_bronze_suite.py \
               tests/b_schema_pipelines/test_silver_suite.py \
               tests/b_schema_pipelines/test_gold_suite.py -v
```

No Spark fixture — these tests only construct `ExpectationSuite` objects and assert on their
expectation types/parameters (mirrors how `test_build_gold.py::test_create_indexes_*` mocks
the DB connection and asserts on the SQL executed, rather than running it against a live
Postgres). 28 tests, all passing.

### Expected output

```
$ uv run pytest tests/b_schema_pipelines/test_bronze_suite.py tests/b_schema_pipelines/test_silver_suite.py tests/b_schema_pipelines/test_gold_suite.py -q
............................                                             [100%]
28 passed in 64.47s (0:01:04)
```

---

## What's still open

- **DataHub assertions** (`pipelines/features/README.md` §11) — linking a validation run's
  pass/fail result to a DataHub assertion is separate, not-yet-started work (Data Governance
  rubric section, distinct from this Data Pipeline Orchestration section).
- **Live validation evidence against the real docker-compose stack** — `validation_runner.py`'s
  logic is proven by construction: unit tests assert the right expectations exist with the
  right parameters (`tests/b_schema_pipelines/test_validation_runner.py`, mocked
  psycopg2/deltalake I/O + real, non-mocked GX validation on small fixtures — 22 tests
  passing), and the three DAGs' task graphs are proven via `tests/dags/test_dags.py` (11
  tests, run in the ephemeral `--no-project --python 3.12 --with apache-airflow` env — see
  that file's docstring). Neither replaces actually running `airflow dags test <dag_id>
  <date>` against the live `docker compose up` stack and a real Bronze/Silver/Gold batch —
  that live run, and the Airflow UI screenshot the rubric scores, are still open
  (**Unverified** in this repo as of this writing).
