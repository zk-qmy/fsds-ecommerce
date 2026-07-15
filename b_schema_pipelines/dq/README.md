# Data Quality Suites — `b_schema_pipelines/dq/`

Great Expectations suite **factories** for the checks documented in
[`docs/02_schema_piplines.md` §5](../docs/02_schema_piplines.md), one per layer:
`bronze_suite.py`, `silver_suite.py`, `gold_suite.py`. Each factory builds and returns an
in-memory `ExpectationSuite` for one table — it does not read Spark/Delta/Postgres itself,
and it does not run the checks. That's deliberate: the same factory is reused by unit tests
(no cluster needed) and, eventually, by an Airflow `GreatExpectationsOperator` task that
supplies the real row counts / dimension keys and actually validates a batch.

**Not built yet:** the `GreatExpectationsOperator` wiring in `dp1_bronze_dag` /
`dp2_gold_dag` — that's `pipelines/features/README.md` §10 (Airflow DAGs), which needs
this package but hasn't started. See "What's still open" below.

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
        ▼  (future — §10) GreatExpectationsOperator task validates it against a real batch
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
| `ORDER_ITEMS_DEDUP_RATE` | `0.02` | matches `generator_config.yaml`'s `duplicate_rate_offline` |
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

- **DAG wiring** (`pipelines/features/README.md` §10) — no `GreatExpectationsOperator` task
  calls these factories yet. That task also owns computing the runtime inputs: reading the
  previous run's baseline row count, collecting a dimension's distinct key set for
  `fk_checks`, and filtering `dim_customer` to `is_current` before validating `unique_column`.
- **DataHub assertions** (`pipelines/features/README.md` §11) — once §10 runs a checkpoint,
  linking its pass/fail result to a DataHub assertion is separate work.
- **Live validation evidence** — everything above is proven by construction (unit tests
  assert the right expectations exist with the right parameters), not by actually running a
  suite against a real Bronze/Silver/Gold batch. That proof only exists once §10 wires a
  checkpoint against live data — same reasoning as why `pipelines/features/README.md` treats
  "passes a mocked unit test" and "verified against a real running stack" as different
  claims, and doesn't conflate them.
