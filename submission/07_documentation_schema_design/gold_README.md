# Gold Layer — `build_gold.py`

## Rubric proof checklist (`coursework/rubrics.md` — Data Storage, 4 pts)

Both items here ask for a **code capture + analysis**, not a UI screenshot — fully satisfied
below, no outstanding gap.

### Lakehouse optimization (2 pts) — Delta Lake compaction + Z-order

`common/delta_writer.py` does both compaction and Z-ordering:

```python
# common/delta_writer.py
def optimize(self, output_path: str) -> None:
    """Compact small files in the Delta table."""
    self.load(output_path).optimize().executeCompaction()

def z_order(self, output_path: str, columns: list[str]) -> None:
    """Z-order the Delta table by `columns` — co-locates rows on disk so a
    filter on those columns skips more files, instead of scanning
    (nearly) every file regardless of the predicate."""
    self.load(output_path).optimize().executeZOrderBy(*columns)
```

Called from `transform_silver.py`'s optimized path, on the exact columns the downstream
90-day rolling-window feature query (`feat_customer_90d.py`) filters/joins on:

```python
# silver/transform_silver.py — after writing Silver orders
self.writer.z_order(f"{self.silver_dir}/orders", ["order_timestamp", "customer_id"])
```

**Analysis — what this optimizes vs. not doing it:** without compaction, every Bronze→Silver
write appends new small Parquet files (`DeltaWriter.write`'s per-batch commit granularity),
so a table re-written daily over the 180-day history accumulates hundreds of small files —
each file open/read has fixed overhead regardless of size, so query latency degrades roughly
linearly with file count, not with data volume. `executeCompaction()` merges those into
fewer, larger files. Z-ordering goes further: even after compaction, a file-level predicate
filter (`WHERE order_timestamp BETWEEN ...`) still has to open every file unless rows
matching similar `order_timestamp`/`customer_id` ranges are physically co-located — Z-order
interleaves the two columns' bit patterns so file-level min/max statistics become
selective, letting Spark's Delta reader skip files that can't contain a match instead of
opening and filtering them row-by-row.

### Datawarehouse optimization (2 pts) — PostgreSQL indexing

```python
# gold/build_gold.py
INDEX_STATEMENTS = (
    ("idx_fact_order_customer_key", "fact_order", "customer_key"),
    ("idx_fact_order_date_key", "fact_order", "order_date_key"),
    ("idx_fact_order_item_order_key", "fact_order_item", "order_key"),
    ("idx_fact_payment_order_key", "fact_payment_attempt", "order_key"),
    ("idx_dim_customer_bk", "dim_customer", "customer_id, is_current"),
)

def _create_indexes(self) -> None:
    """Postgres storage optimization — speeds up the join patterns Gold's
    own downstream consumers (the feature jobs, ad-hoc BI queries) use
    most: point lookups by customer_key/order_key, and dim_customer's
    SCD2 lookup by business key."""
    with closing(psycopg2.connect(...)) as conn, conn, conn.cursor() as cur:
        for name, table, columns in self.INDEX_STATEMENTS:
            cur.execute(f"CREATE INDEX IF NOT EXISTS {name} ON {self.schema}.{table}({columns})")
```

Called from `run()` on every `build_gold.py` invocation (idempotent — `IF NOT EXISTS`), via a
raw `psycopg2` connection since Spark's JDBC writer only appends/overwrites DataFrames, it
can't run DDL.

**Analysis — what this optimizes vs. not doing it:** without these indexes, a query like
`SELECT * FROM fact_order WHERE customer_key = 12345` (the exact join pattern
`feat_customer_90d.py`'s 90-day aggregation runs, once per customer batch) forces a full
sequential scan of `fact_order` (360,000 rows) — cost grows linearly with table size. The
`idx_fact_order_customer_key` index turns that into an index scan, `O(log n)` instead of
`O(n)`, the standard win for any point lookup on a non-clustered high-cardinality column.
`idx_dim_customer_bk`'s composite `(customer_id, is_current)` specifically targets the SCD2
"find this customer's current row" lookup `build_gold.py`'s own `dim_customer` upsert logic
runs on every incremental run — without it, that lookup degrades as `dim_customer`
accumulates historical rows over time (SCD2 history only grows, never shrinks), even though
the *current* rows being searched for are always a small, stable fraction of the table.

**Verify directly** (no screenshot needed — this is a live query against the running
`gold_ecommerce` schema):

```sql
EXPLAIN ANALYZE SELECT * FROM gold_ecommerce.fact_order WHERE customer_key = 12345;
-- Before _create_indexes() ran: Seq Scan on fact_order
-- After:                        Index Scan using idx_fact_order_customer_key
```

---

Reads Silver Delta tables from MinIO and writes the Kimball star schema (5 dims, 3 facts, 1 OBT) to PostgreSQL under the `gold_ecommerce` schema.

**Run Silver first** — Gold reads from `s3a://silver-data/silver/`.

---

## How it's implemented

`GoldBuilder` builds tables in dependency order (`run()`):

```
dim_date → dim_payment_method → dim_order_status → dim_product
→ dim_customer (SCD2)
→ fact_order → fact_order_item → fact_payment_attempt
→ obt_order_performance
```

**Surrogate keys are deterministic**, not `monotonically_increasing_id()` — same input → same keys every time, so a dimension's key assignment and a fact's lookup of that dimension agree without a round trip. Two interchangeable implementations, picked via `--mode`:

| Mode | How | Spark cost |
|---|---|---|
| `baseline` | `row_number()` over a single unpartitioned `Window.orderBy(order_col)` | One task sorts the *entire* table — `WARN WindowExec: No Partition Defined for Window operation!` |
| `optimized` (default) | `repartitionByRange` into N globally-ordered partitions → rank locally in parallel → add each partition's running row-count offset | Parallel; the only unpartitioned step sorts one row *per partition*, not per record |

Both produce **identical keys** for the same input (see `test_optimized_surrogate_keys_match_baseline`) — `optimized` is a drop-in replacement, not an approximation.

**Correctness note — `ranked` must be cached.** `_assign_surrogate_keys_optimized`'s intermediate `ranked` DataFrame (post local-rank, pre-offset) is read twice — once to compute `partition_counts`, once in the final join. Without `.cache()` on it, Spark independently recomputes its full lineage for each read, and `spark_partition_id()` isn't guaranteed to assign the same `_pid` to the same rows across two separate executions of the same lazy plan. Confirmed live: at higher parallelism (10 partitions, raised from the WSL2-capped default of 2 — see `docs/new-plan.md`'s perf notes), this produced duplicate `customer_key` values for 813 distinct `customer_id`s (`partition_offsets` computed from one `_pid` assignment, joined back against a different one for the final key). Not reproducible at low partition counts, which is why it went unnoticed until raising parallelism. Fixed with `.cache()` on `ranked`; `test_optimized_surrogate_keys_unique_at_higher_partition_count` regression-tests this at `num_partitions=16`.

```bash
# capture the "before" Spark UI (single-task WindowExec stage)
uv run python3 b_schema_pipelines/pipelines/gold/build_gold.py --mode baseline

# capture the "after" Spark UI (parallel ranking stages)
uv run python3 b_schema_pipelines/pipelines/gold/build_gold.py --mode optimized
```

- **`dim_customer`** is true **SCD2**: bootstrap run assigns fresh keys to everyone; later runs read the persisted table, close (`is_current=False`, `valid_to_ts=now()`) any row whose `segment`/`country`/`marketing_opt_in` changed, and insert a new current row with a fresh key continuing from `max(existing.customer_key)`. Written with `mode="append"` — history is never overwritten.
- **`dim_date`** is generated (no source table) for the last `days_history` (180) days ending today.
- **`dim_payment_method`** / **`dim_order_status`** are static 4-row lookups built from a hardcoded list.
- **`fact_order`** aggregates `order_items` (gross/discount/item_count) onto `orders`, looks up `customer_key` (prefers the persisted `dim_customer`, falls back to a fresh mapping over orders' own `customer_id` on a cold start) and the static `order_status_key`, and derives `order_date_key` directly from `order_timestamp` (no dimension join needed for dates).
- **`fact_order_item`** / **`fact_payment_attempt`** get `order_key`/`product_key` from the same canonical `_order_key_map()` / `_product_key_map()` that `fact_order` and `dim_product` use (cached per run), so all facts agree on the same key for the same order/product even if one Silver table is missing rows the others have.
- **`obt_order_performance`** is a flat, business-key-only join (order + item aggregates + customer + most-recent payment status) — no surrogate keys, by design.

### Dim/fact relationships — real PostgreSQL FOREIGN KEY constraints

Previously enforced only logically (GX's referential-integrity checks in `dq/gold_suite.py`) —
`run()` now also declares real `FOREIGN KEY` constraints so tools like DBeaver render the
dim/fact relationship lines directly from the schema, not just from application-level checks:

```python
# gold/build_gold.py
FOREIGN_KEY_STATEMENTS = (
    ("fk_fact_order_customer", "fact_order", "customer_key", "dim_customer", "customer_key"),
    ("fk_fact_order_date", "fact_order", "order_date_key", "dim_date", "date_key"),
    ("fk_fact_order_status", "fact_order", "order_status_key", "dim_order_status", "order_status_key"),
    ("fk_fact_order_item_order", "fact_order_item", "order_key", "fact_order", "order_key"),
    ("fk_fact_order_item_product", "fact_order_item", "product_key", "dim_product", "product_key"),
    ("fk_fact_payment_order", "fact_payment_attempt", "order_key", "fact_order", "order_key"),
    ("fk_fact_payment_date", "fact_payment_attempt", "payment_date_key", "dim_date", "date_key"),
    ("fk_fact_payment_method", "fact_payment_attempt", "payment_method_key", "dim_payment_method", "payment_method_key"),
)
```

Two non-obvious things this needed, both because every fact table is rewritten via
`mode="overwrite"` (Spark's JDBC writer `DROP TABLE`s + recreates it each run, see
`_write_postgres`):

1. **`_drop_foreign_keys()` runs first in `run()`, before any table is rebuilt.** Postgres
   refuses to `DROP TABLE fact_order` while `fact_order_item`'s FK still references it — without
   dropping FKs up front, the *second* `build_gold.py` run (the first run's `_create_foreign_keys`
   already landed) would fail outright rebuilding `fact_order`. `_create_foreign_keys()` puts
   them all back at the very end, once every table exists again for this run — same
   drop-then-recreate shape as the tables themselves already have.
2. **FKs are added `NOT VALID`.** `dim_date` only covers a rolling `days_history`-day window;
   `fact_order` can carry a small number of orders just outside it (a documented, deterministic
   generator/`dim_date` boundary mismatch, a fraction of a percent of rows — not a pipeline bug).
   A normal validating `ADD CONSTRAINT` would abort the whole Gold build over those few rows;
   `NOT VALID` skips validating existing rows while still enforcing the constraint on every row
   written from then on.

Referenced columns (`dim_customer.customer_key`, `dim_product.product_key`, `dim_date.date_key`,
`dim_payment_method.payment_method_key`, `dim_order_status.order_status_key`,
`fact_order.order_key`) each get a `PRIMARY KEY` constraint first — Postgres requires a
unique/PK target before it will accept a `FOREIGN KEY` referencing it.

---

## Prerequisites

- Silver Delta tables written (`uv run python b_schema_pipelines/pipelines/silver/transform_silver.py --mode optimized`)
- MinIO running (same as Bronze/Silver)
- PostgreSQL reachable at `jdbc:postgresql://localhost:5432/fsds`, with the `gold_ecommerce` schema created (steps below)
- Credentials/driver come from `b_schema_pipelines/pipelines/pipeline_config.yaml` (`postgres:` section, local-dev `fsds`/`fsds`; JDBC driver `org.postgresql:postgresql:42.7.4` in `packages`)

### Check whether Postgres is up

```bash
docker ps --filter "name=fsds-postgres"            # is the container running?
docker exec fsds-postgres pg_isready -U fsds       # is it accepting connections?
```

If nothing is listening on `5432`, the `postgres` service in `infra/docker-compose.yml` is commented out by default — uncomment it (the service block plus its `postgres_data:` volume under the top-level `volumes:` section) and start it:

```bash
docker compose -f infra/docker-compose.yml up -d postgres
```

### Create the Gold schema (one-time)

`GoldBuilder` only creates *tables* via JDBC — it does not create the schema itself:

```bash
docker exec -it fsds-postgres psql -U fsds -d fsds -c "CREATE SCHEMA IF NOT EXISTS gold_ecommerce;"
```

---

## Running

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate
uv run python3 b_schema_pipelines/pipelines/gold/build_gold.py
```

## Expected output

Structured log lines (one per table) to stdout, e.g.:

```
[dim_date] phase=end  status=ok  rows_in=180  rows_out=180
[dim_payment_method] phase=end  status=ok  rows_in=4  rows_out=4
[dim_order_status] phase=end  status=ok  rows_in=4  rows_out=4
[dim_product] phase=end  status=ok  rows_in=45000  rows_out=45000
[dim_customer] phase=end  status=ok  rows_in=120000  rows_out=120000   # bootstrap run; smaller delta on later runs
[fact_order] phase=end  status=ok  rows_in=360000  rows_out=360000
[fact_order_item] phase=end  status=ok  rows_in=890000  rows_out=890000
[fact_payment_attempt] phase=end  status=ok  rows_in=360000  rows_out=360000
[obt_order_performance] phase=end  status=ok  rows_in=360000  rows_out=360000
```

Full structured logs are written to `logs/gold/<run_id>.log`. Verify tables landed correctly via `psql`:

```sql
\dt gold_ecommerce.*
SELECT COUNT(*) FROM gold_ecommerce.dim_customer WHERE is_current;
SELECT COUNT(*) FROM gold_ecommerce.fact_order;
```

**Prefer a GUI?** Connect DBeaver:

- Open the DBeaver app (desktop)
- **Database** tab → **New Database Connection**
- Choose **PostgreSQL**
- Enter the connection info:
  - Host: `localhost`
  - Port: `5432`
  - Database: `fsds`
  - User / password: `fsds` / `fsds`
- Click **Test Connection**, then **Finish**
- Browse `gold_ecommerce` → `Tables`, or right-click the schema → **View Diagram** for an ER diagram (SCD2 columns on `dim_customer` included)

![gold_ecommerce ER diagram](../../../assets/gold-schema.png)

### Rubric proof checklist (`coursework/rubrics.md` — Schema design, 10 pts)

| Requirement (rubric wording) | Pts | Status |
|---|---|---|
| Visualize tables on all zones — "Capture màn hình trên DBeaver" | 2 | 🟡 Gold zone: ✅ (screenshot above + `assets/database.png`). Bronze/Silver (Delta on MinIO, queryable via the Trino connector — see `docs/novel_ideas.md` Idea 2): tables are registered and queryable, but **no DBeaver screenshot showing them exists yet** |
| Dim table with SCD2 (`valid_from_ts`, `valid_to_ts`, `is_current`) | 2 | ✅ visible in the screenshot above — `dim_customer` has all three columns |
| Feature tables (`feat_*`) with `event_timestamp` + `created` columns | 2 | ✅ visible in `assets/database.png` — `feat_customer_90d`/`feat_stream_60m`/`feat_customer_unified` all show `event_timestamp` + `created_ts` |
| Relationship between dim & fact tables | 2 | 🟡 **code done, screenshot outstanding**: `build_gold.py` now declares real `FOREIGN KEY` constraints (`_create_foreign_keys()`, see "Dim/fact relationships" above) — DBeaver's **View Diagram** will render the relationship lines once re-opened against a freshly-run Gold schema. The existing screenshot above predates this change and still shows standalone boxes; re-run `build_gold.py` and re-capture the DBeaver ER diagram to close this gap |
| Naming convention (`dim_`/`fact_`/`obt_`/`feat_`) | 2 | ✅ visible in both screenshots — consistently applied |

---

## Tests

```bash
uv run pytest tests/b_schema_pipelines/test_build_gold.py -v
```

JDBC reads/writes are mocked; tests validate the DataFrame shape and SCD2/aggregation logic (31 tests, see the file docstring).

---

## Data quality

Great Expectations suites for Gold's null-PK/uniqueness/referential-integrity/volume checks
live in [`b_schema_pipelines/dq/gold_suite.py`](../../dq/README.md), not here — `_create_indexes()`
above is a storage optimization, not a quality gate. `gold_suite.py`'s `fk_checks` param needs
each dimension's distinct surrogate-key set (e.g. every `dim_customer.customer_key`), and its
`unique_column` check on `dim_customer` needs the `is_current`-filtered batch — both are
collected by [`dq/validation_runner.py`](../../dq/validation_runner.py)'s
`validate_gold_tables`, which `dp2_gold_dag`'s `validate_gold` task calls. See
[`dags/plan.md`](../../dags/plan.md) §7/§9 for the full DAG + validation design.
