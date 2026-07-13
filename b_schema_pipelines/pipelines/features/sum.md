# Feature Pipelines — Summary

Reads Gold tables from PostgreSQL and computes ML-ready feature tables, written back to
PostgreSQL under `gold_ecommerce`, ready for Feast ingestion (Section 04, out of scope here).

**Status:** `feat_customer_90d.py` is implemented and tested. `feat_stream_60m.py` and
`feat_customer_unified.py` are not yet built — see `README.md` in this directory (§6, §7) for
the plan.

---

## `feat_customer_90d.py` — rolling 90-day offline features

**Run Gold first** — this reads `gold_ecommerce.{dim_customer, dim_date, dim_product,
fact_order, fact_order_item, fact_payment_attempt}` via JDBC, the same PostgreSQL instance
`build_gold.py` writes to.

### How it's implemented

`CustomerFeature90d` computes, for one `snapshot_date`, the 4 features documented in
`docs/02_schema_piplines.md` §6:

| Feature | Logic |
|---|---|
| `f_customer_total_orders_90d` | `COUNT(DISTINCT order_key)` per customer, orders in the window |
| `f_customer_avg_order_value_90d` | `AVG(order_net_amount)` per customer, orders in the window |
| `f_customer_distinct_categories_90d` | `COUNT(DISTINCT category)` via `fact_order → fact_order_item → dim_product` |
| `f_customer_payment_fail_rate_90d` | `SUM(is_payment_failed) / COUNT(*)` via `fact_order → fact_payment_attempt` |

**Window.** `fact_order` is joined to a `dim_date` slice pre-filtered to
`calendar_date ∈ (snapshot_date − 90d, snapshot_date]`, joining on `date_key` rather than
comparing `order_date_key` as a raw integer. This makes point-in-time correctness structural:
an order dated after `snapshot_date`, or outside the 90-day window, simply has no matching row
in that filtered `dim_date` slice and is dropped by the inner join — there's no separate
"exclude future orders" branch to get wrong.

**Customers with zero orders in the window** still get a feature row (left-joined from
`dim_customer`, current rows only): counts and the fail rate default to `0`, but
`f_customer_avg_order_value_90d` is left `NULL` — the average of an empty set is undefined, and
coalescing it to `0` would misrepresent "no orders" as "orders averaging $0".

**Idempotent write.** `_write()` deletes any existing rows for `snapshot_date` via a direct
`psycopg2` connection (Spark's JDBC `DataFrameWriter` can append or overwrite a whole table,
but can't run a `DELETE ... WHERE`), then appends the new rows via Spark JDBC. The delete is a
no-op (logged, not raised) on a cold start where the table doesn't exist yet.

**Spark session lifecycle.** Unlike `build_gold.py`, `run()` does **not** call
`self.spark.stop()`. This job is meant to be re-invoked per `snapshot_date` (daily run,
backfill loop, or an Airflow task) — the caller owns the session's lifecycle, not the job.

---

## Prerequisites

- Gold tables built: `uv run python3 b_schema_pipelines/pipelines/gold/build_gold.py`
- PostgreSQL reachable at `jdbc:postgresql://localhost:5432/fsds` (see `gold/README.md` for
  how to start/verify it) — credentials come from `pipeline_config.yaml`'s `postgres:` block
- `feat_customer_90d` table doesn't need to be pre-created — Spark's JDBC writer creates it on
  first write

---

## Running

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate

# Snapshot date defaults to today
uv run python3 b_schema_pipelines/pipelines/features/feat_customer_90d.py

# Or an explicit date (useful for backfills)
uv run python3 b_schema_pipelines/pipelines/features/feat_customer_90d.py \
    --snapshot-date 2026-06-01
```

## Expected output

Structured log lines to stdout, e.g.:

```
[feat_customer_90d] phase=end  status=ok  rows_in=120000  rows_out=120000  duration_s=4.812
```

`rows_out` equals the current `dim_customer` row count — every active customer gets a feature
row for the snapshot, whether or not they ordered in the window (see "zero orders" note above).

Full structured logs are written to `logs/feat_90d/<run_id>.log` (same convention as
`bronze/README.md` and `gold/README.md`).

Verify via `psql` or DBeaver:

```sql
SELECT * FROM gold_ecommerce.feat_customer_90d
WHERE event_timestamp = '2026-06-01'
LIMIT 10;
```

Re-running the same `--snapshot-date` overwrites that date's rows in place (delete-then-insert)
— row count for that date stays constant across repeated runs.

---

## Tests

```bash
uv run pytest tests/b_schema_pipelines/test_feat_customer_90d.py -v
```

JDBC reads/writes are mocked; tests validate the feature computation logic (window filtering,
point-in-time correctness, null-fill rules) and the idempotent-write/`run()` call order
(12 tests — see the file docstring).
