# Feature Pipelines

Reads Gold tables (and the Flink-cleaned event stream) from PostgreSQL/local disk, computes
ML-ready feature tables, and writes them back to PostgreSQL under `gold_ecommerce`, ready for
Feast ingestion (Section 04, out of scope here).

**Run order:** Gold → (`feat_customer_90d.py`, Flink pipeline → `feat_stream_60m.py` in
parallel) → `feat_customer_unified.py`.

---

## `feat_customer_90d.py` — rolling 90-day offline features

**Prerequisite:** Gold built (`build_gold.py`) — reads `gold_ecommerce.{dim_customer, dim_date,
dim_product, fact_order, fact_order_item, fact_payment_attempt}` via JDBC.

| Feature | Logic |
|---|---|
| `f_customer_total_orders_90d` | `COUNT(DISTINCT order_key)` per customer, orders in the window |
| `f_customer_avg_order_value_90d` | `AVG(order_net_amount)` per customer, orders in the window |
| `f_customer_distinct_categories_90d` | `COUNT(DISTINCT category)` via `fact_order → fact_order_item → dim_product` |
| `f_customer_payment_fail_rate_90d` | `SUM(is_payment_failed) / COUNT(*)` via `fact_order → fact_payment_attempt` |

Joins `fact_order` to a `dim_date`-filtered window `(window_start, snapshot_date]` **through
an inner join on `date_key`**, not a bare integer comparison on `order_date_key` — this makes
point-in-time correctness structural: an order outside the window is silently dropped by the
join rather than relying on a comparison a future edit could weaken.
`f_customer_avg_order_value_90d` is left `NULL` (not coalesced to 0) for a customer with zero
orders in the window — the average of an empty set is undefined, and forcing it to 0 would
look like "this customer has a $0 average order," a different, wrong claim.

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate

# Snapshot date defaults to today
uv run python3 b_schema_pipelines/pipelines/features/feat_customer_90d.py

# Or an explicit date (useful for backfills)
uv run python3 b_schema_pipelines/pipelines/features/feat_customer_90d.py \
    --snapshot-date 2026-06-01
```

```bash
uv run pytest tests/b_schema_pipelines/test_feat_customer_90d.py -v   # 12 tests
```

---

## `feat_stream_60m.py` — 60-minute streaming session features

**Prerequisite:** Flink pipeline run first (`pipelines/streaming/flink_stream_pipeline.py`,
`streaming/README.md`) for the production source; falls back to raw `events.json` for local
dev without Flink running.

| Feature | Logic |
|---|---|
| `f_stream_views_30m` | COUNT(`view` events) in the first 30 min of the window |
| `f_stream_add_to_cart_30m` | COUNT(`add_to_cart` events) in the first 30 min |
| `f_stream_cart_to_purchase_ratio_60m` | `purchase` count / `add_to_cart` count over the full window (0 if no add-to-carts) |
| `f_stream_burst_activity_flag` | 1 if any event in the window fell inside a burst window (12:00–12:20 or 20:00–20:20) |

```bash
# Default source: raw events.json (local dev, no Flink required)
uv run python3 b_schema_pipelines/pipelines/features/feat_stream_60m.py

# Production source: Flink's cleaned/deduped/watermark-corrected output
uv run python3 b_schema_pipelines/pipelines/features/feat_stream_60m.py \
    --events-source b_schema_pipelines/streaming_data/flink_clean_events/optimized
```

```bash
uv run pytest tests/b_schema_pipelines/test_feat_stream_60m.py -v   # 16 tests
```

---

## `feat_customer_unified.py` — point-in-time join of the above two

**Prerequisite:** both `feat_customer_90d.py` and `feat_stream_60m.py` have written their
tables.

`feat_customer_90d` has one row per customer per **day** (`event_timestamp` = midnight
snapshot date); `feat_stream_60m` has one row per customer per **60-min window**
(`event_timestamp` = window start, any time of day) — an equi-join on `(customer_id,
event_timestamp)` between those grains would only match when a stream window happened to
start at exactly midnight on a snapshot day, effectively never. Output grain instead follows
`feat_stream_60m` (the finer-grained table): for each stream row, an as-of join attaches the
*latest* `feat_customer_90d` row at or before that row's `event_timestamp`, per customer —
via `Window.partitionBy("customer_id", "event_timestamp").orderBy(offline_event_timestamp.desc())`
+ `row_number() == 1` after a left join with `offline.event_timestamp <= stream.event_timestamp`.
This satisfies `CLAUDE.md`'s point-in-time rule directly rather than approximating it with an
equality condition. Null-fill follows the same rule as `feat_customer_90d.py`: a stream row
with no applicable offline snapshot gets counts/rate defaulted to `0`, but
`f_customer_avg_order_value_90d` stays `NULL`.

```bash
uv run python3 b_schema_pipelines/pipelines/features/feat_customer_unified.py
```

```bash
uv run pytest tests/b_schema_pipelines/test_feat_customer_unified.py -v   # 8 tests
```

---

## Shared operational notes (all three jobs)

- **Spark session lifecycle.** None of the three call `self.spark.stop()` in `run()` (unlike
  `build_gold.py`) — each is re-invokable in the same process, the caller owns the session.
- **Idempotent writes.** Every job deletes the rows it's about to re-write via `psycopg2`
  before appending via Spark JDBC, so re-running for the same date/window never duplicates rows.
  Each `_delete_existing_*` method also runs `SET TIME ZONE <spark session tz>` on the psycopg2
  connection before the DELETE — PySpark collects timestamps as naive datetimes in the Spark
  session's local timezone, and without this, Postgres compares them using the connection's own
  default timezone instead, silently matching zero rows and turning every re-run into a
  duplicate-append. Confirmed live: same job run twice against the same snapshot produced 2x
  rows before the fix, exactly 1x after.
- **Logs.** `logs/<PREFIX>/<run_id>.log` — same convention as `bronze/README.md` and
  `gold/README.md`.

## Verify output

```sql
SELECT * FROM gold_ecommerce.feat_customer_90d      WHERE event_timestamp = '2026-06-01' LIMIT 10;
SELECT * FROM gold_ecommerce.feat_stream_60m         ORDER BY event_timestamp DESC LIMIT 10;
SELECT * FROM gold_ecommerce.feat_customer_unified   ORDER BY event_timestamp DESC LIMIT 10;
```

## Status

DataHub lineage (`emit_lineage()` on each job) is not started — see `dq/README.md` for the
full Data Governance gap. All other items in this pipeline are implemented and tested; see
`docs/new-plan.md` §3 for the full rubric self-check across Section 02.
