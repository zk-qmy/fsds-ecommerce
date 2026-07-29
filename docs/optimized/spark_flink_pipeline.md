# Section 02 — Spark & Flink Optimisation Report

Covers every injected data problem from Section 01, how the symptom was observed in the
relevant UI, the code change that fixed it, and what the after-screenshot should show.

Run order for evidence capture:

```bash
# 1 — Baseline (capture "before" screenshots)
python3 b_schema_pipelines/pipelines/silver/transform_silver.py --mode baseline

# 2 — Optimized (capture "after" screenshots)
python3 b_schema_pipelines/pipelines/silver/transform_silver.py --mode optimized
```

Spark UI live: http://localhost:4040 (while job runs)
Spark History Server: http://localhost:18080 (after job finishes)
---
| Layer  | Purpose                  | Data Quality              | Common Operations                             | Typical Spark Bottlenecks                                        |
| ------ | ------------------------ | ------------------------- | --------------------------------------------- | ---------------------------------------------------------------- |
| Bronze | Raw ingestion            | Raw, unvalidated          | Read files, schema inference, append          | Small files, schema evolution, ingestion throughput              |
| Silver | Clean & standardized     | Validated and transformed | Joins, deduplication, filtering, aggregations | Data skew, shuffle, joins, partitioning                          |
| Gold   | Business-ready analytics | Curated                   | Aggregations, KPI calculations, star schemas  | High-cardinality aggregations, large shuffles, expensive groupBy |

---

## Fix 1 — AQE Skew Join (Problem A: 85 % Ho Chi Minh City)

### What was injected

`orders.shipping_city` is set to `"Ho Chi Minh City"` for 85 % of rows.
When Spark shuffles on `shipping_city` (e.g., during any group-by or join), one reducer
receives 85 % of all data while the other 15 % split across the remaining tasks.

### How to identify — Spark UI (baseline run)

**Tab: Stages → click the stage that reads `orders`**

| Metric | What to look for |
|---|---|
| Task Duration | One bar vastly taller than the rest — the skewed task |
| Shuffle Read per task | One task reads 85 % of total shuffle bytes |
| Median vs Max duration | Max task duration > 5× median |

Screenshot to capture: **Stages tab → task duration histogram showing one outlier bar.**

### Fix in code

```python
# transform_silver.py — SilverTransformer._run_optimized(), first two lines
self.spark.conf.set("spark.sql.adaptive.enabled", "true")
self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

Set explicitly per-mode, not left to Spark's own default (AQE and skew-join handling are both
on by default since Spark 3.2) — `_run_baseline()` explicitly sets
`skewJoin.enabled = "false"` first, so the "before" symptom in this fix is actually
reproducible instead of being silently fixed by Spark regardless of mode. (This correction
replaces an earlier version of this doc that claimed the config lived in `pipeline_base.py` —
it never did; the lines existed, commented out, in `transform_silver.py`, and neither mode
touched them.)

AQE detects at runtime that one partition is larger than
`spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes` (default 256 MB) and
automatically splits it into smaller sub-tasks.

### How to verify — Spark UI (optimised run)

**Tab: Stages → same orders stage**

| Metric | What changes |
|---|---|
| Task Duration | All bars roughly equal height — no outlier |
| Stage details | "Skew: true" label appears on the coalesced/split tasks |
| Total stage time | Should be measurably shorter than baseline |

Screenshot to capture: **balanced task duration histogram with no outlier bar.**

---

## Fix 2 — NULL Fill / Schema Evolution (Problem B)

### What was injected

`orders.coupon_code` and `orders.shipping_method` are `NULL` for every row where
`order_timestamp < schema_change_date`. Configured in `generator_config.yaml` as the fraction
`0.5` of the 180-day window (not a fixed calendar date — the window itself slides with real
time, so a literal date would drift; see `a_data_generator/README.md` §8.3).
This simulates a schema that gained new columns mid-history.

### How to identify — data inspection (not Spark UI)

Spark UI does not directly surface NULL counts. The symptom is found in:

1. **Spark UI → SQL tab → select query** — a `Filter(isNull)` node in the plan shows
   the columns are partially null.
2. **DBeaver / query output** — `SELECT COUNT(*) FROM silver_orders WHERE coupon_code IS NULL`
   returns > 0 in baseline Silver but 0 after the fix.

Screenshot to capture: **query result showing NULL count = 0 on Silver after fix.**

### Fix in code

```python
# transform_silver.py — _fix_schema_evolution()
orders_df.withColumn(
    "coupon_code",
    F.when(F.col("coupon_code").isNull(), F.lit("LEGACY")).otherwise(F.col("coupon_code"))
).withColumn(
    "shipping_method",
    F.when(F.col("shipping_method").isNull(), F.lit("UNKNOWN")).otherwise(F.col("shipping_method"))
)
```

Written with `mergeSchema=true` so Delta accepts the schema transition from nullable to
not-null without rejecting the batch.

### How to verify — Spark UI (optimised run)

**Tab: SQL → click the Silver orders write query**

| Metric | What changes |
|---|---|
| Plan nodes | Two `Project` nodes adding the `CASE WHEN isNull THEN ...` expressions |
| Output rows | Same count as input (no rows dropped — only filled) |

Screenshot to capture: **SQL plan showing the two CASE-WHEN fill nodes on the orders write.**

---

## Fix 3 — Window Dedup (Problem C: ~2 % duplicate order_items injected, ~1 % actually removable)

### What was injected

The generator injects `dup_rate/2` exact-copy rows (same `order_item_id`, same
all fields) — with `duplicate_rate_offline: 0.02`, that's ~1 % of rows getting
one extra copy each. The 2 % figure in the config / `quality_report.txt` is
measured via `duplicated(keep=False)`, which flags *both* the original and its
copy per pair, so it reads roughly double the true injected fraction. The
natural key that actually identifies a duplicate pair is
`(order_id, product_id, unit_price, quantity)` — `quantity` matters: two
distinct rows can legitimately share `(order_id, product_id, unit_price)` if a
customer ordered the same product twice at different quantities in one order.

### How to identify — Spark UI (baseline run)

**Tab: Jobs → click the `order_items` count action**

| Metric | What to look for |
|---|---|
| Output rows reported in log | ~909,000 instead of the expected ~900,000 |
| No dedup stage | The plan is a straight scan → count; no Exchange node |

Screenshot to capture: **Jobs tab showing order_items count job with ~909 k rows, plan with no Exchange.**

### Fix in code

```python
# transform_silver.py — _fix_duplicates()
window = Window.partitionBy(
    "order_id", "product_id", "unit_price", "quantity"
).orderBy(
    F.col("ingest_ts").asc(), F.col("order_item_id").asc()
)
order_items_df \
    .withColumn("_rank", F.row_number().over(window)) \
    .filter(F.col("_rank") == 1) \
    .drop("_rank")
```

### How to verify — Spark UI (optimised run)

**Tab: Stages → click the order_items write stage**

| Metric | What changes |
|---|---|
| Stage DAG | An Exchange (shuffle) node appears before the Window aggregate |
| Output rows in log | ~900,000 (≈ 1 % fewer than baseline) |
| Shuffle Read bytes | Non-zero — data moved across partitions for the Window |

Screenshot to capture:
1. **Stages DAG showing the Exchange node for the Window shuffle.**
2. **Log output showing rows_in ~909 k → rows_out ~900 k.**

---

## Fix 4 — Broadcast Join (Problem A: products join cardinality)

### What was injected

No explicit injection — this is a structural problem. `products` has ~45,000 rows
(~5 MB). A join between `orders` (360,000 rows) and `products` on `product_id` using
the default SortMergeJoin forces a full shuffle of both sides.

### How to identify — Spark UI (baseline run)

**Tab: SQL → click a query that joins orders and products**

| Metric | What to look for |
|---|---|
| Plan node | `SortMergeJoin` with an `Exchange` on **both** input sides |
| Exchange bytes | Both sides show non-zero shuffle bytes |
| Stage count | Two extra shuffle stages (one per Exchange) |

Screenshot to capture: **SQL DAG with two Exchange nodes and a SortMergeJoin node.**

### Fix in code

```python
# transform_silver.py — _fix_broadcast_join() — standalone demo method
orders_df.join(broadcast(products_df), on="product_id", how="left")
```

The `broadcast()` hint forces the planner to ship the small `products` table (≤ 5 MB)
to every executor in memory. The `orders` side is never shuffled.

### How to verify — Spark UI (optimised run)

Call `_fix_broadcast_join` standalone after the optimised Silver run to capture the plan:

```python
transformer = SilverTransformer()
orders   = transformer._read_bronze("orders")
products = transformer._read_bronze("products")
transformer._fix_broadcast_join(orders, products).count()
# then open localhost:4040 → SQL tab
```

**Tab: SQL → click the query above**

| Metric | What changes |
|---|---|
| Plan node | `BroadcastHashJoin` replaces `SortMergeJoin` |
| Exchange nodes | Zero on the broadcast (products) side |
| Shuffle Read bytes | Drop to 0 on the broadcast side |

Screenshot to capture: **SQL DAG showing BroadcastHashJoin with zero Exchange on products side.**

---

## Flink Fixes (Problems D / E / F)

Implemented in `b_schema_pipelines/pipelines/streaming/offline_stream_pipeline.py`
(`FlinkStreamPipeline`, `--mode baseline|optimized`) — **not** `feat_stream_60m.py`, which is
a plain Spark job that only computes feature aggregates from whatever clean event stream this
Flink pipeline produces. See `streaming/README.md` for full run instructions, including why
this runs in its own Python 3.12 environment (`apache-flink` has no 3.13 wheel).

These are observed in the **Flink Web UI** (not on by default — `streaming/README.md`
documents how to enable it; `localhost:8081` in these instructions, adjust if that port is
already taken on your machine), not the Spark UI.

### Fix D — Buffer Timeout / Backpressure (Problem D: 30× burst traffic)

#### What was injected

Event rate spikes 30× during 12:00–12:20 and 20:00–20:20.

#### How to identify — Flink UI (before fix — `--mode baseline`)

**Tab: Jobs → click the streaming job → Subtasks**

| Metric | What to look for |
|---|---|
| Backpressure | `HIGH` label on source operator during burst window |
| Checkpoint duration | Spikes above the checkpoint interval during burst |

Screenshot to capture: **Subtasks panel showing Backpressure: HIGH on source.**

#### Fix in code — `FlinkStreamPipeline._build_env`

```python
# offline_stream_pipeline.py
if self.mode == "optimized":
    env.set_buffer_timeout(BUFFER_TIMEOUT_MS)   # 100ms — Flink's own default, made explicit
else:
    env.set_buffer_timeout(-1)   # baseline: flush only when a buffer fills — the
                                  # burst-window symptom, since Flink's default (100ms)
                                  # already flushes promptly and has to be actively
                                  # regressed to reproduce the "before" evidence
```

`get_buffer_timeout()` on a fresh `StreamExecutionEnvironment` already returns `100` — Flink
ships with responsive flushing by default. `baseline` mode has to explicitly set `-1` to
reproduce the buffer-buildup symptom; `optimized` mode just makes the default explicit rather
than relying on it silently.

Screenshot to capture (after — `--mode optimized`): **Backpressure: OK on source.**

---

### Fix E — Bounded Out-of-Orderness Watermark (Problem E: 12 % late arrivals)

#### What was injected

12 % of events have `created_ts` 5–45 minutes after `event_timestamp`.

#### How to identify — Flink UI (before fix — `--mode baseline`)

**Tab: Jobs → Metrics → select `numLateRecordsDropped`**

| Metric | What to look for |
|---|---|
| `numLateRecordsDropped` | Counter > 0 and growing — late events silently discarded |

Screenshot to capture: **Metrics panel showing numLateRecordsDropped > 0.**

#### Fix in code — `FlinkStreamPipeline._apply_watermark_strategy`

```python
# offline_stream_pipeline.py
if self.mode == "optimized":
    strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_minutes(LATE_ARRIVAL_MINUTES)   # 45 — covers the injected 5-45min range
    ).with_timestamp_assigner(EventTimestampAssigner())
else:
    # No out-of-orderness tolerance: any event later than the current
    # watermark is immediately "late" — the Problem E symptom.
    strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
        EventTimestampAssigner()
    )
```

45-minute bounded out-of-orderness covers the generator's full 5–45 min late-arrival range, so
a late event still lands in its correct window instead of being dropped. (No separate
`allowed_lateness`/side-output step on the window operator — the watermark tolerance alone
covers the injected range; a side-output audit path would only matter for events later than
45 min, which this dataset doesn't produce.)

Screenshot to capture (after — `--mode optimized`): **numLateRecordsDropped = 0.**

---

### Fix F — Event Dedup (Problem F: 1.5 % duplicate event_ids)

#### What was injected

1.5 % of events are re-emitted with the same `event_id` but a slightly shifted
`event_timestamp`.

#### How to identify — Flink UI (before fix — `--mode baseline`)

No direct UI counter — identified via a count query against the sink output:

```bash
find b_schema_pipelines/streaming_data/flink_clean_events/baseline -name "*.json" \
    -exec cat {} + | python3 -c "
import sys, json
ids = [json.loads(l)['event_id'] for l in sys.stdin]
print('total:', len(ids), 'distinct:', len(set(ids)))"
```

`total > distinct`, matching the injected 1.5 % rate. Verified during development against a
2000-event sample containing 13 real duplicates: `baseline` → 2000 total / 1987 distinct;
`optimized` → 1987 total / 1987 distinct (zero duplicates reach the sink).

#### Fix in code — `DedupByEventId` (`KeyedProcessFunction`), applied by `_apply_dedup`

```python
# offline_stream_pipeline.py
class DedupByEventId(KeyedProcessFunction):
    def open(self, runtime_context) -> None:
        ttl_config = StateTtlConfig.new_builder(Time.hours(DEDUP_STATE_TTL_HOURS)).build()
        descriptor = ValueStateDescriptor("seen", Types.BOOLEAN())
        descriptor.enable_time_to_live(ttl_config)
        self.seen = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        if self.seen.value():
            return
        self.seen.update(True)
        yield value

# applied only in optimized mode:
stream.key_by(lambda e: e["event_id"]).process(DedupByEventId(), output_type=...)
```

State TTL of `DEDUP_STATE_TTL_HOURS` (2h) prevents unbounded state growth — an `event_id` is
only tracked long enough to catch the generator's same-run duplicate emission, not forever.
`baseline` mode skips this step entirely, so duplicates flow straight through to the sink.

Screenshot to capture (after — `--mode optimized`): **duplicate count query above returns
`total == distinct`.**

---

## Screenshot Checklist

| # | Fix | UI | Tab / Panel | Before label | After label |
|---|---|---|---|---|---|
| 1a | AQE skewJoin | Spark History | Stages → Duration histogram | Skewed outlier bar | Balanced bars |
| 1b | AQE skewJoin | Spark History | Stage detail → skew flag | No flag | "Skew: true" |
| 2 | NULL fill | DBeaver | Query result | NULL count > 0 | NULL count = 0 |
| 3a | Window dedup | Spark History | Stages → DAG | No Exchange | Exchange node |
| 3b | Window dedup | Terminal / log | rows_in vs rows_out | ~909 k = ~909 k | ~909 k → ~900 k |
| 4 | Broadcast join | Spark History | SQL → plan DAG | SortMergeJoin + 2 Exchange | BroadcastHashJoin + 0 Exchange |
| 5 | Buffer timeout / backpressure | Flink UI | Subtasks → Backpressure | HIGH (`--mode baseline`) | OK (`--mode optimized`) |
| 6 | Bounded out-of-orderness watermark | Flink UI | Metrics → numLateRecordsDropped | > 0 (`--mode baseline`) | = 0 (`--mode optimized`) |
| 7 | Event dedup | Terminal (sink query, §Fix F) | total vs. distinct event_id count | total > distinct (`--mode baseline`) | total == distinct (`--mode optimized`) |

Place all screenshots under `b_schema_pipelines/docs/screenshots/` named
`fix<N>_<before|after>_<description>.png`.
