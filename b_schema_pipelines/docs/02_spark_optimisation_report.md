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
# pipeline_base.py — applied before any Silver session starts
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

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
`order_timestamp < 2026-03-24` (the schema change date, ~50 % of the 180-day window).
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

## Fix 3 — Window Dedup (Problem C: 2 % duplicate order_items)

### What was injected

`order_items` has 2 % of rows duplicated by natural key
`(order_id, product_id, unit_price)`. The generator appends exact-copy rows
(same `order_item_id`, same all fields).

### How to identify — Spark UI (baseline run)

**Tab: Jobs → click the `order_items` count action**

| Metric | What to look for |
|---|---|
| Output rows reported in log | ~909,000 instead of the expected ~890,000 |
| No dedup stage | The plan is a straight scan → count; no Exchange node |

Screenshot to capture: **Jobs tab showing order_items count job with ~909 k rows, plan with no Exchange.**

### Fix in code

```python
# transform_silver.py — _fix_duplicates()
window = Window.partitionBy("order_id", "product_id", "unit_price").orderBy(
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
| Output rows in log | ~890,000 (≈ 2 % fewer than baseline) |
| Shuffle Read bytes | Non-zero — data moved across partitions for the Window |

Screenshot to capture:
1. **Stages DAG showing the Exchange node for the Window shuffle.**
2. **Log output showing rows_in ~909 k → rows_out ~890 k.**

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

These are observed in the **Flink Web UI** at http://localhost:8081, not the Spark UI.

### Fix D — Watermarks + Backpressure (Problem D: 30× burst traffic)

#### What was injected

Event rate spikes 30× during 12:00–12:20 and 20:00–20:20.

#### How to identify — Flink UI (before fix)

**Tab: Jobs → click the streaming job → Subtasks**

| Metric | What to look for |
|---|---|
| Backpressure | `HIGH` label on source operator during burst window |
| Checkpoint duration | Spikes above the checkpoint interval during burst |
| Watermark lag | Grows unbounded — watermarks fall behind event time |

Screenshot to capture: **Subtasks panel showing Backpressure: HIGH on source.**

#### Fix in code

```python
# feat_stream_60m.py
env.set_buffer_timeout(100)            # flush every 100ms instead of waiting for full buffer
stream.set_max_parallelism(8)          # allow dynamic rescaling during burst
WatermarkStrategy \
    .for_bounded_out_of_orderness(Duration.of_minutes(5)) \
    .with_idleness(Duration.of_seconds(30))
```

Screenshot to capture (after): **Backpressure: OK on source; checkpoint duration stable.**

---

### Fix E — AllowedLateness (Problem E: 12 % late arrivals)

#### What was injected

12 % of events have `created_ts` 5–45 minutes after `event_timestamp`.

#### How to identify — Flink UI (before fix)

**Tab: Jobs → Metrics → select `numLateRecordsDropped`**

| Metric | What to look for |
|---|---|
| `numLateRecordsDropped` | Counter > 0 and growing — late events silently discarded |

Screenshot to capture: **Metrics panel showing numLateRecordsDropped > 0.**

#### Fix in code

```python
# feat_stream_60m.py
stream \
    .window(TumblingEventTimeWindows.of(Time.minutes(60))) \
    .allowed_lateness(Time.minutes(45)) \
    .side_output_late_data(late_tag)
```

`AllowedLateness(45 min)` holds window state open to accept the injected 5–45 min late
events. Records that arrive after 45 min are routed to a side output for audit rather than
dropped.

Screenshot to capture (after): **numLateRecordsDropped = 0; side output counter > 0.**

---

### Fix F — Event Dedup (Problem F: 1.5 % duplicate event_ids)

#### What was injected

1.5 % of events are re-emitted with the same `event_id` but a slightly shifted
`event_timestamp`.

#### How to identify — Flink UI (before fix)

No direct UI counter — identified via a count query:

```sql
SELECT COUNT(*) - COUNT(DISTINCT event_id) AS duplicates
FROM bronze_events;
```

Returns > 0, matching the injected 1.5 % rate.

#### Fix in code

```python
# feat_stream_60m.py — keyed ValueState dedup
class DedupFunction(KeyedProcessFunction):
    def __init__(self):
        self.seen = None

    def open(self, ctx):
        desc = ValueStateDescriptor("seen", Types.BOOLEAN())
        desc.enable_time_to_live(StateTtlConfig.new_builder(Time.hours(2)).build())
        self.seen = ctx.get_key_value_state(desc)

    def process_element(self, event, ctx, out):
        if not self.seen.value():
            self.seen.update(True)
            out.collect(event)

stream.key_by(lambda e: e.event_id).process(DedupFunction())
```

State TTL of 2 hours prevents unbounded state growth.

Screenshot to capture (after): **Operator state size stable (bounded by TTL); duplicate count query returns 0.**

---

## Screenshot Checklist

| # | Fix | UI | Tab / Panel | Before label | After label |
|---|---|---|---|---|---|
| 1a | AQE skewJoin | Spark History | Stages → Duration histogram | Skewed outlier bar | Balanced bars |
| 1b | AQE skewJoin | Spark History | Stage detail → skew flag | No flag | "Skew: true" |
| 2 | NULL fill | DBeaver | Query result | NULL count > 0 | NULL count = 0 |
| 3a | Window dedup | Spark History | Stages → DAG | No Exchange | Exchange node |
| 3b | Window dedup | Terminal / log | rows_in vs rows_out | ~909 k = ~909 k | ~909 k → ~890 k |
| 4 | Broadcast join | Spark History | SQL → plan DAG | SortMergeJoin + 2 Exchange | BroadcastHashJoin + 0 Exchange |
| 5 | Flink backpressure | Flink UI | Subtasks → Backpressure | HIGH | OK |
| 6 | AllowedLateness | Flink UI | Metrics → numLateRecordsDropped | > 0 | = 0 |
| 7 | Event dedup | DBeaver | Query result | duplicates > 0 | duplicates = 0 |

Place all screenshots under `b_schema_pipelines/docs/screenshots/` named
`fix<N>_<before|after>_<description>.png`.
