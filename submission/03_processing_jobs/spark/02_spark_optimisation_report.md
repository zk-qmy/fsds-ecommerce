# Spark & Flink Optimisation Report — fix-by-fix detail

**Full write-up:** [`docs/optimized/spark_flink_pipeline.md`](../../../docs/optimized/spark_flink_pipeline.md)

Covers every injected data problem (A–F), how the symptom was observed in the relevant UI, the
code change that fixed it, and what the after-screenshot should show. Supports the rubric
checklists in `bronze_silver_README.md` (this folder) and `../flink/README.md`.

| Fix | Problem | Change |
|---|---|---|
| 1 — AQE Skew Join | A: 85% Ho Chi Minh City | Session config — AQE splits skewed partitions at runtime |
| 2 — NULL Fill | B: schema evolution | `coupon_code`/`shipping_method` NULL → `'LEGACY'`/`'UNKNOWN'` |
| 3 — Window Dedup | C: duplicate `order_items` | Dedup on `(order_id, product_id, unit_price, quantity)`, keep earliest `ingest_ts` |
| 4 — Broadcast Join | A: products join cardinality | `SortMergeJoin` → `BroadcastHashJoin` |
| D — Buffer Timeout | 30× burst traffic | Explicit flush interval prevents backpressure buildup |
| E — Watermark Strategy | 12% late arrivals | Bounded out-of-orderness tolerates 45 min lateness |
| F — Keyed Dedup | 1.5% duplicate event_ids | TTL-bounded `ValueState` drops already-seen `event_id`s |

See the linked doc for each fix's exact Spark/Flink UI tab to check, before/after evidence, and
the code diff.
