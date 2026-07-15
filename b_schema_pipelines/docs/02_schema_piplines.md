# Section 02 – Schema Pipelines Design + Implementation

## 1. Goal Setup

### 1.1 Gold-Zone Objective

**Objective:**
Business-ready Gold model for analytics and BI, plus implemented data pipelines with lineage visibility in DataHub. Feature tables feed the ML training and real-time scoring system (Section 04).

**Approach:** Star schema (Fact + Dimension) for the Gold layer, plus a denormalised OBT for BI tooling, plus two feature tables pushed to Feast.

**Coursework requirement:** Design and implement data pipelines end-to-end, and capture lineage for key datasets using DataHub.

**Storage requirement (cost-focused):** Bronze and Silver layers are stored as Delta Lake tables on MinIO (local dev) / GCS (prod) to enable time-travel, schema evolution, and efficient compaction — reducing scan cost vs raw Parquet on a re-query.

---

### 1.2 Modelling Approach

* **Model type:** Star schema (Kimball) in the Gold layer, with one OBT for BI.
* **Reason:** The e-commerce domain maps cleanly to Kimball — customers, products, dates, and statuses are stable dimensions; orders, order items, and payments are additive facts. Star schema gives BI tools simple join paths and enables aggregation at any grain. The OBT trades normalisation for query speed on the most frequent BI queries.

---

### 1.3 Naming Conventions

| Object | Naming Convention |
|---|---|
| Dimension tables | `dim_<entity>` (e.g. `dim_customer`, `dim_date`) |
| Fact tables | `fact_<event>` (e.g. `fact_order`, `fact_payment_attempt`) |
| OBT tables | `obt_<subject>` (e.g. `obt_order_performance`) |
| Feature tables | `feat_<entity>_<window>` (e.g. `feat_customer_90d`) |
| Surrogate keys | `<table>_key` (e.g. `customer_key`, `order_key`) — integer, system-generated |
| Business keys | `<entity>_id` (e.g. `customer_id`, `order_id`) — string, from source |
| Foreign keys | same name as the PK in the referenced dim (e.g. `customer_key` in `fact_order`) |
| Timestamp columns | `_ts` suffix for raw timestamps, `_date_key` for date dimension FK |
| Boolean flags | `is_<condition>` (e.g. `is_current`, `is_payment_success`) |
| Feature columns | `f_<entity>_<description>` (e.g. `f_customer_total_orders_90d`) |
| All identifiers | `snake_case`, lowercase |

---

### 1.4 Required Input Data Profile (from Section 01)

#### Volume

| Table | Row count | Notes |
|---|---|---|
| customers | 120,000 | one row per customer |
| products | 45,000 | one row per product |
| orders | 360,000 | ~3 orders per customer (Poisson λ=3.0) |
| order_items | 909,000 | ~2.5 items per order + 2% injected duplicates |
| payments | 360,000 | one payment per order |
| events (streaming) | ~262,000 / day | base 100 ev/min + two 30x burst windows |

* **Total data size (Parquet at rest):** ~800 MB across all offline tables.
* **Streaming throughput:** median 101 events/min; peak 3,158 events/min during burst windows.

#### Velocity

* **Offline tables:** batch — one full historical load (180-day window) at pipeline bootstrap; incremental appends daily thereafter.
* **Streaming events:** continuous; simulated as one 24-hour day file. In prod this is a Kafka topic consumed by Flink.

#### Key Attributes

* `customer_id` — high-cardinality entity key (120,000 unique values); used in every join path.
* `order_timestamp` — primary time axis for partitioning and point-in-time queries.
* `shipping_city` — skewed 85% to Ho Chi Minh City (Problem A); requires AQE skew join handling in Spark.

#### Known Data Issues

| Problem | Table | Detail | Handler |
|---|---|---|---|
| A — City skew | orders | 85% shipping_city = 'Ho Chi Minh City' | AQE `skewJoin.enabled=true` in Silver; Gold partitioned by city |
| B — Schema evolution | orders | coupon_code + shipping_method NULL before 2026-03-24 | Silver: fill NULL → 'LEGACY' / 'UNKNOWN'; write with `mergeSchema=true` |
| C — Duplicate rows | order_items | 2% duplicated by (order_id, product_id, unit_price) | Silver: Window rank on `created_ts asc`, keep rank=1 |
| D — Burst traffic | events | 30x rate at 12:00–12:20 and 20:00–20:20 | Flink: watermarks + backpressure config |
| E — Late arrivals | events | 12% events delayed 5–45 min | Flink: `WatermarkStrategy` + `AllowedLateness` |
| F — Duplicate events | events | 1.5% duplicate event_ids | Flink: keyed `ValueState` dedup on `event_id` |

---

### 1.5 Assumptions

* Business objective: provide reliable, query-efficient Gold datasets for BI and downstream ML use.
* Decision usage: Gold tables are used for analytics/reporting; feature tables feed ML training and real-time scoring.
* Service level expectation: Gold and feature data must be ready before the 04:00 daily scoring job.
* Data ownership: source tables are owned by Section 01 generator; schema changes are communicated via `schema_change_date` config field.
* Explainability and governance: out of scope for the current phase.

---

### 1.6 SLA Targets

| Metric | Target |
|---|---|
| Bronze pipeline completion | before 01:00 daily |
| Silver pipeline completion | before 02:00 daily |
| Gold pipeline completion | before 03:00 daily |
| Feature store materialisation | before 03:30 daily |
| Scoring job start | 04:00 daily |
| Data freshness (Gold) | < 24h lag from source |
| Pipeline availability | 99% successful daily runs |

---

## 2. Dimension Design

### dim_customer (SCD Type 2)

* **Purpose:** Track each customer's current and historical profile attributes. Required by the ML label join to correctly attribute a customer's segment at the time of each training example.
* **Grain:** One row per customer per version (a new row is added whenever segment, country, or marketing_opt_in changes).
* **Primary Key:** `customer_key` (surrogate integer, monotonically increasing)
* **Business Key:** `customer_id` (string, e.g. "C000001")
* **Attributes:** `signup_ts`, `segment`, `country`, `marketing_opt_in`, `valid_from_ts`, `valid_to_ts`, `is_current`
* **SCD Strategy:** Type 2 — new row on change, old row closed with `valid_to_ts = now()` and `is_current = False`.
* **Reason for SCD2:** Customer segment is a ML feature. SCD2 allows point-in-time joins to retrieve the correct segment for any historical label date, preventing data leakage. SCD1 (overwrite) would destroy this history.

---

### dim_product

* **Purpose:** Lookup table for product attributes used in fact_order_item and feature aggregations.
* **Grain:** One row per product.
* **Primary Key:** `product_key` (surrogate integer)
* **Business Key:** `product_id` (string, e.g. "P000001")
* **Attributes:** `category`, `brand`, `base_price`, `is_active`, `created_ts`
* **SCD Strategy:** Type 1 (overwrite) — product attributes are not used in point-in-time ML joins, so history is not required.
* **Reason for SCD1:** Products are reference data for BI reporting. Price changes are not tracked at product level; `order_items` captures `unit_price` at time of purchase.

---

### dim_date

* **Purpose:** Calendar dimension enabling time-based aggregations without date arithmetic in every query.
* **Grain:** One row per calendar date.
* **Primary Key:** `date_key` (integer, format YYYYMMDD)
* **Business Key:** `calendar_date` (date)
* **Attributes:** `day_of_week`, `month`, `year`, `is_weekend`
* **SCD Strategy:** None — static, generated once for the full simulation window (180 days).
* **Reason:** Date dimensions never change. Generated programmatically from a date range; no source table needed.

---

### dim_payment_method

* **Purpose:** Normalise payment method codes used in `fact_payment_attempt`.
* **Grain:** One row per payment method.
* **Primary Key:** `payment_method_key` (surrogate integer)
* **Business Key:** `payment_method` (string: credit_card, bank_transfer, e_wallet, cod)
* **SCD Strategy:** Static lookup — 4 rows, never changes.

---

### dim_order_status

* **Purpose:** Normalise order status codes used in `fact_order`.
* **Grain:** One row per order status.
* **Primary Key:** `order_status_key` (surrogate integer)
* **Business Key:** `order_status` (string: completed, pending, cancelled, returned)
* **SCD Strategy:** Static lookup — 4 rows, never changes.

---

## 3. Fact Design

### fact_order

* **Purpose:** Central transactional fact — one row per order, with all monetary measures and dimensional references.
* **Grain:** One order.
* **Primary Key:** `order_key` (surrogate integer)
* **Foreign Keys:** `customer_key → dim_customer`, `order_date_key → dim_date`, `order_status_key → dim_order_status`
* **Business Key (degenerate):** `order_id`
* **Measures:**
  * `order_gross_amount` — SUM(unit_price × quantity) across all items
  * `order_discount_amount` — SUM(discount_amount) across all items
  * `order_net_amount` — gross minus discount
  * `item_count` — COUNT(order_items) for the order
* **Additive / Semi-additive measures:** all four measures are fully additive across all dimensions.

### Schema Evolution Strategy

New columns are handled via Delta Lake `mergeSchema=True` on Silver writes. Silver fills Problem B NULLs (`coupon_code → LEGACY`, `shipping_method → UNKNOWN`) before Gold load, so Gold never sees NULL in those columns. Any future new Silver columns propagate to Gold on the next build cycle without a schema migration.

### Deduplication Strategy

* **Detection:** Duplicates detected in Silver using `Window.partitionBy("order_id", "product_id", "unit_price").orderBy("created_ts")` and a `row_number()` rank.
* **Removal:** Only rows with `rank = 1` (earliest `created_ts`) are kept. Applied in `SilverTransformer._fix_duplicates()` before Gold load.

---

### fact_order_item

* **Purpose:** Line-item level fact — one row per product per order.
* **Grain:** One order line item.
* **Primary Key:** `order_item_key` (surrogate integer)
* **Foreign Keys:** `order_key → fact_order`, `product_key → dim_product`
* **Business Key (degenerate):** `order_item_id`
* **Measures:** `quantity`, `unit_price`, `discount_amount`, `line_net_amount = (unit_price × quantity) - discount_amount`
* **Additive / Semi-additive measures:** all fully additive.

---

### fact_payment_attempt

* **Purpose:** Payment-level fact — one row per payment attempt per order.
* **Grain:** One payment attempt.
* **Primary Key:** `payment_key` (surrogate integer)
* **Foreign Keys:** `order_key → fact_order`, `payment_date_key → dim_date`, `payment_method_key → dim_payment_method`
* **Business Key (degenerate):** `payment_id`
* **Measures:** `amount`, `is_payment_success` (boolean), `is_payment_failed` (boolean)
* **Additive / Semi-additive measures:** `amount` is fully additive; boolean flags are additive across orders but semi-additive across time (need COUNT denominator).

---

## 4. OBT (One Big Table) Design

### Purpose

Denormalised wide table for BI tools (Grafana, ad-hoc SQL). Combines order, customer, payment, and product dimensions into a single flat row — eliminates multi-table joins from BI queries.

### Grain

One order (same grain as `fact_order` — no fan-out).

### Core Columns

| Column | Source | Description |
|---|---|---|
| `order_id` | orders | Business key — degenerate dimension |
| `customer_id` | customers | Business key — no surrogate in OBT |
| `order_timestamp` | orders | Raw event time |
| `country` | customers | Customer's country at order time |
| `segment` | customers | Customer segment (bronze / silver / gold) |
| `total_quantity` | order_items | SUM(quantity) per order |
| `order_net_amount` | order_items | SUM(line_net_amount) per order |
| `payment_status_last` | payments | Status of the most recent payment attempt |
| `shipping_city` | orders | Destination city (85% HCMC — Problem A) |
| `coupon_code` | orders | NULL filled → 'LEGACY' for pre-schema-change orders |

**Trade-off:** OBT duplicates some customer attributes, increasing storage ~20%. Benefit is zero-join BI queries — acceptable for this dataset size (~360k rows).

---

## 5. Refresh and Data Quality Plan

## Refresh Plan

| Layer | Refresh Frequency | Airflow DAG | Schedule |
|---|---|---|---|
| Bronze | Daily append | `dp1_bronze_dag` | 00:00 |
| Silver | Daily overwrite | `dp2_gold_dag` (silver stage) | 01:00 |
| Gold | Daily overwrite | `dp2_gold_dag` (gold stage) | 02:00 |
| Feature Store (offline) | Daily append | `dp3_feature_dag` | 02:30 |
| Feature Store (online) | Daily materialise | `materialize_dag` | 03:00 |

## Freshness SLA

Gold tables and feature tables must be fully materialised by **03:30** so that `scoring_dag` (04:00) and `retrain_trigger_dag` (06:00) have fresh input data. Any pipeline failure triggers an Airflow alert; downstream DAGs do not start until upstream is healthy.

## Data Quality Checks

Implemented using Great Expectations suites in `b_schema_pipelines/dq/`. A failed check fails the Airflow task and blocks the next stage.

| Validation | Description | Applied at |
|---|---|---|
| Schema check | Expected columns present with correct types | Bronze, Silver, Gold |
| Null check on PKs | No NULLs in surrogate or business keys | Silver, Gold |
| Uniqueness | No duplicate business keys in Gold dims | Gold |
| Referential integrity | All fact FK values exist in their dim table | Gold |
| Volume check | Output rows within ±30% of previous run baseline | Silver, Gold |
| Skew check | `shipping_city` HCMC rate within ±5pp of 85% | Silver (Problem A) |
| NULL fill check | Zero NULLs in `coupon_code` and `shipping_method` after Silver | Silver (Problem B) |
| Dedup check | order_items row count reduced by ~2% vs Bronze | Silver (Problem C) |

---

## 6. Feature Store Design

## Feature Table: feat_customer_90d (offline)

* **Purpose:** Rolling 90-day behavioural summary per customer. Used for ML training (point-in-time join) and batch scoring.
* **Grain:** One row per (`customer_id`, `event_timestamp` / snapshot date).
* **Primary Key:** (`customer_id`, `event_timestamp`)
* **Feast entity:** `customer_id`
* **TTL:** 90 days

### Features

| Feature | Description | Window |
|---|---|---|
| `f_customer_total_orders_90d` | COUNT(DISTINCT order_id) | Rolling 90 days |
| `f_customer_avg_order_value_90d` | AVG(order_net_amount) | Rolling 90 days |
| `f_customer_distinct_categories_90d` | COUNT(DISTINCT category) | Rolling 90 days |
| `f_customer_payment_fail_rate_90d` | SUM(failed) / COUNT(*) | Rolling 90 days |

---

## Feature Table: feat_stream_60m (streaming)

* **Purpose:** Real-time session behaviour per customer per 60-minute tumbling window. Used for live scoring via Feast online store.
* **Grain:** One row per (`customer_id`, `event_timestamp` / window start).
* **Primary Key:** (`customer_id`, `event_timestamp`)
* **Feast entity:** `customer_id`
* **TTL:** 2 hours — session features go stale quickly; anything older than 2h is not representative of current intent.

### Features

| Feature | Description | Window |
|---|---|---|
| `f_stream_views_30m` | COUNT(view events) in first 30 min of window | 30-min sub-window |
| `f_stream_add_to_cart_30m` | COUNT(add_to_cart events) in first 30 min | 30-min sub-window |
| `f_stream_cart_to_purchase_ratio_60m` | purchase count / add_to_cart count | 60-min tumbling window |
| `f_stream_burst_activity_flag` | 1 if any event fell inside burst window (12:00–12:20 or 20:00–20:20) | Per event |

### Point-in-Time Correctness

Features are always joined to labels using `f.event_timestamp <= l.event_timestamp`. No feature computed after the label timestamp may appear in the training row. The point-in-time join is enforced in `training_table.py` (Section 03) using Feast's `get_historical_features` API.

Any manual SQL join without this condition is a **data leakage bug** and must not be committed.

### Deduplication Policy

If the same (`customer_id`, `event_timestamp`) pair is written twice (e.g. pipeline re-run), the new row replaces the old one via DELETE-then-INSERT using psycopg2 before the Spark JDBC write — ensuring idempotent runs.

### Refresh Target

| Feature table | Feast store | Refresh |
|---|---|---|
| `feat_customer_90d` | Offline (PostgreSQL) | Daily at 02:30 via `dp3_feature_dag` |
| `feat_stream_60m` | Offline (PostgreSQL) + Online (Redis) | Daily push via `push_stream_to_feast.py` |
| `feat_customer_unified` | Offline (PostgreSQL) | After both of the above, same `dp3_feature_dag` run |
| Online store | Redis | Materialised daily at 03:00 via `materialize_dag` |

---

## Feature Table: feat_customer_unified

* **Purpose:** Single feature view combining offline (90-day) and streaming (60-minute) signal per customer, for Section 03's `training_table.py` to join against `ml_customer_label` and for real-time scoring.
* **Grain:** One row per (`customer_id`, `event_timestamp`) — **follows `feat_stream_60m`'s grain**, not `feat_customer_90d`'s. The two source tables sit on different time grains (`feat_customer_90d` is one row per customer per *day*, midnight-stamped; `feat_stream_60m` is one row per customer per 60-minute *window*, stamped at the window start) — an equi-join on `event_timestamp` between them would only match when a stream window happened to start at exactly midnight on a snapshot day, i.e. almost never. This table's grain deliberately follows the finer-grained side.
* **Primary Key:** (`customer_id`, `event_timestamp`)
* **Join type:** As-of join, not an equi-join. For each `feat_stream_60m` row, attach the *latest* `feat_customer_90d` row at or before that row's `event_timestamp`, per customer — implemented as a ranked window (`Window.partitionBy(customer_id, event_timestamp).orderBy(offline_event_timestamp.desc())`, keep rank 1) over a left join with the range condition `offline.event_timestamp <= stream.event_timestamp`. This is the same `f.event_timestamp <= l.event_timestamp` point-in-time rule this doc already states for features-to-labels joins, applied here between two feature tables instead.
* **Null-fill rule:** a stream row with no applicable offline snapshot yet (new customer, or a window before that customer's first snapshot) gets the offline count/rate features defaulted to `0`, but `f_customer_avg_order_value_90d` stays `NULL` — the average of zero orders is undefined, not zero. Same rule `feat_customer_90d` already applies to its own zero-order customers; this table just reaches the same NULL/zero split via a missing as-of match instead of a zero-row aggregation.
* **Deduplication Policy:** same DELETE-then-INSERT-via-psycopg2 pattern as `feat_stream_60m`, keyed on the distinct `event_timestamp` values present in each write batch.

---

## 7. Data Pipeline Plan

## Bronze Layer

* **Purpose:** Faithful copy of source data with ingest lineage metadata appended. No transformations — raw data preserved for auditability and reprocessing.
* **Storage Format:** Delta Lake on MinIO (local dev) / GCS (prod). Append-only. `mergeSchema=True` allows new source columns to land without breaking the pipeline.
* **Update Strategy:** Append — Bronze is never overwritten. Re-runs produce additional rows; Silver deduplicates.
* **Schedule:** 00:00 daily (`dp1_bronze_dag`)

**Ingest metadata columns added to every row:**

| Column | Value |
|---|---|
| `ingest_ts` | `F.current_timestamp()` |
| `source_file` | relative path of source Parquet or NDJSON file |
| `pipeline_run_id` | `self.run_id` from `PipelineBase` |

## Silver Layer

* **Purpose:** Clean, deduplicated, schema-consistent version of Bronze. Handles all six injected data problems.
* **Storage Format:** Delta Lake on MinIO / GCS. Overwrite per run (Silver is idempotent).
* **Transformations:**
  * Problem A: AQE skew join enabled (`spark.sql.adaptive.skewJoin.enabled=true`)
  * Problem B: NULL fill — `coupon_code → LEGACY`, `shipping_method → UNKNOWN`; write with `mergeSchema=True`
  * Problem C: Dedup `order_items` on `(order_id, product_id, unit_price)`, keep earliest `created_ts`
  * Broadcast hint: `dim_product` (45k rows) broadcast to eliminate SortMergeJoin shuffle
* **Schedule:** 01:00 daily (stage inside `dp2_gold_dag`)

## Gold Layer

* **Purpose:** Business-ready star schema in PostgreSQL for BI and ML feature computation.
* **Business Logic:** SCD2 on `dim_customer`; surrogate key generation on all dims; monetary aggregations in `fact_order`; boolean flags in `fact_payment_attempt`.
* **Schedule:** 02:00 daily (stage inside `dp2_gold_dag`)

**Build order (dependency-safe):**

```
dim_date → dim_payment_method → dim_order_status → dim_product
→ dim_customer (SCD2)
→ fact_order → fact_order_item → fact_payment_attempt
→ obt_order_performance
```

## Feature Pipeline

* **Purpose:** Compute ML-ready feature vectors from Gold tables and Flink streaming output.
* **Feature Generation:**
  * `feat_customer_90d` — PySpark reads Gold PostgreSQL, computes 4 rolling 90-day aggregations per customer
  * `feat_stream_60m` — PySpark reads events.json / Flink sink, computes 4 tumbling 60-min window features per customer
* **Schedule:** 02:30 daily (`dp3_feature_dag`)

### Lakehouse Storage

| Layer | Storage Format | Local dev | Prod |
|---|---|---|---|
| Bronze | Delta Lake | MinIO `s3://bronze/` | GCS `gs://fsds-bronze/` |
| Silver | Delta Lake | MinIO `s3://silver/` | GCS `gs://fsds-silver/` |
| Gold | PostgreSQL 15 | `localhost:5432/fsds` | Cloud SQL (asia-southeast1) |
| Features | PostgreSQL 15 | `localhost:5432/fsds` | Cloud SQL (asia-southeast1) |

### Cross-Zone Visualization (DBeaver)

Gold is browsable in DBeaver directly (PostgreSQL connection — `gold/README.md`). Bronze and
Silver are Delta Lake tables on MinIO, not a database DBeaver can connect to on their own —
they're exposed via Trino's Delta Lake connector (`infra/trino/catalog/delta.properties`,
`delta` catalog, `bronze`/`silver` schemas), registered with
`CALL delta.system.register_table(...)` (`b_schema_pipelines/docs/register_bronze_silver_trino.sql`).
Add a second DBeaver connection using the **Trino** driver (`localhost:8080`, no auth) to
browse `delta.bronze.*` / `delta.silver.*` alongside the `gold_ecommerce` PostgreSQL
connection — all three zones visible across the two connections.

### Pipeline Dependencies

```
dp1_bronze_dag (00:00)
    └── dp2_gold_dag (01:00)
            ├── silver stage (01:00)
            └── gold stage  (02:00)
                    └── dp3_feature_dag (02:30)
                            └── materialize_dag (03:00)
                                    └── scoring_dag (04:00)
```

### SLA Targets

All Gold and feature tables ready by 03:30. Scoring job starts at 04:00. Retrain trigger DAG runs at 06:00 and requires feature data to compute PSI.

### Update Strategy

* **Incremental:** Bronze uses append (`mode="append"`). Feature tables use incremental snapshot per `event_timestamp`.
* **Merge / overwrite:** Silver and Gold use overwrite per full daily run. SCD2 `dim_customer` uses append with closure of changed records.
* **Backfill Policy:** Re-run Bronze → Silver → Gold → Features in sequence for the target date range. Bronze append is idempotent with `pipeline_run_id` as a dedup key.
* **Late Data Handling:** Delta Lake time-travel enables Silver re-processing from a prior Bronze version. Flink `AllowedLateness` handles stream late arrivals up to 45 minutes.

### Operational Controls

#### Monitoring and Alerting

* All pipeline jobs log structured JSON to stdout → collected by Loki (90-day retention).
* Airflow sends email alert on any task failure.
* Great Expectations checkpoint failures post to Slack via `SlackNotificationAction`.
* DataHub emits lineage assertions per pipeline; failed assertions visible in DataHub UI.

#### Run Metadata

Every pipeline task logs: `run_id`, `pipeline_name`, `table`, `start_ts`, `end_ts`, `duration_s`, `input_rows`, `output_rows`, `status`, `error_summary`. Implemented in `PipelineBase._log_run()`.

#### Retry and Recovery

* Airflow tasks: 3 retries with exponential backoff (30s, 60s, 120s).
* All Gold writes and feature writes are idempotent — re-running a failed task produces the same result.
* Silver overwrite is safe to re-run; Bronze data is unchanged.

#### Rerun Procedure

1. Identify failed task in Airflow UI.
2. Check Loki log for `error_summary` field.
3. Fix root cause (schema mismatch, source file missing, etc.).
4. Clear the failed task and all downstream tasks in Airflow.
5. Re-trigger the DAG from the failed task.

### Lineage Tracking

* **Tool:** DataHub (deployed in `mlops` namespace on GKE).
* **Objects tracked:** Bronze Delta tables → Silver Delta tables → Gold PostgreSQL tables → Feature tables → ML training table.
* **Emit method:** DataHub Python SDK `emit_mce()` called at the end of each pipeline job, recording upstream and downstream dataset URNs.
* **Data contracts:** Great Expectations suites linked to DataHub as dataset assertions, visible in the DataHub lineage graph.

---

## 8. Warehouse Optimisation Plan

## Indexing Strategy

Applied to PostgreSQL Gold tables to speed up the most frequent join patterns:

| Table | Index | Reason |
|---|---|---|
| `fact_order` | `(customer_key)` | Feature aggregation joins on customer |
| `fact_order` | `(order_date_key)` | Date-range filters for rolling windows |
| `fact_order_item` | `(order_key)` | Join to fact_order in feature computation |
| `fact_payment_attempt` | `(order_key)` | Payment failure rate aggregation |
| `dim_customer` | `(customer_id, is_current)` | SCD2 lookup on business key |
| `feat_customer_90d` | `(customer_id, event_timestamp)` | Point-in-time join key |
| `feat_stream_60m` | `(customer_id, event_timestamp)` | Point-in-time join key |

## Partitioning Strategy

* **Delta Lake (Bronze/Silver):** Partitioned by `ingest_date` (derived from `ingest_ts`) — limits scan to relevant day on re-processing.
* **Gold `obt_order_performance`:** Partitioned by `shipping_city` in Delta Lake staging before PostgreSQL load — exposes Problem A skew explicitly and allows partition-pruning in Spark.
* **PostgreSQL:** Table partitioning not used at this scale (360k orders); indexing is sufficient.

## Clustering Strategy

Applied to Silver tables to co-locate frequently co-queried columns on disk:

```python
# After Silver write, run OPTIMIZE + Z-ORDER via delta Python API
dt.optimize().executeZOrderBy("order_timestamp", "customer_id")
```

Z-ordering `order_timestamp` + `customer_id` on Silver orders reduces data scanned by the 90-day rolling window query from ~100% to ~30% of files.

## Maintenance Operations

| Operation | Layer | Frequency | Purpose |
|---|---|---|---|
| `VACUUM` | Delta Lake (Bronze/Silver) | Weekly | Remove files older than 7-day retention threshold |
| `OPTIMIZE` + Z-Order | Delta Lake (Silver) | Daily after write | Compact small files; improve scan efficiency |
| `ANALYZE` | PostgreSQL (Gold) | Daily after Gold build | Update table statistics for query planner |
| Index rebuild | PostgreSQL (Gold) | Weekly | Prevent index bloat on high-write `fact_order` |

## Measured Impact

*To be filled after running both baseline and optimised Spark jobs and capturing Spark UI metrics.*

| Metric | Before | After |
|---|---|---|
| Silver orders job — max task duration (city skew) | TBD | Target: <3x variance across tasks |
| Silver order_items count | 909,000 rows | ~890,000 rows (−2% dedup) |
| Gold orders join — join strategy | SortMergeJoin (shuffle) | BroadcastHashJoin (no shuffle) |
| Delta Lake file count (Silver orders) | TBD (pre-compaction) | Target: >50% file count reduction |
| PostgreSQL scan (fact_order by customer) | Seq scan | Index scan (verified via EXPLAIN ANALYZE) |

## Summary

Section 02 implements a full Bronze → Silver → Gold → Feature pipeline for the FSDS e-commerce domain.

| Decision | Choice | Trade-off |
|---|---|---|
| Lakehouse format | Delta Lake | ACID + time-travel vs raw Parquet (no transactions) |
| Gold database | PostgreSQL 15 | Simple ops + Feast-compatible vs Redshift (overkill at this scale) |
| Star schema in Gold | Kimball dims + facts | Query flexibility vs wider OBT alone |
| OBT alongside star schema | `obt_order_performance` | BI query speed vs storage duplication (~20% more) |
| SCD2 on dim_customer | Yes | Point-in-time ML join correctness vs SCD1 (simpler but leaks history) |
| Feature store | Feast (PostgreSQL offline, Redis online) | Standard MLOps pattern + Section 04 integration vs custom solution |
| Orchestration | Airflow 2.8 | Production-grade scheduling, retry, alerting vs simple cron |
| Lineage | DataHub | Full pipeline graph visibility vs no lineage tracking |
