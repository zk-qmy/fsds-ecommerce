# Data Platform Roadmap — Real Website Ingestion

Parked until the real website/backend exists. This document captures a discussion from
2026-07-16 comparing the current Section 02 implementation against a target architecture
that adds Kafka/Debezium CDC, Feast, DataHub, and expanded Airflow orchestration — and lays
out what to do, in what order, once there's a real production site to capture data from.

Not yet acted on. Nothing here is implemented; this is a plan to revisit.

---

## 1. Why this came up

The trigger was a description of a target architecture (below) that doesn't match what
Section 02 currently runs. Some of the gap is just aspirational language from `CLAUDE.md`'s
Section 03/04 design docs; some of it (Kafka/Debezium specifically) only becomes worth doing
once there's a real website producing real traffic instead of the synthetic generator.

**Target paragraph that prompted this (as given, verbatim):**

> Generates configurable historical and real-time e-commerce events in PostgreSQL and MinIO,
> then streams CDC records through Debezium and Kafka. Spark builds batch features and Delta
> Bronze/Silver/Gold tables, while Flink handles event-time processing, deduplication,
> watermarking, streaming quality windows, and online feature updates. Airflow orchestrates
> ingestion, validation, compaction, materialization, drift, and analytics workflows; Feast
> serves PostgreSQL offline features and Redis online features; DataHub, Trino, ...
> *(message was cut off after "Trino,")*

---

## 2. Current platform (verified against code, 2026-07-16)

- **Ingestion & storage** — `a_data_generator/generator.py` produces configurable historical
  Parquet + NDJSON event data (with deliberately injected data-quality problems). Spark ingests
  both into **Bronze** Delta tables on **MinIO** (faithful copy + lineage columns only), then
  Bronze → **Silver** (dedup, NULL-fill, AQE skew handling, broadcast join demo) → **Gold**
  (SCD2 `dim_customer`, other dims/facts, an OBT) — still Delta-on-MinIO. A separate
  **PostgreSQL** instance hosts the Gold schema, feature tables, and Airflow's own metadata.
- **Streaming** — `flink_stream_pipeline.py` reads `events.json` as a **bounded file source**
  — its own README documents this explicitly as a stand-in for a Kafka topic that isn't
  implemented. Applies event-time watermarking, late-event handling (Problem E), and
  event-id dedup (Problem F), then windows a per-customer view count; feeds `feat_stream_60m.py`.
- **Features** — `feat_customer_90d.py`, `feat_stream_60m.py`, `feat_customer_unified.py`
  (point-in-time join) write straight to Postgres via JDBC. **No Feast, no online store** —
  `redis` is defined in `docker-compose.yml` but commented out, unused.
- **Orchestration** — Airflow 2.10.5 (LocalExecutor), exactly three DAGs: `dp1_bronze`
  (ingest → validate), `dp2_gold` (transform → build → validate, gated on `dp1`), `dp3_feature`
  (build features → validate, gated on `dp2`). Each validate step runs Great Expectations
  suites via a shared `validation_runner.py`.
- **Query layer** — Trino + Hive Metastore run alongside with a real `delta.properties`
  catalog pointed at MinIO, so Trino can query the Delta tables directly. **Update
  (2026-07-16, after this doc was first written):** a second `postgres.properties` catalog was
  added, pointed at the same `fsds-postgres` instance — Trino can now also reach
  `postgres.gold_ecommerce.*` directly and join it against `delta.bronze.*`/`delta.silver.*`
  in one query. Doesn't change anything else in this document — Feast/Kafka/DataHub are
  still not present.
- **Not present at all:** Debezium/Kafka CDC, Feast, Redis online store, DataHub lineage,
  and any drift/materialization/retrain DAGs (those are Section 03/04 concepts described in
  `CLAUDE.md` with no code yet).

---

## 3. Fix order, once revisited

Not ordered as the target paragraph lists them — ordered by dependency and effort.

**Tier 1 — quick wins, zero new infra, already scoped as known gaps**
1. Delta Z-order on Silver `orders` (`common/delta_writer.py` already compacts, never
   Z-orders — documented ~2pt gap in `pipelines/features/README.md` §8).
2. Postgres indexes on Gold tables (not implemented anywhere; same doc, same section).

**Tier 2 — Feast (no dependency on Tier 3/Kafka)**
3. Stand up Feast: offline store = existing Postgres feature tables, online store = Redis.
4. Uncomment/wire the `redis` service in `docker-compose.yml`.
5. Implement `push_stream_to_feast.py` (doesn't exist yet) to materialize offline → online.

**Tier 3 — Kafka + CDC (see §4 below for the two sub-options) — gated on a real website existing**

**Tier 4 — orchestration expansion (depends on Tier 2)**
6. `materialize_dag` (Feast offline → online) — needs Feast first.
7. "Drift"/"analytics" DAGs — Section 04 concepts in `CLAUDE.md`
   (`retrain_trigger_dag` calls a Kubeflow training pipeline that doesn't exist yet either).
   Pulling these into Section 02 needs a scope decision, not just code.

**Tier 5 — DataHub**
8. Lineage/metadata integration — self-contained, lowest functional risk, mainly
   grading-evidence value. Do last.

---

## 4. Kafka / CDC — two options for when the website exists

Flink already has native `KafkaSource` support, so replacing the bounded `events.json` file
source with a real topic is a drop-in swap for the *clickstream* side regardless of which
option below is chosen for the *transactional* side (orders/order_items/payments).

### Option A — Kafka + Debezium CDC

```
Production Postgres (orders, order_items, payments)  ← wal_level=logical required
        │
        ▼ Debezium Postgres connector (Kafka Connect)
        ▼ Kafka topics: cdc.orders / cdc.order_items / cdc.payments
        │
        ▼ Flink KafkaSource  ← replaces ingest_bronze.py's daily batch pull for these 3 tables
        ▼ Bronze Delta writes, continuous instead of daily

Website clickstream (views/add_to_cart/...)
        │  app publishes directly
        ▼ Kafka topic: events.raw
        ▼ Flink KafkaSource  ← swaps in for flink_stream_pipeline.py's current FileSource
        (rest of the pipeline — ParseEvent/Dedup/Watermark/windowing — unchanged)
```

New infra: Kafka (KRaft mode, no Zookeeper needed), Kafka Connect + Debezium Postgres
connector, logical replication enabled on the **production** Postgres — keep this separate
from the Gold/feature Postgres, don't turn on CDC for the warehouse itself. `dp1_bronze`'s
batch pull for orders/items/payments becomes redundant for those 3 tables (maybe keep it only
for historical backfill).

Pros: captures every row change (insert/update/delete) with zero app code changes; the
textbook CDC pattern. Cons: heaviest ops footprint (Kafka Connect cluster, connector config,
schema registry if going Avro, logical replication).

### Option B — Kafka + app-level publish (no Debezium)

```
Website backend
        │  on write, publish OrderCreated / PaymentCompleted directly
        ▼ Kafka topics: orders.created, payments.completed, events.raw
        ▼ Flink KafkaSource (same swap as Option A)
```

Same Flink-side change as Option A, but skips Kafka Connect + Debezium + logical replication
entirely. Pros: one less system to run/debug, same real-time property. Cons: only captures
what the app explicitly publishes, not every raw table mutation.

**Decision:** not yet made — pending the real website/backend existing. If the goal is mainly
to demonstrate the CDC pattern for the coursework rubric, Option A is worth it. If the goal is
real production data flowing with the least new infra, lean Option B and skip Debezium.

In both options, the actual code change to `flink_stream_pipeline.py` is the same: swap
`FileSource` for `KafkaSource` (`flink-connector-kafka`, already visible in `references/` so
the jar/API shape is known) — everything downstream (dedup, watermarking, windowing,
`feat_stream_60m.py`) stays as-is.

---

## 5. Revisit trigger

Come back to this once the website/backend is real. At that point: confirm whether Tier 1/2
were already done in the meantime (independent of the website, can be done anytime), then
decide Option A vs B for Tier 3, and re-derive Tier 4 from whatever Section 04 ML work has
landed by then.
