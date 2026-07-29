# Plan — real Kafka → Flink → Feast streaming flow

Status: draft, not yet implemented. Scope and decisions below were confirmed with the user
on 2026-07-19; open questions inside each section still need a decision before that piece is
built (per CLAUDE.md's "design doc before code" rule — this file is that doc).

## Goal

Replace the current local-dev shortcut (Flink reads a bounded `events.json` file directly)
with the actual production flow, run for real in local dev instead of only being documented:

```
events.json (Section 01 output, unchanged)
        │
        ▼  kafka_event_producer.py  (NEW — replays file rows onto a topic)
        │
        ▼  Kafka topic: raw_events
        │
        ▼  Flink (flink_stream_pipeline.py, MODIFIED — KafkaSource instead of FileSource)
        │     same Problem D/E/F fixes as today, unchanged
        │
        ├──▶ FileSink (unchanged) ──▶ streaming_data/flink_clean_events/<mode>/
        │                                   │
        │                                   ▼  feat_stream_60m.py (unchanged)
        │                                   ▼  gold_ecommerce.feat_stream_60m (Postgres)
        │                                          │
        │                                          ▼  push_stream_to_feast.py  (NEW)
        │                                          │
        │                                          ├──▶ Feast OFFLINE store (Postgres, same DB)
        │                                          └──▶ Feast ONLINE store (Redis, NEW in compose)
        │
        └──▶ windowed view-count branch (unchanged) ──▶ optimized_window_counts/
```

Decisions locked in (confirmed by user):
1. **Scope spans two sections**: `push_stream_to_feast.py` lives in
   `b_schema_pipelines/pipelines/features/` (matches CLAUDE.md's repo structure); the Feast
   repo itself (`feature_store.yaml`, entity/feature-view definitions) lives under `d_ml/`,
   since Feast is `d_ml`'s feature store, not `b_schema_pipelines`'s. This means Section 02's
   features README claim "ready for Feast ingestion (Section 04, out of scope here)" is now
   wrong and gets updated as part of this work.
2. **Kafka is real in local dev**, not just documented as a prod concept. `docker-compose.yml`
   gets a broker; a new producer script replays `events.json` onto a topic; Flink consumes
   from Kafka instead of the file.
3. **Redis is enabled** in `docker-compose.yml` as Feast's online store (currently commented
   out).
4. **`push_stream_to_feast.py` is wired into `dp3_feature_dag`** as a new task after
   `feat_stream_60m`, not left as a manual-only script.

## What already exists vs. what's new

| Component | Status |
|---|---|
| `a_data_generator` → `events.json` | Exists, unchanged |
| Flink pipeline (D/E/F fixes, windowing, FileSink) | Exists, source swapped (file → Kafka) |
| `feat_stream_60m.py` | Exists, unchanged |
| Kafka/Redpanda broker | **New** |
| `kafka_event_producer.py` | **New** |
| Redis (online store) | **New** (currently commented out) |
| Feast repo (`feature_store.yaml`, feature views) | **New** — nothing Feast-related exists in the repo today |
| `push_stream_to_feast.py` | **New** — referenced in CLAUDE.md's structure but never built |
| `dp3_feature_dag` wiring | Modified — add one task |

## Phase 1 — infra: Kafka + Redis in docker-compose

**File:** `infra/docker-compose.yml`

- Add a single-node broker. Proposed: **Redpanda**, not Apache Kafka + ZooKeeper — one
  container, Kafka-API compatible (so `flink-sql-connector-kafka` and any Python Kafka client
  work unmodified against it), no ZK to operate. This mirrors the project's existing pattern
  of picking the lowest-ops option for solo local dev (e.g. 2 Airflow workers instead of the
  default 4, the ephemeral `uv --python 3.12` env for Flink instead of downgrading the whole
  project). If the grader specifically wants to see "Kafka" (not Redpanda) in a screenshot,
  swap this for `confluentinc/cp-kafka` + `cp-zookeeper` (KRaft mode avoids ZK) — flag this
  as an **open question** before Phase 1 starts.
- Uncomment and configure the existing `redis` block (already drafted, just commented out).
- Both services need healthchecks (matches the existing `postgres` pattern) so dependent
  services (`airflow`, the producer, Flink) can `depends_on: condition: service_healthy`.

**Open question:** Redpanda vs. real Kafka — needs a decision before this phase starts.

## Phase 2 — producer: replay events.json onto Kafka

**File:** `b_schema_pipelines/pipelines/streaming/kafka_event_producer.py` (new)

- Standalone script, same "own environment, don't touch the main venv" spirit as
  `flink_stream_pipeline.py` — but a Kafka producer client has a Python 3.13 wheel (unlike
  `apache-flink`), so this one *can* run in the main project venv. Add a client lib
  (`confluent-kafka` — recommend over `kafka-python`, which is unmaintained) to
  `pyproject.toml`.
- Reads `a_data_generator/outputs/streaming/events.json` line by line, publishes each line
  as-is (no reshaping — Flink's `ParseEvent` already handles raw JSON) to a topic (`raw_events`),
  keyed by `customer_id` (keeps per-customer ordering, which `DedupByEventId`'s keyed state and
  the tumbling window's `keyBy(customer_id)` both implicitly rely on).
- Replay speed: default as-fast-as-possible (matches today's bounded-file behavior — the whole
  point of switching to Kafka is topology, not making the demo slower). Add an optional
  `--rate-limit events/sec` flag for anyone who wants to *watch* Flink's Web UI process a
  live-ish stream instead of a instant burst.
- This script does not touch `generator.py` (Section 01) at all — keeps the section boundary
  CLAUDE.md draws between `a_data_generator` and `b_schema_pipelines` intact.

## Phase 3 — Flink: swap FileSource for KafkaSource

**File:** `b_schema_pipelines/pipelines/streaming/flink_stream_pipeline.py` (modify)

- Add `--source {kafka,file}` (default `kafka`); keep `file` as a fallback so the
  `head -2000 events.json > sample.json` fast-iteration trick documented in `streaming/README.md`
  still works without a broker running.
- `KafkaSource.builder()` reading topic `raw_events`, `group.id` per run (or fixed, with
  `--from-earliest` — needs a decision, see below), value deserializer = raw string (same as
  today's `FileSource` text-line read — downstream `ParseEvent` is unchanged).
- **Real risk, needs a spike before committing to this phase's estimate:** PyFlink's Kafka
  connector requires the `flink-sql-connector-kafka` JAR, which isn't a Python package — it has
  to be fetched and passed via `env.add_jars("file:///...")`. Under the existing
  `uv run --no-project --python 3.12 --with apache-flink` ephemeral-env setup there's no
  natural place to cache a downloaded JAR across runs (unlike apache-flink-libraries, which uv
  itself caches). Needs a documented download step (Makefile target or a one-time `curl` in
  `streaming/README.md`) and a fixed local path the script points `add_jars` at.
- **Open question:** consumer semantics on re-run. `--mode baseline` vs `optimized` today
  each get their own sink subdirectory so re-running never mixes baseline/optimized output —
  but a Kafka consumer group's offset means a second run either reprocesses everything
  (`--from-earliest` on a fresh group.id per run — matches "bounded, rerunnable" behavior of
  today's FileSource) or continues where it left off (fixed group.id). Recommend: fresh
  `group.id` per run (`{PREFIX}_{run_id}`), `--from-earliest`, so grading/demo runs stay
  reproducible like today's bounded file source. Confirm before building.

## Phase 4 — Feast repo

**Location:** `d_ml/feature_repo/` (new directory — nothing under `d_ml` beyond `api/` exists
yet, so this also becomes the first piece of `d_ml`'s data/feature layer)

- `feature_store.yaml`:
  - `provider: local`
  - offline store: `postgres`, pointing at the same `fsds` Postgres instance/DB
    `gold_ecommerce` schema already uses (not a separate database — CLAUDE.md's storage
    decision already puts Gold + features in the one Postgres instance)
  - online store: `redis`, pointing at the new compose Redis service
- `definitions.py`:
  - Entity: `customer` (join key `customer_id`) — matches CLAUDE.md's ML system design
    ("Feast entity: customer_id") already documented for both `feat_customer_90d` and
    `feat_stream_60m`
  - FeatureView `feat_stream_60m_fv` over `gold_ecommerce.feat_stream_60m`
    (`f_stream_views_30m`, `f_stream_add_to_cart_30m`, `f_stream_cart_to_purchase_ratio_60m`,
    `f_stream_burst_activity_flag`), `event_timestamp` = `event_timestamp` column,
    `created_timestamp` = `created_ts` (matches this repo's existing point-in-time-correctness
    convention of separating the two)
  - TTL: CLAUDE.md's grading checklist explicitly asks for a "TTL doc" for Feast materialize —
    propose a short TTL (e.g. 2h, matching the Flink dedup state's own TTL reasoning: stream
    features are only meaningful for a recent window) but **this needs a decision**, not a
    default I should just pick silently.
  - **Open question:** does `feat_customer_90d` (and `feat_customer_unified`) also get
    registered now, or only `feat_stream_60m` (since that's the one CLAUDE.md's
    `push_stream_to_feast.py` description explicitly names)? Registering only the stream
    feature view keeps this change additive and scoped; registering all three sets up Section
    04's training/scoring path (`training_table.py`, `ScoringService`) sooner but is more
    surface area now. Recommend scoping to `feat_stream_60m` only for this plan, leaving
    `feat_customer_90d`/`feat_customer_unified` registration as part of whatever change first
    needs `TrainingDataService`/`ScoringService` to actually read from Feast.
- `feast apply` becomes a one-time (or CI-gated) setup step, documented in a new
  `d_ml/feature_repo/README.md`.

## Phase 5 — push_stream_to_feast.py

**File:** `b_schema_pipelines/pipelines/features/push_stream_to_feast.py` (new)

- Thin script, Feast Python SDK only (no Spark — unlike its sibling scripts in this directory,
  it doesn't transform data, just materializes what `feat_stream_60m.py` already wrote to
  Postgres).
- Offline store: nothing to "push" — Feast's Postgres offline store reads
  `gold_ecommerce.feat_stream_60m` directly once registered via `feast apply`;
  `feat_stream_60m.py`'s existing write already *is* the offline side. Script's job here is
  limited to confirming the row range for this run is visible (a read-back sanity check), not
  a separate write.
- Online store: `FeatureStore.materialize_incremental(end_date=<run's snapshot time>)` —
  this is the actual "push," Postgres → Redis, so `feat_stream_60m` values are servable for
  real-time scoring per CLAUDE.md's online-inference design.
- Idempotency (CLAUDE.md's "all pipeline jobs must be idempotent" rule): `materialize_incremental`
  is naturally idempotent (Feast tracks its own materialization high-water mark), so no
  extra ON CONFLICT handling needed here, unlike this repo's raw Postgres writes elsewhere.
- CLI shape matches its siblings: `--postgres-url`, `--feature-repo-path` (defaults to
  `d_ml/feature_repo`), logs `run_id`/`start_ts`/`end_ts`/`status` the same way
  `PipelineBase`-derived scripts do (even though this one won't subclass `PipelineBase` itself
  — no Spark session needed, would be pure overhead).

## Phase 6 — dp3_feature_dag wiring

**File:** `b_schema_pipelines/dags/dp3_feature_dag.py` (modify)

- New `BashOperator` task `push_stream_to_feast`, downstream of `feat_stream_60m` (not
  `feat_customer_unified` — the push is specific to the stream feature view, no reason to wait
  on the unified join).
- New graph: `run_flink >> [feat_customer_90d, feat_stream_60m] >> feat_customer_unified >> validate_features`,
  plus `feat_stream_60m >> push_stream_to_feast` as a side branch that doesn't block
  `feat_customer_unified`.
- **Open question:** does `run_flink`'s task also need to launch `kafka_event_producer.py`
  first now (Kafka needs something publishing to `raw_events` before Flink's consumer has
  anything to read), or is the producer assumed to be running continuously outside this DAG
  (closer to real prod, where Kafka has continuous upstream producers, not a DAG task)? For a
  bounded/replayable local-dev demo, recommend adding a `produce_events` task before
  `run_flink` in this same DAG, rather than expecting someone to have started the producer
  manually. Needs a decision.

## Phase 7 — deps, tests, docs

- `pyproject.toml`: add `feast[postgres,redis]` and `confluent-kafka`.
- Tests: unit tests for `push_stream_to_feast.py` (mock the Feast SDK, no live broker/Redis
  needed — matches this repo's existing pattern of not spinning up live infra in pytest, e.g.
  `streaming/README.md`'s explanation for why Flink itself has no pytest suite) and for
  `kafka_event_producer.py`'s line-parsing/keying logic (mock the producer client).
- Docs to update: `b_schema_pipelines/pipelines/streaming/README.md` (Kafka now real, not a
  stand-in — architecture diagram changes), `b_schema_pipelines/pipelines/features/README.md`
  (drop the "Section 04, out of scope here" line, add `push_stream_to_feast.py` section), new
  `d_ml/feature_repo/README.md`, `b_schema_pipelines/SCHEMA_DESIGN.md` (its "In prod
  this is a Kafka topic" line becomes "this is a Kafka topic, including in local dev").

## Open questions to resolve before implementation starts

1. Redpanda vs. real Apache Kafka image for docker-compose (Phase 1).
2. Kafka consumer semantics on re-run — fresh `group.id` + `--from-earliest` per run, or fixed
   group with resume (Phase 3).
3. How to fetch/cache the `flink-sql-connector-kafka` JAR for PyFlink's ephemeral env
   (Phase 3).
4. Feast `feat_stream_60m_fv` TTL value (Phase 4).
5. Register only `feat_stream_60m` in Feast now, or all three feature tables (Phase 4).
6. Does `dp3_feature_dag` also own starting the Kafka producer, or is that out of this DAG's
   scope (Phase 6).

## Suggested build order

1. Phase 1 (infra) — nothing downstream works without a broker + Redis up.
2. Phase 2 (producer) — needs Phase 1's broker; can be built/tested independent of Flink.
3. Phase 3 (Flink source swap) — needs Phase 1 + 2 to have something to consume.
4. Phase 4 (Feast repo) — independent of 1-3, can happen in parallel.
5. Phase 5 (push script) — needs Phase 4 registered and Phase 1's Redis up.
6. Phase 6 (DAG wiring) — needs 1-5 all working standalone first.
7. Phase 7 (deps/tests/docs) — incremental alongside each phase, not a final pass.
