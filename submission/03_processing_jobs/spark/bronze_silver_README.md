# Bronze → Silver Pipelines

## Rubric proof checklist (`coursework/rubrics.md` — Spark job to handle offline data problems, 16 pts)

| Requirement (rubric wording) | Pts | Where satisfied here |
|---|---|---|
| Baseline (without optimization) — "Document giải thích từng step optimize từ baseline như thế nào, dùng Spark UI như thế nào (với screenshots), và đã tích hợp vào Airflow pipeline ra sao" | 2 | `docs/02_spark_optimisation_report.md` §1-2 documents the baseline→optimized steps and Spark UI tabs to check; Airflow integration is real (`dp2_gold_dag.py`'s `transform_silver` task runs this exact script) — **Spark UI screenshots not yet captured**, see report's checklist |
| Handle skew (Problem A) with explanation | 3 | `docs/02_spark_optimisation_report.md` Fix 1 (AQE skewJoin) — code + explanation done, **screenshot outstanding** |
| Handle high cardinality with explanation | 3 | Fix 4 (broadcast join, products) — code + explanation done, **screenshot outstanding** |
| Handle schema evolution with explanation | 3 | Fix 2 (NULL fill) below — code + explanation done, **screenshot outstanding** |
| Handle other offline problem (chosen: duplicate rows) with explanation | 3 | Fix 3 (window dedup) below — code + explanation done, **screenshot outstanding** |
| Spark job integrated into Airflow pipeline | 2 | ✅ — `dp1_bronze_dag.py`'s `ingest_bronze` task and `dp2_gold_dag.py`'s `transform_silver`/`build_gold` tasks call these scripts directly (`BashOperator`, see `dags/plan.md`) |

**Screenshot gap**: all 4 fixes' code and written analysis are done and accurate (this file +
`docs/02_spark_optimisation_report.md`'s full checklist), but none of the actual Spark UI
before/after screenshots have been captured yet — that's the one remaining piece for this
section's full point value.

---

Reads raw source files from `a_data_generator/outputs/` and writes them as Delta Lake tables to the Bronze layer in MinIO. No transformation — Bronze is a faithful copy of the source plus three lineage columns: `ingest_ts`, `source_file`, `pipeline_run_id`.

**Tables ingested:** `customers`, `products`, `orders`, `order_items`, `payments`, `events`

**Orchestration:** both pipelines below run standalone via `uv run python3 ...` for local
dev/testing (steps 1–3 below), and are also wired into Airflow — Bronze as `dp1_bronze`'s
`ingest_bronze` task, Silver as `dp2_gold`'s `transform_silver` task, each followed by a
`validate_*` task. See [`dags/plan.md`](../../dags/plan.md) for the full DAG design.

---

## Architecture

```
a_data_generator/outputs/
  offline/  ← .parquet files (customers, products, orders, order_items, payments)
  streaming/ ← events.json (NDJSON)
        │
        ▼  FileReader  (ns→us timestamp downcast for Parquet)
        │
        ▼  MetadataManager  (stamps ingest_ts, source_file, pipeline_run_id)
        │
        ▼  Quality gates  (volume, schema, null PK checks)
        │
        ▼  DeltaWriter  (common/delta_writer.py — MERGE or append_if_new_source, mergeSchema=true)
        │
s3a://bronze-data/bronze/<table>/   ← Delta Lake on MinIO
```

`DeltaWriter` is shared with the Silver pipeline via `b_schema_pipelines/pipelines/common/delta_writer.py`.

### Idempotency

CLAUDE.md requires "re-running a job must not produce duplicate rows." Two strategies, split
by whether a table's source can legitimately contain rows sharing the same primary key:

| Tables | Strategy | Why |
|---|---|---|
| `customers`, `products`, `orders`, `payments` | `DeltaWriter.merge()` — Delta `MERGE INTO` keyed on the table's primary key (`bronze_config.yaml`'s `primary_keys`), `whenMatchedUpdateAll` + `whenNotMatchedInsertAll` | Each source row has a genuinely unique key — a re-run against the same (or updated) source upserts in place instead of duplicating |
| `order_items`, `events` | `DeltaWriter.append_if_new_source()` — skip the whole append if this exact `source_file` is already present in Bronze | Their injected data-quality problems (Problem C, Problem F) produce **intentional same-key duplicates within a single source file** — `order_items`' duplicate rows copy the original's `order_item_id`, and `events`' near-duplicate `event_id`s can even land on the exact same `event_timestamp`. Delta's `MERGE` forbids multiple source rows matching one target row (`DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE`), and pre-deduping the source before merge would silently remove the very duplicates Silver's/Flink's dedup fixes exist to demonstrate — so idempotency has to work at the source-file grain instead of per-row for these two |

Found live, not assumed: the original design used blind `mode="append"` for every table —
re-running `ingest_bronze.py` (or `dp1_bronze`) against the same static demo source endlessly
re-appended a full duplicate copy every time. Surfaced when `build_gold.py`'s new `dim_product`
`PRIMARY KEY` constraint (see `gold/README.md`) failed with a real `UniqueViolation` — Bronze
`products` had accumulated 6x duplication from repeated test runs, which cascaded into Gold.

**Second bug, found live the same way:** `source_file` used to be stamped as the full absolute
path (`str(self.source_dir / "offline" / f"{table}.parquet")`). The same physical file resolves
to a *different* absolute path depending on whether the script runs on the host
(`/mnt/d/fsds-ecommerce/...`) or inside the Airflow container (`/opt/project/...`) — so
`append_if_new_source`'s exact-string comparison never recognized those as the same file.
`order_items` and `events` each ended up ingested twice, once from each context (confirmed live:
`order_items` had grown to 1,818,000 rows, exactly 2× the expected 909,000). Fixed by stamping
`source_file` as a path *relative to* `source_dir` (e.g. `"offline/order_items.parquet"`,
`_relative_source()`) instead — stable regardless of which absolute prefix the script is run
under.

---

## Prerequisites

- WSL2 (Ubuntu) with Python 3.13
- Data generated: `uv run python a_data_generator/generator.py`
- Dependencies installed: `uv sync` from the repo root
- MinIO running (see Step 1 below)

---

## Running the pipeline

**Step 1 — Start**

```bash
docker compose -f infra/docker-compose.yml up -d
```

Verify it is healthy: [http://localhost:9001](http://localhost:9001)
Login: `minio_access_key` / `minio_secret_key`

**Step 2 — Start the Spark History Server** (captures completed-job UI)

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate

export SPARK_HOME=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
mkdir -p /tmp/spark-events
$SPARK_HOME/sbin/start-history-server.sh
```

If you see `HistoryServer running as process XXXX. Stop it first.` — reset it:

```bash
$SPARK_HOME/sbin/stop-history-server.sh && $SPARK_HOME/sbin/start-history-server.sh
```

**Step 3 — Run the pipeline**

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate
uv run python3 b_schema_pipelines/pipelines/bronze/ingest_bronze.py
```

Output is written to `s3a://bronze-data/bronze/` on MinIO.

**UIs**

| UI | URL | Available |
|---|---|---|
| Spark UI (live DAGs, stages, tasks) | http://localhost:4040 | While the job is running |
| Spark History Server (completed jobs) | http://localhost:18080 | After the job finishes |
| MinIO Console (Delta files) | http://localhost:9001 | Once MinIO is started |

---

## Configuration reference

**`b_schema_pipelines/pipelines/pipeline_config.yaml`** — shared across all pipeline stages

| Key | Value | Description |
|---|---|---|
| `minio.endpoint` | `http://localhost:9000` | MinIO S3 API endpoint |
| `minio.access_key` | `minio_access_key` | MinIO access key |
| `minio.secret_key` | `minio_secret_key` | MinIO secret key |
| `layers.bronze.bucket` | `bronze-data` | MinIO bucket for Bronze Delta tables |
| `layers.bronze.prefix` | `bronze` | Key prefix inside the bucket |

**`bronze_config.yaml`** — Bronze-specific quality rules (expected columns, primary keys per table)

---

## Expected output

One JSON line per table is printed to stdout on success, plus structured log entries:

```
=== [0] Waiting for MinIO ===
  MinIO is ready.

=== [1] Creating buckets ===

=== [2] Bronze — Delta table → MinIO ===
{"status": "success", "table": "customers",    "rows": 120000}
{"status": "success", "table": "products",     "rows": 45000}
{"status": "success", "table": "orders",       "rows": 360000}
{"status": "success", "table": "order_items",  "rows": 900000}
{"status": "success", "table": "payments",     "rows": 360000}
{"status": "success", "table": "events",       "rows": ...}
```

Full structured logs (run_id, timings, Delta commit metrics) are written to `logs/bronze/<run_id>.log`.

---

## Silver Transformation Pipeline

**Fuller writeup, including trade-offs, now lives in [`silver/README.md`](../silver/README.md)** —
kept here too since Bronze/Silver have always been documented as one pipeline pair.

Reads Bronze Delta tables from MinIO, applies four targeted fixes for the injected data problems, and writes clean Silver Delta tables back to MinIO.

**Run Bronze first** — Silver reads from `s3a://bronze-data/bronze/`.

### Two modes

Always run **baseline first**, then **optimized**. The Spark UI screenshots from both runs are grading evidence.

| Mode | What it does |
|---|---|
| `baseline` | Raw pass-through — no fixes. Captures Spark UI "before" (skewed tasks, SortMergeJoin) |
| `optimized` | All four fixes applied. Captures Spark UI "after" (balanced tasks, BroadcastHashJoin) |

### Fixes applied in optimized mode

| Fix | Problem | Change |
|---|---|---|
| 1 — AQE skewJoin | A: 85% Ho Chi Minh City | Session config — Spark splits skewed partitions at runtime |
| 2 — NULL fill | B: schema evolution | `coupon_code` NULL → `LEGACY`, `shipping_method` NULL → `UNKNOWN` |
| 3 — Dedup | C: ~2% duplicate rows injected (keep=False measure), ~1% actually removable | Keep earliest `ingest_ts` (tiebreak `order_item_id`) per `(order_id, product_id, unit_price, quantity)` |
| 4 — Broadcast join | A: products join | `SortMergeJoin` → `BroadcastHashJoin` (standalone demo, see note below) |

> **Fix 4 note:** `_fix_broadcast_join` is a standalone demonstration method — call it manually after the optimized run to capture the Spark UI SQL tab screenshot showing exchange bytes drop to 0.

### Running Silver

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate

# Step 1 — run baseline (capture Spark UI before screenshots)
uv run python3 b_schema_pipelines/pipelines/silver/transform_silver.py --mode baseline

# Step 2 — run optimized (capture Spark UI after screenshots)
uv run python3 b_schema_pipelines/pipelines/silver/transform_silver.py --mode optimized
```

`transform_silver.py` no longer takes a `--schema-change-date` flag — it used to be accepted
and stored on `SilverTransformer.schema_change_date`, but `_fix_schema_evolution` never
actually read it. That turned out to be masking a real bug rather than just dead code: NULL
`coupon_code` meant two different things (no coupon used vs. schema didn't have the column
yet), so the unconditional fill was mislabeling ~75-80% of modern no-coupon orders as
`'LEGACY'`. Fixed at the root in the generator instead of by gating Silver's fill on a date —
see [`silver/README.md`](../silver/README.md#trade-offs--things-worth-knowing-before-running-this)
for the full explanation. `a_data_generator/config/generator_config.yaml`'s own
`schema_change_date` (a fraction of the sim window, now persisted and reused across runs — see
`a_data_generator/docs/01_data_generator.md` §Problem B) is what actually controls which rows
get NULLed at generation time.

Output is written to `s3a://silver-data/silver/` on MinIO.

### Expected output (optimized)

```
{"status": "success", "table": "orders",      "rows": 360000}   # rows_in > rows_out not logged here; see .log
{"status": "success", "table": "order_items", "rows": ~900000}  # ~1% deduped from ~909000
{"status": "success", "table": "products",    "rows": 45000}
{"status": "success", "table": "customers",   "rows": 120000}
{"status": "success", "table": "payments",    "rows": 360000}
```

Full structured logs are written to `logs/silver/<run_id>.log`.

# Delete Minio bucket
```bash
# Delete all objects + the bucket bronze-data itself
docker exec fsds-minio mc rb --force local/bronze-data
```

# Stop Spark history server
```bash
$SPARK_HOME/sbin/stop-history-server.sh
rm -rf /tmp/spark-events/*
``` 
