# Bronze → Silver Pipelines

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
        ▼  DeltaWriter  (common/delta_writer.py — append mode, mergeSchema=true)
        │
s3a://bronze-data/bronze/<table>/   ← Delta Lake on MinIO
```

`DeltaWriter` is shared with the Silver pipeline via `b_schema_pipelines/pipelines/common/delta_writer.py`.

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
| 3 — Dedup | C: 2% duplicate rows | Keep earliest `created_ts` per `(order_id, product_id, unit_price)` |
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

Optional — override the schema change date (default `2026-03-24`):

```bash
uv run python3 b_schema_pipelines/pipelines/silver/transform_silver.py \
    --mode optimized \
    --schema-change-date 2026-03-01
```

Output is written to `s3a://silver-data/silver/` on MinIO.

### Expected output (optimized)

```
{"status": "success", "table": "orders",      "rows": 360000}   # rows_in > rows_out not logged here; see .log
{"status": "success", "table": "order_items", "rows": ~890000}  # ~2% deduped from ~909000
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
