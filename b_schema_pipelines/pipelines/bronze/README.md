# Bronze Ingestion Pipeline

Reads raw source files from `a_data_generator/outputs/` and writes them as Delta Lake tables to the Bronze layer. No transformation — Bronze is a faithful copy of the source plus lineage columns (`ingest_ts`, `source_file`, `pipeline_run_id`).

**Tables ingested:** `customers`, `products`, `orders`, `order_items`, `payments`, `events`

---

## Prerequisites

- WSL2 (Ubuntu) with Python 3.13
- Data generated: `a_data_generator/outputs/offline/*.parquet` and `a_data_generator/outputs/streaming/events.json`
- Dependencies installed: `uv sync` from the repo root

---

## Running locally (writes to disk)

`b_schema_pipelines/pipelines/pipeline_config.yaml` must have:

```yaml
storage:
  backend: local
```

Then in WSL2 (use `python3` directly — `uv run` fails on `/mnt/d/` due to a Rust `getcwd()` issue):

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate
python3 b_schema_pipelines/pipelines/bronze/ingest_bronze.py
```

Output is written to:

```
b_schema_pipelines/delta_lake_data/bronze/
├── customers/
├── products/
├── orders/
├── order_items/
├── payments/
└── events/
```

---

## Running with MinIO (Docker)

**Step 1 — Start MinIO**

```bash
docker compose -f infra/docker-compose.yml up -d minio
```

Verify it is healthy: [http://localhost:9001](http://localhost:9001) (login: `minio_access_key` / `minio_secret_key`)

**Step 2 — Start the Spark History Server** (once, before running the pipeline)

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate

export SPARK_HOME=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
mkdir -p /tmp/spark-events
$SPARK_HOME/sbin/start-history-server.sh
```

If you see `HistoryServer running as process XXXX. Stop it first.` it is already running — skip this step.

**Step 3 — Run the pipeline**

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate
python3 b_schema_pipelines/pipelines/bronze/ingest_bronze.py
```

Output is written to `s3a://bronze-data/bronze/` on MinIO.

**UIs**

| UI | URL | Available |
|---|---|---|
| Spark UI (live DAGs, stages, tasks) | http://localhost:4040 | While the job is running |
| Spark History Server (completed jobs) | http://localhost:18080 | After the job finishes |
| MinIO Console (Delta files) | http://localhost:9001 | Once MinIO is started |

To stop the History Server when done:

```bash
$SPARK_HOME/sbin/stop-history-server.sh
```

---

## Configuration reference

Storage routing is shared across all pipelines — configured in one place:

**`b_schema_pipelines/pipelines/pipeline_config.yaml`**

| Key | Description |
|---|---|
| `storage.backend` | `local` writes to disk; `minio` writes to MinIO via S3A |
| `minio.endpoint` | MinIO endpoint — overridden by `MINIO_ENDPOINT` env var |
| `minio.access_key` | MinIO access key — overridden by `MINIO_ACCESS_KEY` env var |
| `minio.secret_key` | MinIO secret key — overridden by `MINIO_SECRET_KEY` env var |
| `layers.bronze.local_path` | Local output path (used when `backend: local`) |
| `layers.bronze.bucket` | MinIO bucket name (used when `backend: minio`) |
| `layers.bronze.prefix` | Key prefix inside the bucket |

Bronze-specific settings (quality rules, expected columns, primary keys) are in `bronze_config.yaml`.

---

## Expected output

```
[customers] phase=start
[customers] quality gates passed  rows=120000
[customers] delta_commit  version=0  rows=120000  files=...  bytes=...  duration_ms=...
[customers] phase=end  status=ok  rows_in=120000  rows_out=120000  duration_s=...
...
[events] phase=end  status=ok  rows_in=...  rows_out=...  duration_s=...
```

Logs are written to `logs/bronze/<run_id>.log`.
