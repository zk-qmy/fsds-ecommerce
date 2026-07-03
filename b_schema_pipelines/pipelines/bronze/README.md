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

`b_schema_pipelines/pipelines/pipeline_config.yaml` must have:

```yaml
storage:
  backend: minio
```

Start the services and run inside the container:

```bash
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml exec spark \
  python3 -m b_schema_pipelines.pipelines.bronze.ingest_bronze
```

Output is written to `s3a://bronze-data/bronze/` on MinIO. View results at [http://localhost:9001](http://localhost:9001) (login: `minio_access_key` / `minio_secret_key`).

The container sets `MINIO_ENDPOINT=http://minio:9000` automatically — no extra env var needed.

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
