# fsds-ecommerce

Full-stack data science + MLOps system for e-commerce, ML track (Section 04.1).

**The problem:** a personalised homepage feed. For each of 120,000 customers, rank the
products they're most likely to click or buy next — not "will this customer buy
*something*," but "which specific products should we show them."

| | |
|---|---|
| **Entity** | `(customer_id, product_id)` candidate pair |
| **Label** | `will_engage_with_product` — click or purchase within 24h of `event_timestamp` |
| **Candidates** | customer's historical-purchase categories ∪ top-K popular products, capped at 50/customer — same policy used at training time and serving time, to avoid train/serve skew |
| **Features** | 16 columns per candidate pair — 8 customer-side (90-day offline + 60-min streaming), 4 product-side (90-day), 4 customer×product interaction (90-day) |
| **Model** | `LogisticRegression(class_weight='balanced')` — a pointwise scorer that ranks candidates per customer |
| **Cold start** | new customer → candidates fall back 100% to popularity; new product → zero-interaction features, excluded from the popularity fallback until it has real purchases |

Full design: [`CLAUDE.md`](CLAUDE.md) (ML system design section); `d_ml/design/04_ml_design.md`
is Section 04's formal design doc, not yet written. The feature pipeline that builds this
model's training data lives in Section 02 (below) — see
[`b_schema_pipelines/docs/02_schema_piplines.md`](b_schema_pipelines/docs/02_schema_piplines.md)
§9 for exactly how candidate generation and cold start are implemented.

---

## Table of Contents

- [Setup](#setup)
- [Local Services & Ports](#local-services--ports)
- [Section 01 — Data Generator](#section-01--data-generator)
- [Section 02 — Schema Pipelines](#section-02--schema-pipelines)
- [Known Follow-ups](#known-follow-ups)
- [Repository Structure](#repository-structure)
- [Git Convention](#git-convention)

**Rubric compliance**: [`docs/new-plan.md`](docs/new-plan.md) has the full item-by-item audit
against `coursework/rubrics.md`'s Data Platform track (what's done, what's proof-incomplete,
what's not started). Each pipeline stage's own README below also carries a "Rubric proof
checklist" section scoped to just that stage. [`docs/novel_ideas.md`](docs/novel_ideas.md)
covers the two Novel Ideas line items.

---

## Setup

Requires Python 3.13 and [uv](https://github.com/astral-sh/uv).

```bash
git clone <repo-url>
cd fsds-ecommerce
uv sync
source .venv/bin/activate
```

---

## Local Services & Ports

Started via `docker compose -f infra/docker-compose.yml up -d`, plus the Spark UIs that come up while running Section 02 pipelines.

| Service | URL | Port(s) | Login | Notes |
|---|---|---|---|---|
| [MinIO Console](http://localhost:9001) | http://localhost:9001 | 9001 (console), 9000 (S3 API) | `minio_access_key` / `minio_secret_key` | Bronze/Silver Delta Lake object storage |
| [Trino Web UI](http://localhost:8080) | http://localhost:8080 | 8080 | — | Query engine, JDBC on same port — two catalogs: `delta` (Bronze/Silver on MinIO, via Hive Metastore) and `postgres` (Gold + feature tables), so it can join across both in one query |
| Hive Metastore | `thrift://localhost:9083` | 9083 | — | Internal — Trino's catalog connection, no web UI |
| Hive Metastore DB | `localhost:5433` | 5433→5432 | `hive` / `hive` | Postgres backing the metastore, not the Gold DB |
| PostgreSQL (Gold + features) | `localhost:5432` | 5432 | `fsds` / `fsds` | `gold_ecommerce` schema, DB `fsds` |
| [Spark UI](http://localhost:4040) | http://localhost:4040 | 4040 | — | Live DAGs/stages — only while a pipeline job is running |
| [Spark History Server](http://localhost:18080) | http://localhost:18080 | 18080 | — | Completed job UIs — start with `$SPARK_HOME/sbin/start-history-server.sh` |
| [Airflow webserver](http://localhost:8081) | http://localhost:8081 | 8081→8080 (8080 taken by Trino) | `admin` / `admin` (fixed local-dev login — pre-created before `airflow standalone` starts, see `infra/docker-compose.yml`'s `airflow` service comment) | DP1/DP2/DP3 DAG orchestration — see [`b_schema_pipelines/dags/plan.md`](b_schema_pipelines/dags/plan.md) |

Commented out in `infra/docker-compose.yml` by default (uncomment + `docker compose up -d <service>` to enable):

| Service | Port | Notes |
|---|---|---|
| Redis | 6379 | Online feature store (Section 04) |
| MLflow | 5000 | Experiment tracking / model registry (Section 04) |

**Browsing Postgres with a GUI** — connect [DBeaver](https://dbeaver.io/) (or any PostgreSQL client):

- Open the DBeaver app (desktop)
- **Database** tab → **New Database Connection**
- Choose **PostgreSQL**
- Enter the connection info:
  - Host: `localhost`
  - Port: `5432`
  - Database: `fsds`
  - User / password: `fsds` / `fsds`
- Click **Test Connection**, then **Finish**
- Right-click the `gold_ecommerce` schema → **View Diagram** for an ER diagram

![gold_ecommerce ER diagram](assets/gold-schema.png)

---

## Section 01 — Data Generator

Generates all synthetic source data consumed by downstream sections.

**Design doc:** [`a_data_generator/docs/01_data_generator.md`](a_data_generator/docs/01_data_generator.md)

### Run

```bash
# Full generation — offline tables + streaming events (~3 min at 120k customers)
uv run python a_data_generator/generator.py

# Offline tables only (faster dev loop)
uv run python a_data_generator/generator.py --skip-stream
```

### Outputs

| Path | Format | Description |
|---|---|---|
| `a_data_generator/outputs/offline/customers.parquet` | Parquet | 120,000 customers |
| `a_data_generator/outputs/offline/products.parquet` | Parquet | 45,000 products |
| `a_data_generator/outputs/offline/orders.parquet` | Parquet | ~360,000 orders |
| `a_data_generator/outputs/offline/order_items.parquet` | Parquet | ~909,000 line items (incl. 2% dups) |
| `a_data_generator/outputs/offline/payments.parquet` | Parquet | ~360,000 payments |
| `a_data_generator/outputs/streaming/events.json` | NDJSON | ~262,000 clickstream events |
| `a_data_generator/outputs/quality_report.txt` | Text | Injected-problem evidence report |

### Injected data problems

| ID | Table | Problem | Target rate | Actual (seed 42) |
|---|---|---|---|---|
| A | orders | 85% shipping_city = Ho Chi Minh City | 85% | 85.1% |
| B | orders | coupon_code + shipping_method NULL before schema_change_date | 100% old-partition | 100% |
| C | order_items | 2% duplicate rows by natural key | 2.0% | 2.0% |
| D | events | 30x burst rate at 12:00-12:20 and 20:00-20:20 | ~3,000 ev/min peak | 3,158 ev/min |
| E | events | 12% late arrivals (created_ts delayed 5-45 min) | 12% | 12.0% |
| F | events | 1.5% duplicate event_ids | 1.5% | 1.5% |

Full evidence table in [`a_data_generator/outputs/quality_report.txt`](a_data_generator/outputs/quality_report.txt).

### Config

All parameters are in [`a_data_generator/config/generator_config.yaml`](a_data_generator/config/generator_config.yaml).
Key fields:

```yaml
n_customers: 120000
random_seed: 42              # reproducible across runs
schema_change_date: 0.5            # Problem B cutoff -- fraction of the sim window, not a
                                    # fixed date (the window itself slides with real time);
                                    # resolved date is persisted and reused across runs (main())
duplicate_rate_offline: 0.02       # Problem C
burst_multiplier: 30               # Problem D
late_arrival_rate: 0.12            # Problem E
duplicate_rate_stream: 0.015       # Problem F
```

---

## Section 02 — Schema Pipelines

Bronze → Silver → Gold → Feature pipelines. Reads Section 01 outputs, lands them as Delta Lake tables (Bronze/Silver, on MinIO), builds the Kimball star schema (Gold, on PostgreSQL), then computes the feature tables the homepage-recommendation model (above) trains and serves on — customer-side, product-side, customer×product interaction, and the final candidate-generation join — ready for Feast.

**Design docs:**
- [`b_schema_pipelines/docs/02_schema_piplines.md`](b_schema_pipelines/docs/02_schema_piplines.md) — schema design
- [`b_schema_pipelines/docs/02_spark_optimisation_report.md`](b_schema_pipelines/docs/02_spark_optimisation_report.md) — Spark optimisation before/after
- [`b_schema_pipelines/pipelines/bronze/README.md`](b_schema_pipelines/pipelines/bronze/README.md) — Bronze run guide
- [`b_schema_pipelines/pipelines/silver/README.md`](b_schema_pipelines/pipelines/silver/README.md) — Silver run guide, fixes, trade-offs
- [`b_schema_pipelines/pipelines/gold/README.md`](b_schema_pipelines/pipelines/gold/README.md) — Gold run guide
- [`b_schema_pipelines/dq/README.md`](b_schema_pipelines/dq/README.md) — Great Expectations data-quality suite factories
- [`b_schema_pipelines/dags/plan.md`](b_schema_pipelines/dags/plan.md) — Airflow DAG design (DP1/DP2/DP3), CI wiring

### Run

```bash
cd /mnt/d/fsds-ecommerce
source .venv/bin/activate

# Step 0 — start MinIO/Trino/Postgres (see Local Services & Ports)
docker compose -f infra/docker-compose.yml up -d

# Step 0b — one-time: create the Gold schema in Postgres
docker exec -it fsds-postgres psql -U fsds -d fsds -c "CREATE SCHEMA IF NOT EXISTS gold_ecommerce;"

# Step 0c — start the Spark History Server (captures completed-job UI)
export SPARK_HOME=$(python3 -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
mkdir -p /tmp/spark-events
$SPARK_HOME/sbin/start-history-server.sh
# If you see "HistoryServer running as process XXXX. Stop it first." — reset it:
#   $SPARK_HOME/sbin/stop-history-server.sh && $SPARK_HOME/sbin/start-history-server.sh

# Step 1 — Bronze: raw parquet/NDJSON → Delta Lake (MinIO)
uv run python3 b_schema_pipelines/pipelines/bronze/ingest_bronze.py

# Step 2 — Silver: dedup, NULL-fill, skew/broadcast join fixes
uv run python3 b_schema_pipelines/pipelines/silver/transform_silver.py --mode baseline
uv run python3 b_schema_pipelines/pipelines/silver/transform_silver.py --mode optimized

# Step 3 — Gold: dim/fact/OBT star schema → PostgreSQL gold_ecommerce
uv run python3 b_schema_pipelines/pipelines/gold/build_gold.py --mode baseline
uv run python3 b_schema_pipelines/pipelines/gold/build_gold.py --mode optimized

# Step 4 — Features: rolling 90d + 60m aggregations → Feast-ready tables
uv run python b_schema_pipelines/pipelines/features/feat_customer_90d.py

# Step 4b — Flink: cleans/dedupes/watermarks the event stream (own Python 3.12 env —
# see b_schema_pipelines/pipelines/streaming/README.md)
uv run --no-project --python 3.12 --with apache-flink python3 \
b_schema_pipelines/pipelines/streaming/offline_stream_pipeline.py --mode optimized

uv run python b_schema_pipelines/pipelines/features/feat_stream_60m.py \
    --events-source b_schema_pipelines/streaming_data/flink_clean_events/optimized

# Step 5 — Unified: point-in-time (as-of) join of the two feature tables above
uv run python b_schema_pipelines/pipelines/features/feat_customer_unified.py

# Step 6 — Product-side features (view/purchase counts, category avg price, recency)
uv run python b_schema_pipelines/pipelines/features/feat_product_90d.py

# Step 7 — Customer x product interaction features (sparse: only pairs with
# real view/cart/purchase history get a row)
uv run python b_schema_pipelines/pipelines/features/feat_customer_product_interaction.py

# Step 8 — Homepage candidate generation + final join. Depends on steps 5-7
# above already having run for the same day. This is the table the
# recommendation model (see top of this README) actually trains/serves on.
uv run python b_schema_pipelines/pipelines/features/feat_homepage_unified.py
```

Steps 6–8 are also wired into `dp3_feature_dag` below as `feat_product_90d`,
`feat_customer_product_interaction`, and `feat_homepage_unified` — the commands above are for
running them standalone (e.g. backfills, local debugging) outside of Airflow.

### Orchestration (Airflow)

All 8 steps above are wired into three Airflow DAGs — `dp1_bronze` (ingest → validate),
`dp2_gold` (Silver → validate → Gold → validate), `dp3_feature` (two parallel branches — Flink →
feat_customer_90d/feat_stream_60m → feat_customer_unified; Gold → feat_product_90d/
feat_customer_product_interaction — converging at feat_homepage_unified → validate), chained via
`ExternalTaskSensor`. Full design, task graphs, and one-time Connections/Variables setup:
[`b_schema_pipelines/dags/plan.md`](b_schema_pipelines/dags/plan.md).

```bash
# Start Airflow (part of the main compose file — see Local Services & Ports)
docker compose -f infra/docker-compose.yml up -d airflow

# Wait for it to become healthy before doing anything else
until curl -s -o /dev/null -w "%{http_code}" http://localhost:8081/health | grep -q 200; do sleep 5; done
echo "Airflow is up"
```

UI: **http://localhost:8081** — login `admin` / `admin`. Unpause all three DAGs in the UI (or
`airflow dags unpause dp1_bronze/dp2_gold/dp3_feature`) before triggering anything below.

#### ⚠️ Triggering manually — do NOT just click "Trigger DAG" on all three

Each downstream DAG's `wait_for_*` task is an `ExternalTaskSensor` with a fixed
`execution_delta` — it looks for an upstream run at **exactly** its own logical date minus
that offset (`dp2_gold` → `dp1_bronze`, `execution_delta=timedelta(hours=1)`; `dp3_feature` →
`dp2_gold`, `execution_delta=timedelta(hours=1, minutes=30)`). Clicking "Trigger DAG" on each
one separately gives every run a logical date of "whenever you clicked," a few seconds apart —
which is **never** exactly 1h / 1h30m apart. The sensor then waits its full 30-minute timeout
and fails, every time, no matter how long you wait. (Found live debugging this exact symptom —
see `dags/plan.md`'s "Correction" note in §16 for the full story.)

**Correct way — trigger with explicit, matched logical dates, and don't move to the next DAG
until the previous one has actually finished** (Bronze alone has taken anywhere from ~5 to
~80 minutes in practice, so don't assume a fixed wait — poll for real state):

```bash
AF="docker compose -f infra/docker-compose.yml exec airflow airflow"

wait_for_dag() {
  local dag_id=$1 logical_date=$2
  while true; do
    state=$($AF dags state "$dag_id" "$logical_date" 2>&1 | tail -1 | tr -d '\r')
    echo "$(date -u +%H:%M:%S) $dag_id state: $state"
    [ "$state" = "success" ] && return 0
    [ "$state" = "failed" ] && return 1
    sleep 30
  done
}

# Base logical date 4h in the past (must be in the past -- Airflow won't start
# a future-dated manual run) -- T2/T3 offset to exactly match each sensor's execution_delta
T1=$(date -u -d '-4 hours'                        +"%Y-%m-%dT%H:%M:%S+00:00")
T2=$(date -u -d '-4 hours +1 hour'                 +"%Y-%m-%dT%H:%M:%S+00:00")
T3=$(date -u -d '-4 hours +2 hours +30 minutes'    +"%Y-%m-%dT%H:%M:%S+00:00")

$AF dags trigger dp1_bronze -e "$T1"
wait_for_dag dp1_bronze "$T1" || { echo "Bronze failed -- check logs before continuing"; exit 1; }

$AF dags trigger dp2_gold -e "$T2"
wait_for_dag dp2_gold "$T2" || { echo "Gold failed -- check logs before continuing"; exit 1; }

$AF dags trigger dp3_feature -e "$T3"
wait_for_dag dp3_feature "$T3" || { echo "Features failed -- check logs before continuing"; exit 1; }

echo "All three DAGs succeeded."
```

**Simpler alternative for routine (non-smoke-test) use:** don't manually trigger at all — just
unpause all three DAGs and let the **scheduler's own** daily-scheduled runs fire them
(`dp1_bronze` 00:00 → `dp2_gold` 01:00 → `dp3_feature` 02:30, Asia/Ho_Chi_Minh). Scheduled runs'
logical dates are computed by Airflow itself from each DAG's `schedule`, so they land exactly
on the required offsets automatically — this is the one case where the sensors "just work"
without any manual timestamp math.

**Verified working** — all three DAGs have run green end-to-end this way (confirmed via
CLI/API, on both DAG-graph-shape tests (`tests/dags/test_dags.py`) and a live cluster run —
see [`b_schema_pipelines/dags/plan.md`](b_schema_pipelines/dags/plan.md)'s "Status" note at the
end of §16 for the full account). What's still missing for full rubric credit is the literal
Airflow UI screenshot ("stages and their order") — the DAG **Graph view** satisfies this per
the rubric's exact wording, not the DAGs-list view.

### Outputs

| Layer | Storage | Location |
|---|---|---|
| Bronze | Delta Lake | `s3a://bronze-data/bronze/<table>/` on MinIO |
| Silver | Delta Lake | `s3a://silver-data/silver/<table>/` on MinIO |
| Gold | PostgreSQL | `gold_ecommerce.{dim_*, fact_*, obt_order_performance}` |
| Features | PostgreSQL | `feat_customer_90d`, `feat_stream_60m`, `feat_customer_unified`, `feat_product_90d`, `feat_customer_product_interaction_90d`, `feat_homepage_unified` |
| Flink clean stream | NDJSON | `b_schema_pipelines/streaming_data/flink_clean_events/<mode>/` (local disk) |
| Logs | Text | `logs/{bronze,silver,gold,feat_90d,feat_60m,feat_unified,feat_product_90d,feat_customer_product_interaction_90d,feat_homepage_unified}/<run_id>.log` |

See [Local Services & Ports](#local-services--ports) for MinIO/Postgres/Spark UI access.

**ER diagram** — `gold_ecommerce` dims/facts/OBT plus the `feat_*` feature tables (DBeaver → right-click schema → **View Diagram**):

![gold_ecommerce + feature tables ER diagram](assets/database.png)

### Cleanup

```bash
# Delete all objects + the bucket bronze-data itself
docker exec fsds-minio mc rb --force local/bronze-data

# Stop the Spark History Server
$SPARK_HOME/sbin/stop-history-server.sh
rm -rf /tmp/spark-events/*
```

---

## Known Follow-ups

Not yet addressed — noted here rather than silently left inconsistent:

- **`a_data_generator/outputs/` (committed `quality_report.txt` + parquet files) are stale.**
  They predate two generator fixes: `schema_change_date` moving from a fixed calendar date to a
  fraction of the sim window (`0.5`), and `coupon_code` using an explicit `"NONE"` sentinel for
  "no coupon used" instead of raw NULL, so NULL means only "legacy" (see
  `a_data_generator/docs/01_data_generator.md` §8.3 / §Problem B). Still internally consistent
  (they document what those historical runs produced), but a fresh
  `uv run python a_data_generator/generator.py` would regenerate them against the current config.

- **`pipeline_config.yaml`'s `postgres.user`/`postgres.password` are plaintext (`fsds`/`fsds`), not
  Vault-sourced.** `host`/`port`/`db` are overridable via `POSTGRES_HOST`/`POSTGRES_PORT`/`POSTGRES_DB`
  env vars (`pipeline_base.py`, same pattern as `MINIO_ENDPOINT`) since those are topology, not
  secrets — but the credentials themselves are still read straight from the committed YAML by every
  script that opens a raw `psycopg2` connection (`build_gold.py`, `feat_*.py`). Fine for local dev
  (matches how the MinIO credentials already work), but violates this project's own "secrets come
  from Vault, never hardcoded" rule and must be fixed — routed through `config/settings.py`'s
  pydantic-settings + Vault Agent Injector — before any real deployment.

- **`feat_homepage_unified.py` OOMs (`java.lang.OutOfMemoryError: Java heap space`) against real
  data.** Confirmed live 2026-07-29: `_candidate_set`'s category-candidate join
  (`pipelines/features/feat_homepage_unified.py`) joins every customer's touched category
  straight to `dim_product` on `category`, materializing one row per (customer, product) in that
  category *before* any cap is applied. `electronics` alone has 36,084 of the 45,000 products
  (80% category skew, `generator_config.yaml`'s `skew_ratio_category`), and 114,472 customers had
  interaction rows on the snapshot date checked — most of those very likely touch `electronics`
  given its catalog share, so this join's intermediate result is plausibly in the billions of
  rows before `_cap_rank` ever trims it to `MAX_CANDIDATES`. `PipelineBase` also never sets
  `spark.driver.memory`/`spark.executor.memory` (Spark's 1g default), which doesn't help. Needs
  the join restructured to cap candidates per (customer, category) *before* joining to the full
  category product list, not after — not yet fixed.

---

## Repository Structure

Reflects what's actually implemented today. `c_drift_labels/` (Section 03) and most of `d_ml/` (Section 04) are scaffolded directories, not yet built out.

```
fsds-ecommerce/
├── a_data_generator/            # Section 01 — synthetic data generator
│   ├── generator.py
│   ├── config/generator_config.yaml
│   ├── docs/01_data_generator.md
│   └── outputs/                 # offline/ + streaming/ (generated, not committed)
├── b_schema_pipelines/          # Section 02 — Bronze/Silver/Gold/Feature pipelines
│   ├── pipelines/
│   │   ├── bronze/               # ingest_bronze.py + README.md
│   │   ├── silver/                # transform_silver.py
│   │   ├── gold/                    # build_gold.py + README.md
│   │   ├── features/              # feat_customer_90d.py, feat_stream_60m.py, feat_customer_unified.py,
│   │   │                          # feat_product_90d.py, feat_customer_product_interaction.py, feat_homepage_unified.py
│   │   ├── streaming/               # offline_stream_pipeline.py + README.md (own Python 3.12 env)
│   │   ├── common/                 # delta_writer.py
│   │   └── pipeline_config.yaml    # shared MinIO/Postgres/Delta config
│   ├── dags/                     # Airflow DAGs — dp1_bronze/dp2_gold/dp3_feature + plan.md
│   ├── dq/                       # Great Expectations suites + validation_runner.py (DAG-facing)
│   └── docs/                     # 02_schema_piplines.md, 02_spark_optimisation_report.md
├── c_drift_labels/               # Section 03 — drift injection + ML labels (scaffolded)
├── d_ml/                         # Section 04 — ML system (partially scaffolded, not verified in this pass)
│   ├── api/                      # FastAPI inference + drift-detection service stubs
│   ├── design/ pipelines/ src/ cicd/
├── infra/
│   ├── docker-compose.yml        # local dev stack (MinIO, Trino, Postgres, Airflow)
│   ├── airflow/                  # Airflow Dockerfile (apache-airflow 2.10.5, own Python 3.12 env)
│   ├── terraform/                # GKE cluster + VPC + IAM (planned)
│   └── ansible/                  # CI runner config (planned)
├── tests/
│   ├── a_data_generator/         # Section 01 tests
│   ├── b_schema_pipelines/       # Section 02 tests (Bronze/Silver/Gold/Features/validation_runner)
│   └── dags/                     # DAG import/task-graph tests (own ephemeral py3.12/airflow env)
├── config/
│   ├── settings.py
│   └── logging.py
├── docs/                         # cross-cutting design notes (e.g. docker_optimize.md)
├── coursework/                   # coursework proposal + implementation plan
└── pyproject.toml
```

---

## Git Convention

```
feature/<name>
       |
       v
    develop
       |
       v
      main
```

Branch off `develop` for all feature work:

```bash
git checkout develop && git pull
git checkout -b feature/<name>
```

### CI check before push

Matches [`.github/workflows/ci.yml`](.github/workflows/ci.yml)'s two jobs — run both before pushing:

```bash
# lint-and-test job
uv run ruff check . && uv run pytest tests/ -v

# dag-tests job — separate ephemeral env, apache-airflow isn't in the main venv (dags/plan.md §3)
uv run --no-project --python 3.12 \
  --with apache-airflow==2.10.5 \
  --with apache-airflow-providers-postgres==6.4.1 \
  --with pytest \
  python3 -m pytest tests/dags/test_dags.py -v
```
