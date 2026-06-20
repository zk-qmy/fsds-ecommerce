# FSDS E-Commerce MLOps Project — Claude Context

## Project overview

Full-Stack Data Science (FSDS) coursework, ML track (Section 04.1).
Domain: e-commerce purchase prediction.
Goal: build an end-to-end ML system predicting `will_purchase_next_session`
(binary classification) for 120 000 customers across a 180-day history window.

This is a student project following the FSDS coursework structure.
All design decisions must be explicit with trade-offs documented.
The grader checks for runnable code + sample outputs + design write-ups.

---

## Repository structure

```
fsds-ecommerce/
├── a_data_generator/
│   ├── config.yaml
│   ├── generator.py          # DataGenerator class
│   ├── outputs/
│   │   ├── offline/          # customers, products, orders, order_items, payments (.parquet)
│   │   └── streaming/        # events.json (newline-delimited)
│   └── quality_report.txt
├── b_schema_pipelines/
│   ├── pipelines/
│   │   ├── bronze/           # ingest raw → Delta Lake
│   │   ├── silver/           # dedup, clean, schema-evolution fill
│   │   ├── gold/             # dim_*, fact_*, obt_*
│   │   └── features/         # feat_customer_90d, feat_stream_60m, feat_customer_unified
│   ├── dags/                 # Airflow DAGs per pipeline group
│   ├── dq/                   # Great Expectations suites
│   └── docs/
│       └── 02_schema_design.md
├── c_drift_labels/
│   ├── generator_v2.py       # extends 01 with Scenario A drift
│   ├── labels.py             # ml_customer_label builder
│   ├── training_table.py     # point-in-time join → ml_customer_purchase_training
│   └── docs/
│       └── 03_drift_report.md
├── d_ml/
│   ├── design/
│   │   └── 04_ml_design.md   # HLD + LLD (write before coding)
│   ├── src/
│   │   ├── training_data_service.py
│   │   ├── split_service.py
│   │   ├── model_service.py
│   │   ├── scoring_service.py
│   │   └── monitoring_service.py
│   ├── pipelines/
│   │   ├── training_dag.py
│   │   ├── scoring_dag.py
│   │   └── retrain_trigger_dag.py
│   ├── api/
│   │   └── main.py           # FastAPI /score endpoint
│   └── cicd/
│       └── .github/workflows/
│           ├── ci_data_ml.yml
│           ├── cd_inference.yml
│           └── iac.yml
├── tests/
├── infra/
│   ├── docker-compose.yml    # postgres, airflow, mlflow, prometheus, grafana, jaeger
│   ├── terraform/
│   └── ansible/
├── config/
│   ├── logging.py
│   └── settings.py           # Settings class (pydantic-settings)
├── pyproject.toml
├── uv.lock
└── README.md
```

---

## Tech stack

| Layer | Tool |
|---|---|
| Language | Python 3.13 |
| Data generation | numpy, pandas, faker, pyyaml |
| Batch processing | PySpark + Delta Lake |
| Stream processing | Apache Flink |
| Orchestration | Apache Airflow 2.8 |
| Data quality | Great Expectations |
| Feature store | Feast (offline: PostgreSQL, online: Redis) |
| ML model | scikit-learn (LogisticRegression baseline) |
| Experiment tracking | MLflow |
| Data versioning | DVC |
| Inference API | FastAPI + Uvicorn |
| Observability | Prometheus + Grafana + Jaeger + OpenTelemetry |
| Drift detection | Evidently |
| CI/CD | GitHub Actions (3 tracks) |
| IaC | Terraform + Ansible |
| Container | Docker + Docker Compose |
| Database | PostgreSQL 15 (Gold + feature store) |
| Object storage | MinIO (Bronze/Silver Delta Lake) |
| Model registry | MLflow model registry |
| Metadata/lineage | DataHub |

---

## Data model

### Offline tables (Bronze → Silver → Gold)

**Source tables (Section 01 outputs):**
- `customers` — customer_id, signup_ts, country, segment, marketing_opt_in
- `products` — product_id, category, brand, base_price, is_active, created_ts
- `orders` — order_id, customer_id, order_timestamp, order_date, status, shipping_city, shipping_method, coupon_code
- `order_items` — order_item_id, order_id, product_id, quantity, unit_price, discount_amount, line_net_amount
- `payments` — payment_id, order_id, payment_timestamp, payment_method, amount, payment_status, attempt_number

**Gold schema: `gold_ecommerce`**
- `dim_customer` (SCD2) — customer_key, customer_id (BK), segment, country, valid_from_ts, valid_to_ts, is_current
- `dim_product` — product_key, product_id (BK), category, brand, base_price
- `dim_date` — date_key, full_date, year, month, day, is_weekend
- `fact_order` — order_key, customer_key, date_key, order_id, order_net_amount, item_count
- `fact_order_item` — order_item_key, order_key, product_key, quantity, unit_price, line_net_amount
- `fact_payment_attempt` — payment_key, order_key, payment_method, payment_status, amount, attempt_number
- `obt_order_performance` — denormalised wide table for BI

**Feature tables:**
- `feat_customer_90d` — customer_id, event_timestamp, created_ts, f_total_orders_90d, f_avg_order_value_90d, f_distinct_categories_90d, f_payment_fail_rate_90d
- `feat_stream_60m` — customer_id, event_timestamp, created_ts, f_views_30m, f_add_to_cart_30m, f_cart_to_purchase_ratio_60m, f_burst_activity_flag
- `feat_customer_unified` — point-in-time join of above two

**ML tables:**
- `ml_customer_label` — customer_id, event_timestamp, created_ts, label (0/1)
- `ml_customer_purchase_training` — label + all feature columns, used for train/val/test split

**Monitoring tables:**
- `agg_feature_health_daily` — monitoring_date, feature_name, mean_value, psi_vs_baseline, alert_flag
- `feature_drift_alerts` — alert_date, feature_name, psi_value, action
- `ml_customer_scores` — customer_id, score, model_version, score_ts

---

## Injected data problems (Section 01)

These are deliberate. Do not treat them as bugs when you see them in the data.

| Problem | Table | Detail | Downstream handler |
|---|---|---|---|
| A — City skew | orders | 85% shipping_city = 'Ho Chi Minh City' | Silver: no special handling; Gold: partitioned by city |
| B — Schema evolution | orders | coupon_code + shipping_method = NULL before schema_change_date | Silver: fill NULL → 'LEGACY' / 'UNKNOWN' |
| C — Duplicate rows | order_items | 2% rows duplicated by (order_id, product_id, unit_price) | Silver: dedup, keep earliest created_ts |
| D — Burst traffic | events stream | 30× rate at 12:00–12:20 and 20:00–20:20 | Flink: watermarks + backpressure config |
| E — Late arrivals | events stream | 12% events: created_ts delayed 5–45 min after event_timestamp | Flink: AllowedLateness + WatermarkStrategy |
| F — Duplicate events | events stream | 1.5% duplicate event_ids with slight ts shift | Stream dedup: key on event_id + event_timestamp |

---

## ML system design

### Prediction task
- Entity: customer_id
- Label: `will_purchase_next_session` — 1 if customer places any order in (event_timestamp, event_timestamp + 24h]
- Features: 8 columns from feat_customer_unified (4 offline + 4 streaming)
- Model: LogisticRegression(class_weight='balanced') as baseline

### Split strategy
Time-based split only — never random split (prevents future data leakage):
- train: first 70% of timeline
- val: next 15%
- test: last 15%

### 5 HLD decisions
1. **Security** — training data readable only by svc-training service account; inference API uses Bearer token auth; secrets via env vars never hardcoded
2. **Resilience** — 3 retries + exponential backoff on all jobs; scoring_job is partition-idempotent; canary auto-rollback if health check fails at t+5min
3. **Serving pattern** — batch precompute (weekly training + PSI-triggered retrain); scores written to ml_customer_scores table; trade-off: 24h staleness vs operational simplicity
4. **Storage** — Bronze/Silver: Delta Lake on MinIO; Gold/Features: PostgreSQL; model artifacts: MLflow local store; logs: 90-day retention structured JSON
5. **Routing** — FastAPI /score endpoint behind NGINX; versioned via ?model_version=latest; rate limit 100 req/s

### 5 LLD classes
- `TrainingDataService` — reads ml_customer_purchase_training, validates schema, deduplicates by created_ts
- `SplitService` — time-based train/val/test split, enforces no leakage
- `ModelService` — train, evaluate (F1/precision/recall/PR-AUC), save to MLflow, load from registry
- `ScoringService` — score_batch, score_online, write_scores to ml_customer_scores
- `MonitoringService` — publish_model_metrics, compute_psi, trigger_alerts, write to agg_feature_health_daily

### Acceptance threshold
- F1 >= 0.60 on test set to register model
- Candidate must beat production F1 by >= 0.02 to be promoted

---
Looking at the `CLAUDE.md`, it's mentioned in two places but never fully detailed — the HLD decision 5 (Routing) and Track B CI/CD. That's not enough for the grader.

Add this section into the `CLAUDE.md` between the **ML system design** and **Pipeline structure** sections:

---
## Inference serving & routing

### Architecture

```
Client
  │
  ▼
NGINX (reverse proxy + load balancer)   ← port 80/443
  │
  ├── /score        → FastAPI inference service (port 8000)
  ├── /health       → FastAPI health check
  └── /metrics      → Prometheus scrape endpoint (port 9090)
```

### NGINX responsibilities

1. **Reverse proxy** — forwards /score requests to FastAPI upstream
2. **Load balancing** — round-robin across multiple FastAPI replicas
3. **Rate limiting** — 100 req/s per IP (protects model from abuse)
4. **SSL termination** — handles HTTPS, FastAPI only sees HTTP internally
5. **Request buffering** — absorbs traffic spikes before they hit the model

### NGINX config (infra/nginx/nginx.conf)

```nginx
upstream inference_api {
    # round-robin across 2 FastAPI replicas
    server api_1:8000;
    server api_2:8000;

    # mark a replica down after 3 failed health checks
    # bring it back after 1 success
}

server {
    listen 80;

    # rate limiting: 100 req/s, burst up to 20
    limit_req_zone $binary_remote_addr zone=score_limit:10m rate=100r/s;

    location /score {
        limit_req zone=score_limit burst=20 nodelay;
        proxy_pass         http://inference_api;
        proxy_set_header   Host $host;
        proxy_set_header   X-Real-IP $remote_addr;
        proxy_read_timeout 30s;
    }

    location /health {
        proxy_pass http://inference_api/health;
        access_log off;   # don't pollute logs with health check noise
    }
}
```

### Docker Compose wiring (infra/docker-compose.yml)

```yaml
services:
  nginx:
    image: nginx:1.25-alpine
    ports:
      - "80:80"
    volumes:
      - ./nginx/nginx.conf:/etc/nginx/nginx.conf:ro
    depends_on:
      - api_1
      - api_2

  api_1:
    build: ../04_ml/api
    environment:
      - MODEL_VERSION=Production
      - MLFLOW_TRACKING_URI=http://mlflow:5000
    expose:
      - "8000"

  api_2:
    build: ../04_ml/api
    environment:
      - MODEL_VERSION=Production
      - MLFLOW_TRACKING_URI=http://mlflow:5000
    expose:
      - "8000"
```

### Canary deployment flow (Track B CD)

During a model version upgrade, NGINX splits traffic between the
current production replica and the canary replica:

```
Step 1 — deploy canary (10% traffic):
  upstream inference_api {
      server api_prod:8000  weight=9;
      server api_canary:8000 weight=1;
  }

Step 2 — monitor for 5 minutes:
  - p95 latency < 200ms
  - error rate < 1%
  - health check passes

Step 3a — promote (if healthy):
  upstream inference_api {
      server api_canary:8000;   # canary becomes production
  }

Step 3b — rollback (if unhealthy):
  upstream inference_api {
      server api_prod:8000;     # revert immediately
  }
```

The cd_inference.yml GitHub Actions workflow automates steps 1–3
by rewriting the NGINX upstream config and sending `nginx -s reload`.

### HLD trade-off to document in 04_ml_design.md

| Option | Chosen? | Reason |
|---|---|---|
| NGINX round-robin | Yes | Simple, zero extra dependencies, sufficient for coursework scale |
| Istio service mesh | No | Overkill for 2 replicas; adds k8s dependency not justified at this scale |
| AWS ALB / GCP LB | No | Cloud-managed but requires real cloud account and cost |
| KServe autoscaling | No | Production-grade but adds significant operational complexity |

State this explicitly in HLD decision 5 (Routing). The grader
wants to see you know these options exist and made a reasoned choice.
```
---

## Pipeline structure

### Section 02 pipelines (Airflow DAGs)

```
bronze_dag:   ingest raw parquet/json → Delta Lake (append, add metadata)
silver_dag:   bronze → clean/dedup/fill → PostgreSQL staging
gold_dag:     silver → dim/fact/obt → gold_ecommerce schema  (schedule: */15 * * * *)
feature_dag:  gold → feat_customer_90d + feat_stream_60m → feat_customer_unified
```

Each pipeline job must log: run_id, pipeline_name, start_ts, end_ts, status, input_rows, output_rows, error_summary.

Quality gates (fail the run if violated):
- Schema check: expected columns present
- Null check: no nulls in primary keys
- Uniqueness: no duplicate business keys in Gold
- Referential integrity: fact FKs exist in dimensions
- Volume check: output rows within ±30% of baseline

### Section 04 ML pipelines (Airflow DAGs)

```
Pipeline A — training_dag (schedule: 0 2 * * 0 — every Sunday 02:00):
  training_dataset_job → train_job → evaluation_job → model_registry_job

Pipeline B — scoring_dag (schedule: 0 4 * * * — every day 04:00):
  scoring_job → write_scores_job

Pipeline C — retrain_trigger_dag (schedule: 0 6 * * * — every day 06:00):
  retrain_trigger_job → retrain_job → retrain_evaluation_job → promotion_or_rollback_job
```

Retrain triggers:
- PSI > 0.15 sustained 3+ consecutive days on any feature
- Production F1 drops below 0.60 on rolling validation window

---

## CI/CD tracks

### Track A — Pipeline CI/CD (ci_data_ml.yml + cd_data_ml.yml)
- CI: ruff lint → pytest (unit + integration) → DAG import check → schema validation
- CD: deploy training DAG + scoring DAG + retrain trigger DAG on merge to main

### Track B — Inference service CI/CD (ci_inference.yml + cd_inference.yml)
- CI: API contract tests → health check → smoke test (locust 10 req/s)
- CD: docker build → canary deploy (10% traffic) → 5-min health check → promote or rollback

### Track C — IaC CI/CD (iac.yml)
- Stages: dev → staging → production
- Jobs: terraform validate → terraform plan → terraform apply → post-check (services healthy)

---

## Monitoring plan

| Signal | Metric | Tool | Alert threshold |
|---|---|---|---|
| Feature drift | PSI per feature, daily | Evidently → Grafana | PSI > 0.15 |
| Prediction drift | Mean score shift | Prometheus → Grafana | >15% shift from 7-day baseline |
| Label drift | Rolling conversion rate | SQL → Grafana | >20% drop from baseline |
| Inference latency | p95 request time | Prometheus | >200ms |
| Pipeline health | Job success/failure count | Airflow → Prometheus | Any failure |
| Model quality | F1 on rolling val window | MLflow → Grafana | F1 < 0.60 |

Observability stack: OpenTelemetry SDK instruments FastAPI → Jaeger (traces), Prometheus (metrics), ELK (logs).

---

## Key constraints and rules

**Point-in-time correctness is mandatory.**
When joining features to labels, features must use only data available at or before event_timestamp.
Any SQL join without `f.event_timestamp <= l.event_timestamp` is a data leakage bug.

**All pipeline jobs must be idempotent.**
Re-running a job must not produce duplicate rows. Use INSERT ... ON CONFLICT DO UPDATE for PostgreSQL writes.

**Never commit secrets.**
All credentials go in .env (gitignored). Load via python-dotenv or pydantic-settings Settings class.

**Design doc before code in every section.**
Write the .md first. Implementation must match what is documented.

**Every section needs run instructions.**
The grader must be able to run `pip install -r requirements.txt && python <entry_point>` and see output.

---

## Grading evidence checklist

Before submitting each section, verify:
- [ ] .md design document complete with explicit trade-offs
- [ ] All code runnable from a fresh clone
- [ ] Sample outputs committed (parquet previews, screenshots, logs)
- [ ] README has install + run instructions + expected output
- [ ] Quality report shows each injected data problem at target rate (Section 01)
- [ ] DataHub lineage screenshot (Section 02)
- [ ] PSI escalation evidence after drift_start_date (Section 03)
- [ ] MLflow run screenshot with F1 metric (Section 04)
- [ ] Grafana dashboard screenshot (Section 04)
- [ ] CI/CD pipeline green log screenshot (Section 04)
- [ ] All 5 grading evidence checklist items from coursework_proposal.md satisfied

---

## Generator configuration reference

Key config fields and what they control:

```yaml
schema_change_date: "2026-03-01"   # ~50% of history falls before this → NULL coupon/shipping
avg_orders_per_customer: 3.0       # Poisson λ for order count per customer
avg_items_per_order: 2.5           # Poisson λ for items per order
marketing_opt_in_rate: 0.70        # used as rng.random(n) < cfg["marketing_opt_in_rate"]
duplicate_rate_offline: 0.02       # fraction of order_items rows to duplicate
duplicate_rate_stream: 0.015       # fraction of stream events to re-emit
late_arrival_rate: 0.12            # fraction of events with created_ts > event_timestamp
burst_multiplier: 30               # rate multiplier during burst windows
```

Distribution fields use `"auto"` for Dirichlet-sampled weights and explicit floats for fixed weights.
Example: `city_distribution: {Ho Chi Minh City: 0.85, Hanoi: auto, ...}`

---

## Common commands

```bash
# Start local platform
docker compose -f infra/docker-compose.yml up -d

# Run data generator (full)
python 01_data_generator/generator.py

# Run generator (skip stream, fast dev loop)
python 01_data_generator/generator.py --skip-stream

# Run all tests
pytest --cov=src tests/ -v

# Lint
ruff check .

# Trigger Airflow DAG manually
airflow dags trigger gold_pipeline

# Run training pipeline manually
airflow dags trigger ml_training_pipeline

# Check MLflow UI
open http://localhost:5000

# Check Grafana
open http://localhost:3000
```
