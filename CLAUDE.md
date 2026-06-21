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
│   ├── config/
│   │   └── generator_config.yaml
│   ├── generator.py              # DataGenerator class
│   ├── outputs/
│   │   ├── offline/              # customers, products, orders, order_items, payments (.parquet)
│   │   └── streaming/            # events.json (newline-delimited)
│   └── quality_report.txt
├── b_schema_pipelines/
│   ├── pipelines/
│   │   ├── bronze/               # ingest raw → Delta Lake
│   │   ├── silver/               # dedup, clean, schema-evolution fill
│   │   ├── gold/                 # dim_*, fact_*, obt_*
│   │   └── features/             # feat_customer_90d, feat_stream_60m, feat_customer_unified
│   ├── dags/                     # Airflow DAGs per pipeline group
│   ├── dq/                       # Great Expectations suites
│   └── docs/
│       └── 02_schema_design.md
├── c_drift_labels/
│   ├── generator_v2.py           # extends Section 01 with Scenario A drift
│   ├── labels.py                 # ml_customer_label builder
│   ├── training_table.py         # point-in-time join → ml_customer_purchase_training
│   └── docs/
│       └── 03_drift_report.md
├── d_ml/
│   ├── design/
│   │   └── 04_ml_design.md       # HLD + LLD — write before coding
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
│   │   ├── main.py               # FastAPI /score endpoint
│   │   └── Dockerfile
│   └── k8s/                      # raw K8s manifests (templated by Helm)
│       ├── deployment.yaml
│       ├── service.yaml
│       ├── ingress.yaml
│       ├── hpa.yaml
│       └── deployment-canary.yaml
├── tests/
│   ├── unit/
│   └── integration/
├── infra/
│   ├── docker-compose.yml        # local dev: postgres, airflow, mlflow, minio, redis
│   ├── helm/
│   │   ├── inference-api/        # Chart.yaml, values*.yaml, templates/
│   │   ├── airflow/
│   │   ├── monitoring/
│   │   ├── postgresql/
│   │   ├── redis/
│   │   ├── minio/
│   │   ├── mlflow/
│   │   ├── datahub/
│   │   └── feast/
│   ├── terraform/
│   │   ├── main.tf
│   │   ├── variables.tf
│   │   ├── outputs.tf
│   │   └── modules/
│   │       ├── gke/
│   │       ├── vpc/
│   │       ├── iam/
│   │       ├── storage/
│   │       └── postgres/
│   ├── ansible/
│   │   ├── inventory/
│   │   │   ├── dev.ini
│   │   │   └── prod.ini
│   │   ├── playbooks/
│   │   │   ├── install_kubectl.yml
│   │   │   ├── install_helm.yml
│   │   │   └── configure_gcloud.yml
│   │   └── roles/
│   │       └── k8s_tools/
│   └── scripts/
│       └── check_canary_health.sh
├── .github/
│   └── workflows/
│       ├── ci_data_ml.yml
│       ├── cd_data_ml.yml
│       ├── ci_inference.yml
│       ├── cd_inference.yml
│       └── iac.yml
├── config/
│   ├── logging.py
│   └── settings.py               # Settings class (pydantic-settings)
├── pyproject.toml
├── uv.lock
└── README.md
```

---

## Tech stack

| Layer | Tool |
|---|---|
| Language | Python 3.13 |
| Package manager | uv |
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
| Container | Docker + Docker Compose (local dev only) |
| Deployment | Kubernetes (GKE) + Helm |
| IaC | Terraform (cluster) + Ansible (CI runner) |
| Database | PostgreSQL 15 (Gold + feature store) |
| Object storage | MinIO / GCS (Bronze/Silver Delta Lake) |
| Model registry | MLflow model registry |
| Metadata/lineage | DataHub |

---

## Data model

### Offline tables (Bronze → Silver → Gold)

**Source tables (Section 01 outputs):**
- `customers` — customer_id, signup_ts, country, segment, marketing_opt_in
- `products` — product_id, category, brand, base_price, is_active, created_ts
- `orders` — order_id, customer_id, order_timestamp, status, shipping_city, shipping_method, coupon_code
- `order_items` — order_item_id, order_id, product_id, quantity, unit_price, discount_amount
- `payments` — payment_id, order_id, payment_timestamp, payment_method, amount, payment_status

**Gold schema: `gold_ecommerce`**
- `dim_customer` (SCD2) — customer_key, customer_id (BK), signup_ts, segment, country, marketing_opt_in, valid_from_ts, valid_to_ts, is_current
- `dim_product` — product_key, product_id (BK), category, brand, base_price, is_active, created_ts
- `dim_date` — date_key, calendar_date, day_of_week, month, year, is_weekend
- `dim_payment_method` — payment_method_key (SK), payment_method (name)
- `dim_order_status` — order_status_key (SK), order_status (name)
- `fact_order` — order_key, customer_key, order_date_key, order_status_key, order_id, order_gross_amount, order_discount_amount, order_net_amount, item_count
- `fact_order_item` — order_item_key, order_key, product_key, quantity, unit_price, discount_amount, line_net_amount
- `fact_payment_attempt` — payment_key, order_key, payment_date_key, payment_method_key, amount, is_payment_success, is_payment_failed
- `obt_order_performance` — denormalised wide table for BI (order_id, customer_id, order_timestamp, country, segment, total_quantity, order_net_amount, payment_status_last, shipping_city, coupon_code)

**Feature tables:**
- `feat_customer_90d` — customer_id, event_timestamp, created_ts, f_customer_total_orders_90d, f_customer_avg_order_value_90d, f_customer_distinct_categories_90d, f_customer_payment_fail_rate_90d
- `feat_stream_60m` — customer_id, event_timestamp, created_ts, f_stream_views_30m, f_stream_add_to_cart_30m, f_stream_cart_to_purchase_ratio_60m, f_stream_burst_activity_flag
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
| C — Duplicate rows | order_items | 2% rows duplicated by (order_id, product_id, quantity, unit_price) | Silver: dedup, keep earliest created_ts |
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
1. **Security** — training data readable only by svc-training service account; inference API uses Bearer token auth; secrets in K8s Secrets, never hardcoded or in env files
2. **Resilience** — 3 retries + exponential backoff on all Airflow jobs; scoring_job is partition-idempotent; canary auto-rollback via Helm if health check fails at t+5min
3. **Serving pattern** — batch precompute (weekly training + PSI-triggered retrain); scores written to ml_customer_scores table; trade-off: 24h staleness vs operational simplicity
4. **Storage** — Bronze/Silver: Delta Lake on MinIO/GCS; Gold/Features: PostgreSQL (Cloud SQL in prod); model artifacts: MLflow registry; logs: 90-day retention structured JSON
5. **Routing** — FastAPI /score behind NGINX Ingress Controller on GKE; K8s Service round-robin across pods; HPA scales 2–8 pods at CPU 70%; rate limit 100 req/s via Ingress annotation

### 5 LLD classes
- `TrainingDataService` — reads ml_customer_purchase_training, validates schema, deduplicates by created_ts
- `SplitService` — time-based train/val/test split, enforces no leakage
- `ModelService` — train, evaluate (F1/precision/recall/PR-AUC), save to MLflow, load from registry
- `ScoringService` — score_batch, score_online, score_stream, write_scores to ml_customer_scores
- `MonitoringService` — publish_model_metrics, publish_drift_metrics, trigger_alerts, write to agg_feature_health_daily

### Acceptance threshold
- F1 >= 0.60 on test set to register model
- Candidate must beat production F1 by >= 0.02 to be promoted

---

## Inference serving & routing

### Architecture

```
Internet
    │
    ▼
GKE Ingress (NGINX Ingress Controller)   ← external IP, port 443
    │
    ▼
NGINX Ingress Resource (namespace: ml-serving)
    │
    ├── /score    → inference-api Service (ClusterIP :80)
    │                   │
    │                   ├── api Pod 1 (FastAPI :8000)
    │                   └── api Pod 2 (FastAPI :8000)
    │                   HPA: 2–8 pods, scale at CPU 70%
    │
    └── /health   → inference-api Service (liveness check)
```

### NGINX Ingress Controller responsibilities

1. **Reverse proxy** — routes external HTTPS to inference-api ClusterIP Service
2. **Load balancing** — K8s Service handles round-robin across FastAPI pods
3. **Rate limiting** — 100 req/s per IP via Ingress annotation
4. **SSL termination** — TLS cert managed by cert-manager; FastAPI sees plain HTTP
5. **Canary routing** — traffic splitting via NGINX Ingress canary annotations during rollout

### Key Helm values (infra/helm/inference-api/values.yaml)

```yaml
replicaCount: 2
image:
  repository: gcr.io/YOUR_PROJECT/inference-api
  tag: latest
  pullPolicy: Always
service:
  type: ClusterIP
  port: 80
ingress:
  enabled: true
  host: api.fsds-ecommerce.com
  rateLimitRps: "100"
  tlsSecret: tls-secret
resources:
  requests:
    cpu: 250m
    memory: 512Mi
  limits:
    cpu: 1000m
    memory: 1Gi
autoscaling:
  enabled: true
  minReplicas: 2
  maxReplicas: 8
  targetCPUUtilizationPercentage: 70
mlflow:
  modelVersion: Production
  trackingUri: http://mlflow.ml-serving.svc.cluster.local:5000
```

### Helm chart structure (infra/helm/inference-api/)

```
infra/helm/inference-api/
├── Chart.yaml
├── values.yaml           # defaults
├── values-dev.yaml       # dev cluster overrides
├── values-staging.yaml   # staging overrides
├── values-prod.yaml      # production overrides
└── templates/
    ├── deployment.yaml
    ├── service.yaml
    ├── ingress.yaml
    ├── hpa.yaml
    └── secret.yaml
```

### Canary deployment flow

```
Step 1 — deploy canary via Helm (1 pod alongside 2 stable ≈ 33% traffic):
  helm upgrade --install inference-api-canary infra/helm/inference-api \
    --namespace ml-serving \
    --set image.tag=$SHA \
    --set replicaCount=1 \
    --set nameOverride=inference-api-canary \
    -f infra/helm/inference-api/values-prod.yaml

Step 2 — monitor 5 minutes via Prometheus:
  - p95 latency < 200ms
  - 5xx error rate < 1%
  - readinessProbe passing on canary pod

Step 3a — promote (canary healthy):
  helm upgrade inference-api infra/helm/inference-api \
    --namespace ml-serving \
    --set image.tag=$SHA \
    -f infra/helm/inference-api/values-prod.yaml
  helm uninstall inference-api-canary --namespace ml-serving

Step 3b — rollback (canary unhealthy):
  helm uninstall inference-api-canary --namespace ml-serving
  # stable Deployment untouched — traffic instantly restored
```

### HLD trade-off to document in 04_ml_design.md

| Option | Chosen? | Reason |
|---|---|---|
| K8s + NGINX Ingress | Yes | Production-grade, native K8s, integrates with HPA and cert-manager |
| Docker Compose + NGINX | No | Local dev only — no HA, no autoscaling |
| Istio service mesh | No | Overkill for 2 services; significant complexity without benefit at this scale |
| KServe | No | Adds operator dependency; plain K8s Deployment sufficient for a binary classifier |
| GCP Cloud Run | No | Less control over autoscaling behaviour and cold start latency |

---

## Pipeline structure

### Section 02 pipelines (Airflow DAGs)

```
bronze_dag:   ingest raw parquet/json → Delta Lake (append, add ingest metadata)
silver_dag:   bronze → clean/dedup/schema-fill → PostgreSQL staging
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

## Deployment plan

### Environments

| Environment | Namespace suffix | Triggered by | URL |
|---|---|---|---|
| dev | `-dev` | push to any feature branch | api-dev.fsds-ecommerce.com |
| staging | `-staging` | merge to `develop` | api-staging.fsds-ecommerce.com |
| production | (none) | merge to `main` | api.fsds-ecommerce.com |

### GKE cluster structure

```
GKE Cluster: fsds-ecommerce (region: asia-southeast1)
├── node-pool: default      e2-standard-4   data pipelines, monitoring, storage
└── node-pool: ml-serving   e2-standard-2   inference API, autoscales 1–4 nodes

Namespaces:
├── data-pipelines    Airflow · Spark operator · Flink operator · Feast offline
├── ml-serving        inference-api · MLflow · NGINX Ingress Controller
├── monitoring        Prometheus · Grafana · Jaeger · Evidently
├── storage           PostgreSQL · Redis · MinIO
└── mlops             DataHub · Feast serving runtime
```

### Step-by-step deployment order (fresh cluster)

```
Step 1 — Provision infrastructure (Terraform)
  cd infra/terraform
  terraform init
  terraform plan -out=tfplan
  terraform apply tfplan
  # provisions: GKE cluster, node pools, VPC, Cloud SQL,
  #             GCS buckets, IAM service accounts, workload identity

Step 2 — Configure CI runner (Ansible)
  cd infra/ansible
  ansible-playbook -i inventory/prod.ini playbooks/install_kubectl.yml
  ansible-playbook -i inventory/prod.ini playbooks/install_helm.yml
  ansible-playbook -i inventory/prod.ini playbooks/configure_gcloud.yml

Step 3 — Connect kubectl + add Helm repos
  gcloud container clusters get-credentials fsds-ecommerce \
    --region asia-southeast1 --project YOUR_PROJECT_ID
  helm repo add apache-airflow https://airflow.apache.org
  helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
  helm repo update

Step 4 — Create namespaces
  kubectl create namespace data-pipelines
  kubectl create namespace ml-serving
  kubectl create namespace monitoring
  kubectl create namespace storage
  kubectl create namespace mlops

Step 5 — Deploy storage layer  (must be ready before all other services)
  helm upgrade --install postgresql infra/helm/postgresql \
    --namespace storage -f infra/helm/postgresql/values-prod.yaml
  helm upgrade --install redis infra/helm/redis \
    --namespace storage -f infra/helm/redis/values-prod.yaml
  helm upgrade --install minio infra/helm/minio \
    --namespace storage -f infra/helm/minio/values-prod.yaml
  kubectl wait --for=condition=ready pod \
    -l app=postgresql -n storage --timeout=120s

Step 6 — Deploy monitoring stack
  helm upgrade --install monitoring \
    prometheus-community/kube-prometheus-stack \
    --namespace monitoring \
    -f infra/helm/monitoring/values-prod.yaml
  helm upgrade --install jaeger infra/helm/jaeger \
    --namespace monitoring
  helm upgrade --install evidently infra/helm/evidently \
    --namespace monitoring

Step 7 — Deploy data pipeline services
  helm upgrade --install airflow apache-airflow/airflow \
    --namespace data-pipelines \
    -f infra/helm/airflow/values-prod.yaml
  # sync DAGs via ConfigMap
  kubectl create configmap airflow-dags \
    --from-file=b_schema_pipelines/dags/ \
    --namespace=data-pipelines \
    --dry-run=client -o yaml | kubectl apply -f -

Step 8 — Deploy MLops tools
  helm upgrade --install datahub infra/helm/datahub \
    --namespace mlops -f infra/helm/datahub/values-prod.yaml
  helm upgrade --install feast infra/helm/feast \
    --namespace mlops -f infra/helm/feast/values-prod.yaml

Step 9 — Deploy inference stack  (MLflow must be ready before API starts)
  helm upgrade --install mlflow infra/helm/mlflow \
    --namespace ml-serving \
    -f infra/helm/mlflow/values-prod.yaml
  kubectl wait --for=condition=ready pod \
    -l app=mlflow -n ml-serving --timeout=120s
  helm upgrade --install inference-api infra/helm/inference-api \
    --namespace ml-serving \
    -f infra/helm/inference-api/values-prod.yaml

Step 10 — Verify
  kubectl get pods --all-namespaces
  kubectl get ingress -n ml-serving
  curl https://api.fsds-ecommerce.com/health
```

---

## CI/CD tracks

### Track A — Pipeline CI/CD (ci_data_ml.yml + cd_data_ml.yml)

CI (every push):
- ruff lint
- pytest unit + integration tests
- DAG import check (no circular dependencies)
- schema validation tests

CD (merge to main):
```yaml
- name: Authenticate to GKE
  uses: google-github-actions/get-gke-credentials@v2
  with:
    cluster_name: fsds-ecommerce
    location: asia-southeast1

- name: Sync Airflow DAGs
  run: |
    kubectl create configmap airflow-dags \
      --from-file=b_schema_pipelines/dags/ \
      --namespace=data-pipelines \
      --dry-run=client -o yaml | kubectl apply -f -
```

### Track B — Inference service CI/CD (ci_inference.yml + cd_inference.yml)

CI (every push):
- API contract tests (request/response schema)
- health check endpoint test
- smoke test (locust 10 req/s)

CD (merge to main):
```yaml
- name: Build and push image
  run: |
    docker build -t gcr.io/$PROJECT_ID/inference-api:$SHA \
      -f d_ml/api/Dockerfile .
    docker push gcr.io/$PROJECT_ID/inference-api:$SHA

- name: Deploy canary
  run: |
    helm upgrade --install inference-api-canary \
      infra/helm/inference-api \
      --namespace ml-serving \
      --set image.tag=$SHA \
      --set replicaCount=1 \
      --set nameOverride=inference-api-canary \
      -f infra/helm/inference-api/values-prod.yaml

- name: Monitor canary (5 min)
  run: bash infra/scripts/check_canary_health.sh

- name: Promote or rollback
  run: |
    if [ "$CANARY_HEALTHY" = "true" ]; then
      helm upgrade inference-api infra/helm/inference-api \
        --namespace ml-serving \
        --set image.tag=$SHA \
        -f infra/helm/inference-api/values-prod.yaml
      helm uninstall inference-api-canary --namespace ml-serving
    else
      helm uninstall inference-api-canary --namespace ml-serving
      echo "Canary failed — stable deployment unchanged"
      exit 1
    fi
```

### Track C — IaC CI/CD (iac.yml)

```yaml
- name: Terraform init
  run: terraform -chdir=infra/terraform init

- name: Terraform validate
  run: terraform -chdir=infra/terraform validate

- name: Terraform plan
  run: terraform -chdir=infra/terraform plan -out=tfplan

- name: Terraform apply        # only on merge to main
  if: github.ref == 'refs/heads/main'
  run: terraform -chdir=infra/terraform apply tfplan

- name: Verify cluster healthy
  run: |
    kubectl get nodes
    kubectl get pods --all-namespaces | grep -v Running | grep -v Completed
```

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

## Local development

Docker Compose runs data pipeline services only.
The inference stack is always deployed via Helm to the dev cluster — never locally.

```
infra/docker-compose.yml starts:
  postgres    — mirrors Cloud SQL (Gold tables, feature store)
  redis       — mirrors GCP Memorystore (online feature store)
  mlflow      — mirrors GKE MLflow (experiment tracking)
  airflow     — DAG development and testing
  minio       — mirrors GCS (Bronze/Silver Delta Lake)
```

### Inference testing against dev cluster

```bash
# Point kubectl at dev cluster
gcloud container clusters get-credentials fsds-ecommerce \
  --region asia-southeast1 --project YOUR_PROJECT_ID

# Deploy to dev namespace
helm upgrade --install inference-api infra/helm/inference-api \
  --namespace ml-serving-dev \
  --create-namespace \
  -f infra/helm/inference-api/values-dev.yaml

# Test
curl https://api-dev.fsds-ecommerce.com/score \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"customer_id": "C0000001", "features": {...}}'
```

---

## Key constraints and rules

**Point-in-time correctness is mandatory.**
When joining features to labels, features must use only data available at or before event_timestamp.
Any SQL join without `f.event_timestamp <= l.event_timestamp` is a data leakage bug.

**All pipeline jobs must be idempotent.**
Re-running a job must not produce duplicate rows. Use INSERT ... ON CONFLICT DO UPDATE for PostgreSQL writes.

**Never commit secrets.**
All credentials go in K8s Secrets (prod) or .env (local, gitignored).
Load via pydantic-settings Settings class. Never hardcode. Never put in Helm values files.

**Design doc before code in every section.**
Write the .md first. Implementation must match what is documented.

**Every section needs run instructions.**
The grader must be able to clone the repo, follow the README, and see output.
Local sections: `uv sync && python <entry_point>`.
Deployed sections: Helm deploy steps + curl to verify.

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
# ── Infrastructure ─────────────────────────────────────────────────────────
cd infra/terraform && terraform init && terraform apply

gcloud container clusters get-credentials fsds-ecommerce \
  --region asia-southeast1

# ── Helm deployments ───────────────────────────────────────────────────────
# Deploy inference API to production
helm upgrade --install inference-api infra/helm/inference-api \
  --namespace ml-serving \
  -f infra/helm/inference-api/values-prod.yaml

# Deploy inference API to dev
helm upgrade --install inference-api infra/helm/inference-api \
  --namespace ml-serving-dev --create-namespace \
  -f infra/helm/inference-api/values-dev.yaml

# List all Helm releases across namespaces
helm list --all-namespaces

# ── K8s operations ─────────────────────────────────────────────────────────
kubectl get pods -n ml-serving
kubectl get pods -n data-pipelines
kubectl get pods -n monitoring
kubectl get hpa -n ml-serving
kubectl get ingress -n ml-serving
kubectl logs -n ml-serving -l app=inference-api -f
kubectl describe pod -n ml-serving -l app=inference-api
kubectl scale deployment inference-api --replicas=4 -n ml-serving

# ── Port-forwarding for local UI access ────────────────────────────────────
kubectl port-forward svc/mlflow 5000:5000 -n ml-serving
kubectl port-forward svc/grafana 3000:3000 -n monitoring
kubectl port-forward svc/airflow-webserver 8080:8080 -n data-pipelines

# ── Data generator ─────────────────────────────────────────────────────────
uv run python a_data_generator/generator.py
uv run python a_data_generator/generator.py --skip-stream

# ── Tests and lint ─────────────────────────────────────────────────────────
ruff check .
uv run pytest --cov=src tests/ -v

# ── Airflow DAG management ─────────────────────────────────────────────────
# Sync DAGs to cluster
kubectl create configmap airflow-dags \
  --from-file=b_schema_pipelines/dags/ \
  --namespace=data-pipelines \
  --dry-run=client -o yaml | kubectl apply -f -

# Trigger pipelines manually
kubectl exec -n data-pipelines \
  $(kubectl get pod -n data-pipelines -l app=airflow-webserver -o name) \
  -- airflow dags trigger bronze_dag

kubectl exec -n data-pipelines \
  $(kubectl get pod -n data-pipelines -l app=airflow-webserver -o name) \
  -- airflow dags trigger gold_pipeline

kubectl exec -n data-pipelines \
  $(kubectl get pod -n data-pipelines -l app=airflow-webserver -o name) \
  -- airflow dags trigger ml_training_pipeline

kubectl exec -n data-pipelines \
  $(kubectl get pod -n data-pipelines -l app=airflow-webserver -o name) \
  -- airflow dags trigger ml_scoring_pipeline

# ── Local dev (Docker Compose — data services only) ────────────────────────
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml down
```