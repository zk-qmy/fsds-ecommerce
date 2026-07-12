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
│   │   └── features/
│   │       ├── feat_customer_90d.py
│   │       ├── feat_stream_60m.py
│   │       ├── feat_customer_unified.py
│   │       └── push_stream_to_feast.py   # Flink output → Feast offline + online store
│   ├── dags/                     # Airflow DAGs per pipeline group (DP1/DP2/DP3 + materialize)
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
│   │   ├── kubeflow/
│   │   │   └── training_pipeline.py   # Kubeflow Pipelines v2 — load data → train → evaluate → register
│   │   ├── scoring_dag.py             # Airflow DAG: score_batch → write_scores (daily 04:00)
│   │   ├── materialize_dag.py         # Airflow DAG: incremental Feast offline→online materialize
│   │   └── retrain_trigger_dag.py     # Airflow DAG: compute PSI → call Kubeflow API to retrain
│   ├── api/
│   │   ├── main.py               # FastAPI /score endpoint (async, Bearer auth, healthcheck)
│   │   ├── schemas.py            # Pydantic request/response models
│   │   ├── services.py           # async Feast feature fetch + model predict
│   │   ├── drift/
│   │   │   ├── main.py           # FastAPI /detect-drift endpoint (async, healthcheck)
│   │   │   ├── schemas.py        # DriftRequest / DriftResponse Pydantic models
│   │   │   └── detector.py       # Evidently DataDriftPreset wrapper
│   │   └── Dockerfile            # multi-stage build (builder + runtime)
│   └── k8s/                      # raw K8s manifests (templated by Helm)
│       ├── deployment.yaml
│       ├── service.yaml
│       ├── ingress.yaml
│       ├── scaledobject.yaml     # KEDA ScaledObject (replaces HPA)
│       └── deployment-canary.yaml
├── tests/
│   ├── unit/
│   ├── integration/
│   └── load/
│       └── locustfile.py         # Locust load test → HTML report
├── infra/
│   ├── docker-compose.yml        # local dev: postgres, airflow, mlflow, minio, redis
│   ├── helm/
│   │   ├── inference-api/        # Chart.yaml, values*.yaml, templates/ (+ scaledobject.yaml)
│   │   ├── drift-api/            # Chart.yaml, values*.yaml, templates/ (+ scaledobject.yaml)
│   │   ├── airflow/
│   │   ├── monitoring/
│   │   ├── postgresql/
│   │   ├── redis/
│   │   ├── minio/
│   │   ├── mlflow/
│   │   ├── keda/                 # KEDA operator Helm chart
│   │   ├── vault/                # HashiCorp Vault Helm chart
│   │   ├── istio/                # Istio base + istiod + PeerAuthentication
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
│   └── settings.py               # Settings class (pydantic-settings), reads secrets from Vault
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
| Data pipeline orchestration | Apache Airflow 2.8 (DP1/DP2/DP3 + materialize + retrain trigger) |
| ML pipeline orchestration | Kubeflow Pipelines v2 standalone (training pipeline only) |
| Data quality | Great Expectations |
| Feature store | Feast (offline: PostgreSQL, online: Redis) |
| ML model | scikit-learn (LogisticRegression baseline) |
| Experiment tracking | MLflow |
| Data versioning | DVC (incremental, MinIO/GCS remote) |
| Inference API | FastAPI + Uvicorn |
| Drift detection API | FastAPI + Evidently (separate service) |
| Observability | Prometheus + Grafana + Jaeger + OpenTelemetry + Loki |
| Drift detection | Evidently |
| Autoscaling | KEDA v2 (HTTP request rate via Prometheus) |
| Secrets management | HashiCorp Vault (Vault Agent injector into pods) |
| Service mesh | Istio (mTLS for service-to-service auth) |
| CI/CD | GitHub Actions (3 tracks) |
| Container | Docker + Docker Compose (local dev only) |
| Deployment | Kubernetes (GKE) + Helm (`--atomic` for auto-rollback) |
| IaC | Terraform (cluster) + Ansible (CI runner) |
| Database | PostgreSQL 15 (Gold + feature store) |
| Object storage | MinIO / GCS (Bronze/Silver Delta Lake) |
| Model registry | MLflow model registry |
| Metadata/lineage | DataHub |
| Testing | pytest + pytest-cov + mutmut + hypothesis + Locust |

---

## Data model

### Offline tables (Bronze → Silver → Gold)

**Source tables (Section 01 outputs):**
- `customers` — customer_id, signup_ts, country, segment, marketing_opt_in
- `products` — product_id, category, brand, base_price, is_active, created_ts
- `orders` — order_id, customer_id, order_timestamp, status, shipping_city, shipping_method, coupon_code
- `order_items` — order_item_id, order_id, product_id, quantity, unit_price, discount, line_total
- `payments` - payment_id, order_id, payment_timestamp, amount, payment_method, payment_status

Stream data:
- `events` - event_id, event_type, event_timestamp, created_ts, customer_id, session_id, product_id, order_id, quantity, price

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
1. **Security** — training data readable only by svc-training service account; inference API uses Bearer token auth; all secrets managed in HashiCorp Vault and injected into pods via Vault Agent Injector — never stored in K8s Secrets directly, never hardcoded, never in Helm values files; service-to-service calls secured by Istio mTLS
2. **Resilience** — 3 retries + exponential backoff on all Airflow jobs; scoring_job is partition-idempotent; `helm upgrade --atomic` auto-rolls back on failed health check within timeout
3. **Serving pattern** — batch precompute (weekly training + PSI-triggered retrain); scores written to ml_customer_scores table; trade-off: 24h staleness vs operational simplicity
4. **Storage** — Bronze/Silver: Delta Lake on MinIO/GCS; Gold/Features: PostgreSQL (Cloud SQL in prod); model artifacts: MLflow registry; training data versions: DVC (incremental, MinIO remote); logs: Loki (90-day retention, structured JSON)
5. **Routing** — FastAPI /score + drift-api /detect-drift both behind NGINX Ingress Controller on GKE; KEDA scales each API 2–8 pods by HTTP request rate (Prometheus metric); rate limit 100 req/s via Ingress annotation

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
    │  (basic auth + rate limit 100 req/s + SSL termination)
    ▼
NGINX Ingress Resource (namespace: ml-serving)
    │
    ├── /score         → inference-api Service (ClusterIP :80)
    │                       │
    │                       ├── api Pod 1 (FastAPI :8000)
    │                       └── api Pod 2 (FastAPI :8000)
    │                       KEDA: 2–8 pods, scale by HTTP req/s (Prometheus)
    │
    ├── /detect-drift  → drift-api Service (ClusterIP :80)
    │                       │
    │                       └── drift Pod 1..N (FastAPI :8000)
    │                       KEDA: 2–8 pods, scale by HTTP req/s (Prometheus)
    │
    ├── /grafana       → Grafana Service (monitoring namespace, basic auth)
    ├── /jaeger        → Jaeger Service  (monitoring namespace, basic auth)
    └── /health        → inference-api Service (liveness check)
```

### NGINX Ingress Controller responsibilities

1. **Reverse proxy** — routes external HTTPS to ClusterIP Services
2. **Load balancing** — K8s Service handles round-robin across FastAPI pods
3. **Rate limiting** — 100 req/s per IP via Ingress annotation
4. **SSL termination** — TLS cert managed by cert-manager; services see plain HTTP
5. **Basic auth** — username/password for observability UIs (Grafana, Jaeger)
6. **Canary routing** — traffic splitting via NGINX Ingress canary annotations during rollout

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
keda:
  enabled: true
  minReplicas: 2
  maxReplicas: 8
  prometheusQuery: sum(rate(http_requests_total{app="inference-api"}[1m]))
  threshold: "100"
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
    ├── scaledobject.yaml  # KEDA ScaledObject (replaces hpa.yaml)
    └── secret.yaml        # Vault Agent annotation only — no secret values here
```

### Canary deployment flow

```
Step 1 — deploy canary via Helm --atomic (auto-rollback on failure):
  helm upgrade --install inference-api-canary infra/helm/inference-api \
    --namespace ml-serving --atomic --timeout 5m \
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
    --namespace ml-serving --atomic --timeout 5m \
    --set image.tag=$SHA \
    -f infra/helm/inference-api/values-prod.yaml
  helm uninstall inference-api-canary --namespace ml-serving

Step 3b — rollback (--atomic already triggered):
  # helm upgrade --atomic rolls back automatically if any pod fails readiness
  # stable Deployment untouched — traffic instantly restored
```

### HLD trade-off to document in 04_ml_design.md

| Option | Chosen? | Reason |
|---|---|---|
| K8s + NGINX Ingress | Yes | Production-grade, native K8s, integrates with KEDA and cert-manager |
| Docker Compose + NGINX | No | Local dev only — no HA, no autoscaling |
| Istio service mesh | Yes (mTLS only) | Required for service-to-service authentication; traffic management features not used |
| KServe | No | Adds operator dependency; plain K8s Deployment sufficient for a binary classifier |
| GCP Cloud Run | No | Less control over autoscaling behaviour and cold start latency |
| HPA (CPU-based) | No | Replaced by KEDA; CPU is a lagging signal — HTTP req/s scales faster |
| K8s Secrets | No (leaf only) | Vault is the source of truth; Vault Agent injects values as files into pods |

---

## Pipeline structure

### Section 02 pipelines (Airflow DAGs)

```
dp1_bronze_dag:   ingest raw parquet/json → Delta Lake (append, add ingest metadata)
dp2_gold_dag:     bronze → silver (dedup/fill) → gold (dim/fact/obt) → gold_ecommerce schema
dp3_feature_dag:  gold + Flink output → feat_customer_90d + feat_stream_60m → feat_customer_unified
```

Each pipeline job must log: run_id, pipeline_name, start_ts, end_ts, status, input_rows, output_rows, error_summary.

Quality gates (fail the run if violated):
- Schema check: expected columns present
- Null check: no nulls in primary keys
- Uniqueness: no duplicate business keys in Gold
- Referential integrity: fact FKs exist in dimensions
- Volume check: output rows within ±30% of baseline

### Section 04 ML pipelines

**Airflow DAGs** (schedule + trigger only — no training logic here):

```
materialize_dag  (schedule: 0 3 * * * — every day 03:00):
  feast_materialize_offline_job → feast_materialize_online_job

scoring_dag  (schedule: 0 4 * * * — every day 04:00):
  scoring_job → write_scores_job

retrain_trigger_dag  (schedule: 0 6 * * * — every day 06:00):
  compute_psi_job → publish_to_pushgateway_job → check_threshold_job → [call_kubeflow_retrain_api_job]
  # publish_to_pushgateway_job: push PSI metrics to Prometheus PushGateway → visible in Grafana
  # call_kubeflow_retrain_api_job: POST to Kubeflow Pipelines REST API to trigger training_pipeline run
```

**Kubeflow Pipelines** (actual training — triggered by retrain_trigger_dag or manual):

```
training_pipeline.py  (Kubeflow Pipelines v2):
  load_data_op → split_op → train_op (distributed) → evaluate_op → register_model_op
```

Retrain triggers:
- PSI > 0.15 sustained 3+ consecutive days on any feature
- Production F1 drops below 0.60 on rolling validation window

### Flink → Feast push jobs

```
push_stream_to_feast.py runs after Flink streaming job:
  - push feat_stream_60m records to Feast OFFLINE store (for point-in-time joins)
  - push feat_stream_60m records to Feast ONLINE store (for real-time scoring)
  Deployed as a Kubernetes Job, triggered by retrain_trigger_dag
```

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
├── data-pipelines    Airflow · Spark operator · Flink operator · Feast offline · Kubeflow Pipelines
├── ml-serving        inference-api · drift-api · MLflow · NGINX Ingress Controller
├── monitoring        Prometheus · Grafana · Jaeger · Evidently · Loki
├── storage           PostgreSQL · Redis · MinIO
├── mlops             DataHub · Feast serving runtime
└── security          HashiCorp Vault
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
  helm repo add kedacore https://kedacore.github.io/charts
  helm repo add hashicorp https://helm.releases.hashicorp.com
  helm repo add istio https://istio-release.storage.googleapis.com/charts
  helm repo update

Step 4 — Create namespaces
  kubectl create namespace data-pipelines
  kubectl create namespace ml-serving
  kubectl create namespace monitoring
  kubectl create namespace storage
  kubectl create namespace mlops
  kubectl create namespace security

Step 5 — Deploy storage layer  (must be ready before all other services)
  helm upgrade --install postgresql infra/helm/postgresql \
    --namespace storage -f infra/helm/postgresql/values-prod.yaml
  helm upgrade --install redis infra/helm/redis \
    --namespace storage -f infra/helm/redis/values-prod.yaml
  helm upgrade --install minio infra/helm/minio \
    --namespace storage -f infra/helm/minio/values-prod.yaml
  kubectl wait --for=condition=ready pod \
    -l app=postgresql -n storage --timeout=120s

Step 6 — Deploy secrets management (before anything else reads secrets)
  helm upgrade --install vault hashicorp/vault \
    --namespace security -f infra/helm/vault/values-prod.yaml
  # initialize Vault, enable K8s auth, create policies + roles for each service

Step 7 — Deploy service mesh (Istio)
  helm upgrade --install istio-base istio/base --namespace istio-system --create-namespace
  helm upgrade --install istiod istio/istiod --namespace istio-system
  # label namespaces for sidecar injection
  kubectl label namespace ml-serving istio-injection=enabled
  kubectl label namespace data-pipelines istio-injection=enabled
  # apply PeerAuthentication STRICT mTLS per namespace

Step 8 — Deploy KEDA
  helm upgrade --install keda kedacore/keda --namespace keda --create-namespace

Step 9 — Deploy monitoring stack
  helm upgrade --install monitoring \
    prometheus-community/kube-prometheus-stack \
    --namespace monitoring \
    -f infra/helm/monitoring/values-prod.yaml
  helm upgrade --install loki grafana/loki-stack \
    --namespace monitoring
  helm upgrade --install jaeger infra/helm/jaeger \
    --namespace monitoring
  helm upgrade --install evidently infra/helm/evidently \
    --namespace monitoring

Step 10 — Deploy data pipeline services
  helm upgrade --install airflow apache-airflow/airflow \
    --namespace data-pipelines \
    -f infra/helm/airflow/values-prod.yaml
  kubectl create configmap airflow-dags \
    --from-file=b_schema_pipelines/dags/ \
    --namespace=data-pipelines \
    --dry-run=client -o yaml | kubectl apply -f -

Step 11 — Deploy Kubeflow Pipelines (standalone)
  # apply Kubeflow Pipelines standalone manifests
  kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/cluster-scoped-resources"
  kubectl apply -k "github.com/kubeflow/pipelines/manifests/kustomize/env/platform-agnostic-pns"

Step 12 — Deploy MLops tools
  helm upgrade --install datahub infra/helm/datahub \
    --namespace mlops -f infra/helm/datahub/values-prod.yaml
  helm upgrade --install feast infra/helm/feast \
    --namespace mlops -f infra/helm/feast/values-prod.yaml

Step 13 — Deploy inference stack  (MLflow must be ready before API starts)
  helm upgrade --install mlflow infra/helm/mlflow \
    --namespace ml-serving \
    -f infra/helm/mlflow/values-prod.yaml
  kubectl wait --for=condition=ready pod \
    -l app=mlflow -n ml-serving --timeout=120s
  helm upgrade --install inference-api infra/helm/inference-api \
    --namespace ml-serving --atomic --timeout 5m \
    -f infra/helm/inference-api/values-prod.yaml
  helm upgrade --install drift-api infra/helm/drift-api \
    --namespace ml-serving --atomic --timeout 5m \
    -f infra/helm/drift-api/values-prod.yaml

Step 14 — Verify
  kubectl get pods --all-namespaces
  kubectl get ingress -n ml-serving
  kubectl get scaledobject -n ml-serving
  curl https://api.fsds-ecommerce.com/health
```

---

## CI/CD tracks

### Track A — Pipeline CI/CD (ci_data_ml.yml + cd_data_ml.yml)

CI (every push):
- ruff lint
- pytest unit + integration tests (coverage > 90%)
- mutmut mutation test on changed files only (score > 80%)
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
- equivalence partitioning + boundary value test cases
- smoke test (locust 10 req/s, inline)

CD (merge to main):
```yaml
- name: Build and push image
  run: |
    docker build -t gcr.io/$PROJECT_ID/inference-api:$SHA \
      -f d_ml/api/Dockerfile .
    docker push gcr.io/$PROJECT_ID/inference-api:$SHA

- name: Deploy canary (--atomic auto-rollback)
  run: |
    helm upgrade --install inference-api-canary \
      infra/helm/inference-api \
      --namespace ml-serving \
      --atomic --timeout 5m \
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
        --namespace ml-serving --atomic --timeout 5m \
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

Observability stack: OpenTelemetry SDK instruments FastAPI and drift-api → Jaeger (traces), Prometheus (metrics), Loki (logs, structured JSON, 90-day retention).

**Retrain flow:**
`retrain_trigger_dag` (Airflow) → computes PSI daily from Feast offline store → if threshold breached → POST to Kubeflow Pipelines REST API → `training_pipeline` run is submitted automatically.

---

## Testing strategy

| Test type | Tool | Target | Threshold |
|---|---|---|---|
| Unit tests | pytest + pytest-cov | `d_ml/api/`, `d_ml/src/` | Coverage > 90% |
| Mutation testing | mutmut | Changed files only (not whole codebase) | Mutation score > 80% |
| Property-based / idempotency | hypothesis + crosshair | Score endpoint determinism, pipeline idempotency | No counterexample found |
| Equivalence partitioning / BVA | pytest parametrize | API input partitions + boundary values | All partitions covered |
| Integration tests | pytest | Airflow DAG imports, Feast feature fetch | No import errors |
| Load test | Locust | `/score` endpoint | HTML report committed; p95 < 200ms |

```bash
# Run all tests with coverage
uv run pytest tests/ --cov=d_ml --cov-report=html --cov-report=term-missing -v

# Mutation test — changed files only
mutmut run --paths-to-mutate d_ml/api/services.py
mutmut results

# Load test — generates reports/load_test.html
locust -f tests/load/locustfile.py --headless -u 50 -r 10 --run-time 60s \
  --html reports/load_test.html
```

---

## Design patterns

These patterns are implemented across `d_ml/src/` and must be demonstrated with screenshots in `docs/design_patterns.md`:

| Pattern | Class | Purpose |
|---|---|---|
| Strategy | `ScoringService` — `score_batch` / `score_online` / `score_stream` | Swap scoring mode without changing caller |
| Factory | `ModelService.load_from_registry` | Return correct model class from MLflow registry metadata |
| Repository | `TrainingDataService.read_training_table` | Abstract Feast/PostgreSQL data access behind interface |
| Observer | `MonitoringService.trigger_alerts` | Decouple alert dispatch from drift computation |

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
  --create-namespace --atomic --timeout 5m \
  -f infra/helm/inference-api/values-dev.yaml

# Test
curl https://api-dev.fsds-ecommerce.com/score \
  -H "Authorization: Bearer $TOKEN" \
  -d '{"customer_id": "C0000001"}'
```

---

## Key constraints and rules

**Point-in-time correctness is mandatory.**
When joining features to labels, features must use only data available at or before event_timestamp.
Any SQL join without `f.event_timestamp <= l.event_timestamp` is a data leakage bug.

**All pipeline jobs must be idempotent.**
Re-running a job must not produce duplicate rows. Use INSERT ... ON CONFLICT DO UPDATE for PostgreSQL writes.

**Secrets come from Vault, never from code or Helm values.**
All credentials managed in HashiCorp Vault. Vault Agent Injector writes them as files into pods at `/vault/secrets/`. `config/settings.py` reads from those files via pydantic-settings. Never put secret values in Helm values files, K8s Secrets manifests, or environment variables in Dockerfile.

**Training uses Kubeflow Pipelines; scheduling uses Airflow.**
The `training_pipeline.py` (Kubeflow) contains all ML logic. Airflow's `retrain_trigger_dag` only decides *when* to retrain and calls the Kubeflow Pipelines REST API — it does not contain any training code.

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
- [ ] All code runnable from a fresh clone; all functions/classes have docstrings
- [ ] Sample outputs committed (parquet previews, screenshots, logs)
- [ ] README has install + run instructions + expected output + deployment diagram
- [ ] Quality report shows each injected data problem at target rate (Section 01)
- [ ] Spark UI screenshots showing before/after for each optimization (Section 02)
- [ ] Flink UI screenshots showing before/after for each optimization (Section 02)
- [ ] Airflow UI green run screenshot for DP1, DP2, DP3, materialize (Section 02)
- [ ] DataHub lineage + assertions screenshot per pipeline (Section 02)
- [ ] DBeaver ER diagram + SCD2 columns + feat_ columns (Section 02)
- [ ] PSI escalation evidence after drift_start_date (Section 03)
- [ ] Kubeflow Pipelines UI green run screenshot with distributed training (Section 04)
- [ ] MLflow run screenshot with F1 metric + model registry (Section 04)
- [ ] Feast materialize pipeline screenshot + TTL doc (Section 04)
- [ ] pytest coverage > 90% screenshot (Section 04)
- [ ] mutmut mutation score > 80% screenshot (Section 04)
- [ ] hypothesis property-based test run screenshot (Section 04)
- [ ] Locust HTML report committed (Section 04)
- [ ] KEDA ScaledObject screenshot + pod count increasing under load (Section 04)
- [ ] Vault UI screenshot + Vault Agent injection working (Section 04)
- [ ] Istio mTLS + Kiali screenshot (Section 04)
- [ ] NGINX Ingress with HTTPS + basic auth + rate limit screenshot (Section 04)
- [ ] Grafana dashboard screenshot (web API + infra + ML drift) (Section 04)
- [ ] Jaeger trace screenshot (Section 04)
- [ ] Loki log query screenshot (Section 04)
- [ ] A/B test canary Ingress + Grafana dual-model dashboard (Section 04)
- [ ] CI/CD pipeline green log screenshot for each track (Section 04)
- [ ] Terraform apply + Ansible run screenshots (Section 04)
- [ ] Design patterns documented with code evidence (Section 04)

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
drift_start_date: "2026-04-01"     # after this date, segment distribution shifts (Section 03)
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
# Deploy inference API to production (atomic = auto-rollback on failure)
helm upgrade --install inference-api infra/helm/inference-api \
  --namespace ml-serving --atomic --timeout 5m \
  -f infra/helm/inference-api/values-prod.yaml

# Deploy drift API to production
helm upgrade --install drift-api infra/helm/drift-api \
  --namespace ml-serving --atomic --timeout 5m \
  -f infra/helm/drift-api/values-prod.yaml

# Deploy inference API to dev
helm upgrade --install inference-api infra/helm/inference-api \
  --namespace ml-serving-dev --create-namespace --atomic --timeout 5m \
  -f infra/helm/inference-api/values-dev.yaml

# List all Helm releases across namespaces
helm list --all-namespaces

# ── K8s operations ─────────────────────────────────────────────────────────
kubectl get pods -n ml-serving
kubectl get pods -n data-pipelines
kubectl get pods -n monitoring
kubectl get pods -n security
kubectl get scaledobject -n ml-serving   # KEDA autoscaling status
kubectl get ingress -n ml-serving
kubectl logs -n ml-serving -l app=inference-api -f
kubectl describe pod -n ml-serving -l app=inference-api

# ── Port-forwarding for local UI access ────────────────────────────────────
kubectl port-forward svc/mlflow 5000:5000 -n ml-serving
kubectl port-forward svc/grafana 3000:3000 -n monitoring
kubectl port-forward svc/airflow-webserver 8080:8080 -n data-pipelines
kubectl port-forward svc/vault 8200:8200 -n security
kubectl port-forward svc/ml-pipeline-ui 3001:80 -n kubeflow   # Kubeflow Pipelines UI

# ── Data generator ─────────────────────────────────────────────────────────
uv run python a_data_generator/generator.py
uv run python a_data_generator/generator.py --skip-stream

# ── Tests and lint ─────────────────────────────────────────────────────────
ruff check .
uv run pytest tests/ --cov=d_ml --cov-report=term-missing -v
mutmut run --paths-to-mutate d_ml/api/services.py && mutmut results
locust -f tests/load/locustfile.py --headless -u 50 -r 10 --run-time 60s \
  --html reports/load_test.html

# ── Airflow DAG management ─────────────────────────────────────────────────
kubectl create configmap airflow-dags \
  --from-file=b_schema_pipelines/dags/ \
  --namespace=data-pipelines \
  --dry-run=client -o yaml | kubectl apply -f -

kubectl exec -n data-pipelines \
  $(kubectl get pod -n data-pipelines -l app=airflow-webserver -o name) \
  -- airflow dags trigger dp1_bronze_dag

kubectl exec -n data-pipelines \
  $(kubectl get pod -n data-pipelines -l app=airflow-webserver -o name) \
  -- airflow dags trigger dp2_gold_dag

kubectl exec -n data-pipelines \
  $(kubectl get pod -n data-pipelines -l app=airflow-webserver -o name) \
  -- airflow dags trigger materialize_dag

# ── Kubeflow Pipelines ─────────────────────────────────────────────────────
# Compile and run training pipeline
uv run python d_ml/pipelines/kubeflow/training_pipeline.py  # compiles to pipeline.yaml
# Upload pipeline.yaml via Kubeflow UI or REST API

# ── Local dev (Docker Compose — data services only) ────────────────────────
docker compose -f infra/docker-compose.yml up -d
docker compose -f infra/docker-compose.yml down
```
