# fsds-ecommerce

Full-stack data science + MLOps system for e-commerce purchase prediction.
Predicts `will_purchase_next_session` for 120,000 customers across a 180-day history window.

---

## Table of Contents

- [Setup](#setup)
- [Section 01 — Data Generator](#section-01--data-generator)
- [Repository Structure](#repository-structure)
- [Git Convention](#git-convention)

---

## Setup

```bash
# Clone and install
git clone <repo-url>
cd fsds-ecommerce
uv sync
```

Requires Python 3.13 and [uv](https://github.com/astral-sh/uv).

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
schema_change_date: "2026-03-24"   # Problem B cutoff
duplicate_rate_offline: 0.02       # Problem C
burst_multiplier: 30               # Problem D
late_arrival_rate: 0.12            # Problem E
duplicate_rate_stream: 0.015       # Problem F
```

---

## Repository Structure

```
fsds-ecommerce/
├── a_data_generator/         # Section 01 — synthetic data generator
│   ├── generator.py
│   ├── config/generator_config.yaml
│   ├── docs/01_data_generator.md
│   └── outputs/              # offline/ + streaming/ (generated, not committed)
├── b_schema_pipelines/       # Section 02 — Bronze/Silver/Gold/Feature pipelines
│   ├── pipelines/            # bronze/ silver/ gold/ features/
│   ├── dags/                 # Airflow DAGs (DP1/DP2/DP3 + materialize)
│   ├── dq/                   # Great Expectations suites
│   └── docs/02_schema_design.md
├── c_drift_labels/           # Section 03 — drift injection + ML labels
├── d_ml/                     # Section 04 — ML system (train, serve, monitor)
│   ├── design/04_ml_design.md
│   ├── src/                  # TrainingDataService, SplitService, ModelService, ...
│   ├── pipelines/            # Kubeflow training + Airflow scoring/retrain DAGs
│   └── api/                  # FastAPI inference + drift detection APIs
├── infra/
│   ├── docker-compose.yml    # local dev stack (postgres, airflow, mlflow, minio, redis)
│   ├── helm/                 # Helm charts for all services
│   ├── terraform/            # GKE cluster + VPC + IAM
│   └── ansible/              # CI runner config (kubectl, helm, gcloud)
├── tests/
│   ├── unit/
│   ├── integration/
│   └── load/locustfile.py
├── config/
│   ├── settings.py
│   └── logging.py
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