# fsds-ecommerce

# Project Structure
```
fsds-ecommerce/
├── a_data_generator/
│   ├── generator.py
│   ├── config.yaml
│   └── outputs/          # parquet + json samples
├── b_schema_pipelines/
│   ├── pipelines/        # bronze/ silver/ gold/ features/
│   ├── dags/             # airflow DAGs
│   ├── dq/               # great_expectations/ deequ/
│   └── docs/
├── c_drift_labels/
├── d_ml/
│   ├── design/           # HLD + LLD .md files
│   ├── src/              # ML classes
│   ├── pipelines/        # training/ scoring/ retrain/
│   ├── api/              # FastAPI inference service
│   └── cicd/             # github actions / jenkins
├── infra/
│   ├── docker-compose.yml
│   ├── terraform/
│   └── ansible/
└── README.md
```

# Setup Env
```
uv init

uv venv
source .venv/bin/activate

uv sync
```


# Dev Plan

## Sprint 0: Setup

**Goal:** Every tool installed, Git repo initialized, Docker Compose running.

## Sprint 1: Generator

**Goal:** A single `generator.py` that produces all 5 offline Parquet tables and a streaming JSON file, with all required data problems injected. Config-driven, seeded, reproducible.

**How to run:** 
```bash
python a_data_generator/generator.py
```
After run that line, 2 folders: offline and streaming will be created in the outputs folder.

## Sprint 2: Pipeline

**Goal:** Four working pipeline groups (Bronze, Silver, Gold, Features) with quality gates, run metadata, and DataHub lineage.

## Sprint 3: Drift & Labels

**Goal:** Extend the generator with Scenario A drift, produce `agg_feature_health_daily` with PSI values, create `ml_customer_label`, and join everything into `ml_customer_purchase_training`.

## Sprint 4: ML Design

**Goal:** Complete `04_ml_design.md` covering all 8 required sections.

## Sprint 5: ML Code

**Goal:** All 3 ML pipelines running, FastAPI inference endpoint serving predictions, MLflow tracking experiments, CI/CD pipelines green on all three tracks.

## Sprint 6: Monitor & Ship

**Goal:** Observability stack running, drift dashboard live, all 5 evidence checklist items satisfied, every section has run instructions and sample outputs. Final submission.

## Git convention
For new features:
```bash
git checkout develop
git pull

git checkout -b feature/new-feature
```
Then:

```bash
feature/new-feature
       │
       ▼
     develop
       │
       ▼
      main
```