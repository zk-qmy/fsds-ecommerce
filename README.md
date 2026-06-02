# fsds-ecommerce

# Project Structure
```
fsds-ecommerce/
├── 01_data_generator/
│   ├── generator.py
│   ├── config.yaml
│   └── outputs/          # parquet + json samples
├── 02_schema_pipelines/
│   ├── pipelines/        # bronze/ silver/ gold/ features/
│   ├── dags/             # airflow DAGs
│   ├── dq/               # great_expectations/ deequ/
│   └── docs/
├── 03_drift_labels/
├── 04_ml/
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

uv add \
  faker pandas pyarrow pyspark \
  great_expectations deequ delta-spark \
  apache-flink scikit-learn mlflow \
  fastapi uvicorn pytest pytest-cov \
  evidently prometheus-client opentelemetry-sdk \
  opentelemetry-exporter-jaeger python-dotenv
```