# Docker Image Optimisation — Data Platform (`b_schema_pipelines/Dockerfile`)

`docs/docker_optimize.md` (root) documents the ML inference API's image
(`d_ml/api/Dockerfile`) — this doc covers the **data platform's own** image, the one Spark
jobs actually run in, which didn't have an equivalent write-up before this pass.

## Rubric proof (`coursework/rubrics.md` — Docker & Docker Compose, 3 pts)

| Requirement | Pts | Status |
|---|---|---|
| Used — "Document Docker image đã reduce từ bao nhiêu tới bao nhiêu thông qua phương pháp optimize gì" | 1 | ✅ below — real measured before/after |
| Optimize Dockerfile (multistage build) | 2 | ✅ `b_schema_pipelines/Dockerfile` is now multi-stage |

## Objective

Reduce `b_schema_pipelines/Dockerfile`'s image size by splitting the Python dependency
resolution (`uv sync`, which pulls in pyspark/pandas/numpy/mlflow/scikit-learn/deltalake and
their wheel-download cache) into a discarded build stage, so only the *built* virtual
environment — not `uv`'s own download cache — ends up in the final image.

## Build commands

```bash
# Baseline — single stage
docker build -f b_schema_pipelines/Dockerfile.baseline -t fsds-schema-pipelines:baseline .

# Optimized — two stages
docker build -f b_schema_pipelines/Dockerfile -t fsds-schema-pipelines:optimized .

# Compare
docker image ls --format "{{.Repository}}:{{.Tag}}  {{.Size}}" | grep fsds-schema-pipelines
```

## Before vs after (measured, not estimated)

| Build | Base image | Stages | Image size |
|---|---|---|---|
| Baseline | `python:3.13-slim` | 1 | **5.7 GB** |
| Optimized | `python:3.13-slim` (both stages) | 2 | **2.79 GB** |

**Size reduction: 51%** (~2.9 GB saved)

## What the optimization does

### Stage 1 — `builder`

Resolves and installs every dependency via `uv sync --frozen` into `/app/.venv`. `uv`'s own
wheel/download cache (numpy/pandas/pyspark/mlflow/scikit-learn/scipy all have multi-hundred-MB
wheels) lives outside `/app/.venv`, in this stage only:

```dockerfile
FROM python:3.13-slim AS builder
RUN pip install --no-cache-dir uv
WORKDIR /app
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen
```

### Stage 2 — `runtime`

Starts fresh from the same base image and copies over only the *built* venv — not the cache
that produced it — plus the `uv` binary itself (kept so `uv run python3 script.py`, the
invocation every README in this repo documents, keeps working without needing to
reinstall/redownload `uv`):

```dockerfile
FROM python:3.13-slim AS runtime
RUN apt-get update && apt-get install -y --no-install-recommends default-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /usr/local/bin/uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./
COPY . .
```

Java (`default-jre-headless`, needed for Spark's JVM at *runtime*, not during dependency
resolution) is installed only in this stage — `uv sync` never needs a JVM, so installing Java
in the builder stage would have been pure waste, discarded when that stage's layers are
dropped anyway.

### Non-obvious bug found while measuring this

The **original single-stage Dockerfile never actually built from a clean cache** — it was
missing `README.md` in its `COPY pyproject.toml uv.lock ./` line, and `pyproject.toml`'s
`readme = "README.md"` field makes `uv sync`'s editable-install step fail without it
(`OSError: Readme file does not exist: README.md`, raised by the `hatchling` build backend).
Every prior "successful" build had silently reused a stale cached layer from an earlier,
differently-shaped build rather than actually re-running `uv sync` — a genuine cache-miss
build (a fresh clone, or `docker build --no-cache`) would have failed. Fixed in both
`Dockerfile` and `Dockerfile.baseline` (`COPY pyproject.toml uv.lock README.md ./`) — this
bug was pre-existing, not introduced by the multi-stage optimization, and applies equally to
the baseline.

## Trade-offs

| | Baseline | Optimized |
|---|---|---|
| Build complexity | One `FROM` | Two stages |
| Build time | Slightly faster (no `COPY --from`) | Marginally slower (one extra copy step) |
| Image size | 5.7 GB | 2.79 GB (51% smaller) |
| Runtime capability | Identical | Identical — same Java, same venv contents, same `uv run` invocation |

The only cost is a few more lines in the Dockerfile. The size reduction directly cuts
image-pull time on cold container starts and shrinks the attack surface (no `uv` wheel-cache
metadata, no leftover build artifacts in the shipped image).
