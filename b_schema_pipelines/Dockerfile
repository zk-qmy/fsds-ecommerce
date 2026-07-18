# Multi-stage build — see docs/02_docker_optimize.md for the measured before/after.
#
# Stage 1 (builder) resolves and installs every Python dependency via `uv sync`
# into /app/.venv. uv's own download/wheel cache (numpy/pandas/pyspark/mlflow/
# scikit-learn/scipy all have sizeable wheels) lives outside /app/.venv and is
# never copied into the runtime stage below -- that cache is the single
# biggest thing this split removes from the final image.
FROM python:3.13-slim AS builder

RUN pip install --no-cache-dir uv

WORKDIR /app

# README.md too -- pyproject.toml's `readme = "README.md"` field makes hatchling
# (the build backend) fail `uv sync`'s editable-install step without it present.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen

# Stage 2 (runtime) -- only what's needed to actually run the pipelines.
# Java is only needed here (Spark's JVM at runtime), not in the builder stage
# (`uv sync` doesn't touch a JVM) -- installing it in stage 1 would have been
# wasted work that gets discarded anyway, but skipping it there also means it
# never pollutes stage 1's own image size accounting.
FROM python:3.13-slim AS runtime

RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java

WORKDIR /app

# Just the built venv (no uv wheel cache) + the uv binary itself (so `uv run
# python3 script.py` -- the invocation every README in this repo documents --
# keeps working without reinstalling/redownloading uv in this stage).
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /usr/local/bin/uv /usr/local/bin/uv
COPY pyproject.toml uv.lock ./

ENV PATH=/app/.venv/bin:$PATH
ENV PYTHONPATH=/app
ENV UV_PROJECT_ENVIRONMENT=/app/.venv

# Source is volume-mounted in dev — COPY is only for standalone builds
COPY . .
