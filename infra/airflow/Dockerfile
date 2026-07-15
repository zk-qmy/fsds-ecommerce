# Extends the official Airflow image rather than building Airflow from scratch
# on top of this project's own python:3.13-slim image (b_schema_pipelines/Dockerfile)
# -- Airflow's package installs are picky about being done via its own constraints
# mechanism, and fighting that against pyspark's version pins is more trouble than
# starting from the image that already has Airflow correctly resolved.
#
# Two independent Python environments live in this image (dags/plan.md §4):
#   - Airflow's own env (this image's system Python, 3.12) -- runs the DAG
#     scheduler/webserver/sensors, resolves Connections, orchestrates.
#   - The project's own env (/opt/venvs/project, Python 3.13, self-managed by
#     uv via UV_PROJECT_ENVIRONMENT, built at container START not image build
#     time -- see docker-compose.yml's `command:`/`environment:` for the
#     airflow service) -- runs every BashOperator command and every
#     ExternalPythonOperator's validate_* callable.

FROM apache/airflow:2.10.5-python3.12

USER root

# Java -- needed by the PySpark subprocesses BashOperator tasks shell out to
# (ingest_bronze.py, transform_silver.py, build_gold.py, feat_*.py). OS-level,
# shared by both Python environments in this image, not venv-scoped.
RUN apt-get update && \
    apt-get install -y --no-install-recommends default-jre-headless && \
    apt-get clean && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/default-java

USER airflow

# apache-airflow-providers-postgres -- resolves the fsds_postgres Connection via
# {{ conn.fsds_postgres.* }} Jinja macros in the DAG files (dags/plan.md §10).
#
# No version pinned here on purpose -- Airflow's own constraints file for this
# exact release is authoritative on which provider version is actually
# compatible, and pinning a version ourselves fights it. Learned this the hard
# way during implementation: dags/plan.md §3 originally named 6.8.0 (requires
# apache-airflow>=2.11.0, rejected by uv's resolver against the 2.10.5 pin
# above), then 6.4.1 (imports fine standalone, but the *official* 2.10.5
# constraints file pins apache-airflow-providers-postgres==6.0.0 specifically
# and a docker build with `pip install ...==6.4.1 --constraint ...` fails with
# ResolutionImpossible over that exact conflict). Letting the constraint file
# pick resolves to 6.0.0 and actually builds.
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.12.txt"

# uv -- a CLI tool only, to bootstrap the project's OWN venv at container start.
# Never used to manage Airflow's own dependencies.
RUN pip install --no-cache-dir uv

# docker-compose.yml mounts an anonymous volume at /opt/venvs (NOT at
# /opt/project/.venv itself -- confirmed live that mounting exactly at the
# venv's own path means `uv sync` can never fully rebuild a stale venv: it can
# delete files INSIDE a mounted directory but can't rmdir the mountpoint
# itself ("Device or resource busy"), which is exactly what uv does when an
# existing venv's Python interpreter no longer resolves, e.g. after an image
# rebuild). UV_PROJECT_ENVIRONMENT (docker-compose.yml) points the project's
# own venv at /opt/venvs/project instead -- a plain, freely-recreatable
# subdirectory of that mount, never the mountpoint itself.
#
# Docker initializes an anonymous volume's content/ownership from whatever
# already exists at that path in the image -- without this, it defaults to
# root:root, and `uv sync` (run as the non-root airflow user below) fails with
# "Permission denied" trying to create files in it. Pre-creating the directory
# here, owned by airflow, fixes that -- confirmed by hitting the permission
# error live (at the old /opt/project/.venv path) and rebuilding with this fix.
USER root
RUN mkdir -p /opt/venvs && chown -R airflow:root /opt/venvs
USER airflow
