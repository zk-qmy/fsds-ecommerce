# Novel Ideas — `coursework/rubrics.md` (Novel ideas, 10 pts)

Both ideas below are real, already-implemented, and verified working in this repo — not
speculative. Neither was taught in EDAI; both were adopted to solve a concrete problem this
project actually hit.

---

## Idea 1 — `uv run --no-project` for ephemeral, on-demand multi-Python-version environments (5 pts)

### The problem

`apache-flink` has no Python 3.13 wheel (max supported is 3.12 as of PyFlink 2.3.0), but this
repo's main venv and `pyproject.toml` are pinned to `>=3.13` (PySpark 4.1.0, Delta Lake, and
the rest of the pipeline code all need 3.13). Airflow itself has the same kind of constraint
in the other direction — `apache-airflow==2.10.5` needs its own carefully pinned dependency
set that conflicts with the project's own `pyproject.toml` if installed into the same
environment (confirmed live — see `dags/plan.md` §3/§4's "two-environment problem" writeup).

The conventional fixes are heavyweight: downgrade the whole project to Python 3.12 (breaks
everything else that wants 3.13), maintain a second `pyproject.toml`/lockfile/CI job per
extra Python version, or reach for a container-per-dependency-set setup even for local dev.

### The idea

`uv run --no-project --python <version> --with <packages>` creates a throwaway, uv-managed
virtual environment resolved on the fly, with no `pyproject.toml` of its own and no
interaction with the main project's environment at all:

```bash
# Flink job — separate Python 3.12 env, apache-flink installed on demand
uv run --no-project --python 3.12 --with apache-flink python3 \
    b_schema_pipelines/pipelines/streaming/offline_stream_pipeline.py --mode optimized

# DAG tests — separate env with the exact Airflow version pinned, isolated
# from the main project's pyspark/deltalake/great-expectations dependency set
uv run --no-project --python 3.12 \
    --with apache-airflow==2.10.5 \
    --with apache-airflow-providers-postgres==6.4.1 \
    --with pytest \
    python3 -m pytest tests/dags/test_dags.py -v
```

Nothing in `pyproject.toml` changes for either use case — both sidestep the root project
entirely rather than editing it. The environment is cached after first resolution (uv's own
package cache), so the ~450MB `apache-flink-libraries` download only happens once, not on
every invocation.

### Where it's used

- `b_schema_pipelines/pipelines/streaming/offline_stream_pipeline.py` — every run, documented
  in `pipelines/streaming/README.md`.
- `tests/dags/test_dags.py` — every CI run, wired into `.github/workflows/ci.yml`'s
  `dag-tests` job (per `dags/plan.md` §13).
- `b_schema_pipelines/dags/dp3_feature_dag.py`'s `run_flink` task — the exact same command,
  invoked from inside the Airflow container via `BashOperator` (a *third* Python environment
  active in that one container: Airflow's own 3.12, the project's 3.13 venv, and this
  ephemeral Flink 3.12 env, all coexisting without conflict — see `dags/plan.md` §4).

### Proof it worked

- Flink pipeline verified end-to-end against real sample data — `uv run --no-project`'s
  on-demand resolution worked with no environment setup beyond having `uv` installed.
- `tests/dags/test_dags.py`'s 11 tests pass in this isolated environment without ever
  installing `apache-airflow` into the main project venv — confirmed by `apache-airflow`'s
  absence from `pyproject.toml`/`uv.lock`.

---

## Idea 2 — Trino as a federation layer joining Delta Lake + PostgreSQL in one SQL query (5 pts)

### The problem

Bronze/Silver live as Delta Lake tables on MinIO (object storage, no native SQL query
engine); Gold lives in PostgreSQL. A question that spans zones — e.g. "how many Bronze rows
were dropped by Silver's dedup, joined against Gold's current row count" — normally requires
either exporting data out of one system into the other, or writing it as two separate queries
and joining the results in application code.

### The idea

Trino (already running for Bronze/Silver browsing, per `docs/02_schema_piplines.md`) can hold
**two catalogs simultaneously** — a `delta` catalog (Bronze/Silver, via the Hive Metastore
pointed at MinIO) and a `postgres` catalog (Gold, connected directly to `fsds-postgres`) — and
join across them in a single SQL statement, something neither Delta Lake nor PostgreSQL can
do alone:

```sql
-- one query, two storage engines, no data movement
SELECT b.customer_id, COUNT(*) AS bronze_rows, g.segment
FROM delta.bronze.customers b
JOIN postgres.gold_ecommerce.dim_customer g
  ON b.customer_id = g.customer_id AND g.is_current
GROUP BY b.customer_id, g.segment;
```

### What it took to get working (not just a config toggle)

`infra/trino/catalog/delta.properties` already had the Delta connector configured, but two
real problems blocked it (both found and fixed by testing directly against the running
container, not assumed from docs — see `features/README.md` §12 for the full account):

1. **Delta Lake tables Spark wrote to MinIO were never registered into the Hive Metastore
   Trino reads from** — writing a Delta table to a path doesn't auto-register it. Fixed with
   `b_schema_pipelines/docs/register_bronze_silver_trino.sql`, using the connector's
   `register_table` system procedure (the more obvious `CREATE TABLE ... WITH (location=...)`
   syntax hit a parser error on this Trino version — confirmed live, not a guess) — one line
   in `delta.properties` (`delta.register-table-procedure.enabled=true`) plus a container
   restart to pick it up.
2. **A second catalog for Gold** — `infra/trino/catalog/postgres.properties` (new file,
   `connector.name=postgresql`, pointed at `fsds-postgres`) — added after the Delta catalog
   was working, specifically to enable the cross-zone join above.

### Proof it worked

Verified live against the running container (`features/README.md` §12):
```
SHOW CATALOGS;                          -- delta, postgres, system
SHOW TABLES FROM delta.bronze;          -- all 6 Bronze tables
SHOW TABLES FROM delta.silver;          -- all 5 Silver tables
SHOW TABLES FROM postgres.gold_ecommerce; -- all 12 Gold/feature tables
SELECT COUNT(*) FROM delta.bronze.customers;  -- real data, not a parse error or empty result
```

This also directly helps the Schema Design rubric item ("visualize tables on all zones") —
a single Trino JDBC connection in DBeaver can now browse Bronze, Silver, *and* Gold, rather
than needing a separate native PostgreSQL connection just for Gold.
