# Novel Ideas — 10 pts

**Full write-up:** [`docs/requirements/novel_ideas.md`](../../docs/requirements/novel_ideas.md)

Two real, already-implemented, verified-working techniques (not speculative) — neither taught
in EDAI, both adopted to solve a concrete problem this project hit.

## Idea 1 — `uv run --no-project` for ephemeral, on-demand multi-Python-version environments (5 pts)

`apache-flink` has no Python 3.13 wheel, and `apache-airflow`'s own dependency set conflicts
with the main project's if installed into the same environment. Instead of downgrading the
whole project or maintaining a second lockfile/CI job, `uv run --no-project --python <version>
--with <packages>` creates a throwaway, uv-managed environment resolved on the fly, no
`pyproject.toml` of its own. Used for the Flink pipeline, DAG tests, and the Airflow
container's isolated venv.

**Proof:** real commands + confirmed working — see the linked doc's "Proof it worked" section.

## Idea 2 — Trino as a federation layer joining Delta Lake + PostgreSQL in one query (5 pts)

A single Trino JDBC connection (two catalogs — `delta` via Hive Metastore over MinIO,
`postgres` direct) lets one SQL query, and one DBeaver connection, join Bronze/Silver (object
storage) against Gold (PostgreSQL) — instead of needing a separate tool/connection per storage
layer. Also directly helps the Schema Design rubric item ("visualize tables on all zones").

**Proof:** verified live against the running container (`SHOW CATALOGS`, cross-catalog
queries against real data) — see the linked doc's "Proof it worked" section.
