# Airflow DAGs — Implementation Plan

**Status: planning only — no DAG code in this PR.** This document is the design for
`dp1_bronze_dag.py`, `dp2_gold_dag.py`, `dp3_feature_dag.py`, and the one new supporting
module they both depend on (`b_schema_pipelines/dq/validation_runner.py`). Implementation
follows in a separate change, against this plan.

## 0. Sources consulted

- `coursework/rubrics.md` — the actual scored rubric (mini-coursework "Data Pipeline
  Orchestration" + "Data Governance" sections; final-coursework has no DAG-specific lines
  beyond the materialize pipeline, which is out of scope here — see §12).
- `coursework/IMPLEMENTATION_GUIDE.md` §1.8/§1.9 — the (already-corrected, per
  `pipelines/features/README.md`'s note) implementation guidance matching `rubrics.md`.
- `CLAUDE.md` — tech stack, "Quality gates", "All pipeline jobs must be idempotent",
  "Secrets come from Vault" (production-only, not local dev — see §4), Airflow
  Variables/Connections rule.
- `b_schema_pipelines/docs/02_schema_piplines.md` §7 ("Data Pipeline Plan") — schedule times,
  pipeline dependency chain, retry policy, run-metadata format, rerun procedure.
- `b_schema_pipelines/pipelines/features/README.md` §10 — the existing DAG sketch (operator
  choice, task list) this plan supersedes with a concrete, verified design.
- `b_schema_pipelines/dq/README.md` — the §9 suite factories this plan wires up.
- Existing pipeline code: `pipeline_base.py`, `ingest_bronze.py`, `bronze_config.yaml`,
  `transform_silver.py`, `build_gold.py`, `feat_customer_90d.py`, `feat_stream_60m.py`,
  `feat_customer_unified.py`, `flink_stream_pipeline.py`.
- `infra/docker-compose.yml` — the existing (commented-out, unused) `airflow` and `spark`
  service stubs, and `b_schema_pipelines/Dockerfile` (the existing, also-unused, Spark image).
- Live PyPI/Docker Hub checks (not assumed from memory) for every version number below.

---

## 1. Rubric mapping

Only `rubrics.md`'s "Data Pipeline Orchestration" and "Data Governance" sections mention
DAGs/pipelines. Quoting the literal proof requirement for each line item this plan targets:

| Rubric item | Points | Literal proof required |
|---|---|---|
| DP1 Ingest stage | 2.0 | Airflow UI screenshot showing the stages and their order |
| DP1 Validate stage | 2.0 | *(same screenshot — a validate stage exists and runs after ingest)* |
| DP2 Ingest stage | 2.0 | Airflow UI screenshot showing the stages and their order |
| DP2 Validate stage | 2.0 | *(same)* |
| DP3 Ingest stage | 2.0 | Airflow UI screenshot showing the stages and their order |
| DP3 Validate stage | 2.0 | *(same)* |

**None of these six lines name a specific operator class.** The rubric's own general
instruction block says "all connections and variables should be put inside Airflow to reuse
across pipelines" — that's the one concrete technical constraint, and it matches CLAUDE.md's
existing rule. DP1/DP2/DP3's "linked with related tables" (DataHub lineage + data
contract, 12 pts) is a separate rubric section, out of scope for this plan (§12).

This is why the operator decisions below (§3) don't force `SparkSubmitOperator` or
`GreatExpectationsOperator` just because `IMPLEMENTATION_GUIDE.md` names them as examples —
the repo already has a precedent for this exact deviation (`BashOperator` instead of
`SparkSubmitOperator`, documented in `features/README.md` §10), and the rubric's actual proof
bar doesn't require either literal class.

---

## 2. What's already in the repo vs. what's new

| Exists today | New in this plan |
|---|---|
| `ingest_bronze.py`, `transform_silver.py`, `build_gold.py`, `feat_customer_90d.py`, `feat_stream_60m.py`, `feat_customer_unified.py`, `flink_stream_pipeline.py` — all runnable via `uv run python3 <script>.py`, **unmodified by this plan** | `b_schema_pipelines/dags/dp1_bronze_dag.py`, `dp2_gold_dag.py`, `dp3_feature_dag.py` |
| `b_schema_pipelines/dq/{common,bronze_suite,silver_suite,gold_suite}.py` — pure suite factories, **unmodified by this plan** | `b_schema_pipelines/dq/validation_runner.py` — the only new *pipeline* code; collects runtime inputs (row counts, FK key sets) and calls the existing factories |
| `infra/docker-compose.yml`'s commented-out `airflow`/`spark` service stubs | `infra/airflow/Dockerfile` (new); `infra/docker-compose.yml`'s `airflow` service uncommented + rewritten (needs your approval — root-level file, per this repo's standing rule) |
| — | Airflow Variables (`repo_root`) and Connections (`fsds_postgres`, `fsds_minio`), set once via the Airflow UI/CLI, never hardcoded in DAG files |

No existing pipeline script's code or CLI contract changes. The DAGs call them exactly as
their own READMEs already document.

---

## 3. Version decisions (verified live, not assumed)

You asked me to pick current, compatible versions rather than guess. What I checked and why:

| Package | Version chosen | How verified |
|---|---|---|
| `apache-airflow` | **2.10.5** | `pip index versions apache-airflow` on PyPI — full 2.x list confirmed; **2.8.x is not listed at all** (pulled/unavailable), so CLAUDE.md's literal "Airflow 2.8" can't be installed today. 2.10.5 is the newest *2.x* release (avoids Airflow 3.x's breaking DAG-authoring/execution-model changes, which would be a much bigger deviation from CLAUDE.md's stated stack than a patch-version bump). |
| Python (Airflow's own env) | **3.12** | `apache/airflow:2.10.5-python3.12` confirmed to exist on Docker Hub (both slim and full variants). Airflow's own published constraints file exists for `constraints-2.10.5/constraints-3.12.txt` (HTTP 200) but **not** for `constraints-3.13.txt` (HTTP 404) — Airflow 2.x genuinely doesn't support the project's own Python 3.13 pin. This is why Airflow's process and the project's pipeline code run in two separate Python environments in the same container — see §4. |
| `apache-airflow-providers-postgres` | **6.8.0** | Latest on PyPI; used only for `PostgresHook`/`Connection` resolution (see §3 in this table's note below) — not `PostgresOperator`, no raw SQL lives in the DAG files. |
| `apache-airflow-providers-docker` | **not included** | No `DockerOperator` in this design — every task runs inside the one Airflow container via `BashOperator`/`ExternalPythonOperator` (§4, §8), matching the existing single-container pattern. Adding it would mean either Docker-in-Docker or mounting the host's Docker socket into the Airflow container — real extra complexity with no task in this plan that needs it. |
| `airflow-provider-great-expectations` | **not included** | Per your answer in §8's design question — the ephemeral, parameterized `dq/` suite factories from §9 don't map onto this provider's checkpoint-based model without standing up a persisted GX project first (see §8 for the full reasoning). |

**Not added to the project's own `pyproject.toml`.** All four Airflow packages above install
into the *new*, separate `infra/airflow/Dockerfile` image — the main project's `uv.lock`
never sees Airflow, exactly like the existing Flink precedent (`apache-flink` was never added
to `pyproject.toml` either, per `pipelines/streaming/README.md` §8's reasoning: sidestep a
tool with a narrow supported-Python-version window entirely, rather than fight it into the
shared lockfile).

---

## 4. Execution environment

### The two-environment problem

Airflow 2.10.5 needs Python 3.12. The project's pipeline code (PySpark, Delta, the `dq/`
suites) needs Python 3.13 per `pyproject.toml`. Both must run in the same container so tasks
can reach the same Delta/Postgres/MinIO services without cross-container networking. Solution:
**two independent Python environments in one image, bridged only at the point a DAG task
actually needs to call into project code.**

```
infra/airflow/Dockerfile
  FROM apache/airflow:2.10.5-python3.12       ← Airflow's own env: /usr/local/bin/python3 (3.12)
                                                  pip install apache-airflow-providers-postgres==6.8.0
                                                  (installed with Airflow's own constraints file,
                                                   so it can't silently upgrade an Airflow-pinned dep)

  + apt-get install default-jre-headless       ← Java, needed by the PySpark subprocess below
                                                  (installed once at the OS level — shared by
                                                   both Python environments, not venv-scoped)

  + pip install uv (into Airflow's own env, just as a CLI tool — not used to manage
    Airflow's own dependencies, only to bootstrap the project's separate venv below)

  + at container start (not build time — see "why a bind mount" below):
      uv sync --frozen --project /opt/project   ← creates /opt/project/.venv (Python 3.13,
                                                    self-managed by uv, decoupled from the
                                                    image's system Python entirely)
```

- **Why a bind mount, not `COPY . .`:** `infra/docker-compose.yml`'s existing (unused)
  `spark` service already established this pattern — `volumes: [ "..:/opt/project", "/opt/project/.venv" ]`
  (the second entry is an anonymous volume that shadows the mount at exactly `.venv/`, so a
  Linux venv built inside the container is never clobbered by a Windows-native `.venv` the
  host might have, and vice versa). Reusing it means DAG/pipeline code edits on the host are
  picked up without rebuilding the image — the same fast local-dev loop every other pipeline
  in this repo already has.
- **Why `network_mode: host`:** every existing pipeline script's config
  (`pipeline_config.yaml`'s `postgres.host: localhost` / `minio.endpoint: http://localhost:9000`,
  `build_gold.py`'s `postgres_url` default) is hardcoded to `localhost`. Host networking makes
  `localhost` inside the Airflow container resolve to the same `postgres`/`minio`/`trino`
  containers every other README already tells you to reach at `localhost:5432` /
  `localhost:9000` — zero changes to any existing script or config file. This is
  Linux-only (fine — WSL2 is Linux; would need revisiting for a real multi-host deployment,
  but `infra/docker-compose.yml` is explicitly local-dev-only per CLAUDE.md).

### How a task actually runs

| Task type | Runs as | Which Python | How it reaches project code |
|---|---|---|---|
| Ingest/transform/build/feature (`ingest_bronze.py`, `transform_silver.py`, `build_gold.py`, `feat_*.py`, `flink_stream_pipeline.py`) | `BashOperator` | `/opt/project/.venv` (3.13) | `cd {{ var.value.repo_root }} && uv run python3 <script>.py <args>` — identical to what every pipeline's own README already documents; Airflow never imports this code |
| Validate (all three DAGs) | `ExternalPythonOperator` | `/opt/project/.venv/bin/python3` (3.13) — **not** Airflow's own 3.12 interpreter | Airflow's DAG-parsing code (3.12) resolves the Postgres/MinIO Airflow Connection into plain values, then hands them as `op_kwargs` to a callable in `b_schema_pipelines.dq.validation_runner`, executed via cloudpickle in the target venv (built-in Airflow 2.4+ mechanism, no new framework) |
| Cross-DAG wait (`dp2`→`dp1`, `dp3`→`dp2`) | `ExternalTaskSensor` | Airflow's own 3.12 env | Core Airflow sensor — reads Airflow's own metadata DB, no project code involved |

This keeps the property every other part of this repo already has: **Airflow orchestrates;
it never hosts pipeline business logic in its own process.** `ExternalPythonOperator` is the
one exception, and even then the actual GX/psycopg2/pandas code still runs in the project's
own venv — Airflow's 3.12 process only touches Connection objects and passes plain values
across.

---

## 5. DAG inventory

Three DAGs, one per rubric line item (DP1/DP2/DP3) — not a single unified DAG, so each has
its own Airflow UI screenshot with its own stages/order, matching the rubric's literal
per-pipeline proof requirement. `materialize_dag`/`scoring_dag`/`retrain_trigger_dag` are
documented in `docs/02_schema_piplines.md`'s dependency chain but belong to Section 04 (ML) —
out of scope here (§12).

| DAG | Schedule | Zone(s) | Rubric item |
|---|---|---|---|
| `dp1_bronze` | `0 0 * * *` (00:00) | raw → Bronze (Delta on MinIO) | DP1 |
| `dp2_gold` | `0 1 * * *` (01:00) | Bronze → Silver → Gold (Delta → Postgres) | DP2 |
| `dp3_feature` | `30 2 * * *` (02:30) | Gold + Flink stream → feature tables (Postgres) | DP3 |

Schedule times match `docs/02_schema_piplines.md` §7 exactly (Bronze 00:00, Silver 01:00 /
Gold 02:00 folded into one `dp2_gold` DAG per the rubric's "(or bronze -> gold only)" note,
Features 02:30).

---

## 6. `dp1_bronze` — tasks

```
ingest_bronze  →  validate_bronze
```

| Task | Operator | Command / callable | Input | Output |
|---|---|---|---|---|
| `ingest_bronze` | `BashOperator` | `cd {{ var.value.repo_root }} && uv run python3 b_schema_pipelines/pipelines/bronze/ingest_bronze.py` | `a_data_generator/outputs/{offline,streaming}/*` | 6 Bronze Delta tables at `s3a://bronze-data/bronze/<table>/` |
| `validate_bronze` | `ExternalPythonOperator` | `validation_runner.validate_bronze_tables(minio_conn, tables=[...])` | the 6 Bronze Delta tables just written | raises on any suite failure → task fails; nothing downstream reads Bronze's validate result programmatically today (no DAG currently depends on it beyond `dp2`'s cross-DAG wait) |

`ingest_bronze.py` already runs its own inline `_check_quality` (schema/volume/null-PK) per
table before every Delta write — `validate_bronze` is a second, independent check
(the DataHub/Airflow-facing data contract per `dq/README.md`), not a replacement. Redundant
by design: the inline check can't block a run that already got past ingestion; the Airflow
gate can block `dp2` from starting on visibly bad Bronze data.

**`validate_bronze_tables`** (in the new `validation_runner.py`) loops the 6 tables in
`bronze_config.yaml`, reads each with `deltalake.DeltaTable(path).to_pandas()` (the `deltalake`
package — already in `pyproject.toml` — reads a Delta table directly via Rust/Arrow, no JVM/
Spark session needed just to validate a table the ingest task already wrote seconds ago), and
validates against `bronze_expectation_suite(table, expected_columns)` — `expected_columns`
read straight from `bronze_config.yaml`'s `quality.expected_columns`, the same file
`ingest_bronze.py` itself already uses, so there's one source of truth for "what columns
should be here," not two.

---

## 7. `dp2_gold` — tasks

```
wait_for_bronze (ExternalTaskSensor)
        │
        ▼
transform_silver  →  validate_silver  →  build_gold  →  validate_gold
```

| Task | Operator | Command / callable | Input | Output |
|---|---|---|---|---|
| `wait_for_bronze` | `ExternalTaskSensor` | waits on `dp1_bronze.validate_bronze`, `execution_delta=timedelta(hours=1)`, `mode="reschedule"`, `timeout=1800` | `dp1_bronze`'s run for the corresponding logical date | unblocks once Bronze's validate task succeeds |
| `transform_silver` | `BashOperator` | `uv run python3 b_schema_pipelines/pipelines/silver/transform_silver.py --mode optimized` | the 5 non-event Bronze Delta tables | 5 Silver Delta tables at `s3a://silver-data/silver/<table>/` |
| `validate_silver` | `ExternalPythonOperator` | `validation_runner.validate_silver_tables(minio_conn, baseline_row_counts, bronze_row_count_order_items)` | the 5 Silver Delta tables | raises on failure |
| `build_gold` | `BashOperator` | `uv run python3 b_schema_pipelines/pipelines/gold/build_gold.py --mode optimized` | the 5 Silver Delta tables | 5 dims + 3 facts + 1 OBT in `gold_ecommerce`, plus the 5 Postgres indexes from `_create_indexes()` |
| `validate_gold` | `ExternalPythonOperator` | `validation_runner.validate_gold_tables(postgres_conn, baseline_row_counts)` | the 9 Gold tables | raises on failure |

**Why one DAG for Silver+Gold, not two:** the rubric explicitly allows "bronze -> silver and
gold zone (or bronze -> gold only)" as a single DP2 line — splitting it into two DAGs would
buy nothing against the rubric and would need a second `ExternalTaskSensor` hop for no reason.

**Row-count baselines.** `baseline_row_counts` for the volume check (§9 dq/README's ±30%
check) comes from the *previous* run's row count, not a hardcoded number. `validation_runner.py`
reads it from `PipelineBase`'s own structured JSON log line (the `STRUCTURED {...}` entry
`log_run` already writes to `logs/<PREFIX>/<run_id>.log` — see §9 for exactly how). First-ever
run has no prior baseline; the volume check is skipped that run (matches `dq/`'s own factory
contract: `baseline_row_count=None` omits the check, not "fails closed").

**Gold's `fk_checks` / `unique_column` inputs.** Per `dq/gold_suite.py`'s existing contract,
`validate_gold_tables` collects: each dimension's full distinct surrogate-key set (one
`SELECT DISTINCT <key>` per dim, small — the largest is `dim_customer` at ~120k rows) for
`fk_checks`, and the `is_current`-filtered slice of `dim_customer` for the `unique_column`
check. Both via the same `psycopg2` connection pattern `build_gold.py` already uses (see §8).

---

## 8. `dp3_feature` — tasks

```
wait_for_gold (ExternalTaskSensor)
        │
        ▼
run_flink  ──────────────┐
        │                │
        ▼                ▼
feat_customer_90d   feat_stream_60m
        │                │
        └───────┬────────┘
                 ▼
        feat_customer_unified
                 │
                 ▼
          validate_features
```

| Task | Operator | Command / callable | Input | Output |
|---|---|---|---|---|
| `wait_for_gold` | `ExternalTaskSensor` | waits on `dp2_gold.validate_gold`, `execution_delta=timedelta(hours=1, minutes=30)` | `dp2_gold`'s run | unblocks once Gold's validate task succeeds |
| `run_flink` | `BashOperator` | `uv run --no-project --python 3.12 --with apache-flink python3 b_schema_pipelines/pipelines/streaming/flink_stream_pipeline.py --mode optimized` | `a_data_generator/outputs/streaming/events.json` | cleaned/deduped/windowed events at `b_schema_pipelines/streaming_data/flink_clean_events/optimized/` |
| `feat_customer_90d` | `BashOperator` | `uv run python3 b_schema_pipelines/pipelines/features/feat_customer_90d.py --snapshot-date {{ ds }}` | Gold `fact_order`/`fact_order_item`/`dim_date` | rows in Postgres `feat_customer_90d` |
| `feat_stream_60m` | `BashOperator` | `uv run python3 b_schema_pipelines/pipelines/features/feat_stream_60m.py --events-source b_schema_pipelines/streaming_data/flink_clean_events/optimized` | Flink's cleaned output | rows in Postgres `feat_stream_60m` |
| `feat_customer_unified` | `BashOperator` | `uv run python3 b_schema_pipelines/pipelines/features/feat_customer_unified.py` | `feat_customer_90d` + `feat_stream_60m` | rows in Postgres `feat_customer_unified` |
| `validate_features` | `ExternalPythonOperator` | `validation_runner.validate_feature_tables(postgres_conn, baseline_row_counts)` | all three feature tables | raises on failure |

`feat_customer_90d` and `feat_stream_60m` run in parallel (both depend only on `run_flink` +
`wait_for_gold`, not on each other) — `feat_customer_unified` depends on both, matching
`feat_customer_unified.py`'s own as-of join needing both tables populated first.

**`--snapshot-date {{ ds }}`** is templated to the DAG's logical date rather than left at
`feat_customer_90d.py`'s own default (`datetime.now()`) — this is the one place this plan adds
a CLI argument beyond what a script's README already shows, and it's for a concrete reason:
`{{ ds }}` makes a backfill (re-running `dp3_feature` for a past date) compute features
*as of that past date*, not today's date. Without it, backfills would silently compute the
wrong window. `feat_stream_60m.py`/`feat_customer_unified.py` have no date parameter (they
process whatever is currently in their input tables) — nothing to template there.

**`dq/gold_suite.py`/`silver_suite.py` aren't reused for feature tables** — `feat_customer_90d`
and `feat_stream_60m` aren't Gold tables in `build_gold.py`'s sense (no surrogate keys, no
dims), but they share Gold's storage (Postgres) and most of its check shape (schema, null-PK
on `(customer_id, event_timestamp)`, volume). `validate_feature_tables` calls
`gold_expectation_suite(table, expected_columns, pk_columns=["customer_id", "event_timestamp"], baseline_row_count=...)`
directly — no `unique_column`/`fk_checks` (feature tables have neither) — rather than adding a
fourth `dq/*_suite.py` file for two checks that already exist. This is a deliberate reuse, not
a new module, called out here so it doesn't look like an oversight.

---

## 9. `validation_runner.py` — design

New file: `b_schema_pipelines/dq/validation_runner.py`. This is the one module in this plan
that touches Postgres/Delta directly — everything else in `dq/` stays pure per its own
contract (`dq/README.md`: "None of dq/'s three files touch Spark or Postgres themselves").

```python
def validate_bronze_tables(minio_cfg: dict, tables: list[str]) -> None:
    """Read each Bronze Delta table via deltalake.to_pandas(), build its suite via
    bronze_expectation_suite(), validate. Raises AssertionError listing every failing
    table+expectation (not just the first) so one Airflow task failure tells you
    everything that's wrong, not one problem at a time across N re-runs."""

def validate_silver_tables(minio_cfg: dict, baseline_row_counts: dict[str, int],
                            bronze_order_items_row_count: int | None) -> None: ...

def validate_gold_tables(postgres_cfg: dict, baseline_row_counts: dict[str, int]) -> None:
    """Also collects fk_checks (distinct key per dim) and the is_current-filtered
    dim_customer batch before building gold_expectation_suite per table."""

def validate_feature_tables(postgres_cfg: dict, baseline_row_counts: dict[str, int]) -> None: ...

def _read_baseline_row_count(pipeline_prefix: str, table: str) -> int | None:
    """Parses the most recent STRUCTURED JSON log_run entry for this table from
    logs/<pipeline_prefix>/*.log. Returns None if no prior run exists (first run —
    volume check is skipped, per dq/'s own factory contract)."""

def _validate_suite(suite, df) -> None:
    """Builds a GX ephemeral pandas Validator for df, runs suite, raises with the
    list of failed expectation types + observed values if suite.validate(df).success
    is False."""
```

Each `validate_*_tables` function:

1. Reads the same `expected_columns`/`pk_columns` source the pipeline scripts themselves use
   (`bronze_config.yaml` for Bronze/Silver; a small static mapping mirroring `build_gold.py`'s
   `_build_*` `.select(...)` column lists for Gold — see the known limitation below).
2. Collects whatever runtime inputs that layer's suite factory needs (§6–§8 above).
3. Calls the existing `dq/{bronze,silver,gold}_suite.py` factory — unmodified.
4. Validates the actual table contents (not just column names) via GX's ephemeral
   pandas Validator.
5. Raises on any failure — an `ExternalPythonOperator` task that raises fails the Airflow
   task, which is what blocks the next DAG stage / surfaces red in the UI.

**Known limitation, stated explicitly rather than hidden:** Gold's `expected_columns` has no
single source of truth to read from at runtime the way Bronze/Silver's do — `build_gold.py`
selects columns inline in Python (`.select("order_key", "customer_key", ...)`), not from a
config file. `validation_runner.py` keeps a small static `GOLD_TABLE_COLUMNS` dict mirroring
those `.select(...)` lists. If `build_gold.py`'s selected columns change, this dict must be
updated in the same PR — there is no automatic check that they stay in sync (a comment in
both files points at each other). Considered and rejected: deriving `expected_columns` from
the Gold table's *own* live schema at validation time — that makes the schema check
tautological (it would always pass, since it'd compare the table against itself).

---

## 10. Airflow Variables & Connections

Per CLAUDE.md: "All Airflow connections (postgres, spark, minio) must be defined via Airflow
Variables/Connections UI — not hardcoded in DAG files." Scope of this rule for this plan: it
applies to the *new* code this plan adds (the validate tasks), not a retrofit of the existing
pipeline scripts' own `pipeline_config.yaml`-based config loading, which stays as-is and
unmodified (see §2).

| Name | Type | Value (local dev) | Used by |
|---|---|---|---|
| `repo_root` | Variable | `/opt/project` | every `BashOperator`'s `cd {{ var.value.repo_root }}` |
| `fsds_postgres` | Connection (Postgres) | host=`localhost`, port=`5432`, schema=`fsds`, login=`fsds`, password=`fsds` | `validate_gold`, `validate_features` — resolved via `PostgresHook`, then passed as plain values into `validation_runner.py` |
| `fsds_minio` | Connection (generic HTTP, no dedicated provider needed) | host=`localhost`, port=`9000`, login=`minio_access_key`, password=`minio_secret_key` | `validate_bronze`, `validate_silver` — resolved via `BaseHook.get_connection()`, passed into `deltalake`'s S3 storage options |

All four set once via `airflow connections add` / `airflow variables set` (documented as
setup steps in the DAGs' module docstrings, per this repo's existing docstring-header
convention) — never as literals in the three DAG files.

---

## 11. Retry & error handling

Matches `docs/02_schema_piplines.md` §7's "Retry and Recovery" exactly — this plan doesn't
invent a new policy:

```python
default_args = {
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(seconds=120),
}
```

applied at the DAG level in all three files (30s → 60s → 120s, Airflow's built-in exponential
backoff — no custom retry code).

- **Idempotency:** every task this plan wires up is already idempotent by the underlying
  script's own design (`docs/02_schema_piplines.md` §7's "Update Strategy" table:
  Bronze append is `pipeline_run_id`-keyed, Silver/Gold overwrite, feature tables
  delete-then-insert by snapshot/window). A retried task re-running produces the same
  output, not duplicates — nothing new to build here.
- **`ExternalTaskSensor` timeout:** `mode="reschedule"` + a 30-minute timeout (§7, §8) means a
  stuck upstream DAG fails the sensor with a clear timeout error in the UI rather than
  blocking a worker slot indefinitely.
- **Validate task failure blocks downstream:** an `ExternalPythonOperator` that raises fails
  the task natively — Airflow's own dependency graph (`>>`) already stops `build_gold` from
  running if `validate_silver` failed, no extra branching logic needed.
- **Email alerting:** `docs/02_schema_piplines.md` §7 documents "Airflow sends email alert on
  any task failure" as the intended design. Out of scope for this plan — it needs SMTP
  configuration in `infra/docker-compose.yml`'s airflow service (a root-level file change
  beyond what this plan already needs approval for) and isn't required by any rubric line
  (same precedent as the existing "Slack alerting explicitly excluded" scoping decision in
  `pipelines/features/README.md` §9's note).

---

## 12. Out of scope (explicit, not an oversight)

| Item | Why excluded |
|---|---|
| `materialize_dag`, `scoring_dag`, `retrain_trigger_dag` | Section 04 (ML), not Section 02's DP1/DP2/DP3 rubric lines. `pipelines/features/README.md` §3 already scoped this the same way. |
| DataHub lineage emission as an Airflow task | Rubric scores it separately ("DP1/DP2/DP3 linked with related tables", 12 pts, distinct from "Data Pipeline Orchestration"'s 12 pts). `emit_lineage()` doesn't exist yet (`pipelines/features/README.md` §11, not started) — can't wire a DAG task to a function that isn't written. When §11 lands, each pipeline script's own `run()` calls `emit_lineage()` internally (per §11's design) — no separate Airflow task needed for it. |
| Literal `GreatExpectationsOperator` / `airflow-provider-great-expectations` | Per your answer in this session — the §9 suite factories are ephemeral-context and parameter-injected per run; this provider expects a persisted `great_expectations.yml` FileDataContext + Checkpoint config, which doesn't exist and is materially more infrastructure than the rubric's proof bar needs. |
| `DockerOperator` / per-task containers | No task in this plan needs per-task isolation; adds Docker-in-Docker or host-socket-mount complexity for no rubric benefit. |
| SMTP/email alert configuration | Documented as intended design in `docs/02_schema_piplines.md`, not required by any rubric line, needs a root-level docker-compose change beyond this plan's scope. |
| Great Expectations Slack alerting | Already explicitly excluded in `pipelines/features/README.md` §9 — this plan doesn't reopen that. |

---

## 13. Testing plan (description only — no test code in this PR)

### Unit tests

| Test | What it proves | Why it's the important one |
|---|---|---|
| DAG import / no cycles, one per DAG file | `from b_schema_pipelines.dags.dp1_bronze_dag import dag; assert not dag.test_cycle()` (or Airflow's `DagBag(...).import_errors == {}`) | This is CLAUDE.md's own explicit CI requirement ("DAG import check (no circular dependencies)") — a DAG with a typo'd task dependency fails silently in the UI otherwise, and it's the cheapest possible test to write. |
| Task count + `upstream_task_ids` per DAG matches §6/§7/§8's task graphs | e.g. `dp2_gold_dag`'s `build_gold` task has `upstream_task_ids == {"validate_silver"}` | Directly verifies the execution order the rubric's screenshot proof depends on — if this drifts from the plan, the screenshot won't match what's documented, which is exactly the kind of doc/code drift `pipelines/features/README.md` has already caught twice (AQE skew config, feature-job idempotency) by testing behavior, not assuming the sketch is what shipped. |
| `validation_runner._read_baseline_row_count` | Parses a `STRUCTURED {...}` log line correctly; returns `None` on no prior log | Mirrors the existing testing pattern for `PipelineBase.log_run` (mock the logger, assert on the JSON entry) — this is the one piece of new parsing logic in the whole plan, worth isolating. |
| `validation_runner.validate_gold_tables` (mocked psycopg2 + a small in-memory `dim_customer`/`fact_order` pandas fixture) | Builds the *right* `fk_checks`/`unique_column` inputs and calls `gold_expectation_suite` with them; raises when a fixture violates a check (e.g. a `customer_key` not in the dim's key set) | This is the one function in the whole plan with real logic (not just orchestration) — it's where a bug would actually hide, unlike the DAG files themselves which just wire existing pieces together. |

### Integration / end-to-end tests

| Test | What it proves | Why only this one (not more) |
|---|---|---|
| `airflow dags test dp1_bronze <date>` (or `dp2_gold`, `dp3_feature`) against the real local docker-compose stack, run manually/in CI after `docker compose up`, asserting the run exits 0 and the expected Bronze/Silver/Gold/feature row counts land | Proves the whole chain — Docker image, both Python environments, the Connections, the actual `deltalake`/`psycopg2` reads inside `validation_runner.py` — works together, not just in isolation. Every piece above is mocked in its unit test; this is the only place that catches an environment-wiring bug (e.g. a Connection misconfigured, `network_mode: host` not actually resolving `localhost` the way §4 assumes). | This mirrors `pipelines/features/README.md`'s own stated philosophy: "verified against a real running stack... not just passing unit tests" caught two real bugs unit tests missed. One live end-to-end run per DAG (three total) is enough to catch an environment problem — running it more than once per DAG doesn't buy additional confidence, since the failure mode this test exists to catch is "the whole chain is wired wrong," not per-branch logic (unit tests already cover that). |
| A deliberately-broken-input run of `dp1_bronze` (e.g. point `--source-dir` at a Parquet file missing a required column) asserting `validate_bronze` fails and `dp2_gold`'s `wait_for_bronze` sensor times out rather than proceeding | Proves the actual gate behavior the rubric's "Validate stage" line item is scored on — that a bad upstream table blocks downstream, not just that a validate task exists and always passes | This is the other half of "prove the gate works," and it's cheap to add once the happy-path e2e test above already stands up the stack — not a second full environment test, just a different input to the same harness. |

**Not proposed:** a full pytest-coverage/mutation-testing gate on the new DAG/validation
code. CLAUDE.md's coverage (>90%) and mutation-score (>80%) thresholds are explicitly scoped
to `d_ml/api/` and `d_ml/src/` in the Testing Strategy table — Section 02's existing
bronze/silver/gold/feature test suites (184 passing tests today) follow a functional-testing
pattern, not a coverage-gated one, and this plan keeps that consistency rather than
introducing a stricter bar for just the DAG code.

### Post-implementation refinement (found during review, not part of the original plan)

- **`validation_runner.validate_silver_tables` / `validate_feature_tables` had zero direct
  unit tests** — only `validate_gold_tables`/`validate_bronze_tables` were covered, even
  though `validate_silver_tables` carries the most logic of the four (Problem A/B/C-specific
  suite params, auto-deriving `bronze_order_items_row_count` via a live Delta read when not
  supplied). Added, mirroring `mock_gold_io`'s pattern: `mock_silver_io`/`mock_feature_io`
  fixtures patching `_read_delta_table`/`_read_postgres_table` with schema-correct empty (or
  deliberately-broken) pandas fixtures — no live MinIO/Postgres needed, same as every other
  test in this file.
- **`tests/dags/test_dags.py` was written but never wired into CI.** It only runs in the
  ephemeral `--no-project --python 3.12 --with apache-airflow==2.10.5` environment this
  file's own docstring documents — the repo's one CI workflow (`.github/workflows/ci.yml`)
  runs `pytest tests/` in the main 3.13 venv, where `apache-airflow` is deliberately absent
  (§3), so every test in that file hit `pytest.importorskip("airflow")` and silently skipped.
  Fixed by adding a second `dag-tests` job to `ci.yml` that runs the exact command this file's
  docstring already specifies — closes CLAUDE.md's Track A CI requirement ("DAG import check
  (no circular dependencies)") for real, not just on paper.

---

## 14. Approval checklist

Per this repo's standing rule (files outside `b_schema_pipelines/` need sign-off before
touching):

| File | Change | Needs approval |
|---|---|---|
| `infra/airflow/Dockerfile` | new file | yes — new root-level infra file |
| `infra/docker-compose.yml` | uncomment + rewrite the `airflow` service (image build, `network_mode: host`, bind mounts, env) | yes — root-level file |
| `b_schema_pipelines/dags/*.py` | new files | no — inside `b_schema_pipelines/` |
| `b_schema_pipelines/dq/validation_runner.py` | new file | no — inside `b_schema_pipelines/` |
| `pyproject.toml` / `uv.lock` | **no change** | n/a — Airflow and its providers live only in `infra/airflow/Dockerfile`, per §3 |

---

## 15. Open risks / things to confirm during implementation

- **RESOLVED — `ExternalPythonOperator` + `deltalake`/GX pandas validation.** This was
  originally flagged as "a design, not yet a proven spike" (the ephemeral-context pandas
  validation API hit a `DatasourceError` during suite-building work). Implementation resolved
  it: `validation_runner._validate_suite` builds a *second*, batch-only ephemeral context
  (`context.data_sources.add_pandas(...)` → `add_dataframe_asset` → `add_batch_definition_whole_dataframe`
  → `batch.validate(suite)`), deliberately never re-registering the suite object itself (which
  already carries its own factory-created context) against that second context — re-adding it
  was the actual source of the original `DatasourceError`. Proven by `test_validate_suite_returns_
  empty_list_when_data_passes` / `..._reports_expectation_type_and_observed_value_on_failure` in
  `tests/b_schema_pipelines/test_validation_runner.py`, which run *real* (non-mocked) GX
  validation and pass. No fallback to the raw `Validator`/`PandasExecutionEngine` API was needed.
- **`apache/airflow:2.10.5-python3.12` image size / cold-start time** for local dev wasn't
  measured — first `docker compose up` will be slow (image pull + `uv sync` on first
  container start). Acceptable for coursework-grade local dev; not optimized further here.
- **`GOLD_TABLE_COLUMNS`/`GOLD_TABLE_KEYS` manual sync with `build_gold.py`** (§9's "Known
  limitation") remains an accepted, unfixed trade-off — considered adding a drift-detection
  test (e.g. parsing `build_gold.py`'s AST for `.select(...)` calls) during this review and
  rejected it as more complexity than a coursework-scale, already-documented risk warrants.
