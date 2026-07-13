# Section 02 — Remaining Implementation Plan

This is the ground-truth plan for finishing Section 02 (Schema Pipelines, mini-coursework
items 1.5–1.10 in `coursework/IMPLEMENTATION_GUIDE.md`). It exists here — rather than in
`bronze/README.md` or `gold/README.md` — because the largest remaining surface is the
`features/` pipeline, and everything else (Flink, Airflow, DataHub) exists to feed it or
consume its output.

**Do not start coding from this doc without re-reading it against the current repo state.**
Code and design drift; if something below no longer matches `git log` / the file tree, the
code wins and this doc must be updated in the same PR.

---

## 0. What's already done (do not re-build)

| Component | File | Status |
|---|---|---|
| Bronze ingest | `pipelines/bronze/ingest_bronze.py` | ✅ done, tested (`tests/b_schema_pipelines/test_ingest_bronze.py`) |
| Silver transform | `pipelines/silver/transform_silver.py` | ✅ done, tested — `--mode baseline\|optimized`, fixes A/B/C + broadcast join demo |
| Gold build | `pipelines/gold/build_gold.py` | ✅ done, tested (31 tests) — `--mode baseline\|optimized`, SCD2 `dim_customer`, deterministic surrogate keys |
| Shared infra | `pipelines/pipeline_base.py`, `common/delta_writer.py`, `minio_client.py` | ✅ done, tested |
| Delta compaction | `common/delta_writer.py:100` (`.optimize().executeCompaction()`) | ✅ done — Z-order is **not** done (see §4) |
| Schema design doc | `docs/02_schema_piplines.md` | ✅ complete (dims, facts, OBT, naming, SLAs, indexing/partitioning plan — not yet implemented in code) |
| Spark optimisation report (Fixes 1–4) | `docs/02_spark_optimisation_report.md` | ✅ Fixes 1–4 (AQE skew, NULL fill, dedup, broadcast join) accurate. **Fixes D/E/F (Flink section) are currently wrong** — see §2 |

## 1. What's stubbed (tests exist, implementation raises `NotImplementedError`)

| File | Class | Test file | Status |
|---|---|---|---|
| `pipelines/features/feat_customer_90d.py` | `CustomerFeature90d` | `tests/b_schema_pipelines/test_feat_customer_90d.py` | ✅ done — renamed + implemented (§5), 12/12 tests pass |
| `pipelines/features/feat_stream_60m.py` | `StreamFeature60m` | `tests/b_schema_pipelines/test_feat_stream_60m.py` | ⬜ renamed, not yet implemented (§6) |

Every test in both files was `@pytest.mark.xfail(strict=True, raises=NotImplementedError, ...)`.
As each private method is implemented, **delete its `xfail` marker** — `strict=True` means
a passing test under an `xfail` marker is itself a failure, so leaving markers in place
after implementing the code will break CI, not fix it. (Done for `feat_customer_90d.py`;
still pending for `feat_stream_60m.py`.)

**Known pre-existing bug in `test_feat_stream_60m.py` (found while verifying §5's changes
didn't regress the suite, not caused by them):** `test_burst_flag_value`'s `parametrize` builds
`spark.createDataFrame([("E_TEST", event_time, "C001")], schema=schema)` where `schema`
declares `event_timestamp` as `TimestampType()` but `event_time` is a raw Python string — the
`.withColumn("event_timestamp", F.to_timestamp(...))` cast that follows never gets a chance to
run, because `createDataFrame` itself rejects the type mismatch first
(`PySparkTypeError: ... can not accept object '...' in type <class 'str'>`). Since that's not
`NotImplementedError`, the `xfail(strict=True, raises=NotImplementedError)` marker doesn't
match and pytest reports a hard failure instead of an expected xfail — all 11 parametrized
cases of that one test currently fail on `main`/this branch regardless of `_add_burst_flag`'s
implementation state. Fix when starting §6: either type the schema's `event_timestamp` field
as `StringType()` and cast after `createDataFrame`, or pass already-cast values in.

## 2. Known doc/code mismatch to resolve first

`docs/02_spark_optimisation_report.md` §"Flink Fixes (Problems D/E/F)" documents real PyFlink
code (`WatermarkStrategy`, `KeyedProcessFunction`, Flink Web UI at `localhost:8081`) as if it
lives inside `feat_stream_60m.py`. It doesn't — that file is a Spark job. There is no Flink
anywhere in `pyproject.toml` or `infra/docker-compose.yml` today.

**Resolution (confirmed):** build real PyFlink jobs. `feat_stream_60m.py` stays a Spark job
that computes the 4 offline-style feature aggregates; a new Flink pipeline sits upstream of it
and owns Problems D/E/F. See §3 for the split. Once §3 is implemented, update
`02_spark_optimisation_report.md`'s Fix D/E/F code blocks to point at
`pipelines/streaming/flink_stream_pipeline.py` instead of `feat_stream_60m.py`.

---

## 3. File & rename plan — ✅ done

Renames (confirmed: code follows `CLAUDE.md` naming, not the other way round):

```
pipelines/features/feature_customer_90d.py  →  pipelines/features/feat_customer_90d.py
pipelines/features/feature_customer_60m.py  →  pipelines/features/feat_stream_60m.py
tests/b_schema_pipelines/test_feature_customer_90d.py → test_feat_customer_90d.py
tests/b_schema_pipelines/test_feature_customer_60m.py → test_feat_stream_60m.py
```

Done via `git mv` (preserves history) + import-line updates in both test files + the two
run-command docstring lines in each pipeline file + `README.md` (root, 3 lines — approved) +
`pyproject.toml` (root, added `psycopg2-binary` — approved, see §5).

Class names (`CustomerFeature90d`, `StreamFeature60m`) and `PREFIX`/`FEAT_TABLE` constants
are unchanged — only the module filenames and their imports move. Update the two import
lines in the renamed test files and nowhere else (no other file imports these modules yet).

New files to create:

```
b_schema_pipelines/pipelines/features/
├── feat_customer_90d.py            # renamed, then implemented (§5)
├── feat_stream_60m.py              # renamed, then implemented (§6)
├── feat_customer_unified.py        # NEW — point-in-time join of the above two (§7)
└── README.md                       # this file

b_schema_pipelines/pipelines/streaming/                # NEW (§8)
└── flink_stream_pipeline.py        # baseline/optimized PyFlink job (Problems D/E/F)

b_schema_pipelines/dq/                                  # NEW (§9)
├── bronze_suite.py                 # GE expectations: schema + null-PK checks
├── silver_suite.py                 # GE expectations: skew%, null-fill, dedup rate
└── gold_suite.py                   # GE expectations: uniqueness, referential integrity, volume

b_schema_pipelines/dags/                                # NEW (§10)
├── dp1_bronze_dag.py
├── dp2_gold_dag.py
└── dp3_feature_dag.py

b_schema_pipelines/docs/
├── 02_storage_optimization.md      # NEW (§4)
└── 02_datahub_lineage.md           # NEW (§11)
```

**Explicitly out of scope for this plan** (belongs to Final ML Coursework / Section 04, not
Section 02): `push_stream_to_feast.py`, `features/feature_repo/` (Feast entities/feature
views), `materialize_dag.py`, Great-Expectations→Slack alerting, Delta Lake CDF. These are
listed in `CLAUDE.md`'s repo tree and `IMPLEMENTATION_GUIDE.md` §2.7 / §1.11 but graded under
different rubric items. Don't build them here — flag it if a future task asks you to "finish
section 2" and it turns out to mean these too.

---

## 4. Storage optimization (`docs/02_storage_optimization.md`) — IMPLEMENTATION_GUIDE 1.7, 4 pts

Two gaps vs. the design doc's §8 (already written, not yet implemented):

**Lakehouse — Z-order (Delta Lake), 2 pts.** `common/delta_writer.py` already compacts
(`.optimize().executeCompaction()`) but never Z-orders. Add a `z_order_by: list[str] | None`
param to `DeltaWriter.write()` (or a separate `optimize_and_zorder()` method) and call it from
`transform_silver.py`'s `--mode optimized` path on the Silver `orders` table:

```python
dt.optimize().executeZOrderBy("order_timestamp", "customer_id")
```

Evidence: `DESCRIBE HISTORY` before/after showing an `OPTIMIZE` operation, plus a scan-size
comparison on a 90-day-window query (filter by `order_timestamp` range) before/after.

**Datawarehouse — Postgres indexes, 2 pts.** Not implemented anywhere. Add index creation to
`build_gold.py` (a new `_create_indexes()` step at the end of `run()`, executed via a raw JDBC
`psycopg2` connection — Spark's JDBC writer doesn't run DDL). Use the table from
`docs/02_schema_piplines.md` §8 exactly (already documented, just needs code):

```sql
CREATE INDEX IF NOT EXISTS idx_fact_order_customer_key   ON gold_ecommerce.fact_order(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_date_key        ON gold_ecommerce.fact_order(order_date_key);
CREATE INDEX IF NOT EXISTS idx_fact_order_item_order_key  ON gold_ecommerce.fact_order_item(order_key);
CREATE INDEX IF NOT EXISTS idx_fact_payment_order_key     ON gold_ecommerce.fact_payment_attempt(order_key);
CREATE INDEX IF NOT EXISTS idx_dim_customer_bk            ON gold_ecommerce.dim_customer(customer_id, is_current);
```

`feat_customer_90d`/`feat_stream_60m` indexes (`(customer_id, event_timestamp)`) get created
by `feat_customer_90d.py`/`feat_stream_60m.py`'s own `_write()` on first run — add the same
`CREATE INDEX IF NOT EXISTS` pattern there.

Evidence: `EXPLAIN ANALYZE SELECT * FROM fact_order WHERE customer_key = 12345;` before (Seq
Scan) vs. after (Index Scan), captured in the doc.

**Test:** `tests/b_schema_pipelines/test_build_gold.py` — mock the psycopg2 connection, assert
`_create_indexes()` executes the 5 statements above (`execute.call_count == 5` or similar).

---

## 5. `feat_customer_90d.py` — ✅ implemented (IMPLEMENTATION_GUIDE 1.5, part of the 12 pts)

All 12 tests in `test_feat_customer_90d.py` pass (`uv run pytest tests/b_schema_pipelines/test_feat_customer_90d.py -v`). Trimmed from
the original 15: dropped `test_feat_table_name` and `test_window_days_constant` (asserted a
class constant equals its own literal — no real behavior under test), and folded
`test_snapshot_date_stored` into `test_window_start_is_90_days_before_snapshot` (both used the
same fixture/construction; one test now checks both derived values).

- `_build_spark()` — returns `self.spark` (Delta + S3A + Postgres JDBC already loaded via
  `pipeline_config.yaml`'s `packages` list at session creation — same as `GoldBuilder`, no
  extra jar wiring needed).
- `_read_gold(table)` — JDBC read, identical shape to `GoldBuilder._jdbc()`.
- `_compute_features()` — joins `fact_order` to a `dim_date`-filtered window
  `(window_start, snapshot_date]` **through an inner join on `date_key`**, not a bare integer
  comparison on `order_date_key` — this makes point-in-time correctness structural: an order
  whose `order_date_key` has no matching row in the window-filtered `dim_date` slice (i.e. it's
  outside the window, or dated after `snapshot_date` and thus not even in `dim_date` yet) is
  silently dropped by the join, rather than relying on a comparison that a future edit could
  weaken. `f_customer_avg_order_value_90d` is left `NULL` (not coalesced to 0) for a customer
  with zero orders in the window — the average of an empty set is undefined, and forcing it to
  0 would look like "this customer has a $0 average order," which is a different, wrong claim.
- `_write(df)` — added a `psycopg2` DELETE (by `event_timestamp = snapshot_date`) ahead of the
  Spark JDBC append, per the stub's original docstring and `docs/02_schema_piplines.md`.
  **Deviation from the plan's draft:** added `psycopg2-binary` to `pyproject.toml` (asked and
  got approval) instead of the JVM-`DriverManager` workaround originally sketched — simpler,
  more conventional code, and matches what was already documented as the intended design.
- `run()` — wires `_compute_features → _write → log_run`. **Deviation from the plan's draft
  and from `GoldBuilder`'s convention:** does **not** call `self.spark.stop()`. The test suite
  (`test_write_deletes_existing_snapshot_before_insert`, `test_run_calls_compute_then_write`)
  calls `.run()` against the shared session-scoped `spark` pytest fixture without patching
  `spark.stop`, including twice in the same test — stopping the session inside `run()` would
  kill that shared fixture for every other test in the suite. Session lifecycle is left to the
  caller (CLI entrypoint, or an Airflow task/backfill loop that may reuse one instance across
  multiple `snapshot_date`s).

Delete all 10 `xfail` markers in the test file as each lands; run
`uv run pytest tests/b_schema_pipelines/test_feat_customer_90d.py -v` after each method.

---

## 6. `feat_stream_60m.py` — implement (IMPLEMENTATION_GUIDE 1.5)

Same approach — implement to the existing docstrings in `test_feat_stream_60m.py`'s target.
One change from the current stub's assumption: **`_read_events()`'s default source becomes
the Flink pipeline's clean-sink output**, not raw `events.json`, once §8 exists —
`events_source` stays a constructor param so tests keep passing raw NDJSON directly
(that part of the test suite is source-format-agnostic; it just checks the cast + schema).

Implementation order:

1. `_build_spark()` — same JDBC-jar pattern as §5.
2. `_read_events()` — explicit schema (given in docstring), `to_timestamp` casts on
   `event_timestamp`/`created_ts`.
3. `_add_burst_flag(events_df)` — `hour==12 & minute<20` / `hour==20 & minute<20` per the
   parametrized boundary tests (12:20 and 20:20 are exclusive).
4. `_compute_features(events_df)` — `F.window("event_timestamp", "60 minutes")` groupBy,
   with the 30-min sub-window filters for views/add_to_cart per the docstring.
5. `_write(df)` — same DELETE-then-INSERT idempotency pattern as §5, keyed on window start.
6. `run()` — wire `_read_events → _add_burst_flag → _compute_features → _write` (order is
   asserted by `test_run_pipeline_order`).

---

## 7. `feat_customer_unified.py` — NEW

Per `CLAUDE.md`'s data model: `feat_customer_unified` is the point-in-time join of
`feat_customer_90d` and `feat_stream_60m` into one row per `(customer_id, event_timestamp)`.
This is what Section 03's `training_table.py` (out of scope here, but depends on this table
existing) ultimately joins against `ml_customer_label`.

Model this file after `feat_customer_90d.py`'s shape (`PipelineBase` subclass,
`FEAT_TABLE = "feat_customer_unified"`), but its `_compute_features()` is a **join, not an
aggregation**:

```python
# LEFT JOIN — a customer may have offline features without a recent stream session
feat_customer_90d.join(
    feat_stream_60m,
    on=["customer_id", "event_timestamp"],
    how="left",
)
```

Any stream feature columns that are `NULL` after the left join (no session in that window)
should default to `0` (counts/ratios) rather than `NULL` — document this fill choice inline,
it's a modeling decision that affects the ML features downstream (Section 04) and is easy to
get silently wrong.

**Point-in-time correctness**: both inputs already carry `event_timestamp` from their own
snapshot/window-start logic — this join must not introduce a leak by joining on anything
looser than exact `(customer_id, event_timestamp)` equality.

Write a new test file `tests/b_schema_pipelines/test_feat_customer_unified.py` mirroring the
xfail-then-implement pattern used for the other two feature files — write the tests *before*
the implementation (same convention already established in this repo), covering: left-join
correctness, null-fill on missing stream features, one-row-per-customer-per-timestamp,
output schema.

---

## 8. Flink streaming pipeline (`pipelines/streaming/flink_stream_pipeline.py`) — NEW, IMPLEMENTATION_GUIDE 1.6, 10 pts

**Design choice:** one file with `--mode baseline|optimized`, mirroring
`transform_silver.py`'s and `build_gold.py`'s existing convention in this repo, rather than
four separate `flink_baseline.py`/`flink_burst_handler.py`/etc. files as
`IMPLEMENTATION_GUIDE.md`'s generic template suggests. Each fix is still its own private
method (documented separately, screenshotted separately) — only the *file* is shared, to stay
consistent with how Silver and Gold are already structured. If a grader specifically wants 4
separate files, this is a 10-minute mechanical split — the logic doesn't change.

```python
class FlinkStreamPipeline:
    """Reads a_data_generator/outputs/streaming/events.json, applies backpressure/watermark/
    dedup/windowing fixes, writes a cleaned event stream that feat_stream_60m.py consumes."""

    def run(self, mode: str) -> None:
        env = self._build_env(mode)              # buffer timeout only differs by mode
        source = self._file_source()
        stream = env.from_source(source, ...)
        if mode == "optimized":
            stream = self._apply_watermark_strategy(stream)   # Fix E — 45min out-of-orderness
            stream = self._apply_dedup(stream)                 # Fix F — keyed ValueState on event_id
        windowed = self._apply_windowing(stream)                # Problem D demo — tumbling 1h window
        self._write_sink(windowed, mode)
```

- **Backpressure (Problem D):** `env.set_buffer_timeout(100)` in `optimized` mode only —
  baseline leaves Flink's default (unbounded buffering under burst load, causing the
  `Backpressure: HIGH` UI symptom the report doc already describes).
- **Watermark + AllowedLateness (Problem E):**
  `WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(45))` +
  `.allowed_lateness(Time.minutes(45))`, matching the 12%/5–45min late-arrival injection rate
  from `a_data_generator`.
- **Dedup (Problem F):** keyed `ValueState[bool]` on `event_id` with a 2-hour TTL (bounds
  state growth) — this is the piece currently mis-attributed to `feat_stream_60m.py` in the
  report doc; move it here.
- **Windowing:** `TumblingEventTimeWindows.of(Time.hours(1))` keyed by `customer_id` —
  this is the "window processing" line item IMPLEMENTATION_GUIDE 1.6 grades separately (2 pts).

**Local dev execution:** Flink needs a local MiniCluster to expose the Web UI at
`localhost:8081` (same idea as the Spark History Server pattern in `bronze/README.md`). This
requires adding `apache-flink` to `pyproject.toml` (root-level file — **ask before editing**,
per your instruction to get permission for anything outside Section 02) and documenting
startup steps in a new `streaming/README.md` modeled on `bronze/README.md`'s Spark History
Server section.

**Sink target:** write cleaned NDJSON to `b_schema_pipelines/streaming_data/flink_clean_events/`
(same schema as `events.json`, this is a purely local-dev path — no MinIO/S3A dependency
needed for Flink specifically). `feat_stream_60m.py`'s `events_source` default becomes this
path once the Flink job exists; `events.json` remains a fallback for running the feature job
standalone without Flink running.

**Report doc update:** once this file exists, edit `docs/02_spark_optimisation_report.md`'s
Fix D/E/F code blocks to reference `streaming/flink_stream_pipeline.py` methods instead of
`feat_stream_60m.py` (§2). This is a docs-only change inside `b_schema_pipelines/`, no
permission needed.

---

## 9. Data quality gates (`b_schema_pipelines/dq/`) — NEW, part of IMPLEMENTATION_GUIDE 1.8's 12 pts

Scope: **basic quality gates only** (schema check, null-PK check, uniqueness, referential
integrity, volume ±30%) — this is a base Airflow DAG requirement (`CLAUDE.md`'s
"Quality gates" section, and `docs/02_schema_piplines.md` §5's table), independent of the
Slack-alerting novel idea, which is explicitly excluded from this plan (§3).

```python
# dq/bronze_suite.py — Great Expectations suite factory
def bronze_expectation_suite(table: str, expected_columns: list[str], pk_column: str) -> ExpectationSuite:
    """Schema-present + null-PK checks, used by dp1_bronze_dag's validate task."""
```

One suite factory per layer (`bronze_suite.py`, `silver_suite.py`, `gold_suite.py`), each
producing a GE `ExpectationSuite` consumed by a `GreatExpectationsOperator` task in the
matching DAG (§10). Silver's suite additionally encodes the Problem A/B/C checks already
documented in `docs/02_schema_piplines.md` §5's table (skew ±5pp, zero NULLs post-fill,
~2% dedup rate). Gold's suite adds referential-integrity checks (fact FK exists in dim) and
the SCD2 invariant (`is_current` uniqueness per `customer_id`).

**`great_expectations` needs adding to `pyproject.toml`** — root-level file, ask first.

---

## 10. Airflow DAGs (`b_schema_pipelines/dags/`) — NEW, IMPLEMENTATION_GUIDE 1.8, 12 pts

Three DAGs, matching `docs/02_schema_piplines.md`'s already-documented schedule
(`dp1_bronze_dag` 00:00 → `dp2_gold_dag` 01:00/02:00 → `dp3_feature_dag` 02:30). Excludes
`materialize_dag` (Feast, out of scope — §3).

**Operator choice — deviates from `IMPLEMENTATION_GUIDE.md`'s `SparkSubmitOperator` template
deliberately:** every pipeline job in this repo runs as `uv run python3 <script>.py` against a
local `local[*]` Spark session (see `pipeline_base.py`) — there is no `spark-submit` binary,
cluster manager, or `SparkSubmitOperator` connection configured anywhere in this repo.
Using `BashOperator` wrapping the exact same `uv run python3 ...` command each README already
documents keeps the DAGs truthful to how the pipelines actually run. Document this as an
explicit trade-off in the DAG docstrings (mirrors `CLAUDE.md`'s instruction: "design decisions
must be explicit with trade-offs documented").

```python
# dags/dp1_bronze_dag.py — skeleton
with DAG(
    "dp1_bronze",
    schedule="0 0 * * *",
    default_args={"retries": 3, "retry_delay": timedelta(seconds=30)},  # exponential backoff per CLAUDE.md
    catchup=False,
) as dag:
    ingest = BashOperator(
        task_id="ingest_bronze",
        bash_command="cd {{ var.value.repo_root }} && uv run python3 b_schema_pipelines/pipelines/bronze/ingest_bronze.py",
    )
    validate = GreatExpectationsOperator(
        task_id="validate_bronze",
        checkpoint_name="bronze_checkpoint",
    )
    ingest >> validate
```

- **Connections/Variables, not hardcoded** (`CLAUDE.md` requirement): `repo_root`,
  `postgres_conn_id`, `minio_conn_id` come from Airflow Variables/Connections, set once via
  `airflow variables set` / the Connections UI — never literal paths/credentials in the DAG
  files.
- `dp2_gold_dag.py`: `transform_silver.py --mode optimized` → `build_gold.py` → validate (Silver
  + Gold suites).
- `dp3_feature_dag.py`: `flink_stream_pipeline.py --mode optimized` → `feat_customer_90d.py` +
  `feat_stream_60m.py` (parallel) → `feat_customer_unified.py` → validate.
- Retry policy: 3 retries, exponential backoff (30s/60s/120s) per `CLAUDE.md`.

**Airflow needs uncommenting in `infra/docker-compose.yml`** — root-level file, ask first.
Once running, DAGs load via the existing (commented-out) volume mount
`../b_schema_pipelines/dags:/opt/airflow/dags`.

---

## 11. DataHub lineage (`docs/02_datahub_lineage.md`) — NEW, IMPLEMENTATION_GUIDE 1.9, 14 pts

Add a shared `emit_lineage()` helper to `pipeline_base.py` (in-scope — this file is inside
`b_schema_pipelines/`):

```python
def emit_lineage(self, upstream_urns: list[str], downstream_urn: str) -> None:
    """Emit a dataset-lineage edge to DataHub via the REST emitter. Called at the end of
    run() in every Bronze/Silver/Gold/Feature job. No-ops with a warning log if the DataHub
    GMS endpoint is unreachable — pipelines must not fail because lineage emission failed."""
```

Call it from the end of `run()` in `ingest_bronze.py`, `transform_silver.py`, `build_gold.py`,
`feat_customer_90d.py`, `feat_stream_60m.py`, `feat_customer_unified.py` — one call per table
produced, upstream URNs pointing at the table(s) it read.

Link GE suites (§9) to DataHub as dataset assertions so failed checks are visible in the
DataHub UI's assertion tab, per `docs/02_schema_piplines.md`'s "Data contracts" line.

**Standing up DataHub locally needs a new service block in `infra/docker-compose.yml`
(GMS + frontend, `acryl-datahub` quickstart pattern) and `acryl-datahub` added to
`pyproject.toml`** — both root-level, ask first.

---

## 12. Sequencing

```
§3  Renames                         ← no dependency, do first (unblocks everything else)
§5  feat_customer_90d.py            ← needs nothing new (Gold already exists)
§8  Flink streaming pipeline        ← no dependency on §5/§6, can run in parallel
§6  feat_stream_60m.py              ← works standalone against events.json; swap source to
                                        Flink's sink once §8 lands
§7  feat_customer_unified.py        ← needs §5 + §6 done
§4  Storage optimization            ← needs Gold (already exists) — can run anytime
§9  dq/ GE suites                   ← needs nothing new, but is consumed by §10
§10 Airflow DAGs                    ← needs §5, §6, §7, §8, §9 all done (DAGs call all of them)
§11 DataHub lineage                 ← needs §5–§8 done (emits from each job)
```

## 13. Approval checklist — files outside `b_schema_pipelines/`

Per your instruction, none of these are touched without asking first. Flag each when its
phase is reached:

| File | Why it needs to change | Which phase |
|---|---|---|
| `pyproject.toml` | add `apache-flink`, `great-expectations`, `acryl-datahub` | §8, §9, §11 |
| `infra/docker-compose.yml` | uncomment `airflow` service; add `datahub` service block | §10, §11 |
| `CLAUDE.md` | optionally add `pipelines/streaming/` and `dq/` suite filenames to the repo-structure tree (currently silent on exact `dq/` contents and doesn't show `streaming/` at all) | any time, cosmetic only |

## 14. Grading evidence checklist (Section 02 remainder only)

- [ ] §4 — `DESCRIBE HISTORY` before/after Z-order; `EXPLAIN ANALYZE` seq-scan → index-scan
- [ ] §5/§6/§7 — all xfail markers removed, full test suite green, `pytest --cov` unaffected elsewhere
- [ ] §8 — Flink UI screenshots: backpressure HIGH→OK, `numLateRecordsDropped` >0→0, dedup query >0→0, windowed aggregation output
- [ ] §9/§10 — Airflow UI green run screenshot for `dp1_bronze_dag`, `dp2_gold_dag`, `dp3_feature_dag`, each showing the validate task
- [ ] §11 — DataHub lineage graph screenshot per pipeline; assertions-passing screenshot; browse view across Bronze/Silver/Gold/Feature zones
- [ ] `docs/02_spark_optimisation_report.md` Fix D/E/F sections repointed at `streaming/flink_stream_pipeline.py`
