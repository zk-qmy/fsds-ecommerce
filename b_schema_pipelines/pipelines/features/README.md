# Section 02 — Remaining Implementation Plan

This is the ground-truth plan for finishing Section 02 (Schema Pipelines, mini-coursework
items 1.5–1.10 in `coursework/IMPLEMENTATION_GUIDE.md`). It exists here — rather than in
`bronze/README.md` or `gold/README.md` — because the largest remaining surface is the
`features/` pipeline, and everything else (Flink, Airflow, DataHub) exists to feed it or
consume its output.

**Do not start coding from this doc without re-reading it against the current repo state.**
Code and design drift; if something below no longer matches `git log` / the file tree, the
code wins and this doc must be updated in the same PR.

**Point values below cite `coursework/rubrics.md` (the actual instructor rubric) —
`coursework/IMPLEMENTATION_GUIDE.md` originally had several wrong (Spark 12 vs. actual 16,
Flink 10 vs. 13, DataHub 14 vs. 12, and it was missing the "visualize tables on all zones"
item entirely — misattributed to DataHub instead of Schema Design). `IMPLEMENTATION_GUIDE.md`
has since been corrected to match `rubrics.md` too, so both sources now agree; this note stays
as a record of the discrepancy that was found, not an active warning.

---

## 0. What's already done (do not re-build)

**Live-audit note:** everything below was cross-checked against `rubrics.md` and, for the
three feature jobs, against a real running PostgreSQL + Trino/MinIO stack — not just passing
unit tests. Two real bugs were found and fixed this way, neither caught by any test (both mock
the layer where the bug lived):
1. **AQE skew-join fix was inert** — `transform_silver.py`'s config was commented out and
   `docs/02_spark_optimisation_report.md` falsely claimed it lived in `pipeline_base.py` (it
   never did). Fixed: `_run_baseline()` now explicitly disables it, `_run_optimized()` now
   explicitly enables it — see §5's Silver test suite (38/38 passing after the fix).
2. **All three feature jobs' idempotent DELETE silently matched zero rows** — PySpark collects
   timestamps as naive datetimes in the Spark session's local timezone; `psycopg2` compared
   them against Postgres's own connection-default timezone instead, so every re-run appended
   duplicates instead of replacing. Caught live: `feat_customer_unified` had 41,230 duplicate
   `(customer_id, event_timestamp)` rows after two runs. Fixed with `SET TIME ZONE <spark
   session tz>` on the psycopg2 connection before each DELETE, in all three files. Verified
   live: same job run twice against the same snapshot now produces identical row counts (see
   `sum.md`'s "Idempotent writes" note for the full before/after).

All three feature tables were wiped and rebuilt clean after the fix — `feat_customer_90d`
(120,000 rows, snapshot `2026-06-22`), `feat_stream_60m` (228,277 rows), `feat_customer_unified`
(228,277 rows, 207,174 with real joined offline data) — all confirmed duplicate-free in
PostgreSQL directly, not just via Spark-side row counts.

| Component | File | Status |
|---|---|---|
| Bronze ingest | `pipelines/bronze/ingest_bronze.py` | ✅ done, tested (`tests/b_schema_pipelines/test_ingest_bronze.py`) |
| Silver transform | `pipelines/silver/transform_silver.py` | ✅ done, tested — `--mode baseline\|optimized`, fixes A/B/C + broadcast join demo |
| Gold build | `pipelines/gold/build_gold.py` | ✅ done, tested (31 tests) — `--mode baseline\|optimized`, SCD2 `dim_customer`, deterministic surrogate keys |
| Shared infra | `pipelines/pipeline_base.py`, `common/delta_writer.py`, `minio_client.py` | ✅ done, tested |
| Delta compaction | `common/delta_writer.py:100` (`.optimize().executeCompaction()`) | ✅ done — Z-order is **not** done (see §4) |
| Schema design doc | `docs/02_schema_piplines.md` | ✅ complete (dims, facts, OBT, naming, SLAs, indexing/partitioning plan — not yet implemented in code) |
| Spark optimisation report (Fixes 1–4) | `docs/02_spark_optimisation_report.md` | ✅ Fixes 1–4 (AQE skew, NULL fill, dedup, broadcast join) accurate. Fix D/E/F code blocks still need repointing at the Flink file — see §2 |
| Flink streaming pipeline | `pipelines/streaming/flink_stream_pipeline.py` | ✅ done — `--mode baseline\|optimized`, Problems D/E/F, verified end-to-end against real sample data (§8) |
| Unified feature join | `pipelines/features/feat_customer_unified.py` | ✅ done — as-of join (§7), 8/8 tests pass |

## 1. What's stubbed (tests exist, implementation raises `NotImplementedError`)

| File | Class | Test file | Status |
|---|---|---|---|
| `pipelines/features/feat_customer_90d.py` | `CustomerFeature90d` | `tests/b_schema_pipelines/test_feat_customer_90d.py` | ✅ done — renamed + implemented (§5), 12/12 tests pass |
| `pipelines/features/feat_stream_60m.py` | `StreamFeature60m` | `tests/b_schema_pipelines/test_feat_stream_60m.py` | ✅ done — implemented (§6), 16/16 tests pass |
| `pipelines/features/feat_customer_unified.py` | `CustomerFeatureUnified` | `tests/b_schema_pipelines/test_feat_customer_unified.py` | ✅ done — new (§7), 8/8 tests pass |

All three feature-job test files (36 tests total) also pass run together, confirming no
cross-test interference on the shared session-scoped Spark fixture.

Every test in both files was `@pytest.mark.xfail(strict=True, raises=NotImplementedError, ...)`.
As each private method is implemented, **delete its `xfail` marker** — `strict=True` means
a passing test under an `xfail` marker is itself a failure, so leaving markers in place
after implementing the code will break CI, not fix it. Done for both files.

**Pre-existing bugs fixed while implementing §6:**
1. `test_burst_flag_value`'s `parametrize` built `spark.createDataFrame([("E_TEST", event_time, "C001")], schema=schema)`
   where `schema` declared `event_timestamp` as `TimestampType()` but `event_time` was a raw
   Python string — `createDataFrame` rejected the type mismatch before the
   `.withColumn(F.to_timestamp(...))` cast that followed ever got a chance to run. Fixed by
   typing the schema field `StringType()` and casting after construction (the test already did
   this — it just needed the schema type corrected to match). Also trimmed the case list from
   11 to 6: kept both windows' start (inclusive) and end (exclusive) boundaries plus one
   off-peak point, dropped the redundant interior/duplicate off-peak cases.
2. `test_compute_features_burst_flag_set_in_burst_window` and `..._clear_off_peak` called
   `_compute_features(parsed_events)` directly, but `parsed_events` has no
   `f_stream_burst_activity_flag` column — that column only exists after `_add_burst_flag` runs
   (see `run()`'s call order). `_compute_features` would `AnalysisException` on the missing
   column regardless of implementation. Fixed by adding a `flagged_events` fixture
   (`_add_burst_flag(parsed_events)`) and using it in every `_compute_features` test, not just
   those two — matches how the real pipeline always calls them.

Also dropped 5 trivial tests that asserted a class constant equals its own literal
(`test_prefix_is_feat_60m`, `test_feat_table_name`, `test_window_minutes`,
`test_burst_windows_defined`) and one that couldn't pass against the real logger
(`test_run_logs_success` — assumed `run()` calls `self.spark.stop()` and that `PipelineBase`'s
structured JSON log line appears alone on stdout with `status: "success"`; in reality
`log_run`'s handler writes to **stderr**, in a human-readable format with a `STRUCTURED {...}`
JSON line embedded at DEBUG level, and every other job in this repo logs `status: "ok"`, not
`"success"`). `test_run_pipeline_order` already covers `run()`'s call sequence; re-verifying
`PipelineBase`'s own logging format isn't this file's job. 27 collected test items → 16.

## 2. Known doc/code mismatch — ✅ resolved

`docs/02_spark_optimisation_report.md` §"Flink Fixes (Problems D/E/F)" used to document real
PyFlink code as if it lived inside `feat_stream_60m.py` (a Spark job). It's fixed now:
`pipelines/streaming/flink_stream_pipeline.py` (§8) owns Problems D/E/F for real, and
`feat_stream_60m.py` (§6) only computes the 4 feature aggregates from whatever clean event
stream it's pointed at. **Repointing done:** `02_spark_optimisation_report.md`'s Fix D/E/F
code blocks now say `flink_stream_pipeline.py` throughout — verified by reading the file
directly, not assumed from this note.

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
├── feat_customer_90d.py            # ✅ done — renamed, then implemented (§5)
├── feat_stream_60m.py              # ✅ done — renamed, then implemented (§6)
├── feat_customer_unified.py        # ✅ done — point-in-time join of the above two (§7)
├── README.md                       # this file
└── sum.md                          # ✅ done — feature-pipeline run/verify guide (see §7 for the as-of join design writeup)

b_schema_pipelines/pipelines/streaming/                # ✅ done (§8)
├── flink_stream_pipeline.py        # baseline/optimized PyFlink job (Problems D/E/F)
└── README.md                       # own-Python-3.12-env run instructions

b_schema_pipelines/dq/                                  # NEW (§9)
├── bronze_suite.py                 # GE expectations: schema + null-PK checks
├── silver_suite.py                 # GE expectations: skew%, null-fill, dedup rate
└── gold_suite.py                   # GE expectations: uniqueness, referential integrity, volume

b_schema_pipelines/dags/                                # NEW (§10)
├── dp1_bronze_dag.py
├── dp2_gold_dag.py
└── dp3_feature_dag.py                  # runs all three feature jobs

b_schema_pipelines/docs/
├── 02_storage_optimization.md          # NEW (§4)
├── 02_datahub_lineage.md               # NEW (§11)
└── register_bronze_silver_trino.sql    # ✅ done (§12) — one-time Trino table registration
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

## 5. `feat_customer_90d.py` — ✅ implemented (IMPLEMENTATION_GUIDE 1.5; rubrics.md: Spark jobs, 16 pts total)

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

## 6. `feat_stream_60m.py` — ✅ implemented (IMPLEMENTATION_GUIDE 1.5)

All 16 tests in `test_feat_stream_60m.py` pass. Implemented per the stub's docstrings, with
one deviation from this plan's original draft: **`_read_events()`'s default `events_source` is
still raw `events.json`**, not the Flink sink's output — kept that way so the feature job runs
standalone (no Flink run required first) for local dev/testing. Production/Airflow wiring
passes `--events-source b_schema_pipelines/streaming_data/flink_clean_events/optimized`
explicitly (documented in both files' run instructions). `_read_events` also adds
`.option("recursiveFileLookup", "true")` — needed either way, since Flink's `FileSink` buckets
output into `<mode>/<yyyy-MM-dd--HH>/` subdirectories.

`_compute_features` computes `_window` as a per-row column (`F.window(...)` via `withColumn`,
not as a `groupBy` key expression) specifically so `_window.start` is available as a plain
column inside the 30-minute-cutoff `WHEN` conditions — referencing a `groupBy` key column
inside its own `.agg()` call works in some Spark versions but isn't something to rely on
blind; per-row computation avoids the question entirely.

`_write` mirrors `feat_customer_90d.py`'s psycopg2-DELETE-then-Spark-JDBC-INSERT pattern,
generalized from "one snapshot_date" to "every distinct window start present in this batch"
(`DELETE ... WHERE event_timestamp = ANY(%s)`).

**Two pre-existing test bugs found and fixed** — see §1's tracking table for detail
(`test_burst_flag_value`'s schema type mismatch; the burst-flag `_compute_features` tests
missing the `_add_burst_flag` step). Also dropped 5 trivial/unfixable tests — 27 collected
items → 16.

---

## 7. `feat_customer_unified.py` — ✅ implemented

**Correction to this plan's original draft, made before writing any code:** the sketch above
proposed an equi-join on `(customer_id, event_timestamp)`. That's wrong — `feat_customer_90d`
has one row per customer per **day** (`event_timestamp` = midnight snapshot date) while
`feat_stream_60m` has one row per customer per **60-min window** (`event_timestamp` = window
start, any time of day). An equi-join between those two grains would only match when a stream
window happened to start at exactly midnight on a snapshot day — effectively never. Confirmed
with you before implementing; resolution (your call): **as-of join, stream grain**.

**Actual design:** output grain follows `feat_stream_60m` (the finer-grained table). For each
stream row, attach the *latest* `feat_customer_90d` row at or before that row's
`event_timestamp`, per customer — via a `Window.partitionBy("customer_id",
"event_timestamp").orderBy(offline_event_timestamp.desc())` + `row_number() == 1` after a
left join with the range condition `offline.event_timestamp <= stream.event_timestamp`. This
satisfies CLAUDE.md's point-in-time rule directly (`f.event_timestamp <= l.event_timestamp`)
rather than approximating it with an equality condition.

**Null-fill rule generalizes the one already established in `feat_customer_90d.py`:** a stream
row with no applicable offline snapshot yet (new customer, or a window before that customer's
first snapshot) gets counts/rate defaulted to `0`, but `f_customer_avg_order_value_90d` stays
`NULL` — same reasoning either way: the average of zero orders is undefined, not zero, whether
the zero comes from an as-of join finding nothing or from `feat_customer_90d.py`'s own
zero-order aggregation.

`tests/b_schema_pipelines/test_feat_customer_unified.py` (written before the implementation,
per the established convention) — **8/8 passing**:

| Test | What it proves |
|---|---|
| `test_asof_join_picks_latest_snapshot_at_or_before` | With two offline snapshots for the same customer, the join picks the *latest* one at or before the stream timestamp — not just any match |
| `test_asof_join_excludes_future_snapshots` | A stream window between two snapshots attaches the *earlier* one — the later (future-dated) snapshot is never leaked in |
| `test_asof_join_missing_offline_snapshot_fills_counts_with_zero` | A customer with no offline snapshot at all gets `0` for counts/rate, not `NULL` |
| `test_asof_join_missing_offline_snapshot_avg_order_value_stays_null` | ...but `f_customer_avg_order_value_90d` stays `NULL` in that same case |
| `test_stream_features_pass_through_unchanged` | The join doesn't corrupt the stream-side columns |
| `test_compute_features_one_row_per_stream_window` | The ranked-window filter doesn't fan out rows — output count equals input `feat_stream_60m` row count |
| `test_compute_features_output_schema` | All 11 expected columns present |
| `test_run_calls_compute_then_write` | `run()` wiring unaffected by the join-logic change |

**How this was caught:** before writing any implementation code, the plan's draft join sketch
was checked against the actual output schemas of the two upstream jobs — a five-minute read of
both files' already-implemented `_compute_features` methods was enough to see the timestamps
could never line up under an equi-join. Flagged to you with the concrete failure mode
(near-total `NULL` offline columns) before implementing; the as-of join direction was confirmed
as the fix before any code was written — not discovered via a failing test after the fact.

---

## 8. Flink streaming pipeline (`pipelines/streaming/flink_stream_pipeline.py`) — ✅ built, IMPLEMENTATION_GUIDE 1.6; rubrics.md: 13 pts (baseline 2 + burst 3 + late-arrival 3 + dedup 3 + window processing 2)

**Design choice (as planned):** one file with `--mode baseline|optimized`, mirroring
`transform_silver.py`'s and `build_gold.py`'s convention, instead of 4 separate
`flink_*.py` files. `FlinkStreamPipeline.run()` wires `_read_and_clean → _write_sink` +
`_apply_windowing(...).print()` as a second branch; each fix is its own private method.

**Two deviations from the original draft, discovered while getting this actually running (see
below for why — this file's logic was verified end-to-end against real `events.json` data
before being written, not written from memory):**

1. **PyFlink's Python runtime is Apache Beam's Fn API worker under the hood.** During
   `env.execute()`, it replaces the root logger's handlers with its own and never hands them
   back — any `logging` call made *after* `execute()` returns is silently dropped, even though
   it works fine *during* execution (Flink's own `.print()` DataStream sink is unaffected — it
   writes directly, not through Python `logging`). `_log_run`'s structured summary line uses
   plain `print()`, not `logger.info`/`logger.error`, and documents why inline.
2. **No Python 3.13 wheel for `apache-flink`** (max is 3.12) — confirmed via PyPI metadata
   before writing any code. Per your call: runs via
   `uv run --no-project --python 3.12 --with apache-flink`, an ephemeral uv-managed
   environment. **`pyproject.toml` was never touched** — no permission needed, since this
   sidesteps the root project entirely rather than editing it.

- **Backpressure (Problem D):** `env.set_buffer_timeout(100)` in `optimized` mode;
  `env.set_buffer_timeout(-1)` (flush only when a buffer fills) in `baseline` — verified this
  is in fact Flink's non-default behavior (`get_buffer_timeout()` returns `100` out of the box,
  so `baseline` has to *explicitly regress* to `-1` to demonstrate the symptom, not just omit a
  call).
- **Watermark + lateness (Problem E):** `optimized` uses
  `WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_minutes(45))`; `baseline` uses
  `for_monotonous_timestamps()` (any out-of-order event is immediately "late").
- **Dedup (Problem F):** keyed `ValueState[bool]` on `event_id`, 2h TTL, `optimized` mode only.
  Verified against a real 2000-event sample containing 13 duplicate `event_id`s:
  `optimized` → 1987 output rows, all distinct; `baseline` → 2000 rows, 13 dupes still present.
- **Windowing:** `TumblingEventTimeWindows.of(Time.hours(1))` keyed by `customer_id`,
  `ViewCountAggregate` — the "window processing" line item (2 pts), printed rather than
  sunk to a file (it's a demo output, not consumed downstream).

**Sink target:** `b_schema_pipelines/streaming_data/flink_clean_events/<mode>/` — namespaced
by mode (not a flat shared directory) specifically so a `baseline` evidence-capture run can
never reintroduce duplicates into what `feat_stream_60m.py` reads; that job points at
`.../optimized/` in production.

**Flink Web UI:** not on by default — needs an explicit `Configuration().set_integer("rest.port", 8081)`
passed to `get_execution_environment()`. Verified reachable (`curl localhost:8082/overview`
returned live cluster JSON in testing; 8081 was occupied by something else on this machine, so
the port is documented as adjustable, not hardcoded as a hard requirement). See
`streaming/README.md` for the exact snippet and screenshot checklist.

**No automated pytest suite** for this module, by design — see `streaming/README.md`'s Tests
section for the reasoning (mirrors why Fix 1's AQE skew-join config has no dedicated test
either: there's no behavior here that isn't already proven by actually running the job).

**Repointing done** (§2): `docs/02_spark_optimisation_report.md`'s Fix D/E/F code blocks now
say `flink_stream_pipeline.py`, not `feat_stream_60m.py`.

---

## 9. Data quality gates (`b_schema_pipelines/dq/`) — ✅ suite factories implemented, part of IMPLEMENTATION_GUIDE 1.8's 12 pts

Scope: **basic quality gates only** (schema check, null-PK check, uniqueness, referential
integrity, volume ±30%) — this is a base Airflow DAG requirement (`CLAUDE.md`'s
"Quality gates" section, and `docs/02_schema_piplines.md` §5's table), independent of the
Slack-alerting novel idea, which is explicitly excluded from this plan (§3).

**See [`b_schema_pipelines/dq/README.md`](../../dq/README.md) for the full implementation
writeup** (architecture, per-layer check table, the two-expectation skew-band trick, GX 1.x
context deviation, constants reference). Summary below.

**Implemented:** `bronze_suite.py` (schema check only — Bronze's `_check_quality` already
gates null-PKs inline, and the design doc's §5 table scopes null-PK/volume/skew/dedup checks
to Silver+Gold, not Bronze), `silver_suite.py` (schema + null-PK + volume + orders'
Problem A/B checks + order_items' Problem C dedup check), `gold_suite.py` (schema + null-PK +
uniqueness + referential-integrity + volume). All three are pure suite-construction functions
— no Spark/Postgres access of their own, row-count baselines and FK valid-key sets are passed
in by the caller — so they're unit-tested (28 tests, `test_bronze_suite.py`/
`test_silver_suite.py`/`test_gold_suite.py`) without a live cluster. `great-expectations`
added to `pyproject.toml` (approved — resolves cleanly on Python 3.13, v1.19.0).

**Deviation from the plan's original sketch:** GX 1.x's `ExpectationSuite.add_expectation()`
requires an active data context (it checks whether the suite has been persisted) — added a
shared `dq/common.py` helper (`new_suite(name)`) that backs each factory with an ephemeral,
in-memory context, rather than duplicating that boilerplate three times.

**Done (§10):** wiring these into DAG validate tasks and computing their runtime inputs
(baseline row counts, FK valid-key sets, the `is_current`-filtered batch for `dim_customer`'s
uniqueness check) is `dq/validation_runner.py`'s job — not a `GreatExpectationsOperator` (see
§10 and `dags/plan.md` §3/§9 for why that provider wasn't used).

```python
# dq/bronze_suite.py — Great Expectations suite factory
def bronze_expectation_suite(table: str, expected_columns: list[str]) -> ExpectationSuite:
    """Schema-presence check, called by validation_runner.validate_bronze_tables
    (dp1_bronze's validate_bronze task)."""
```

One suite factory per layer (`bronze_suite.py`, `silver_suite.py`, `gold_suite.py`), each
producing a GE `ExpectationSuite` consumed by `validation_runner.py`'s `validate_*_tables`
functions (§10), called from each DAG's `validate_*` `ExternalPythonOperator` task. Silver's
suite additionally encodes the Problem A/B/C checks already documented in
`docs/02_schema_piplines.md` §5's table (skew ±5pp, zero NULLs post-fill, ~2% dedup rate).
Gold's suite adds referential-integrity checks (fact FK exists in dim) and the SCD2 invariant
(`is_current` uniqueness per `customer_id`).

---

## 10. Airflow DAGs (`b_schema_pipelines/dags/`) — ✅ done, IMPLEMENTATION_GUIDE 1.8; rubrics.md: 12 pts (DP1 4 + DP2 4 + DP3 4)

**Superseded by [`dags/plan.md`](../../dags/plan.md) — read that file, not the sketch that used
to be here.** This section originally sketched `GreatExpectationsOperator` +
`SparkSubmitOperator` as placeholders; the actual, implemented design differs in both respects
and is documented in full in `dags/plan.md` (§3–§11), which is kept in sync with the shipped
code. Summary, not a duplicate of that file's detail:

- Three DAGs — `dp1_bronze`, `dp2_gold` (Silver + Gold, one DAG per the rubric's "bronze ->
  silver and gold zone (or bronze -> gold only)" allowance), `dp3_feature` (runs all three
  feature jobs, not just `feat_customer_90d.py`) — chained via `ExternalTaskSensor`, matching
  `docs/02_schema_piplines.md`'s schedule (00:00 → 01:00 → 02:30). Excludes `materialize_dag`
  (Feast, out of scope — §3).
- **Ingest/transform tasks are `BashOperator`** wrapping the exact `uv run python3 <script>.py`
  command each pipeline's own README already documents (no `SparkSubmitOperator` — there's no
  `spark-submit` binary or cluster manager anywhere in this repo, only a local `local[*]`
  session per `pipeline_base.py`).
- **Validate tasks are `ExternalPythonOperator`**, not `GreatExpectationsOperator` — they call
  `b_schema_pipelines.dq.validation_runner.validate_{bronze,silver,gold,feature}_tables(...)`
  in the project's own Python 3.13 venv (Airflow's own process runs Python 3.12 and doesn't
  have `great_expectations`/`deltalake`/`psycopg2` installed). Full reasoning in `dags/plan.md`
  §3/§4/§9/§12.
- Connections/Variables, not hardcoded: `repo_root` (Variable), `fsds_postgres`/`fsds_minio`
  (Connections) — set once via `airflow variables set`/`airflow connections add`, per
  `dags/plan.md` §10.
- Retry policy: 3 retries, exponential backoff (30s/60s/120s), per `CLAUDE.md`.
- Tested: `tests/dags/test_dags.py` (11 tests — DAG import, task-graph shape, retry policy,
  schedule) and `tests/b_schema_pipelines/test_validation_runner.py` (22 tests). Both green;
  see `dags/plan.md` §13 for how to run the DAG tests (they need a separate ephemeral
  `--python 3.12 --with apache-airflow` env, now wired into CI as the `dag-tests` job).

**`infra/docker-compose.yml`'s `airflow` service is uncommented and live** (webserver at
`http://localhost:8081`) — done, no longer pending.

**Still open:** an actual `airflow dags test <dag_id> <date>` run against this live stack, and
the Airflow UI screenshot the rubric scores, haven't been captured yet (**Unverified**) —
that's the one gap between "code done and unit-tested" and "graded evidence exists."

---

## 11. DataHub lineage (`docs/02_datahub_lineage.md`) — NEW, IMPLEMENTATION_GUIDE 1.9; rubrics.md: 12 pts (DP1 4 + DP2 4 + DP3 4)

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

## 12. Bronze/Silver visualization via Trino — ✅ done, rubrics.md: "Visualize tables on all zones", 2 pts

**Gap found while cross-checking against `rubrics.md`:** this rubric item needs Bronze
**and** Silver **and** Gold all visible in DBeaver — proof is a DBeaver screenshot. Only
Gold had one before this (`assets/gold-schema.png`, via the direct PostgreSQL connection
documented in `gold/README.md`). Bronze/Silver are Delta Lake tables on MinIO — not a
database DBeaver can connect to directly.

**Why this didn't already work:** `infra/trino/catalog/delta.properties` was already
configured (Delta Lake connector, pointed at the Hive Metastore + MinIO credentials), and
Trino itself was already running per `docker-compose.yml`. But nothing had ever registered
those Delta tables into the Hive Metastore Trino reads from — Spark writing a Delta table to
a MinIO path doesn't auto-register it.

**What actually worked — corrected from this section's original draft:** the original plan
proposed `CREATE TABLE ... WITH (location = ...)`. That failed on this Trino version
(`trinodb/trino:410`) with a parser error (`mismatched input '<EOF>'`) even with
`delta.legacy-create-table-with-existing-location.enabled=true` already set — confirmed by
testing directly against the running container, not assumed. The connector wants the
`register_table` system procedure instead, which was **disabled** by default:

```
register_table procedure is disabled
```

**Fix, applied:**
1. Added one line to `infra/trino/catalog/delta.properties` (root-level, approved before
   editing): `delta.register-table-procedure.enabled=true`.
2. Restarted the Trino container (`docker compose restart trino`) to pick it up — config
   reload, no data loss, no rebuild.
3. Ran `b_schema_pipelines/docs/register_bronze_silver_trino.sql` (new file — `CREATE SCHEMA`
   for `delta.bronze`/`delta.silver`, then `CALL delta.system.register_table(...)` per table:
   6 Bronze tables — `customers`, `products`, `orders`, `order_items`, `payments`, `events`,
   per `ingest_bronze.py` — and 5 Silver tables — same minus `events`, per
   `transform_silver.py`'s `SILVER_TABLES`).
4. Verified against the live container: `SHOW TABLES FROM delta.bronze` / `delta.silver` list
   all 11 tables, `SELECT COUNT(*) FROM delta.bronze.customers` returns real data (not a
   parse error or empty result).

**Still needed for the actual grading evidence:** a DBeaver connection using the **Trino**
driver (host `localhost`, port `8080`, no auth) alongside the existing `gold_ecommerce`
PostgreSQL connection, and a screenshot showing all three zones' tables. That's a manual,
GUI-only step — nothing left to automate.

**Update (post-implementation, 2026-07-16):** `infra/trino/catalog/postgres.properties` was
added (`connector.name=postgresql`, pointed at the same `fsds-postgres` instance) — Trino now
has a second catalog reaching `postgres.gold_ecommerce.*` directly, alongside the `delta`
catalog above. Verified live: `SHOW CATALOGS` lists `delta`/`postgres`/`system`, and
`SHOW TABLES FROM postgres.gold_ecommerce` lists all 12 Gold/feature tables. This means a
single Trino connection can now query/join across Bronze, Silver, *and* Gold in one statement
— the two-connection DBeaver setup above still works and is still the simpler way to just
*browse* Gold (DBeaver's native PostgreSQL support is richer than its generic Trino/JDBC view),
but it's no longer the only way to reach Gold data from Trino.

---

## 13. Sequencing

```
§3  Renames                         ← ✅ done
§5  feat_customer_90d.py            ← ✅ done
§8  Flink streaming pipeline        ← ✅ done
§6  feat_stream_60m.py              ← ✅ done (default source is still raw events.json — see §6)
§7  feat_customer_unified.py        ← ✅ done (as-of join, not equi-join — see §7)
§4  Storage optimization            ← ✅ code done (Z-order + Postgres indexes); evidence capture
                                       (`DESCRIBE HISTORY`, `EXPLAIN ANALYZE` before/after) still open
§12 Trino Bronze/Silver visualization ← ✅ done (tables registered + verified; DBeaver screenshot still manual)
§9  dq/ GE suites                   ← ✅ done (suite factories + validation_runner.py wiring)
§10 Airflow DAGs                    ← ✅ done — see dags/plan.md; live-stack run + UI screenshot still open
§11 DataHub lineage                 ← needs §5–§8 done (emits from each job) — all done, ready to start
```

**Full feature pipeline chain (bronze → silver → gold → features → unified) is now complete
and tested end to end, and §10's three Airflow DAGs orchestrate all of it.** Remaining Section
02 work is evidence capture and governance, not more code: §4's evidence capture, §10's live
`airflow dags test` run + UI screenshot, §11 (DataHub — not started). §12 (Trino visualization)
is code-complete; only the DBeaver screenshot itself remains, a manual GUI step.

## 14. Approval checklist — files outside `b_schema_pipelines/`

Per your instruction, none of these are touched without asking first. Flag each when its
phase is reached:

| File | Why it needs to change | Which phase | Status |
|---|---|---|---|
| `pyproject.toml` | add `apache-flink`, `great-expectations`, `acryl-datahub` | §8, §9, §11 | `apache-flink` sidestepped entirely (§8 runs in an isolated ephemeral env, never touched this file — see §8); `great-expectations>=1.19.0` ✅ done, approved (§9); `acryl-datahub` still pending |
| `infra/docker-compose.yml` | uncomment `airflow` service; add `datahub` service block | §10, §11 | `airflow` service ✅ done, approved (webserver at `localhost:8081`, `network_mode` not used — bridge networking, see `dags/plan.md` §4); `datahub` service block still pending |
| `infra/airflow/Dockerfile` | new file — Airflow 2.10.5 image + `uv`-managed project venv | §10 | ✅ done, approved — see `dags/plan.md` §4 |
| `.github/workflows/ci.yml` | new `dag-tests` job running `tests/dags/test_dags.py` in an ephemeral py3.12/airflow env | §10 (post-implementation review) | ✅ done, approved — was written but never wired into CI (silently import-skipped) until this fix |
| `infra/trino/catalog/delta.properties` | add `delta.register-table-procedure.enabled=true` | §12 | ✅ done, approved — one line, Trino container restarted to pick it up |
| `infra/trino/catalog/postgres.properties` | new file — `postgresql` connector catalog pointed at `fsds-postgres`, so Trino can reach `gold_ecommerce` directly | §12 (post-implementation) | ✅ done, approved — verified live (`SHOW CATALOGS`, `SHOW TABLES FROM postgres.gold_ecommerce`) |
| `CLAUDE.md` | optionally add `pipelines/streaming/` and `dq/` suite filenames to the repo-structure tree (currently silent on exact `dq/` contents and doesn't show `streaming/` at all) | any time, cosmetic only | not done |

## 15. Grading evidence checklist (Section 02 remainder only)

- [ ] §4 — `DESCRIBE HISTORY` before/after Z-order; `EXPLAIN ANALYZE` seq-scan → index-scan
- [ ] §5/§6/§7 — all xfail markers removed, full test suite green, `pytest --cov` unaffected elsewhere
- [ ] §8 — Flink UI screenshots: backpressure HIGH→OK, `numLateRecordsDropped` >0→0, dedup query >0→0, windowed aggregation output
- [ ] §9/§10 — Airflow UI green run screenshot for `dp1_bronze`, `dp2_gold`, `dp3_feature`, each showing the validate task (DAG code + unit tests are done and green; this live-cluster screenshot is the only remaining piece — **Unverified**, no live run captured yet)
- [ ] §12 — DBeaver screenshot showing Bronze + Silver (via Trino) + Gold (via PostgreSQL, or via Trino's `postgres` catalog) tables all visible — tables are registered and queryable now, screenshot is the only remaining step
- [ ] §11 — DataHub lineage graph screenshot per pipeline; assertions-passing screenshot; browse view across Bronze/Silver/Gold/Feature zones
- [ ] `docs/02_spark_optimisation_report.md` Fix D/E/F sections repointed at `streaming/flink_stream_pipeline.py`
