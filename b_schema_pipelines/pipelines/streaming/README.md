# Flink Streaming Pipeline

Reads `a_data_generator/outputs/streaming/events.json` as a bounded file source (the
local-dev stand-in for the Kafka topic described in `docs/02_schema_piplines.md`), fixes
Problems D/E/F from the injected data-quality issues, and windows a per-customer view count
over 1-hour tumbling windows. The cleaned event stream is written to
`streaming_data/flink_clean_events/<mode>/`, which `feat_stream_60m.py` reads.

---

## Why this runs in its own Python environment

`apache-flink` has no Python 3.13 wheel (max is 3.12 as of 2.3.0), but this repo's main venv
and `pyproject.toml` are pinned to `>=3.13`. Rather than downgrade the whole project, the Flink
job runs via `uv run --no-project --python 3.12 --with apache-flink`, an ephemeral,
uv-managed 3.12 environment resolved on demand (cached after the first run — the first
invocation downloads ~450MB of `apache-flink-libraries` + transitive deps, subsequent runs are
fast). Nothing in `pyproject.toml` changes; this module is intentionally standalone (no import
of `pipeline_base.py` or anything from the main venv).

---

## Architecture

```
a_data_generator/outputs/streaming/events.json
        │
        ▼  FileSource (bounded text-line read)
        │
        ▼  ParseEvent (JSON parse, drops malformed lines)
        │
        ▼  [optimized mode only] DedupByEventId — Problem F
        │     keyed ValueState[bool] on event_id, 2h TTL
        │
        ▼  WatermarkStrategy — Problem E
        │     optimized: bounded out-of-orderness (45 min)
        │     baseline:  monotonous timestamps (late events dropped)
        │
        ├──▶ FileSink (NDJSON) ──▶ streaming_data/flink_clean_events/<mode>/
        │
        └──▶ keyBy(customer_id) → TumblingEventTimeWindows(1h) → view count → .print()
                   (window-processing demo, IMPLEMENTATION_GUIDE 1.6)
```

Buffer flushing (Problem D) is a session-level config, not a stream stage:
`optimized` sets `env.set_buffer_timeout(100)`; `baseline` sets `-1` (flush only when a
buffer fills — the burst-window backpressure symptom).

---

## Prerequisites

- Data generated: `uv run python a_data_generator/generator.py` (from the main venv — this
  step is unrelated to Flink)
- `uv` installed (same tool already used for the rest of the repo)
- Java 17 (`java -version`) — already required for Spark elsewhere in this repo, reused here

---

## Running

Always run **baseline first**, then **optimized** — the Flink Web UI screenshots from both
runs are the grading evidence for Fixes D/E/F.

```bash
cd /mnt/d/fsds-ecommerce

# Step 1 — baseline (capture Flink UI "before" screenshots: backpressure HIGH,
# numLateRecordsDropped > 0, duplicate event_ids reaching the sink)
uv run --no-project --python 3.12 --with apache-flink python3 \
    b_schema_pipelines/pipelines/streaming/flink_stream_pipeline.py --mode baseline

# Step 2 — optimized (capture Flink UI "after" screenshots)
uv run --no-project --python 3.12 --with apache-flink python3 \
    b_schema_pipelines/pipelines/streaming/flink_stream_pipeline.py --mode optimized
```

Optional overrides:

```bash
uv run --no-project --python 3.12 --with apache-flink python3 \
    b_schema_pipelines/pipelines/streaming/flink_stream_pipeline.py \
    --mode optimized \
    --events-source a_data_generator/outputs/streaming/events.json \
    --sink-dir b_schema_pipelines/streaming_data/flink_clean_events
```

The full 262k-event daily file takes a few minutes end to end (parallelism is capped at 2 for
local dev, same "leave headroom" reasoning as `PipelineBase`'s reduced Spark core count — see
`_build_env`). For faster iteration while developing, point `--events-source` at a truncated
sample (`head -2000 events.json > sample.json`).

### Flink Web UI

Not enabled by default — `StreamExecutionEnvironment.get_execution_environment()` starts an
embedded MiniCluster with no REST endpoint unless one is configured. To get the UI at
**http://localhost:8081**, add before `_build_env`'s `get_execution_environment()` call (or
pass `--web-ui` if you add that flag):

```python
from pyflink.common import Configuration
config = Configuration()
config.set_integer("rest.port", 8081)
env = StreamExecutionEnvironment.get_execution_environment(config)
```

If 8081 is already taken on your machine (common — check with `ss -ltnp | grep 8081`), use a
different port and browse to that instead; nothing else in this pipeline depends on the
specific port number.

**Screenshot checklist** (see `docs/02_spark_optimisation_report.md`'s Flink Fixes section for
what each one should show):

| Tab | What to capture |
|---|---|
| Subtasks → Backpressure | `HIGH` (baseline) → `OK` (optimized) during the burst-window portion of the run |
| Metrics → `numLateRecordsDropped` | `> 0` (baseline) → `0` (optimized) |
| Overview → job graph | Shows the dedup `KeyedProcessFunction` and the tumbling-window operator in the pipeline |

---

## Expected output

```
STRUCTURED {"run_id": "flink_stream_optimized_20260714_020657", "pipeline_name": "flink_stream_pipeline", "mode": "optimized", "start_ts": "...", "end_ts": "...", "duration_s": 40.0, "status": "ok", "job_id": "41c17d4e...", "error_summary": ""}
```

This line is printed with plain `print()`, not `logging` — see `_log_run`'s docstring for why
(PyFlink's Python UDF execution runs through Apache Beam's Fn API worker, which takes over the
root logger's handlers for the duration of `env.execute()` and never hands them back;
`logging` calls made after `execute()` returns are silently dropped in-process, but stdout is
unaffected).

Verify the sink:

```bash
find b_schema_pipelines/streaming_data/flink_clean_events/optimized -name "*.json" -exec cat {} + | wc -l
```

Row count should be `(input events) - (duplicate event_ids in the input)` — the generator
injects duplicates at `duplicate_rate_stream: 0.015` (`a_data_generator/config/generator_config.yaml`),
so expect roughly 1.5% fewer rows than the input file in `optimized` mode. `baseline` mode's
sink keeps the duplicates (that's the point — it's the "before" evidence).

---

## Tests

No automated pytest suite for this module. Its logic — AQE-style session config
(`set_buffer_timeout`), a `WatermarkStrategy` choice, and a keyed `ValueState` dedup — has no
meaningful behavior to assert outside of actually running a Flink job: an automated test would
either need a live MiniCluster (slow, and would need to run in this module's separate 3.12
environment, not the main pytest suite the rest of the repo uses) or would just be re-asserting
that a config call was made, which doesn't verify anything a screenshot doesn't already prove.
This mirrors `transform_silver.py`'s AQE skew-join fix (`docs/02_spark_optimisation_report.md`
Fix 1), which also has no dedicated unit test — both are validated via UI evidence, not pytest.
The one function worth unit-testing in isolation, `ParseEvent.map`'s malformed-JSON handling,
would need to import `pyflink.datastream.functions.MapFunction` to subclass it, which pulls the
same environment problem back in — not worth a second Python environment in the main test run
for one `try/except json.loads`.
