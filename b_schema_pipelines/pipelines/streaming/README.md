# Flink Streaming Pipeline

## Rubric proof checklist (`coursework/rubrics.md` — Flink job to handle streaming data problems, 13 pts)

| Requirement (rubric wording) | Pts | Where satisfied here |
|---|---|---|
| Baseline (without optimization) — "Document giải thích từng step optimize từ baseline như thế nào, dùng Flink UI như thế nào (với screenshots)" | 2 | §"Running" below documents the baseline→optimized procedure and exact Flink UI tabs to check — **Flink UI screenshots not yet captured**, see the checklist below |
| Handle burst with explanation (Problem D) | 3 | `_build_env`'s buffer-timeout config, documented in "Architecture" below and `docs/02_spark_optimisation_report.md` Fix D — code + explanation done, **screenshot outstanding** |
| Handle late arrival with explanation (Problem E) | 3 | `WatermarkStrategy` choice, Fix E — code + explanation done, **screenshot outstanding** |
| Handle other streaming problem with explanation (chosen: duplicate event_ids, Problem F) | 3 | `DedupByEventId` keyed `ValueState`, Fix F — code + explanation done, **screenshot outstanding** |
| Window processing — "Capture đoạn code thể hiện khả năng xử lý Window trong Flink" | 2 | ✅ **code capture, not a screenshot** — satisfied directly below, no gap |

**Window processing code capture** (the one item in this table that's a code capture, not a
UI screenshot — fully satisfiable here):

```python
# b_schema_pipelines/pipelines/streaming/offline_stream_pipeline.py
# keyBy(customer_id) -> 1-hour tumbling event-time windows -> per-window view count
(
    cleaned_stream
    .key_by(lambda event: event["customer_id"])
    .window(TumblingEventTimeWindows.of(Time.hours(1)))
    .aggregate(ViewCountAggregate(), window_function=ViewCountWindowResult())
)
```

This is the actual windowing branch inside `FlinkStreamPipeline.run()` — a second branch off
the same cleaned/deduped/watermarked stream that also feeds the file sink (see "Architecture"
below). `TumblingEventTimeWindows` + `ViewCountAggregate` is genuine Flink window processing
(event-time semantics, not processing-time), not a placeholder. `ViewCountWindowResult`
(a `WindowFunction`) attaches the `customer_id` key and window start/end to the aggregate's
bare count — without it the output is just an anonymous integer with no way to tell which
customer/window it belongs to.

**Not `.print()`.** An earlier version routed this straight to `.print()`, which round-trips
every result back to the client process one at a time — this window fires once per
`(customer, hour)` combination with activity, tens of thousands of results on the full
dataset. Now written via `FileSink` to `streaming_data/flink_clean_events/<mode>_window_counts/`
instead — every parallel subtask writes its own buffer straight to disk, no per-record round
trip; strictly better practice regardless of dataset size. **Note:** measured live, this swap
alone did not meaningfully reduce the full-dataset run's wall-clock time — see `_build_env`'s
comment for the full isolation-testing results. The job's real bottleneck on the full 262K-event
dataset (consistently 50-70 minutes across several configurations) is still unidentified;
Flink Web UI profiling (below) is the next step, not more timing experiments.

**Screenshot gap**: Fixes D/E/F have real code and written analysis (this file's Architecture
section + `docs/02_spark_optimisation_report.md`'s Flink Fixes section), but the actual Flink
Web UI before/after screenshots (backpressure, `numLateRecordsDropped`, dedup evidence) listed
in the checklist near the bottom of this file haven't been captured yet.

---

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
        └──▶ keyBy(customer_id) → TumblingEventTimeWindows(1h) → view count
                   → FileSink (NDJSON) ──▶ streaming_data/flink_clean_events/<mode>_window_counts/
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
    b_schema_pipelines/pipelines/streaming/offline_stream_pipeline.py --mode baseline

# Step 2 — optimized (capture Flink UI "after" screenshots)
uv run --no-project --python 3.12 --with apache-flink python3 \
    b_schema_pipelines/pipelines/streaming/offline_stream_pipeline.py --mode optimized
```

Optional overrides:

```bash
uv run --no-project --python 3.12 --with apache-flink python3 \
    b_schema_pipelines/pipelines/streaming/offline_stream_pipeline.py \
    --mode optimized \
    --events-source a_data_generator/outputs/streaming/events.json \
    --sink-dir b_schema_pipelines/streaming_data/flink_clean_events
```

Parallelism scales with available cores (same "leave headroom" reasoning as `PipelineBase`'s
reduced Spark core count — see `_build_env`), not a fixed cap. For faster iteration while
developing, point `--events-source` at a truncated sample (`head -2000 events.json > sample.json`).

### Flink Web UI

Not enabled by default — `StreamExecutionEnvironment.get_execution_environment()` starts an
embedded MiniCluster with no REST endpoint unless one is configured. Pass `--web-ui` to bind
one:

```bash
uv run --no-project --python 3.12 --with apache-flink python3 \
    b_schema_pipelines/pipelines/streaming/offline_stream_pipeline.py \
    --mode optimized --web-ui
# UI at http://localhost:8081 while the job runs (the process stays alive for
# the job's full duration — the UI won't be reachable after it exits)
```

`--web-ui` alone binds port 8081; pass `--web-ui 8090` for a specific port. Verified live:
`curl http://localhost:8081/overview` returns real cluster status
(`{"taskmanagers":1,"slots-total":...,"jobs-running":1,...}`) while a job is running.

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

**Window-processing demo output** (the "window processing" rubric evidence):

```bash
find b_schema_pipelines/streaming_data/flink_clean_events/optimized_window_counts -name "*.json" -exec cat {} + | head -5
```

Each line is one `(customer_id, 1-hour window)` result:

```json
{"customer_id": "C050976", "window_start": "2026-06-22T00:00:00", "window_end": "2026-06-22T01:00:00", "view_count": 1}
```

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


# Explanation

---

# Block 1. Setup

```python
import ...
```

**Purpose:**

* Import Flink libraries
* Define constants

```python
LATE_ARRIVAL_MINUTES = 45
WINDOW_HOURS = 1
```

Think of it as:

> "Configure the pipeline."

---

# Block 2. Read Events

```python
class ParseEvent(MapFunction):
```

**Input**

```json
"{\"customer_id\":\"C1\",\"event\":\"view\"}"
```

↓

**Output**

```python
{
    "customer_id":"C1",
    "event":"view"
}
```

It simply **reads each JSON line**.

---

# Block 3. Remove Duplicates

```python
class DedupByEventId(KeyedProcessFunction):
```

Input

```
Event 1
Event 2
Event 1
```

↓

Output

```
Event 1
Event 2
```

Uses `event_id` to remember what has already been seen.

---

# Block 4. Handle Late Events

```python
EventTimestampAssigner
```

and

```python
_apply_watermark_strategy()
```

This tells Flink:

> "Events can arrive up to 45 minutes late."

Without this

```
10:00
10:30
10:10 (late)
```

The last event might be ignored.

---

# Block 5. Window Aggregation

```python
ViewCountAggregate
```

This is basically

```python
count += 1
```

for every `"view"` event.

Example

```
Alice viewed
Alice viewed
Alice purchased
```

Result

```
Alice viewed = 2
```

---

# Block 6. Add Window Information

```python
ViewCountWindowResult
```

After counting, Flink only knows

```
2
```

This class changes it into

```json
{
  "customer_id":"Alice",
  "window":"10-11",
  "view_count":2
}
```

Now the result is meaningful.

---

# Block 7. Pipeline

This is the important part.

```python
run()

    ↓

_build_env()

    ↓

_read_and_clean()

    ↓

_apply_windowing()

    ↓

_write_sink()

    ↓

execute()
```

This is the whole pipeline.

Or even simpler:

```text
Read events
      ↓
Parse JSON
      ↓
Remove duplicates
      ↓
Handle late events
      ↓
Count views every hour
      ↓
Save results
```

---

# Everything else

Most of the remaining code is **production infrastructure**, not Flink logic:

* `logging` → logs
* `_build_env()` → create Flink environment
* `_write_sink()` → save files
* `_log_run()` → record pipeline execution
* `argparse` → read command-line arguments

These don't implement the streaming algorithm—they make the pipeline easier to run, monitor, and debug.

## If this were written for teaching

The entire file could be reduced conceptually to:

```python
env = StreamExecutionEnvironment.get_execution_environment()

events = read_json()

events = parse(events)

events = remove_duplicates(events)

events = handle_late_events(events)

view_counts = count_views_per_hour(events)

save(events)

save(view_counts)

env.execute()
```
