"""
flink_stream_pipeline — Section 02 Flink streaming pipeline (Problems D/E/F).

Reads a_data_generator/outputs/streaming/events.json as a bounded file source
(the local-dev stand-in for the Kafka topic described in docs/02_schema_piplines.md),
fixes the three injected streaming problems in `--mode optimized`, and windows a
per-customer view count over 1-hour tumbling windows (the "window processing"
rubric item — IMPLEMENTATION_GUIDE 1.6). The cleaned event stream is written to
`sink_dir/<mode>/`, one subdirectory per mode so a baseline run never
reintroduces duplicates into what feat_stream_60m.py reads. The windowed
view-count results are written to `sink_dir/<mode>_window_counts/` — not
printed: `.print()` round-trips every result back to the client process one
at a time, and this window fires once per (customer, hour) combination with
activity, tens of thousands of results on the full dataset. (The full
262K-event run still takes 50-70 minutes regardless of sink type or
parallelism — see `_build_env`'s comment; the actual bottleneck is
unidentified and needs Flink Web UI profiling, not more timing experiments.)

Problems fixed in `--mode optimized` (docs/02_spark_optimisation_report.md, Fixes D/E/F):
    D — 30x burst traffic (12:00-12:20, 20:00-20:20): a short, explicit buffer
        timeout keeps the network buffers flushing instead of filling up and
        backpressuring the source during the spike.
    E — 12% of events arrive 5-45 min late: WatermarkStrategy tolerates up to
        45 min of out-of-orderness instead of treating every late event as
        droppable.
    F — 1.5% duplicate event_ids: a keyed ValueState (TTL-bounded) drops an
        event_id it has already seen in this run.

`--mode baseline` skips all three fixes so the Flink Web UI (localhost:8081)
shows the raw symptoms for the "before" evidence screenshots.

Runs in an isolated Python 3.12 environment — apache-flink has no Python 3.13
wheel, and this repo's main venv is pinned to >=3.13. See streaming/README.md.

Run (from repo root):
    uv run --no-project --python 3.12 --with apache-flink \
        python3 b_schema_pipelines/pipelines/streaming/flink_stream_pipeline.py --mode baseline
    uv run --no-project --python 3.12 --with apache-flink \
        python3 b_schema_pipelines/pipelines/streaming/flink_stream_pipeline.py --mode optimized
"""

from __future__ import annotations

import argparse
import json
import logging
import os
from datetime import datetime

from pyflink.common import Configuration, Duration, Time, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import (
    Encoder,
    FileSink,
    FileSource,
    OutputFileConfig,
    StreamFormat,
)
from pyflink.datastream.functions import (
    AggregateFunction,
    KeyedProcessFunction,
    MapFunction,
    WindowFunction,
)
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
from pyflink.datastream.window import TumblingEventTimeWindows

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
logger = logging.getLogger("flink_stream_pipeline")

LATE_ARRIVAL_MINUTES = 45   # Problem E: generator delays 12% of events by 5-45 min
DEDUP_STATE_TTL_HOURS = 2   # bounds Problem F's keyed dedup state growth
BUFFER_TIMEOUT_MS = 100     # Problem D: network buffer flush interval in optimized mode
WINDOW_HOURS = 1            # tumbling window size for the windowed-aggregation demo


class ParseEvent(MapFunction):
    """NDJSON line -> dict. A malformed line becomes None and is filtered out
    immediately downstream rather than failing the job."""

    def map(self, line: str):
        try:
            return json.loads(line)
        except json.JSONDecodeError:
            logger.warning("dropping malformed event line: %.120s", line)
            return None


class EventTimestampAssigner(TimestampAssigner):
    """Watermark source: event_timestamp (ISO 8601), converted to epoch millis."""

    def extract_timestamp(self, value: dict, record_timestamp: int) -> int:
        return int(datetime.fromisoformat(value["event_timestamp"]).timestamp() * 1000)


class DedupByEventId(KeyedProcessFunction):
    """Problem F fix: keyed ValueState[bool] on event_id, dropping any event_id
    already seen in this run. TTL bounds state growth — an event_id is only
    tracked long enough to catch the generator's same-run duplicate emission,
    not forever.
    """

    def open(self, runtime_context) -> None:
        ttl_config = StateTtlConfig.new_builder(Time.hours(DEDUP_STATE_TTL_HOURS)).build()
        descriptor = ValueStateDescriptor("seen", Types.BOOLEAN())
        descriptor.enable_time_to_live(ttl_config)
        self.seen = runtime_context.get_state(descriptor)

    def process_element(self, value, ctx):
        if self.seen.value():
            return
        self.seen.update(True)
        yield value


class ViewCountAggregate(AggregateFunction):
    """Counts `view` events per key per window — the windowed-aggregation demo."""

    def create_accumulator(self):
        return 0

    def add(self, value, accumulator):
        return accumulator + (1 if value.get("event_type") == "view" else 0)

    def get_result(self, accumulator):
        return accumulator

    def merge(self, acc_a, acc_b):
        return acc_a + acc_b


class ViewCountWindowResult(WindowFunction):
    """Attaches the customer_id key and window boundaries to
    ViewCountAggregate's bare count. Without this, the aggregate's output
    type is just an anonymous integer — no way to tell which customer or
    which window a given count belongs to once it leaves the aggregation
    step. `inputs` holds exactly one element here: `.aggregate()` already
    reduced the window to ViewCountAggregate's single running accumulator
    before this window function ever runs.
    """

    def apply(self, key, window, inputs):
        (count,) = inputs
        yield json.dumps({
            "customer_id": key,
            "window_start": datetime.fromtimestamp(window.start / 1000).isoformat(),
            "window_end": datetime.fromtimestamp(window.end / 1000).isoformat(),
            "view_count": count,
        })


class FlinkStreamPipeline:
    """Reads the simulated event stream, applies the Problem D/E/F fixes
    (optimized mode only), and windows a per-customer view count.

    `sink_dir/<mode>/` receives the cleaned event stream; feat_stream_60m.py
    should point at `sink_dir/optimized/` in production (see streaming/README.md).
    `sink_dir/<mode>_window_counts/` receives the windowed view-count demo
    output (customer_id, window_start, window_end, view_count per line) —
    not consumed downstream, evidence only.
    """

    def __init__(
        self,
        events_source: str = "a_data_generator/outputs/streaming/events.json",
        sink_dir: str = "b_schema_pipelines/streaming_data/flink_clean_events",
        mode: str = "optimized",
        web_ui_port: int | None = None,
    ):
        if mode not in ("baseline", "optimized"):
            raise ValueError(f"Unknown mode: {mode!r}. Choose 'baseline' or 'optimized'.")
        self.events_source = events_source
        self.sink_dir = sink_dir
        self.mode = mode
        self.web_ui_port = web_ui_port
        self.run_id = f"flink_stream_{mode}_{datetime.now():%Y%m%d_%H%M%S}"

    # ── public ────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Wire and execute the pipeline, logging a structured summary line on
        success or failure (same field set as PipelineBase.log_run, so Loki
        queries work the same way across Spark and Flink jobs)."""
        start_ts = datetime.now()
        try:
            env = self._build_env()
            watermarked = self._read_and_clean(env)
            self._write_sink(watermarked)
            self._write_window_sink(self._apply_windowing(watermarked))

            result = env.execute(self.run_id)
            self._log_run(start_ts, "ok", job_id=str(result.get_job_id()))
        except Exception as exc:
            self._log_run(start_ts, "error", error=str(exc))
            raise

    # ── private: environment ────────────────────────────────────────────────

    def _build_env(self) -> StreamExecutionEnvironment:
        # No REST endpoint by default — get_execution_environment() alone
        # starts an embedded MiniCluster with the Web UI unreachable. Only
        # bind a port when explicitly asked (--web-ui): most runs (CI,
        # scripted re-runs) don't need it, and binding a port that's
        # already in use on the host would otherwise break every run.
        if self.web_ui_port is not None:
            config = Configuration()
            config.set_integer("rest.port", self.web_ui_port)
            env = StreamExecutionEnvironment.get_execution_environment(config)
            print(f"Flink Web UI: http://localhost:{self.web_ui_port}")
        else:
            env = StreamExecutionEnvironment.get_execution_environment()
        # PyFlink's Python UDFs (ParseEvent, DedupByEventId,
        # EventTimestampAssigner, ViewCountAggregate) all execute through
        # Apache Beam's Fn API — each record round-trips to a separate
        # Python worker process, far more expensive per-record than a
        # native Flink operator. Scaling parallelism with available cores,
        # same "leave headroom" reasoning as PipelineBase's Spark core
        # count, lets more Python workers run concurrently for that part of
        # the job.
        #
        # Measured live, isolating each variable on the full 262K-event
        # dataset: parallelism=10 + .print() = 54 min; parallelism=10 +
        # FileSink (ViewCountWindowResult/_write_window_sink) = 71 min;
        # parallelism=2 + FileSink = 51 min. None of these are close to a
        # previously-documented ~30 min figure, and neither parallelism nor
        # the sink swap explains the gap — that ~30 min number was likely
        # never a reliable, matched-conditions measurement in this
        # environment. Left at cpu-scaled parallelism (not reverted to a
        # fixed 2) since forcing it down showed no real benefit either;
        # TumblingEventTimeWindows + keyed aggregation are deterministic
        # regardless of parallelism either way — this only affects
        # throughput, not results. Unresolved: where the ~50-70 min
        # actually goes needs Flink Web UI profiling (streaming/README.md's
        # "Flink Web UI" section), not more blind timing runs.
        env.set_parallelism(max(2, (os.cpu_count() or 4) - 2))
        if self.mode == "optimized":
            env.set_buffer_timeout(BUFFER_TIMEOUT_MS)
        else:
            # Flush only when a buffer fills, rather than on a timer — under
            # the 30x burst windows this is exactly the Problem D symptom:
            # buffers build up and backpressure the source (Flink UI, Subtasks
            # tab, Backpressure: HIGH during 12:00-12:20 / 20:00-20:20).
            env.set_buffer_timeout(-1)
        return env

    # ── private: source + fixes ─────────────────────────────────────────────

    def _read_and_clean(self, env: StreamExecutionEnvironment):
        source = FileSource.for_record_stream_format(
            StreamFormat.text_line_format(), self.events_source
        ).build()
        raw = env.from_source(source, WatermarkStrategy.no_watermarks(), "events_source")

        parsed = (
            raw.map(ParseEvent(), output_type=Types.PICKLED_BYTE_ARRAY())
            .filter(lambda e: e is not None)
        )

        if self.mode == "optimized":
            parsed = self._apply_dedup(parsed)

        return self._apply_watermark_strategy(parsed)

    def _apply_dedup(self, stream):
        """Problem F fix — see DedupByEventId. Skipped entirely in baseline
        mode so duplicate event_ids flow straight through to the sink."""
        return stream.key_by(lambda e: e["event_id"]).process(
            DedupByEventId(), output_type=Types.PICKLED_BYTE_ARRAY()
        )

    def _apply_watermark_strategy(self, stream):
        """Problem E fix — bounded out-of-orderness tolerates the generator's
        5-45 min late arrivals. Baseline uses monotonous timestamps instead:
        any event later than the current watermark is treated as late and
        dropped (Flink UI, Metrics tab, numLateRecordsDropped > 0)."""
        if self.mode == "optimized":
            strategy = WatermarkStrategy.for_bounded_out_of_orderness(
                Duration.of_minutes(LATE_ARRIVAL_MINUTES)
            ).with_timestamp_assigner(EventTimestampAssigner())
        else:
            strategy = WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
                EventTimestampAssigner()
            )
        return stream.assign_timestamps_and_watermarks(strategy)

    # ── private: windowing + sink ───────────────────────────────────────────

    def _apply_windowing(self, stream):
        """Window-processing demo (IMPLEMENTATION_GUIDE 1.6): view count per
        customer per 1-hour tumbling window. The window_function
        (ViewCountWindowResult) attaches the customer_id key and window
        boundaries to ViewCountAggregate's bare count — without it the
        output is just an anonymous integer, no way to tell which
        customer/window a given count belongs to downstream."""
        return (
            stream.key_by(lambda e: e["customer_id"])
            .window(TumblingEventTimeWindows.of(Time.hours(WINDOW_HOURS)))
            .aggregate(
                ViewCountAggregate(),
                window_function=ViewCountWindowResult(),
                output_type=Types.STRING(),
            )
        )

    def _write_sink(self, stream) -> None:
        """Writes the cleaned event stream (same NDJSON shape as events.json)
        to sink_dir/<mode>/ — feat_stream_60m.py reads this directory."""
        output_dir = f"{self.sink_dir}/{self.mode}"
        sink = (
            FileSink.for_row_format(output_dir, Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix(f"events_{self.mode}")
                .with_part_suffix(".json")
                .build()
            )
            .build()
        )
        stream.map(json.dumps, output_type=Types.STRING()).sink_to(sink)

    def _write_window_sink(self, stream) -> None:
        """Writes the windowed view-count results (already JSON strings —
        see ViewCountWindowResult) to sink_dir/<mode>_window_counts/.

        Not `.print()`: that sink round-trips every result back to the
        client process one at a time, and this window fires once per
        (customer, hour) combination with activity — tens of thousands of
        results on the full dataset. A FileSink lets every parallel subtask
        write its own buffer straight to disk instead — strictly better
        practice, though see `_build_env`'s comment: this swap alone did not
        meaningfully change the full run's wall-clock time in testing.
        """
        output_dir = f"{self.sink_dir}/{self.mode}_window_counts"
        sink = (
            FileSink.for_row_format(output_dir, Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix(f"window_counts_{self.mode}")
                .with_part_suffix(".json")
                .build()
            )
            .build()
        )
        stream.sink_to(sink)

    # ── private: logging ────────────────────────────────────────────────────

    def _log_run(self, start_ts: datetime, status: str, job_id: str = "", error: str = "") -> None:
        """Prints (not `logging`) the structured run summary.

        PyFlink's DataStream API executes Python UDFs (our MapFunction /
        KeyedProcessFunction classes) through Apache Beam's Fn API worker
        under the hood. That worker replaces the root logger's handlers with
        its own FnApiLogRecordHandler for the duration of env.execute(), and
        it does not hand them back afterwards — any `logging` call made after
        execute() returns is silently dropped. Plain stdout is unaffected, so
        the one line every downstream consumer (Loki, a human) actually needs
        goes through print(), not logger.info/error.
        """
        end_ts = datetime.now()
        entry = {
            "run_id": self.run_id,
            "pipeline_name": "flink_stream_pipeline",
            "mode": self.mode,
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "duration_s": round((end_ts - start_ts).total_seconds(), 3),
            "status": status,
            "job_id": job_id,
            "error_summary": error,
        }
        print(f"STRUCTURED {json.dumps(entry)}")


# ── entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["baseline", "optimized"], default="optimized")
    parser.add_argument(
        "--events-source", default="a_data_generator/outputs/streaming/events.json"
    )
    parser.add_argument(
        "--sink-dir", default="b_schema_pipelines/streaming_data/flink_clean_events"
    )
    parser.add_argument(
        "--web-ui", type=int, nargs="?", const=8081, default=None, metavar="PORT",
        help="Enable the Flink Web UI (default port 8081 if given with no value).",
    )
    args = parser.parse_args()

    FlinkStreamPipeline(
        events_source=args.events_source,
        sink_dir=args.sink_dir,
        mode=args.mode,
        web_ui_port=args.web_ui,
    ).run()


if __name__ == "__main__":
    main()
