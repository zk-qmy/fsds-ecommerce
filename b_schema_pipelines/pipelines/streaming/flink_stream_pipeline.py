"""
flink_stream_pipeline — Section 02 Flink streaming pipeline (Problems D/E/F).

Reads a_data_generator/outputs/streaming/events.json as a bounded file source
(the local-dev stand-in for the Kafka topic described in docs/02_schema_piplines.md),
fixes the three injected streaming problems in `--mode optimized`, and windows a
per-customer view count over 1-hour tumbling windows (the "window processing"
rubric item — IMPLEMENTATION_GUIDE 1.6). The cleaned event stream is written to
`sink_dir/<mode>/`, one subdirectory per mode so a baseline run never
reintroduces duplicates into what feat_stream_60m.py reads.

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
from datetime import datetime

from pyflink.common import Duration, Time, Types, WatermarkStrategy
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.file_system import (
    Encoder,
    FileSink,
    FileSource,
    OutputFileConfig,
    StreamFormat,
)
from pyflink.datastream.functions import AggregateFunction, KeyedProcessFunction, MapFunction
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


class FlinkStreamPipeline:
    """Reads the simulated event stream, applies the Problem D/E/F fixes
    (optimized mode only), and windows a per-customer view count.

    `sink_dir/<mode>/` receives the cleaned event stream; feat_stream_60m.py
    should point at `sink_dir/optimized/` in production (see streaming/README.md).
    """

    def __init__(
        self,
        events_source: str = "a_data_generator/outputs/streaming/events.json",
        sink_dir: str = "b_schema_pipelines/streaming_data/flink_clean_events",
        mode: str = "optimized",
    ):
        if mode not in ("baseline", "optimized"):
            raise ValueError(f"Unknown mode: {mode!r}. Choose 'baseline' or 'optimized'.")
        self.events_source = events_source
        self.sink_dir = sink_dir
        self.mode = mode
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
            self._apply_windowing(watermarked).print()

            result = env.execute(self.run_id)
            self._log_run(start_ts, "ok", job_id=str(result.get_job_id()))
        except Exception as exc:
            self._log_run(start_ts, "error", error=str(exc))
            raise

    # ── private: environment ────────────────────────────────────────────────

    def _build_env(self) -> StreamExecutionEnvironment:
        env = StreamExecutionEnvironment.get_execution_environment()
        # Local dev: the default parallelism is one task per CPU core, which
        # scatters output across that many part-files for a dataset this
        # small. Same "leave headroom, keep output tidy" reasoning as
        # PipelineBase's reduced Spark core count.
        env.set_parallelism(2)
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
        customer per 1-hour tumbling window."""
        return (
            stream.key_by(lambda e: e["customer_id"])
            .window(TumblingEventTimeWindows.of(Time.hours(WINDOW_HOURS)))
            .aggregate(ViewCountAggregate(), output_type=Types.LONG())
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
    args = parser.parse_args()

    FlinkStreamPipeline(
        events_source=args.events_source, sink_dir=args.sink_dir, mode=args.mode
    ).run()


if __name__ == "__main__":
    main()
