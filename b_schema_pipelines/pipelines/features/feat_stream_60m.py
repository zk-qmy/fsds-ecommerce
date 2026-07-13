"""
feat_stream_60m — 60-minute session aggregations from streaming events.

Reads Flink-processed events (or raw events.json in local dev), computes
4 streaming features per customer per 60-minute tumbling window, and writes
to feat_stream_60m (PostgreSQL) ready for Feast ingestion.

Run:
    uv run python b_schema_pipelines/pipelines/features/feat_stream_60m.py
"""

from __future__ import annotations

import argparse

from b_schema_pipelines.pipelines.pipeline_base import PipelineBase

# TODO: from pyspark.sql import functions as F
# TODO: from pyspark.sql.types import StructType, StructField, StringType, ...


class StreamFeature60m(PipelineBase):
    """Computes 60-minute tumbling window streaming features per customer."""

    PREFIX         = "feat_60m"
    FEAT_TABLE     = "feat_stream_60m"
    WINDOW_MINUTES = 60
    BURST_WINDOWS  = [("12:00", "12:20"), ("20:00", "20:20")]

    def __init__(
        self,
        events_source: str = "a_data_generator/outputs/streaming/events.json",
        postgres_url: str = "jdbc:postgresql://localhost:5432/fsds",
    ):
        super().__init__()
        self.events_source = events_source
        self.postgres_url = postgres_url
        self.spark = self._build_spark()

    # ── public ────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        TODO:
            events_df = self._read_events()
            events_df = self._add_burst_flag(events_df)
            feat_df   = self._compute_features(events_df)
            self._write(feat_df)
            self._log_run(...)
            self.spark.stop()
        """
        raise NotImplementedError

    # ── private: spark ────────────────────────────────────────────────────────

    def _build_spark(self):
        """
        TODO: Needs Delta + PostgreSQL JDBC jar.

            spark = super()._build_spark()
            spark.conf.set("spark.jars", "/path/to/postgresql-42.x.x.jar")
            return spark
        """
        raise NotImplementedError

    # ── private: reader ───────────────────────────────────────────────────────

    def _read_events(self):
        """
        TODO: Read events from NDJSON (local dev) or Flink sink path (prod).

        Use an explicit schema — do not infer (mixed nullable columns cause issues):
            event_id, event_type, event_timestamp (string → cast to timestamp),
            created_ts (string → cast), customer_id, session_id,
            product_id (nullable), order_id (nullable),
            quantity (LongType, nullable), price (DoubleType, nullable)

        After reading:
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
            .withColumn("created_ts",      F.to_timestamp("created_ts"))
        """
        raise NotImplementedError

    # ── private: feature computation ──────────────────────────────────────────

    def _add_burst_flag(self, events_df):
        """
        TODO: Add f_stream_burst_activity_flag column.

        Flag = 1 if the event falls inside a burst window (12:00-12:20 or 20:00-20:20).

            hour, minute = F.hour("event_timestamp"), F.minute("event_timestamp")
            is_burst = ((hour == 12) & (minute < 20)) | ((hour == 20) & (minute < 20))
            events_df.withColumn(
                "f_stream_burst_activity_flag", F.when(is_burst, 1).otherwise(0)
            )
        """
        raise NotImplementedError

    def _compute_features(self, events_df):
        """
        TODO: Aggregate into 60-minute tumbling windows per customer.

        Window key:
            F.window("event_timestamp", "60 minutes")
            Use window.start as output event_timestamp.

        Features per (customer_id, window):

            f_stream_views_30m
                COUNT of event_type='view' in first 30 min of window.
                Filter: event_timestamp < window.start + interval 30 minutes

            f_stream_add_to_cart_30m
                COUNT of event_type='add_to_cart' in first 30 min of window.

            f_stream_cart_to_purchase_ratio_60m
                SUM(event_type='purchase') / NULLIF(SUM(event_type='add_to_cart'), 0)
                over the full 60-min window.
                Use F.sum(F.when(...)) to avoid a second groupBy.

            f_stream_burst_activity_flag
                MAX(f_stream_burst_activity_flag) over window.

        Output schema:
            customer_id                          STRING
            event_timestamp                      TIMESTAMP  (window.start)
            created_ts                           TIMESTAMP  (F.current_timestamp())
            f_stream_views_30m                   LONG
            f_stream_add_to_cart_30m             LONG
            f_stream_cart_to_purchase_ratio_60m  DOUBLE
            f_stream_burst_activity_flag         INT
        """
        raise NotImplementedError

    # ── private: writer ───────────────────────────────────────────────────────

    def _write(self, df) -> int:
        """
        TODO: Append features to PostgreSQL feat_stream_60m.

        Idempotency: delete rows for affected window starts before inserting
        (use psycopg2 for DELETE, then Spark JDBC for INSERT).
        Return output row count.
        """
        raise NotImplementedError


# ── entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--events-source",
        default="a_data_generator/outputs/streaming/events.json",
    )
    parser.add_argument("--postgres-url", default="jdbc:postgresql://localhost:5432/fsds")
    args = parser.parse_args()

    StreamFeature60m(
        events_source=args.events_source,
        postgres_url=args.postgres_url,
    ).run()


if __name__ == "__main__":
    main()
