"""
feat_stream_60m — 60-minute session aggregations from streaming events.

Reads Flink-processed events (or raw events.json in local dev — see
streaming/README.md for how the Flink pipeline's clean-sink output becomes
this file's default source), computes 4 streaming features per customer per
60-minute tumbling window, and writes to feat_stream_60m (PostgreSQL) ready
for Feast ingestion.

Run:
    uv run python b_schema_pipelines/pipelines/features/feat_stream_60m.py
    uv run python b_schema_pipelines/pipelines/features/feat_stream_60m.py \
        --events-source b_schema_pipelines/streaming_data/flink_clean_events/optimized
"""

from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime

import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from b_schema_pipelines.pipelines.pipeline_base import PipelineBase

EVENTS_SCHEMA = StructType([
    StructField("event_id",        StringType(), False),
    StructField("event_type",      StringType(), False),
    StructField("event_timestamp", StringType(), False),
    StructField("created_ts",      StringType(), False),
    StructField("customer_id",     StringType(), False),
    StructField("session_id",      StringType(), False),
    StructField("product_id",      StringType(), True),
    StructField("order_id",        StringType(), True),
    StructField("quantity",        LongType(),   True),
    StructField("price",           DoubleType(), True),
])


class StreamFeature60m(PipelineBase):
    """Computes 60-minute tumbling window streaming features per customer."""

    PREFIX         = "feat_60m"
    FEAT_TABLE     = "feat_stream_60m"
    GOLD_SCHEMA    = "gold_ecommerce"
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

        pg_cfg = self.shared_cfg.get("postgres", {})
        self.postgres_host = pg_cfg.get("host", "localhost")
        self.postgres_port = pg_cfg.get("port", 5432)
        self.postgres_db = pg_cfg.get("db", "fsds")
        self.postgres_user = pg_cfg.get("user", "fsds")
        self.postgres_password = pg_cfg.get("password", "fsds")

        self.spark = self._build_spark()

    # ── public ────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Compute 60-minute streaming features for every window present in
        `events_source` and write them.

        Does not stop the Spark session — same reasoning as
        feat_customer_90d.py: the caller (Airflow task, or a process that
        chains this after the Flink pipeline) owns the session's lifecycle.
        """
        start_ts = datetime.now()
        try:
            events_df = self._read_events()
            events_df = self._add_burst_flag(events_df)
            feat_df = self._compute_features(events_df)
            rows_out = self._write(feat_df)
            self.log_run(self.FEAT_TABLE, start_ts, datetime.now(), rows_out, rows_out, "ok")
        except Exception as exc:
            self.log_run(self.FEAT_TABLE, start_ts, datetime.now(), 0, 0, "error", str(exc))
            raise

    # ── private: spark ────────────────────────────────────────────────────────

    def _build_spark(self):
        """Delta + PostgreSQL JDBC are already loaded via the shared `packages`
        list in pipeline_config.yaml at session creation time (see PipelineBase)."""
        return self.spark

    # ── private: reader ───────────────────────────────────────────────────────

    def _read_events(self):
        """Read events from NDJSON — a single file (local dev, raw events.json)
        or a directory of part-files (the Flink pipeline's bucketed sink
        output, one subdirectory per processing hour).

        Timestamps are read as strings and cast explicitly rather than
        inferred — event_timestamp/created_ts carry microsecond precision
        that schema inference handles inconsistently across files.
        """
        return (
            self.spark.read.schema(EVENTS_SCHEMA)
            .option("recursiveFileLookup", "true")
            .json(self.events_source)
            .withColumn("event_timestamp", F.to_timestamp("event_timestamp"))
            .withColumn("created_ts", F.to_timestamp("created_ts"))
        )

    # ── private: feature computation ──────────────────────────────────────────

    def _add_burst_flag(self, events_df):
        """Flag = 1 if the event falls inside a burst window (12:00-12:20 or
        20:00-20:20) — end time is exclusive, matching BURST_WINDOWS."""
        hour = F.hour("event_timestamp")
        minute = F.minute("event_timestamp")
        is_burst = hour.isin(12, 20) & (minute < 20)
        return events_df.withColumn(
            "f_stream_burst_activity_flag", F.when(is_burst, F.lit(1)).otherwise(F.lit(0))
        )

    def _compute_features(self, events_df):
        """Aggregate into 60-minute tumbling windows per customer.

        `_window` is computed per-row (not as a groupBy key expression) so
        `_window.start` is available as a plain column inside the WHEN
        conditions below — comparing each row's own timestamp to its own
        window's start, without needing a second pass over the data.
        """
        windowed = events_df.withColumn(
            "_window", F.window("event_timestamp", f"{self.WINDOW_MINUTES} minutes")
        )
        first_half_cutoff = F.col("_window.start") + F.expr("INTERVAL 30 MINUTES")
        in_first_half = F.col("event_timestamp") < first_half_cutoff

        aggregated = windowed.groupBy("customer_id", "_window").agg(
            F.sum(
                F.when((F.col("event_type") == "view") & in_first_half, 1).otherwise(0)
            ).alias("f_stream_views_30m"),
            F.sum(
                F.when((F.col("event_type") == "add_to_cart") & in_first_half, 1).otherwise(0)
            ).alias("f_stream_add_to_cart_30m"),
            F.sum(F.when(F.col("event_type") == "purchase", 1).otherwise(0)).alias("_purchases"),
            F.sum(
                F.when(F.col("event_type") == "add_to_cart", 1).otherwise(0)
            ).alias("_add_to_carts"),
            F.max("f_stream_burst_activity_flag").alias("f_stream_burst_activity_flag"),
        )

        return (
            aggregated.withColumn(
                "f_stream_cart_to_purchase_ratio_60m",
                F.when(F.col("_add_to_carts") > 0, F.col("_purchases") / F.col("_add_to_carts"))
                .otherwise(F.lit(0.0)),
            )
            .withColumn("event_timestamp", F.col("_window.start"))
            .withColumn("created_ts", F.current_timestamp())
            .select(
                "customer_id",
                "event_timestamp",
                "created_ts",
                "f_stream_views_30m",
                "f_stream_add_to_cart_30m",
                "f_stream_cart_to_purchase_ratio_60m",
                "f_stream_burst_activity_flag",
            )
        )

    # ── private: writer ───────────────────────────────────────────────────────

    def _delete_existing_windows(self, window_starts: list) -> None:
        """Delete any prior rows for the window starts about to be written, so
        re-running the job for the same events is idempotent. A no-op
        (logged, not raised) on a cold start where the table doesn't exist yet.

        The psycopg2 connection's session timezone is set to match Spark's
        (spark.sql.session.timeZone) before the DELETE — see
        feat_customer_90d.py's `_delete_existing_snapshot` docstring for why:
        without it, the naive datetimes PySpark collects get compared against
        a different instant than Spark's JDBC writer actually stored, and the
        DELETE silently matches zero rows.
        """
        if not window_starts:
            return
        try:
            with closing(psycopg2.connect(
                host=self.postgres_host,
                port=self.postgres_port,
                dbname=self.postgres_db,
                user=self.postgres_user,
                password=self.postgres_password,
            )) as conn, conn, conn.cursor() as cur:
                cur.execute("SET TIME ZONE %s", (self.spark.conf.get("spark.sql.session.timeZone"),))
                cur.execute(
                    f"DELETE FROM {self.GOLD_SCHEMA}.{self.FEAT_TABLE} "
                    f"WHERE event_timestamp = ANY(%s)",
                    (window_starts,),
                )
        except psycopg2.errors.UndefinedTable:
            self.logger.info(
                "[%s] skip delete, table not yet created", self.FEAT_TABLE
            )

    def _write(self, df) -> int:
        """Append features to PostgreSQL feat_stream_60m.

        Idempotency: delete rows for the affected window starts before
        inserting (psycopg2 for the DELETE, then Spark JDBC for the INSERT —
        same split as feat_customer_90d.py, for the same reason: Spark's
        JDBC writer can't run a DELETE).
        """
        df = df.cache()
        window_starts = [r["event_timestamp"] for r in df.select("event_timestamp").distinct().collect()]
        self._delete_existing_windows(window_starts)

        count = df.count()
        (
            df.write.format("jdbc")
            .option("url", self.postgres_url)
            .option("dbtable", f"{self.GOLD_SCHEMA}.{self.FEAT_TABLE}")
            .option("user", self.postgres_user)
            .option("password", self.postgres_password)
            .option("driver", "org.postgresql.Driver")
            .mode("append")
            .save()
        )
        df.unpersist()
        return count


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
