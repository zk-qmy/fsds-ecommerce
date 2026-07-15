"""
feat_customer_unified — point-in-time join of feat_customer_90d + feat_stream_60m.

The two upstream feature tables are on different grains: feat_customer_90d has
one row per customer per DAY (event_timestamp = midnight snapshot date),
feat_stream_60m has one row per customer per 60-MINUTE WINDOW (event_timestamp
= window start, any time of day). A plain equi-join on (customer_id,
event_timestamp) would only ever match when a stream window happens to start
at exactly midnight on a snapshot day — effectively never.

This job instead does an as-of join: the output grain follows feat_stream_60m
(the finer-grained table), and for each stream row it attaches the *latest*
feat_customer_90d snapshot at or before that window's timestamp per customer —
never a snapshot taken after it (point-in-time correctness, CLAUDE.md).

Run:
    uv run python b_schema_pipelines/pipelines/features/feat_customer_unified.py
"""

from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime

import psycopg2
from pyspark.sql import Window
from pyspark.sql import functions as F

from b_schema_pipelines.pipelines.pipeline_base import PipelineBase


class CustomerFeatureUnified(PipelineBase):
    """Joins feat_customer_90d onto feat_stream_60m via an as-of (point-in-time) join."""

    PREFIX      = "feat_unified"
    FEAT_TABLE  = "feat_customer_unified"
    GOLD_SCHEMA = "gold_ecommerce"

    def __init__(self, postgres_url: str = "jdbc:postgresql://localhost:5432/fsds"):
        super().__init__()
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
        """Compute the unified feature table and write it.

        Does not stop the Spark session — same reasoning as the other two
        feature jobs: the caller owns the session's lifecycle.
        """
        start_ts = datetime.now()
        try:
            df = self._compute_features()
            rows_out = self._write(df)
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

    def _read_feature_table(self, table: str):
        """Read a feature table (feat_customer_90d or feat_stream_60m) from
        PostgreSQL via JDBC — both live in GOLD_SCHEMA, same as the Gold
        dim/fact tables."""
        return (
            self.spark.read.format("jdbc")
            .option("url", self.postgres_url)
            .option("dbtable", f"{self.GOLD_SCHEMA}.{table}")
            .option("user", self.postgres_user)
            .option("password", self.postgres_password)
            .option("driver", "org.postgresql.Driver")
            .load()
        )

    # ── private: feature computation ──────────────────────────────────────────

    def _compute_features(self):
        """As-of join: for each feat_stream_60m row, attach the latest
        feat_customer_90d row at or before that row's event_timestamp, per
        customer.

        A stream row with no applicable offline snapshot yet (new customer,
        or a window before the customer's first snapshot) gets NULL from the
        left join. Counts/rate default to 0 (no offline history = zero
        activity); avg_order_value stays NULL (undefined, not zero) — same
        rule feat_customer_90d.py already applies to its own zero-order
        customers.
        """
        offline = self._read_feature_table("feat_customer_90d")
        stream = self._read_feature_table("feat_stream_60m")

        offline_asof = (
            offline
            .withColumnRenamed("customer_id", "_o_customer_id")
            .withColumnRenamed("event_timestamp", "_o_event_timestamp")
            .drop("created_ts")
        )

        joined = stream.join(
            offline_asof,
            (stream["customer_id"] == offline_asof["_o_customer_id"])
            & (offline_asof["_o_event_timestamp"] <= stream["event_timestamp"]),
            how="left",
        )

        # Latest applicable snapshot per stream row. Real matches always
        # outrank the join's NULL padding in a DESC ordering (Spark sorts
        # NULLS LAST by default for DESC), so rank=1 is the true latest
        # snapshot when one exists, or the single NULL-padded row when none does.
        latest_rank = Window.partitionBy("customer_id", "event_timestamp").orderBy(
            F.col("_o_event_timestamp").desc()
        )
        latest_offline = (
            joined.withColumn("_rank", F.row_number().over(latest_rank))
            .filter(F.col("_rank") == 1)
            .drop("_rank", "_o_customer_id", "_o_event_timestamp")
        )

        return (
            latest_offline
            .withColumn(
                "f_customer_total_orders_90d",
                F.coalesce(F.col("f_customer_total_orders_90d"), F.lit(0)),
            )
            .withColumn(
                "f_customer_distinct_categories_90d",
                F.coalesce(F.col("f_customer_distinct_categories_90d"), F.lit(0)),
            )
            .withColumn(
                "f_customer_payment_fail_rate_90d",
                F.coalesce(F.col("f_customer_payment_fail_rate_90d"), F.lit(0.0)),
            )
            .withColumn("created_ts", F.current_timestamp())
            .select(
                "customer_id",
                "event_timestamp",
                "created_ts",
                "f_customer_total_orders_90d",
                "f_customer_avg_order_value_90d",
                "f_customer_distinct_categories_90d",
                "f_customer_payment_fail_rate_90d",
                "f_stream_views_30m",
                "f_stream_add_to_cart_30m",
                "f_stream_cart_to_purchase_ratio_60m",
                "f_stream_burst_activity_flag",
            )
        )

    # ── private: writer ───────────────────────────────────────────────────────

    def _delete_existing_windows(self, window_starts: list) -> None:
        """Delete any prior rows for the window starts about to be written, so
        re-running the job is idempotent. A no-op (logged, not raised) on a
        cold start where the table doesn't exist yet.

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
        """Append features to PostgreSQL feat_customer_unified.

        Idempotency: same delete-then-insert split as feat_stream_60m.py,
        keyed on the window starts present in this batch.
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
    parser.add_argument("--postgres-url", default="jdbc:postgresql://localhost:5432/fsds")
    args = parser.parse_args()

    CustomerFeatureUnified(postgres_url=args.postgres_url).run()


if __name__ == "__main__":
    main()
