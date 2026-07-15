"""
feat_customer_90d — rolling 90-day offline feature aggregations.

Reads from Gold tables in PostgreSQL, computes 4 features per customer
per snapshot date, and writes to feat_customer_90d (PostgreSQL) ready
for Feast ingestion.

Run:
    uv run python b_schema_pipelines/pipelines/features/feat_customer_90d.py
    uv run python b_schema_pipelines/pipelines/features/feat_customer_90d.py \
        --snapshot-date 2026-06-01
"""

from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime, timedelta

import psycopg2
from pyspark.sql import functions as F

from b_schema_pipelines.pipelines.pipeline_base import PipelineBase


class CustomerFeature90d(PipelineBase):
    """Computes rolling 90-day offline features per customer per snapshot date."""

    PREFIX      = "feat_90d"
    WINDOW_DAYS = 90
    FEAT_TABLE  = "feat_customer_90d"
    GOLD_SCHEMA = "gold_ecommerce"

    def __init__(
        self,
        postgres_url: str = "jdbc:postgresql://localhost:5432/fsds",
        snapshot_date: str | None = None,
    ):
        super().__init__()
        self.postgres_url = postgres_url
        self.snapshot_date = snapshot_date or datetime.now().strftime("%Y-%m-%d")
        self.window_start = (
            datetime.strptime(self.snapshot_date, "%Y-%m-%d")
            - timedelta(days=self.WINDOW_DAYS)
        ).strftime("%Y-%m-%d")

        pg_cfg = self.shared_cfg.get("postgres", {})
        self.postgres_host = pg_cfg.get("host", "localhost")
        self.postgres_port = pg_cfg.get("port", 5432)
        self.postgres_db = pg_cfg.get("db", "fsds")
        self.postgres_user = pg_cfg.get("user", "fsds")
        self.postgres_password = pg_cfg.get("password", "fsds")

        self.spark = self._build_spark()

    # ── public ────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Compute rolling 90-day features for `snapshot_date` and write them.

        Does not stop the Spark session — callers (Airflow task, backfill
        loop, or PipelineBase._SHARED_CONFIG-driven scheduler) may reuse this
        instance for another snapshot_date and are responsible for the
        session's lifecycle.
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

    def _read_gold(self, table: str):
        """Read a Gold table from PostgreSQL via JDBC."""
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
        """Compute 4 rolling 90-day features per customer.

        Window: fact_order rows whose order_date_key resolves (via dim_date)
        to a calendar_date in (window_start, snapshot_date]. Joining through
        dim_date rather than comparing order_date_key to a computed integer
        bound also enforces point-in-time correctness for free — an order
        dated after snapshot_date has no matching row in the window-filtered
        dim_date slice and is silently excluded by the inner join.
        """
        dim_customer = self._read_gold("dim_customer").filter(F.col("is_current"))
        dim_date = self._read_gold("dim_date")
        dim_product = self._read_gold("dim_product")
        fact_order = self._read_gold("fact_order")
        fact_order_item = self._read_gold("fact_order_item")
        fact_payment = self._read_gold("fact_payment_attempt")

        window_dates = (
            dim_date.filter(
                (F.col("calendar_date") > F.to_date(F.lit(self.window_start)))
                & (F.col("calendar_date") <= F.to_date(F.lit(self.snapshot_date)))
            )
            .select(F.col("date_key").alias("order_date_key"))
        )

        orders_in_window = fact_order.join(window_dates, on="order_date_key", how="inner")
        orders_slim = orders_in_window.select("order_key", "customer_key")

        order_stats = orders_in_window.groupBy("customer_key").agg(
            F.countDistinct("order_key").alias("f_customer_total_orders_90d"),
            F.avg("order_net_amount").alias("f_customer_avg_order_value_90d"),
        )

        categories = (
            orders_slim.join(fact_order_item, on="order_key", how="inner")
            .join(dim_product, on="product_key", how="inner")
            .groupBy("customer_key")
            .agg(F.countDistinct("category").alias("f_customer_distinct_categories_90d"))
        )

        payment_fail_rate = (
            orders_slim.join(fact_payment, on="order_key", how="inner")
            .groupBy("customer_key")
            .agg(
                (
                    F.sum(F.col("is_payment_failed").cast("int")) / F.count(F.lit(1))
                ).alias("f_customer_payment_fail_rate_90d")
            )
        )

        features = (
            dim_customer.select("customer_key", "customer_id")
            .join(order_stats, on="customer_key", how="left")
            .join(categories, on="customer_key", how="left")
            .join(payment_fail_rate, on="customer_key", how="left")
            .withColumn("event_timestamp", F.to_timestamp(F.lit(self.snapshot_date)))
            .withColumn("created_ts", F.current_timestamp())
            # A customer with zero orders in the window has no order_stats row —
            # counts/rate default to 0, but avg_order_value stays NULL (the
            # average of an empty set is undefined, not 0).
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
        )

        return features.select(
            "customer_id",
            "event_timestamp",
            "created_ts",
            "f_customer_total_orders_90d",
            "f_customer_avg_order_value_90d",
            "f_customer_distinct_categories_90d",
            "f_customer_payment_fail_rate_90d",
        )

    # ── private: writer ───────────────────────────────────────────────────────

    def _delete_existing_snapshot(self) -> None:
        """Delete any prior rows for `snapshot_date` so re-running the job is
        idempotent. A no-op (logged, not raised) on a cold start where the
        table doesn't exist yet — Spark's JDBC writer creates it on first
        write.

        The psycopg2 connection's session timezone is set to match Spark's
        (spark.sql.session.timeZone) before the DELETE. PySpark collects
        timestamps as naive datetimes in the Spark session's local timezone;
        without this, Postgres would interpret that naive value using its own
        connection-default timezone instead, comparing against a different
        instant than the one Spark's JDBC writer actually stored — silently
        matching zero rows and turning every re-run into a duplicate-append
        instead of a replace.
        """
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
                    f"WHERE event_timestamp = %s",
                    (self.snapshot_date,),
                )
        except psycopg2.errors.UndefinedTable:
            self.logger.info(
                "[%s] skip delete, table not yet created", self.FEAT_TABLE
            )

    def _write(self, df) -> int:
        """Append features to PostgreSQL feat_customer_90d.

        Idempotency: delete rows for self.snapshot_date before inserting
        (psycopg2 for the DELETE — Spark's JDBC writer can only append/
        overwrite a full table, not run a DML statement — then Spark JDBC
        for the INSERT).
        """
        self._delete_existing_snapshot()

        df = df.cache()
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
    parser.add_argument("--snapshot-date", default=None, help="YYYY-MM-DD")
    parser.add_argument("--postgres-url", default="jdbc:postgresql://localhost:5432/fsds")
    args = parser.parse_args()

    CustomerFeature90d(
        postgres_url=args.postgres_url,
        snapshot_date=args.snapshot_date,
    ).run()


if __name__ == "__main__":
    main()
