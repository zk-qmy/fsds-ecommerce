"""
feat_customer_90d — rolling 90-day offline feature aggregations.

Reads from Gold tables in PostgreSQL, computes 4 features per customer
per snapshot date, and writes to feat_customer_90d (PostgreSQL) ready
for Feast ingestion.

Run:
    uv run python b_schema_pipelines/pipelines/features/feature_customer_90d.py
    uv run python b_schema_pipelines/pipelines/features/feature_customer_90d.py \
        --snapshot-date 2026-06-01
"""

from __future__ import annotations

import argparse
from datetime import datetime, timedelta

from b_schema_pipelines.pipelines.pipeline_base import PipelineBase

# TODO: from pyspark.sql import functions as F


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
        self.spark = self._build_spark()

    # ── public ────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        TODO:
            df = self._compute_features()
            self._write(df)
            self._log_run(...)
            self.spark.stop()
        """
        raise NotImplementedError

    # ── private: spark ────────────────────────────────────────────────────────

    def _build_spark(self):
        """
        TODO: Needs Delta + PostgreSQL JDBC jar (same as GoldBuilder).

            spark = super()._build_spark()
            spark.conf.set("spark.jars", "/path/to/postgresql-42.x.x.jar")
            return spark
        """
        raise NotImplementedError

    # ── private: reader ───────────────────────────────────────────────────────

    def _read_gold(self, table: str):
        """
        TODO: Read a Gold table from PostgreSQL via JDBC.

            self.spark.read.format("jdbc")
                .option("url", self.postgres_url)
                .option("dbtable", f"{self.GOLD_SCHEMA}.{table}")
                ...
                .load()
        """
        raise NotImplementedError

    # ── private: feature computation ──────────────────────────────────────────

    def _compute_features(self):
        """
        TODO: Compute 4 rolling 90-day features per customer.

        Window: rows where calendar_date in (self.window_start, self.snapshot_date]

        Inputs (via _read_gold):
            fact_order           — order_key, customer_key, order_date_key, order_net_amount
            fact_order_item      — order_key, product_key
            dim_product          — product_key, category
            fact_payment_attempt — order_key, is_payment_failed
            dim_customer         — customer_key, customer_id (BK), is_current=True
            dim_date             — date_key, calendar_date

        Features:
            f_customer_total_orders_90d
                COUNT(DISTINCT order_key) per customer in window

            f_customer_avg_order_value_90d
                AVG(order_net_amount) per customer in window

            f_customer_distinct_categories_90d
                COUNT(DISTINCT category) per customer in window
                Join: fact_order → fact_order_item → dim_product

            f_customer_payment_fail_rate_90d
                SUM(is_payment_failed) / COUNT(*) per customer in window
                Join: fact_order → fact_payment_attempt

        Output schema:
            customer_id                         STRING
            event_timestamp                     TIMESTAMP  (= self.snapshot_date)
            created_ts                          TIMESTAMP  (= F.current_timestamp())
            f_customer_total_orders_90d         LONG
            f_customer_avg_order_value_90d      DOUBLE
            f_customer_distinct_categories_90d  LONG
            f_customer_payment_fail_rate_90d    DOUBLE
        """
        raise NotImplementedError

    # ── private: writer ───────────────────────────────────────────────────────

    def _write(self, df) -> int:
        """
        TODO: Append features to PostgreSQL feat_customer_90d.

        Idempotency: delete rows for self.snapshot_date before inserting
        (use psycopg2 for DELETE, then Spark JDBC for INSERT).
        Return output row count.
        """
        raise NotImplementedError


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
