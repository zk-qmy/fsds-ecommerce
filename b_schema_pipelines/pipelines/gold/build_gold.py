"""
Gold layer builder.

Reads from Silver Delta Lake and writes all dim/fact/obt tables into
PostgreSQL under the `gold_ecommerce` schema.

Run:
    uv run python b_schema_pipelines/pipelines/gold/build_gold.py
"""

from __future__ import annotations

from pathlib import Path

from b_schema_pipelines.pipelines.pipeline_base import PipelineBase

# TODO: from pyspark.sql import Window
# TODO: from pyspark.sql import functions as F


class GoldBuilder(PipelineBase):
    """Builds all Gold dimension, fact, and OBT tables from Silver Delta."""

    PREFIX = "gold"

    def __init__(
        self,
        silver_dir: str | Path = "b_schema_pipelines/outputs/silver",
        postgres_url: str = "jdbc:postgresql://localhost:5432/fsds",
        schema: str = "gold_ecommerce",
    ):
        super().__init__()
        self.silver_dir = Path(silver_dir)
        self.postgres_url = postgres_url
        self.schema = schema
        self.spark = self._build_spark()

    # ── public ────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """
        TODO: Build all Gold tables in dependency order, then spark.stop().

            1. self._build_dim_date()
            2. self._build_dim_payment_method()
            3. self._build_dim_order_status()
            4. self._build_dim_product()
            5. self._build_dim_customer()           <- SCD2
            6. self._build_fact_order()             <- needs all dims
            7. self._build_fact_order_item()        <- needs fact_order + dim_product
            8. self._build_fact_payment()           <- needs fact_order + dim_payment_method
            9. self._build_obt_order_performance()
        """
        raise NotImplementedError

    # ── private: spark ────────────────────────────────────────────────────────

    def _build_spark(self):
        """
        TODO: Gold needs Delta + PostgreSQL JDBC jar.

            spark = super()._build_spark()
            spark.conf.set("spark.jars", "/path/to/postgresql-42.x.x.jar")
            return spark
        """
        raise NotImplementedError

    # ── private: readers / writers ────────────────────────────────────────────

    def _read_silver(self, table: str):
        """
        TODO: self.spark.read.format("delta").load(str(self.silver_dir / table))
        """
        raise NotImplementedError

    def _read_postgres(self, table: str):
        """
        TODO: Read an existing Gold table from PostgreSQL via JDBC.
        Used by _build_dim_customer to load current SCD2 state.
        """
        raise NotImplementedError

    def _write_postgres(self, df, table: str, mode: str = "overwrite") -> int:
        """
        TODO: Write DataFrame to PostgreSQL via JDBC.

            df.write.format("jdbc")
              .option("url", self.postgres_url)
              .option("dbtable", f"{self.schema}.{table}")
              .option("user", ...).option("password", ...)
              .mode(mode).save()

        Return output row count.
        """
        raise NotImplementedError

    # ── private: dimensions ───────────────────────────────────────────────────

    def _build_dim_date(self) -> None:
        """
        TODO: Generate dim_date for the full 180-day simulation window.

        Columns: date_key (YYYYMMDD int), calendar_date, day_of_week,
                 month, year, is_weekend

        No source table — generate with spark.range() over date range,
        derive columns with F.date_format / F.dayofweek / F.month / F.year.
        Write with mode="overwrite".
        """
        raise NotImplementedError

    def _build_dim_payment_method(self) -> None:
        """
        TODO: Static lookup — credit_card, bank_transfer, e_wallet, cod.

        Columns: payment_method_key (surrogate int), payment_method (name)
        Build from hardcoded list. Write with mode="overwrite".
        """
        raise NotImplementedError

    def _build_dim_order_status(self) -> None:
        """
        TODO: Static lookup — completed, pending, cancelled, returned.

        Columns: order_status_key (surrogate int), order_status (name)
        Build from hardcoded list. Write with mode="overwrite".
        """
        raise NotImplementedError

    def _build_dim_product(self) -> None:
        """
        TODO: dim_product — no SCD, overwrite each run.

        Columns: product_key (surrogate), product_id (BK), category,
                 brand, base_price, is_active, created_ts

        Read silver products, add surrogate via F.monotonically_increasing_id().
        Write with mode="overwrite".
        """
        raise NotImplementedError

    def _build_dim_customer(self) -> None:
        """
        TODO: dim_customer — SCD Type 2.

        Columns: customer_key (surrogate), customer_id (BK), signup_ts,
                 segment, country, marketing_opt_in,
                 valid_from_ts, valid_to_ts, is_current

        SCD2 logic:
            1. self._read_postgres("dim_customer") — empty on first run.
            2. Read silver customers.
            3. Detect changed rows (segment / country / marketing_opt_in changed).
            4. Close old records: is_current=False, valid_to_ts=now().
            5. Insert new records: is_current=True, valid_to_ts="9999-12-31".
            6. self._write_postgres(..., mode="append") — never overwrite history.
        """
        raise NotImplementedError

    # ── private: facts ────────────────────────────────────────────────────────

    def _build_fact_order(self) -> None:
        """
        TODO: fact_order — one row per order.

        Columns: order_key, customer_key, order_date_key, order_status_key,
                 order_id, order_gross_amount, order_discount_amount,
                 order_net_amount, item_count

        Joins:
            silver orders → dim_customer (customer_id, is_current=True)
            silver orders → dim_date     (date(order_timestamp) = calendar_date)
            silver orders → dim_order_status (status)
            silver order_items agg → gross / discount / net / count per order_id
        """
        raise NotImplementedError

    def _build_fact_order_item(self) -> None:
        """
        TODO: fact_order_item — one row per line item.

        Columns: order_item_key, order_key, product_key, quantity,
                 unit_price, discount_amount, line_net_amount

        line_net_amount = (unit_price * quantity) - discount_amount
        Joins: silver order_items → fact_order (order_id) → dim_product (product_id)
        """
        raise NotImplementedError

    def _build_fact_payment(self) -> None:
        """
        TODO: fact_payment_attempt — one row per payment.

        Columns: payment_key, order_key, payment_date_key, payment_method_key,
                 amount, is_payment_success, is_payment_failed

        Derive boolean flags from payment_status.
        Joins: silver payments → fact_order (order_id) → dim_payment_method
        """
        raise NotImplementedError

    # ── private: OBT ─────────────────────────────────────────────────────────

    def _build_obt_order_performance(self) -> None:
        """
        TODO: obt_order_performance — denormalized wide table for BI.

        Columns: order_id, customer_id, order_timestamp, country, segment,
                 total_quantity, order_net_amount, payment_status_last,
                 shipping_city, coupon_code

        Single wide join — BK columns only, no surrogate keys.
        """
        raise NotImplementedError


# ── entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    GoldBuilder().run()


if __name__ == "__main__":
    main()
