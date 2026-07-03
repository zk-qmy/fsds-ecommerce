"""
Silver transformation pipeline.

Reads from Bronze Delta Lake, applies four targeted fixes for the injected
data problems, and writes clean Silver Delta tables.

Two modes:
    baseline   — naive read/write, no fixes (run first, capture Spark UI)
    optimised  — all four fixes applied (run second, capture Spark UI)

Run:
    uv run python b_schema_pipelines/pipelines/silver/transform_silver.py --mode baseline
    uv run python b_schema_pipelines/pipelines/silver/transform_silver.py --mode optimised
"""

from __future__ import annotations

import argparse
from pathlib import Path

from b_schema_pipelines.pipelines.pipeline_base import PipelineBase

# TODO: from pyspark.sql import Window
# TODO: from pyspark.sql import functions as F


class SilverTransformer(PipelineBase):
    """Cleans and transforms Bronze Delta tables into Silver Delta tables."""

    PREFIX = "silver"

    def __init__(
        self,
        bronze_dir: str | Path = "b_schema_pipelines/outputs/bronze",
        silver_dir: str | Path = "b_schema_pipelines/outputs/silver",
        schema_change_date: str = "2026-03-24",
    ):
        super().__init__()
        self.bronze_dir = Path(bronze_dir)
        self.silver_dir = Path(silver_dir)
        self.schema_change_date = schema_change_date
        self.spark = self._build_spark()

    # ── public ────────────────────────────────────────────────────────────────

    def run(self, mode: str = "optimised") -> None:
        """
        TODO: Dispatch to _run_baseline or _run_optimised, then spark.stop().
        """
        raise NotImplementedError

    # ── private: spark ────────────────────────────────────────────────────────

    def _build_spark(self):
        """
        TODO: Silver only needs Delta — no JDBC.
        AQE skewJoin is already on in super()._build_spark() (Fix 1 — Problem A).
            return super()._build_spark()
        """
        raise NotImplementedError

    # ── private: readers / writers ────────────────────────────────────────────

    def _read_bronze(self, table: str):
        """
        TODO: self.spark.read.format("delta").load(str(self.bronze_dir / table))
        """
        raise NotImplementedError

    def _write_silver(self, df, table: str, merge_schema: bool = False) -> int:
        """
        TODO: Write to self.silver_dir / table, format="delta", mode="overwrite".
        Pass option("mergeSchema", "true") when merge_schema=True (Fix 2).
        Return output row count.
        """
        raise NotImplementedError

    # ── private: baseline ─────────────────────────────────────────────────────

    def _run_baseline(self) -> None:
        """
        TODO: Naive pass-through — no fixes.

        For each table in (orders, order_items, products, customers, payments):
            df = self._read_bronze(table)
            self._write_silver(df, table)
            self._log_run(table, ...)

        PURPOSE: Capture Spark UI "before" screenshots:
            Stages tab — skewed task durations on orders (Problem A: 85% HCMC)
            SQL tab    — SortMergeJoin for the products join (no broadcast hint)
        """
        raise NotImplementedError

    # ── private: fixes ────────────────────────────────────────────────────────

    def _fix_schema_evolution(self, orders_df):
        """
        TODO: Fill NULLs from Problem B (schema evolution).

            orders_df
                .withColumn("coupon_code",
                    F.when(F.col("coupon_code").isNull(), F.lit("LEGACY"))
                     .otherwise(F.col("coupon_code")))
                .withColumn("shipping_method",
                    F.when(F.col("shipping_method").isNull(), F.lit("UNKNOWN"))
                     .otherwise(F.col("shipping_method")))

        Write with merge_schema=True so Delta accepts old NULL partitions
        alongside new non-NULL partitions.
        """
        raise NotImplementedError

    def _fix_duplicates(self, order_items_df):
        """
        TODO: Dedup order_items on natural key, keep earliest created_ts (Problem C).

            window = Window
                .partitionBy("order_id", "product_id", "unit_price")
                .orderBy(F.col("created_ts").asc())

            order_items_df
                .withColumn("_rank", F.row_number().over(window))
                .filter(F.col("_rank") == 1)
                .drop("_rank")

        Evidence: before count = 909,000 → after count ~= 890,000.
        """
        raise NotImplementedError

    def _fix_broadcast_join(self, orders_df, products_df):
        """
        TODO: Replace SortMergeJoin with BroadcastHashJoin for products join (Problem A cardinality).

            from pyspark.sql.functions import broadcast
            orders_df.join(broadcast(products_df), on="product_id", how="left")

        products (45k rows, ~5 MB) fits in executor memory.
        Evidence: SQL plan exchange bytes drop to 0 after broadcast.
        """
        raise NotImplementedError

    # ── private: optimised run ────────────────────────────────────────────────

    def _run_optimised(self) -> None:
        """
        TODO: Apply all four fixes in sequence.

            orders_df      = self._read_bronze("orders")
            orders_df      = self._fix_schema_evolution(orders_df)       # Fix 2

            order_items_df = self._read_bronze("order_items")
            order_items_df = self._fix_duplicates(order_items_df)        # Fix 3

            products_df    = self._read_bronze("products")
            _              = self._fix_broadcast_join(orders_df, products_df)  # Fix 4

            # Fix 1 (AQE skewJoin) is passive — active via _build_spark config.

            self._write_silver(orders_df, "orders", merge_schema=True)
            self._write_silver(order_items_df, "order_items")
            self._write_silver(products_df, "products")

            for table in ("customers", "payments"):
                self._write_silver(self._read_bronze(table), table)

            self._log_run(...)
        """
        raise NotImplementedError


# ── entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["baseline", "optimised"], default="optimised")
    parser.add_argument("--schema-change-date", default="2026-03-24")
    args = parser.parse_args()

    SilverTransformer(schema_change_date=args.schema_change_date).run(mode=args.mode)


if __name__ == "__main__":
    main()
