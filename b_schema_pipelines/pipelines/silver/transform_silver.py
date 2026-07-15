"""
Silver transformation pipeline.

Reads from Bronze Delta Lake, applies four targeted fixes for the injected
data problems, and writes clean Silver Delta tables.

Two modes:
    baseline   — naive read/write, no fixes (run first, capture Spark UI)
    optimized  — all four fixes applied (run second, capture Spark UI)

Run:
    uv run python b_schema_pipelines/pipelines/silver/transform_silver.py --mode baseline
    uv run python b_schema_pipelines/pipelines/silver/transform_silver.py --mode optimized
"""

from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast

from b_schema_pipelines.pipelines.common.delta_writer import DeltaWriter
from b_schema_pipelines.pipelines.pipeline_base import PipelineBase

SILVER_TABLES = ("orders", "order_items", "products", "customers", "payments")


class SilverTransformer(PipelineBase):
    """Cleans and transforms Bronze Delta tables into Silver Delta tables."""

    PREFIX = "silver"

    def __init__(
        self,
        bronze_dir: str | None = None,
        silver_dir: str | None = None,
        schema_change_date: str = "2026-03-24",
    ):
        super().__init__()
        self.bronze_dir = bronze_dir or self._resolve_s3_config("bronze")[0]
        self.silver_dir = silver_dir or self._resolve_s3_config("silver")[0]
        self.schema_change_date = schema_change_date
        self.spark = self._build_spark()
        self.writer = DeltaWriter(spark=self.spark)

    # public

    def run(self, mode: str = "optimized") -> None:
        """Dispatch to baseline or optimized pipeline."""
        if mode == "baseline":
            self._run_baseline()
        elif mode == "optimized":
            self._run_optimized()
        else:
            raise ValueError(
                f"Unknown mode: {mode!r}. Choose 'baseline' or 'optimized'."
            )

    # private: spark

    def _build_spark(self):
        # Fix 1 (AQE skew join, Problem A) is mode-dependent, not session-wide —
        # set explicitly in _run_baseline (disabled) / _run_optimized (enabled)
        # so the two modes actually differ in the Spark UI instead of both
        # silently getting Spark's own default (on since 3.2).
        return self.spark

    # ── private: readers / writers

    def _read_bronze(self, table: str):
        return self.spark.read.format(
            "delta"
            ).load(f"{self.bronze_dir}/{table}")

    def _write_silver(self, df, table: str, merge_schema: bool = False) -> int:
        """Write df to Silver Delta table and return the row count.

        Caches df so count() and save() share one materialized scan instead of two.
        """
        df = df.cache()
        count = df.count()
        self.writer.write(
            df,
            f"{self.silver_dir}/{table}",
            table,
            mode="overwrite",
            row_count=count,
            merge_schema=merge_schema,
        )
        df.unpersist()
        return count

    # private: baseline

    def _run_baseline(self) -> None:
        """Naive pass-through — no fixes.

        PURPOSE: Capture Spark UI "before" screenshots:
            Stages tab — skewed task durations on orders (Problem A: 85% HCMC)
            SQL tab    — SortMergeJoin for the products join (no broadcast hint)

        Explicitly disables AQE skew-join handling — Spark 3.2+ enables it by
        default, which would silently fix the skew and hide the "before"
        symptom this mode exists to capture.
        """
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "false")
        for table in SILVER_TABLES:
            start_ts = datetime.now()
            df = self._read_bronze(table)
            rows_out = self._write_silver(df, table)
            self.log_run(
                table, start_ts, datetime.now(),
                rows_out, rows_out, "ok"
            )

    # private: fixes

    def _fix_schema_evolution(self, orders_df):
        """Fix Problem B — fill NULLs in orders from before schema_change_date."""
        return orders_df.withColumn(
            "coupon_code",
            F.when(F.col("coupon_code").isNull(), F.lit("LEGACY")).otherwise(
                F.col("coupon_code")
            ),
        ).withColumn(
            "shipping_method",
            F.when(F.col("shipping_method").isNull(),
                   F.lit("UNKNOWN")).otherwise(
                F.col("shipping_method")
            ),
        )

    def _fix_duplicates(self, order_items_df):
        """Fix Problem C — dedup order_items on natural key, keep one row per group.

        The generator injects exact-copy duplicates (no created_ts in order_items);
        ingest_ts is constant per batch so order_item_id is the stable tiebreaker.
        Evidence: before count ~909,000 → after count ~890,000.
        """
        window = Window.partitionBy("order_id", "product_id", "unit_price").orderBy(
            F.col("ingest_ts").asc(), F.col("order_item_id").asc()
        )
        return (
            order_items_df.withColumn("_rank", F.row_number().over(window))
            .filter(F.col("_rank") == 1)
            .drop("_rank")
        )

    def _fix_broadcast_join(self, orders_df, products_df):
        """Fix Problem A (cardinality) — replace SortMergeJoin with BroadcastHashJoin.

        products (~45k rows, ~5 MB) fits in executor memory.
        Call standalone to capture Spark UI SQL tab for Fix 4 evidence:
            exchange bytes drop to 0 after broadcast hint is applied.
        """
        return orders_df.join(broadcast(products_df), on="product_id", how="left")

    # private: optimized run

    def _run_optimized(self) -> None:
        """Apply all four fixes in sequence."""
        # Fix 1 — AQE skew join handles Problem A (85% Ho Chi Minh City partition
        # skew). Set explicitly (not relied upon as Spark's default) so this
        # mode is verifiably different from _run_baseline's explicit disable.
        self.spark.conf.set("spark.sql.adaptive.enabled", "true")
        self.spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

        # Fix 2 — schema evolution: fill NULL coupon_code / shipping_method.
        # Cache Bronze so rows_in count, fix transform, and _write_silver all
        # share one S3A scan instead of three.
        orders_start = datetime.now()
        orders_bronze = self._read_bronze("orders").cache()
        orders_rows_in = orders_bronze.count()
        orders_df = self._fix_schema_evolution(orders_bronze)
        rows_out = self._write_silver(orders_df, "orders", merge_schema=True)
        orders_bronze.unpersist()
        self.log_run(
            "orders", orders_start, datetime.now(), orders_rows_in, rows_out, "ok"
        )

        # Fix 3 — dedup: remove duplicate order_items, keep earliest ingest_ts.
        # Same caching pattern: one S3A scan covers rows_in, window dedup, and write.
        items_start = datetime.now()
        items_bronze = self._read_bronze("order_items").cache()
        items_rows_in = items_bronze.count()
        order_items_df = self._fix_duplicates(items_bronze)
        rows_out = self._write_silver(order_items_df, "order_items")
        items_bronze.unpersist()
        self.log_run(
            "order_items", items_start, datetime.now(), items_rows_in, rows_out, "ok"
        )

        # Fix 1 (AQE skewJoin) is passive — enabled via the spark.conf.set calls
        # at the top of this method, applies to every stage below/above it.
        # Fix 4 (_fix_broadcast_join) is a Spark-plan demonstration — call it standalone
        # against order_items + products to capture Spark UI SQL tab screenshot.

        for table in ("products", "customers", "payments"):
            start_ts = datetime.now()
            df = self._read_bronze(table)
            rows_out = self._write_silver(df, table)
            self.log_run(table, start_ts, datetime.now(), rows_out, rows_out, "ok")


# entrypoint


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--mode", choices=["baseline", "optimized"], default="optimized"
    )
    parser.add_argument("--schema-change-date", default="2026-03-24")
    args = parser.parse_args()

    SilverTransformer(schema_change_date=args.schema_change_date).run(mode=args.mode)


if __name__ == "__main__":
    main()
