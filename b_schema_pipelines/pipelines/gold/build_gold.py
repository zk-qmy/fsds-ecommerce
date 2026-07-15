"""
Gold layer builder.

Reads from Silver Delta Lake and writes all dim/fact/obt tables into
PostgreSQL under the `gold_ecommerce` schema.

Two surrogate-key modes (see gold/README.md for the Spark UI before/after):
    baseline   — global row_number() over a single unpartitioned Window
    optimized  — range-partitioned, parallel ranking (default)

Run:
    uv run python b_schema_pipelines/pipelines/gold/build_gold.py --mode baseline
    uv run python b_schema_pipelines/pipelines/gold/build_gold.py --mode optimized
"""

from __future__ import annotations

import argparse
from contextlib import closing
from datetime import datetime
from pathlib import Path

import psycopg2
from pyspark.sql import Window
from pyspark.sql import functions as F

from b_schema_pipelines.pipelines.pipeline_base import PipelineBase

# Static reference lookups — order fixes the surrogate key (index + 1).
ORDER_STATUSES = ["completed", "pending", "cancelled", "returned"]
PAYMENT_METHODS = ["credit_card", "bank_transfer", "e_wallet", "cod"]

SILVER_SOURCE_TABLES = ("orders", "order_items", "products", "customers", "payments")


class GoldBuilder(PipelineBase):
    """Builds all Gold dimension, fact, and OBT tables from Silver Delta."""

    PREFIX = "gold"

    def __init__(
        self,
        silver_dir: str | Path | None = None,
        postgres_url: str = "jdbc:postgresql://localhost:5432/fsds",
        schema: str = "gold_ecommerce",
        days_history: int = 180,
        mode: str = "optimized",
    ):
        super().__init__()
        if mode not in ("baseline", "optimized"):
            raise ValueError(f"Unknown mode: {mode!r}. Choose 'baseline' or 'optimized'.")
        self.silver_dir = silver_dir or self._resolve_s3_config("silver")[0]
        self.postgres_url = postgres_url
        self.schema = schema
        self.days_history = days_history
        self.mode = mode

        pg_cfg = self.shared_cfg.get("postgres", {})
        self.postgres_host = pg_cfg.get("host", "localhost")
        self.postgres_port = pg_cfg.get("port", 5432)
        self.postgres_db = pg_cfg.get("db", "fsds")
        self.postgres_user = pg_cfg.get("user", "fsds")
        self.postgres_password = pg_cfg.get("password", "fsds")

        self.spark = self._build_spark()
        self._order_key_map_cache = None
        self._product_key_map_cache = None

    # ── public ────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Build all Gold tables in dependency order, then stop Spark (even on failure)."""
        builders = (
            ("dim_date", self._build_dim_date),
            ("dim_payment_method", self._build_dim_payment_method),
            ("dim_order_status", self._build_dim_order_status),
            ("dim_product", self._build_dim_product),
            ("dim_customer", self._build_dim_customer),
            ("fact_order", self._build_fact_order),
            ("fact_order_item", self._build_fact_order_item),
            ("fact_payment_attempt", self._build_fact_payment),
            ("obt_order_performance", self._build_obt_order_performance),
        )
        try:
            for table, build_fn in builders:
                start_ts = datetime.now()
                self.log_table_start(table, source="silver" if table not in ("dim_date",) else "n/a")
                try:
                    rows_out = build_fn()
                    self.log_run(table, start_ts, datetime.now(), rows_out or 0, rows_out or 0, "ok")
                except Exception as exc:
                    self.log_run(table, start_ts, datetime.now(), 0, 0, "error", str(exc))
                    raise
            self._create_indexes()
        finally:
            self.spark.stop()

    # ── private: storage optimization ───────────────────────────────────────

    INDEX_STATEMENTS = (
        ("idx_fact_order_customer_key", "fact_order", "customer_key"),
        ("idx_fact_order_date_key", "fact_order", "order_date_key"),
        ("idx_fact_order_item_order_key", "fact_order_item", "order_key"),
        ("idx_fact_payment_order_key", "fact_payment_attempt", "order_key"),
        ("idx_dim_customer_bk", "dim_customer", "customer_id, is_current"),
    )

    def _create_indexes(self) -> None:
        """Postgres storage optimization — speeds up the join patterns Gold's
        own downstream consumers (the feature jobs, ad-hoc BI queries) use
        most: point lookups by customer_key/order_key, and dim_customer's
        SCD2 lookup by business key. Table/columns match
        docs/02_schema_piplines.md §8's indexing plan.

        Uses a raw psycopg2 connection since Spark's JDBC writer only
        appends/overwrites DataFrames — it can't run DDL.
        """
        with closing(psycopg2.connect(
            host=self.postgres_host,
            port=self.postgres_port,
            dbname=self.postgres_db,
            user=self.postgres_user,
            password=self.postgres_password,
        )) as conn, conn, conn.cursor() as cur:
            for name, table, columns in self.INDEX_STATEMENTS:
                cur.execute(
                    f"CREATE INDEX IF NOT EXISTS {name} "
                    f"ON {self.schema}.{table}({columns})"
                )
        self.logger.info(
            "[indexes] created/verified %d indexes on %s",
            len(self.INDEX_STATEMENTS), self.schema,
        )

    # private: spark

    def _build_spark(self):
        """Gold needs Delta + PostgreSQL JDBC — both are loaded via the shared
        `packages` list in pipeline_config.yaml at session creation time."""
        return self.spark

    # ── private: readers / writers ────────────────────────────────────────────

    def _table_path(self, table: str) -> str:
        if isinstance(self.silver_dir, Path):
            return str(self.silver_dir / table)
        return f"{self.silver_dir}/{table}"

    def _read_silver(self, table: str):
        return self.spark.read.format("delta").load(self._table_path(table))

    def _read_order_items(self):
        """order_items from Silver, with the source `discount` column renamed
        to `discount_amount` to match the Gold naming convention
        (`fact_order_item.discount_amount`). A no-op rename if the column is
        already named `discount_amount`.
        """
        return self._read_silver("order_items").withColumnRenamed("discount", "discount_amount")

    def _jdbc(self):
        return (
            self.spark.read.format("jdbc")
            .option("url", self.postgres_url)
            .option("user", self.postgres_user)
            .option("password", self.postgres_password)
            .option("driver", "org.postgresql.Driver")
        )

    def _read_postgres(self, table: str):
        """Read an existing Gold table from PostgreSQL via JDBC.

        Returns None if the table doesn't exist yet (first run) — callers
        must treat that as "no prior Gold history".
        """
        try:
            return self._jdbc().option("dbtable", f"{self.schema}.{table}").load()
        except Exception as exc:
            self.logger.info("[%s] no existing Gold table found (%s)", table, exc)
            return None

    def _write_postgres(self, df, table: str, mode: str = "overwrite") -> int:
        """Write DataFrame to PostgreSQL via JDBC. Returns output row count."""
        df = df.cache()
        count = df.count()
        (
            df.write.format("jdbc")
            .option("url", self.postgres_url)
            .option("dbtable", f"{self.schema}.{table}")
            .option("user", self.postgres_user)
            .option("password", self.postgres_password)
            .option("driver", "org.postgresql.Driver")
            .mode(mode)
            .save()
        )
        df.unpersist()
        return count

    # ── private: shared surrogate-key helpers ──────────────────────────────────

    def _assign_surrogate_keys(self, df, key_col: str, order_col: str, start: int = 1):
        """Deterministic surrogate key: rank ordered by a business key.

        Reproducible across independent calls over the same input (unlike
        F.monotonically_increasing_id(), which is partition-order dependent),
        so a dim's key assignment and a fact's lookup of that same dim agree.
        Dispatches to baseline or optimized implementation per `self.mode`
        (see gold/README.md for the Spark UI before/after comparison).
        """
        if self.mode == "baseline":
            return self._assign_surrogate_keys_baseline(df, key_col, order_col, start)
        return self._assign_surrogate_keys_optimized(df, key_col, order_col, start)

    def _assign_surrogate_keys_baseline(self, df, key_col: str, order_col: str, start: int = 1):
        """Global row_number() over a single unpartitioned Window.

        Correct and simple, but Spark must funnel the entire DataFrame through
        one task to compute it (WARN WindowExec: No Partition Defined) — every
        row is sorted on a single executor with no parallelism. Fine at small
        scale; a real bottleneck as tables grow. Kept only as the "before"
        side of the baseline/optimized comparison.
        """
        w = Window.orderBy(order_col)
        return df.withColumn(key_col, F.row_number().over(w) + F.lit(start - 1))

    def _assign_surrogate_keys_optimized(
        self, df, key_col: str, order_col: str, start: int = 1, num_partitions: int | None = None
    ):
        """Same deterministic, gap-free global ranking as the baseline, computed
        in parallel instead of on a single task.

        1. `repartitionByRange` splits rows into N partitions that are already
           globally ordered by `order_col` (every value in partition i sorts
           before every value in partition i + 1).
        2. Rank locally within each partition — this Window *is* partitioned,
           so Spark runs it as N parallel tasks instead of one.
        3. Add each partition's row count as a running offset, so local ranks
           become globally sequential. The offset step still uses an
           unpartitioned Window, but over one row per partition (tens, not
           hundreds of thousands) — negligible compared to sorting the full
           table on a single task.

        Requires `order_col` values to be unique (true for every caller here —
        business keys like order_id/product_id/customer_id, or order_item_id).
        """
        num_partitions = num_partitions or self.spark.sparkContext.defaultParallelism
        ranged = df.repartitionByRange(num_partitions, order_col).withColumn(
            "_pid", F.spark_partition_id()
        )

        local_rank_w = Window.partitionBy("_pid").orderBy(order_col)
        ranked = ranged.withColumn("_local_rank", F.row_number().over(local_rank_w))

        partition_counts = ranked.groupBy("_pid").agg(F.count(F.lit(1)).alias("_count"))
        offset_w = Window.orderBy("_pid").rowsBetween(Window.unboundedPreceding, -1)
        partition_offsets = partition_counts.withColumn(
            "_offset", F.coalesce(F.sum("_count").over(offset_w), F.lit(0))
        ).select("_pid", "_offset")

        return (
            ranked.join(F.broadcast(partition_offsets), on="_pid", how="left")
            .withColumn(key_col, F.col("_local_rank") + F.col("_offset") + F.lit(start - 1))
            .drop("_pid", "_local_rank", "_offset")
        )

    def _static_dim(self, values: list[str], business_col: str, key_col: str):
        rows = [(i + 1, v) for i, v in enumerate(values)]
        return self.spark.createDataFrame(rows, [key_col, business_col])

    def _current_dim_customer(self, fallback_source_df):
        """customer_id -> customer_key for currently-active customers.

        Prefers the persisted Gold dim_customer (keys must stay stable across
        SCD2 history); falls back to a fresh deterministic mapping derived
        from `fallback_source_df`'s own customer_id column when no Gold
        history exists yet (first run / cold start). The fallback is scoped
        to the caller's own input (e.g. orders) rather than a separate read
        of the full customers table, so a Gold build never depends on more
        Silver tables than the one it's actually building from.
        """
        existing = self._read_postgres("dim_customer")
        has_history = (
            existing is not None
            and {"customer_id", "customer_key", "is_current"}.issubset(set(existing.columns))
            and existing.count() > 0
        )
        if has_history:
            return existing.filter(F.col("is_current")).select("customer_id", "customer_key")
        return self._assign_surrogate_keys(
            fallback_source_df.select("customer_id").distinct(), "customer_key", "customer_id"
        )

    def _order_key_map(self):
        """Canonical order_id -> order_key mapping, built once per run from
        the full orders table and reused by every fact that references an
        order. Facts that instead ranked over their own (order_items' or
        payments') distinct order_id set would disagree on order_key
        whenever that set differs from orders' — e.g. a pending order with
        no items yet shifts every order_key ranked after it in one fact but
        not the other, silently breaking joins between facts.
        """
        if self._order_key_map_cache is None:
            orders = self._read_silver("orders")
            self._order_key_map_cache = self._assign_surrogate_keys(
                orders.select("order_id").distinct(), "order_key", "order_id"
            ).cache()
        return self._order_key_map_cache

    def _product_key_map(self):
        """Canonical product_id -> product_key mapping — same reasoning as
        `_order_key_map`, shared by dim_product and fact_order_item."""
        if self._product_key_map_cache is None:
            products = self._read_silver("products")
            self._product_key_map_cache = self._assign_surrogate_keys(
                products.select("product_id").distinct(), "product_key", "product_id"
            ).cache()
        return self._product_key_map_cache

    # ── private: dimensions ───────────────────────────────────────────────────

    def _build_dim_date(self) -> int:
        """Generate dim_date for the full simulation window ending today."""
        end_date = datetime.now().date()
        dates = (
            self.spark.range(self.days_history)
            .withColumn(
                "calendar_date",
                F.date_add(F.lit(end_date), (F.col("id") - (self.days_history - 1)).cast("int")),
            )
            .drop("id")
        )
        df = (
            dates.withColumn("date_key", F.date_format("calendar_date", "yyyyMMdd").cast("int"))
            .withColumn("day_of_week", F.date_format("calendar_date", "EEEE"))
            .withColumn("month", F.month("calendar_date"))
            .withColumn("year", F.year("calendar_date"))
            .withColumn("is_weekend", F.dayofweek("calendar_date").isin(1, 7))
            .select("date_key", "calendar_date", "day_of_week", "month", "year", "is_weekend")
        )
        return self._write_postgres(df, "dim_date", mode="overwrite")

    def _build_dim_payment_method(self) -> int:
        """Static lookup — credit_card, bank_transfer, e_wallet, cod."""
        df = self._static_dim(PAYMENT_METHODS, "payment_method", "payment_method_key")
        return self._write_postgres(df, "dim_payment_method", mode="overwrite")

    def _build_dim_order_status(self) -> int:
        """Static lookup — completed, pending, cancelled, returned."""
        df = self._static_dim(ORDER_STATUSES, "order_status", "order_status_key")
        return self._write_postgres(df, "dim_order_status", mode="overwrite")

    def _build_dim_product(self) -> int:
        """dim_product — no SCD, overwrite each run."""
        products = self._read_silver("products")
        df = self._product_key_map().join(products, on="product_id", how="inner").select(
            "product_key", "product_id", "category", "brand", "base_price", "is_active", "created_ts"
        )
        return self._write_postgres(df, "dim_product", mode="overwrite")

    def _build_dim_customer(self) -> int:
        """dim_customer — SCD Type 2."""
        FAR_FUTURE = "9999-12-31"
        now = F.current_timestamp()
        silver = self._read_silver("customers")
        existing = self._read_postgres("dim_customer")

        has_history = (
            existing is not None
            and {"customer_id", "customer_key", "is_current"}.issubset(set(existing.columns))
            and existing.count() > 0
        )

        if not has_history:
            out = (
                self._assign_surrogate_keys(silver, "customer_key", "customer_id")
                .withColumn("valid_from_ts", now)
                .withColumn("valid_to_ts", F.lit(FAR_FUTURE).cast("timestamp"))
                .withColumn("is_current", F.lit(True))
                .select(
                    "customer_key", "customer_id", "signup_ts", "segment", "country",
                    "marketing_opt_in", "valid_from_ts", "valid_to_ts", "is_current",
                )
            )
            return self._write_postgres(out, "dim_customer", mode="append")

        current = existing.filter(F.col("is_current"))
        joined = silver.alias("s").join(current.alias("c"), on="customer_id", how="left")

        # eqNullSafe (NULL <=> NULL) so a field moving to/from NULL still
        # counts as a change — plain != returns NULL (dropped by filter())
        # when either side is NULL, which would hide that transition.
        changed_cond = (
            F.col("c.customer_key").isNull()
            | ~F.col("s.segment").eqNullSafe(F.col("c.segment"))
            | ~F.col("s.country").eqNullSafe(F.col("c.country"))
            | ~F.col("s.marketing_opt_in").eqNullSafe(F.col("c.marketing_opt_in"))
        )
        changed = joined.filter(changed_cond)

        to_close = (
            current.join(changed.select("customer_id"), on="customer_id", how="inner")
            .withColumn("is_current", F.lit(False))
            .withColumn("valid_to_ts", now)
        )

        max_key = existing.agg(F.max("customer_key")).first()[0] or 0
        to_insert_source = changed.select(
            F.col("s.customer_id").alias("customer_id"),
            F.col("s.signup_ts").alias("signup_ts"),
            F.col("s.segment").alias("segment"),
            F.col("s.country").alias("country"),
            F.col("s.marketing_opt_in").alias("marketing_opt_in"),
        )
        new_rows = (
            self._assign_surrogate_keys(
                to_insert_source, "customer_key", "customer_id", start=max_key + 1
            )
            .withColumn("valid_from_ts", now)
            .withColumn("valid_to_ts", F.lit(FAR_FUTURE).cast("timestamp"))
            .withColumn("is_current", F.lit(True))
        )

        cols = [
            "customer_key", "customer_id", "signup_ts", "segment", "country",
            "marketing_opt_in", "valid_from_ts", "valid_to_ts", "is_current",
        ]
        out = to_close.select(*cols).unionByName(new_rows.select(*cols))
        return self._write_postgres(out, "dim_customer", mode="append")

    # ── private: facts ────────────────────────────────────────────────────────

    def _build_fact_order(self) -> int:
        """fact_order — one row per order."""
        orders = self._read_silver("orders")
        items = self._read_order_items()

        item_agg = items.groupBy("order_id").agg(
            F.sum(F.col("unit_price") * F.col("quantity")).alias("order_gross_amount"),
            F.sum("discount_amount").alias("order_discount_amount"),
            F.count(F.lit(1)).alias("item_count"),
        )
        customer_map = self._current_dim_customer(orders)
        status_map = self._static_dim(ORDER_STATUSES, "order_status", "order_status_key")

        df = (
            orders.join(item_agg, on="order_id", how="left")
            .join(customer_map, on="customer_id", how="left")
            .join(self._order_key_map(), on="order_id", how="left")
            .join(
                status_map,
                orders["status"] == status_map["order_status"],
                how="left",
            )
            .withColumn("order_date_key", F.date_format("order_timestamp", "yyyyMMdd").cast("int"))
            .withColumn("order_gross_amount", F.coalesce("order_gross_amount", F.lit(0.0)))
            .withColumn("order_discount_amount", F.coalesce("order_discount_amount", F.lit(0.0)))
            .withColumn("item_count", F.coalesce("item_count", F.lit(0)))
            .withColumn(
                "order_net_amount",
                F.col("order_gross_amount") - F.col("order_discount_amount"),
            )
            .select(
                "order_key", "customer_key", "order_date_key", "order_status_key",
                "order_id", "order_gross_amount", "order_discount_amount",
                "order_net_amount", "item_count",
            )
        )
        return self._write_postgres(df, "fact_order", mode="overwrite")

    def _build_fact_order_item(self) -> int:
        """fact_order_item — one row per line item.

        product_key/order_key come from the shared `_product_key_map`/
        `_order_key_map` (same mapping dim_product and fact_order use) so
        they agree across facts, rather than being re-ranked over just the
        product/order ids that happen to appear in order_items.
        """
        items = self._read_order_items()

        df = (
            items.join(self._product_key_map(), on="product_id", how="left")
            .join(self._order_key_map(), on="order_id", how="left")
            .withColumn(
                "line_net_amount",
                F.col("unit_price") * F.col("quantity") - F.col("discount_amount"),
            )
        )
        df = self._assign_surrogate_keys(df, "order_item_key", "order_item_id").select(
            "order_item_key", "order_key", "product_key",
            "quantity", "unit_price", "discount_amount", "line_net_amount",
        )
        return self._write_postgres(df, "fact_order_item", mode="overwrite")

    def _build_fact_payment(self) -> int:
        """fact_payment_attempt — one row per payment."""
        payments = self._read_silver("payments")
        method_map = self._static_dim(PAYMENT_METHODS, "payment_method", "payment_method_key")

        df = (
            payments.join(self._order_key_map(), on="order_id", how="left")
            .join(method_map, on="payment_method", how="left")
            .withColumn(
                "payment_date_key", F.date_format("payment_timestamp", "yyyyMMdd").cast("int")
            )
            .withColumn("is_payment_success", F.col("payment_status") == F.lit("paid"))
            .withColumn("is_payment_failed", F.col("payment_status") == F.lit("failed"))
        )
        df = self._assign_surrogate_keys(df, "payment_key", "payment_id").select(
            "payment_key", "order_key", "payment_date_key", "payment_method_key",
            "amount", "is_payment_success", "is_payment_failed",
        )
        return self._write_postgres(df, "fact_payment_attempt", mode="overwrite")

    # ── private: OBT ─────────────────────────────────────────────────────────

    def _build_obt_order_performance(self) -> int:
        """obt_order_performance — denormalized wide table for BI."""
        orders = self._read_silver("orders")
        items = self._read_order_items()
        customers = self._read_silver("customers").select("customer_id", "country", "segment")
        payments = self._read_silver("payments")

        item_agg = items.groupBy("order_id").agg(
            F.sum("quantity").alias("total_quantity"),
            F.sum(
                F.col("unit_price") * F.col("quantity") - F.col("discount_amount")
            ).alias("order_net_amount"),
        )

        payment_rank = Window.partitionBy("order_id").orderBy(F.col("payment_timestamp").desc())
        last_payment = (
            payments.withColumn("_rank", F.row_number().over(payment_rank))
            .filter(F.col("_rank") == 1)
            .select("order_id", F.col("payment_status").alias("payment_status_last"))
        )

        df = (
            orders.join(item_agg, on="order_id", how="left")
            .join(customers, on="customer_id", how="left")
            .join(last_payment, on="order_id", how="left")
            .select(
                "order_id", "customer_id", "order_timestamp", "country", "segment",
                "total_quantity", "order_net_amount", "payment_status_last",
                "shipping_city", "coupon_code",
            )
        )
        return self._write_postgres(df, "obt_order_performance", mode="overwrite")


# ── entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["baseline", "optimized"], default="optimized")
    args = parser.parse_args()

    GoldBuilder(mode=args.mode).run()


if __name__ == "__main__":
    main()
