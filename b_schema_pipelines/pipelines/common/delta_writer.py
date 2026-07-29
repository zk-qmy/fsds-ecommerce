"""
Shared Delta Lake read/write utilities used by Bronze, Silver, and Gold pipelines.
"""

from __future__ import annotations

from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import functions as F
from config.logging import setup_logger


class DeltaWriter:
    """Writes DataFrames to Delta Lake via PySpark's Delta connector.

    All metadata reads (table_exists, history, version) go through the same
    SparkSession, so S3A credentials configured in _create_spark_session()
    cover both reads and writes uniformly.
    """

    def __init__(self, spark) -> None:
        self.spark = spark
        self.logger = setup_logger(
            name="DeltaWriter",
            filename="DeltaWriter.log"
        )

    def write(
        self,
        df,
        output_path: str,
        table: str,
        mode: str = "overwrite",
        row_count: int = 0,
        merge_schema: bool = True,
    ) -> int:
        """Write df to a Delta table. Returns the number of rows written.

        Args:
            df:           Spark DataFrame to write.
            output_path:  Destination path (local or s3a://).
            table:        Table name used in log messages.
            mode:         "overwrite" (Silver/Gold) or "append" (Bronze).
            row_count:    Pre-counted rows — avoids an extra df.count() call.
            merge_schema: Pass mergeSchema=true to Delta (safe default; required
                          for Bronze schema-evolution writes and Silver Fix 2).
        """
        if self.table_exists(output_path):
            self.logger.info("[%s] existing table detected — will %s", table, mode)

        t0 = datetime.now()
        self.logger.info("[%s] writing %d rows → %s", table, row_count, output_path)
        try:
            writer = df.write.format("delta").mode(mode)
            if merge_schema:
                writer = writer.option("mergeSchema", "true")
            writer.save(output_path)
        except Exception as exc:
            self.logger.error("[%s] write failed: %s", table, exc)
            raise

        # Single history(1) call covers both version and commit metrics —
        # avoids the two separate dt.history() + dt.history(1) jobs from before.
        duration_ms = round((datetime.now() - t0).total_seconds() * 1000)
        latest = self.load(output_path).history(1).collect()
        row = latest[0].asDict() if latest else {}
        metrics = row.get("operationMetrics") or {}
        self.logger.info(
            "[%s] delta_commit  version=%d  rows=%d  files=%s  bytes=%s  duration_ms=%d",
            table,
            row.get("version", -1),
            row_count,
            metrics.get("numFiles", "?"),
            metrics.get("numOutputBytes", "?"),
            duration_ms,
        )
        return row_count

    def merge(
        self,
        df,
        output_path: str,
        table: str,
        key_columns: list[str],
        merge_schema: bool = True,
    ) -> int:
        """Upsert df into a Delta table, keyed on key_columns.

        Makes ingestion idempotent (CLAUDE.md: "Re-running a job must not
        produce duplicate rows") — re-running against an unchanged source
        re-stamps matching rows in place instead of appending a second copy
        of everything, and a source row whose non-key columns changed
        between runs updates its existing row rather than sitting alongside
        a stale duplicate. Falls back to a plain append when the table
        doesn't exist yet — there's nothing to merge against on the first run.
        """
        row_count = df.count()
        if not self.table_exists(output_path):
            return self.write(
                df, output_path, table, mode="append",
                row_count=row_count, merge_schema=merge_schema,
            )

        t0 = datetime.now()
        condition = " AND ".join(f"target.{c} = source.{c}" for c in key_columns)
        self.logger.info(
            "[%s] merging %d rows → %s  on (%s)",
            table, row_count, output_path, ", ".join(key_columns),
        )
        target = self.load(output_path)
        try:
            (
                target.alias("target")
                .merge(df.alias("source"), condition)
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        except Exception as exc:
            self.logger.error("[%s] merge failed: %s", table, exc)
            raise

        duration_ms = round((datetime.now() - t0).total_seconds() * 1000)
        latest = self.load(output_path).history(1).collect()
        row = latest[0].asDict() if latest else {}
        metrics = row.get("operationMetrics") or {}
        self.logger.info(
            "[%s] delta_commit  version=%d  rows_source=%d  updated=%s  inserted=%s  duration_ms=%d",
            table,
            row.get("version", -1),
            row_count,
            metrics.get("numTargetRowsUpdated", "?"),
            metrics.get("numTargetRowsInserted", "?"),
            duration_ms,
        )
        return row_count

    def append_if_new_source(
        self,
        df,
        output_path: str,
        table: str,
        source_file: str,
        row_count: int = 0,
        merge_schema: bool = True,
    ) -> int:
        """Append-only idempotency for tables that can legitimately contain
        same-key duplicate rows within a single source file — order_items'
        injected Problem C duplicates copy the original row's
        order_item_id too, so a primary-key-keyed `merge()` can't be used
        here: Delta's MERGE forbids multiple source rows matching the same
        target row, and pre-deduping the source before merge would silently
        drop the very duplicates Silver's dedup fix exists to demonstrate.

        Idempotency instead works at the source-file grain: skip the whole
        append if this exact source_file is already present in the target
        table. Correct for this repo's static demo source files, which
        don't change content between re-runs of the same path.

        `source_file` should be a canonical/relative identifier, not a raw
        absolute path — the caller (`ingest_bronze.py`'s `_relative_source`)
        learned this live: the same physical file resolves to a different
        absolute path depending on whether the script runs on the host or
        inside the Airflow container, which silently defeated this exact
        "already ingested" check and let order_items/events each get
        ingested twice.
        """
        if self.table_exists(output_path):
            already_ingested = (
                self.load(output_path).toDF()
                .filter(F.col("source_file") == source_file)
                .limit(1)
                .count() > 0
            )
            if already_ingested:
                self.logger.info(
                    "[%s] source_file already ingested — skipping (idempotent no-op): %s",
                    table, source_file,
                )
                return 0
        return self.write(
            df, output_path, table, mode="append",
            row_count=row_count, merge_schema=merge_schema,
        )

    def table_exists(self, output_path: str) -> bool:
        try:
            return DeltaTable.isDeltaTable(self.spark, output_path)
        except Exception:
            return False

    def load(self, output_path: str) -> DeltaTable:
        return DeltaTable.forPath(self.spark, output_path)

    def history(self, dt: DeltaTable) -> list[dict]:
        return [row.asDict() for row in dt.history().collect()]

    def version(self, dt: DeltaTable) -> int:
        rows = dt.history(1).collect()
        return rows[0]["version"] if rows else -1

    def schema(self, dt: DeltaTable):
        return dt.toDF().schema

    def optimize(self, output_path: str) -> None:
        """Compact small files in the Delta table."""
        self.load(output_path).optimize().executeCompaction()

    def z_order(self, output_path: str, columns: list[str]) -> None:
        """Z-order the Delta table by `columns` — co-locates rows on disk so a
        filter on those columns skips more files, instead of scanning
        (nearly) every file regardless of the predicate. Run after write(),
        on the columns a downstream query actually filters/joins on."""
        t0 = datetime.now()
        self.load(output_path).optimize().executeZOrderBy(*columns)
        duration_ms = round((datetime.now() - t0).total_seconds() * 1000)
        self.logger.info(
            "[z_order] path=%s  columns=%s  duration_ms=%d",
            output_path, columns, duration_ms,
        )
