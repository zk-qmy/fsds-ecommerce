"""
Shared Delta Lake read/write utilities used by Bronze, Silver, and Gold pipelines.
"""

from __future__ import annotations

from datetime import datetime

from delta.tables import DeltaTable
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
