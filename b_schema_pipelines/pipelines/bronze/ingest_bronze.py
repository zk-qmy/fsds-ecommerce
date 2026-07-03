"""
Bronze ingestion pipeline.

Reads raw Parquet (offline tables) and NDJSON (events) from the source
directory, stamps each row with ingest metadata, and writes to Delta Lake
via PySpark's Delta connector (delta-spark).

No transformation logic here — Bronze is a faithful copy of the source
plus lineage columns.

Run:
    python b_schema_pipelines/pipelines/bronze/ingest_bronze.py
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

from delta.tables import DeltaTable
from pyspark.sql import functions as F

from config.logging import setup_logger
from b_schema_pipelines.pipelines.pipeline_base import PipelineBase

OFFLINE_TABLES = ["customers", "products", "orders", "order_items", "payments"]


class FileReader:
    """Reads raw Parquet (offline tables) and NDJSON (events) from the source directory."""

    def __init__(self, spark) -> None:
        self.spark = spark
        self.logger = setup_logger(name="FileReader", filename="FileReader.log")

    def read_parquet(self, path: str):
        import pyarrow as pa
        import pyarrow.parquet as pq

        self.logger.info("reading parquet: %s", path)
        table = pq.read_table(path)
        # PySpark rejects TIMESTAMP(NANOS) in Parquet (pandas default); downcast ns → us.
        new_fields = [
            (
                f.with_type(pa.timestamp("us", tz=f.type.tz))
                if pa.types.is_timestamp(f.type) and f.type.unit == "ns"
                else f
            )
            for f in table.schema
        ]
        table = table.cast(pa.schema(new_fields))
        df = self.spark.createDataFrame(table.to_pandas())
        self.logger.info("schema: %s  rows: %d", df.schema.simpleString(), df.count())
        return df

    def read_json(self, path: str):
        """Read newline-delimited JSON (NDJSON) into a Spark DataFrame."""
        self.logger.info("reading ndjson: %s", path)
        return self.spark.read.option("multiLine", False).json(path)

    def exists(self, path: str) -> bool:
        return Path(path).exists()


class MetadataManager:
    """Stamps each row with ingest lineage columns."""

    def __init__(self, run_id: str) -> None:
        self.run_id = run_id
        self.logger = setup_logger(
            name="MetadataManager", filename="MetadataManager.log"
        )

    def add_processing_metadata(self, df, source_file: str):
        """Append ingest_ts, source_file, pipeline_run_id to every row."""
        self.logger.info(
            "adding processing metadata: source_file=%s  pipeline_run_id=%s",
            source_file,
            self.run_id,
        )
        return (
            df.withColumn("ingest_ts", F.current_timestamp())
            .withColumn("source_file", F.lit(source_file))
            .withColumn("pipeline_run_id", F.lit(self.run_id))
        )


class DeltaWriter:
    """Writes DataFrames to Delta Lake using PySpark's Delta connector.

    All metadata reads (table_exists, history, version) go through the same
    SparkSession, so S3A credentials configured in _build_spark() cover both
    reads and writes uniformly.
    """

    def __init__(self, spark) -> None:
        self.spark = spark
        self.logger = setup_logger(name="DeltaWriter", filename="DeltaWriter.log")

    def write(self, df, output_path: str, table: str, mode: str = "append") -> int:
        """Append df to a Delta table. Logs before/after state and commit metrics.

        Returns the number of rows written.
        """
        if self.table_exists(output_path):
            existing = self.load(output_path)
            self.logger.info(
                "[%s] existing table detected  version=%d",
                table,
                self.version(existing),
            )

        t0 = datetime.now()
        rows_to_write = df.count()
        self.logger.info(
            "[%s] Writing %d rows to %s", table, rows_to_write, output_path
        )
        try:
            (
                df.write.format("delta")
                .mode(mode)
                .option("mergeSchema", "true")
                .save(output_path)
            )
        except Exception as e:
            self.logger.error("[%s] Error writing to Delta table: %s", table, str(e))
            raise
        duration_ms = round((datetime.now() - t0).total_seconds() * 1000)

        dt = self.load(output_path)
        hist = self.history(dt)
        metrics = (hist[0].get("operationMetrics") or {}) if hist else {}

        self.logger.info(
            "[%s] delta_commit  version=%d  rows=%d  files=%s  bytes=%s  duration_ms=%d",
            table,
            self.version(dt),
            rows_to_write,
            metrics.get("numFiles", "?"),
            metrics.get("numOutputBytes", "?"),
            duration_ms,
        )
        return rows_to_write

    def table_exists(self, output_path: str) -> bool:
        try:
            return DeltaTable.isDeltaTable(self.spark, output_path)
        except Exception:
            self.logger.info("no existing Delta table at %s", output_path)
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


class BronzeIngester(PipelineBase):
    """Ingests raw source files into the Bronze Delta Lake layer.

    Reads each raw dataset, enriches rows with ingestion metadata,
    runs quality gates, and writes to Delta tables in the Bronze layer.
    """

    PREFIX = "bronze"

    def __init__(self, source_dir: str | Path = "a_data_generator/outputs") -> None:
        super().__init__(config_file=Path(__file__).parent / "bronze_config.yaml")
        self.source_dir = Path(source_dir).resolve()
        self.output_dir, s3_config = self._resolve_s3_config("bronze")
        self.spark = self._build_spark(s3_config=s3_config)
        self.reader = FileReader(spark=self.spark)
        self.meta = MetadataManager(run_id=self.run_id)
        self.writer = DeltaWriter(spark=self.spark)

    @property
    def _PRIMARY_KEYS(self) -> dict[str, list[str]]:
        return self.cfg["quality"]["primary_keys"]

    @property
    def _EXPECTED_COLUMNS(self) -> dict[str, list[str]]:
        return self.cfg["quality"]["expected_columns"]

    # ── public ────────────────────────────────────────────────────────────────

    def run(self) -> None:
        """Ingest all offline tables then the event stream."""
        for table in OFFLINE_TABLES:
            self._ingest_table(
                table,
                str(self.source_dir / "offline" / f"{table}.parquet"),
                self.reader.read_parquet,
            )
        self._ingest_table(
            "events",
            str(self.source_dir / "streaming" / "events.json"),
            self.reader.read_json,
        )

    # ── private ───────────────────────────────────────────────────────────────

    def _ingest_table(self, table: str, source_file: str, read_fn) -> None:
        start_ts = datetime.now()
        input_rows = 0
        try:
            self.log_table_start(table, source_file)
            if not self.reader.exists(source_file):
                raise FileNotFoundError(f"source file not found: {source_file}")
            df = read_fn(source_file)
            input_rows = df.count()
            rows_written = self._ingest_dataset(df, source_file, table, input_rows)
            self.log_run(
                table, start_ts, datetime.now(), input_rows, rows_written, "ok"
            )
        except Exception as exc:
            self.log_run(
                table, start_ts, datetime.now(), input_rows, 0, "error", str(exc)
            )
            raise

    def _check_quality(self, df, table: str, row_count: int) -> None:
        """Run Bronze quality gates: non-empty volume, schema presence, PK null-free.

        Raises ValueError listing all failures so the ingestion job is halted
        and the error surfaces in Airflow / log_run as status='error'.
        """
        self.logger.info("[%s] running quality gates", table)
        errors: list[str] = []

        if row_count == 0:
            errors.append("no rows ingested")

        col_set = set(df.columns)
        missing = [c for c in self._EXPECTED_COLUMNS.get(table, []) if c not in col_set]
        if missing:
            errors.append(f"missing columns: {missing}")

        for pk in self._PRIMARY_KEYS.get(table, []):
            if pk in col_set:
                null_count = df.filter(F.col(pk).isNull()).count()
                if null_count:
                    errors.append(f"null PKs in {pk}: {null_count} rows")

        if errors:
            msg = f"[{table}] quality gate failed: {'; '.join(errors)}"
            self.logger.error(msg)
            raise ValueError(msg)

        self.logger.info("[%s] quality gates passed  rows=%d", table, row_count)

    def _ingest_dataset(self, df, source_file: str, table: str, row_count: int) -> int:
        """Stamp metadata, run quality gates, write to Delta Lake. Returns rows written."""
        self.logger.info("[%s] ingesting dataset from %s", table, source_file)
        df = self.meta.add_processing_metadata(df, source_file)
        self._check_quality(df, table, row_count)
        output_path = f"{self.output_dir}/{table}"
        self.logger.info("[%s] writing to Delta Lake at %s", table, output_path)
        return self.writer.write(df, output_path, table)


# ── entrypoint ────────────────────────────────────────────────────────────────


def main() -> None:
    BronzeIngester().run()


if __name__ == "__main__":
    main()
