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

import json
import sys
import time
import urllib.request
from datetime import datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql import functions as F

from b_schema_pipelines.pipelines.common.delta_writer import DeltaWriter
from b_schema_pipelines.pipelines.pipeline_base import PipelineBase
from config.logging import setup_logger

sys.path.insert(0, str(Path(__file__).parent.parent))

from minio_client import MinioClient  # noqa: E402  (after sys.path insert)
from utils.config import load_config   # noqa: E402

OFFLINE_TABLES = ["customers", "products", "orders", "order_items", "payments"]

# Tables whose source data can legitimately contain rows that share the same
# primary/merge key — order_items' Problem C duplicates copy the original
# row's order_item_id, and events' Problem F duplicates were found (live,
# against the real generated data) to sometimes land on the exact same
# (event_id, event_timestamp) pair too, not just a "slightly shifted" one.
# A primary-key-keyed MERGE can't be used for either (Delta forbids multiple
# source rows matching one target row) without pre-deduping the source first
# — which would silently remove the very duplicates Silver/Flink's dedup
# fixes exist to demonstrate. Idempotency instead skips re-ingesting a
# source_file already present in Bronze; see DeltaWriter.append_if_new_source.
APPEND_IF_NEW_SOURCE_TABLES = {"order_items", "events"}


class FileReader:
    """Reads raw Parquet (offline tables) and NDJSON (events)."""

    def __init__(self, spark) -> None:
        self.spark = spark
        self.logger = setup_logger(name="FileReader", filename="FileReader.log")

    def read_parquet(self, path: str):
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
        self.logger.info("schema: %s", df.schema.simpleString())
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
        self.logger = setup_logger(name="MetadataManager", filename="MetadataManager.log")

    def add_processing_metadata(self, df, source_file: str):
        """Append ingest_ts, source_file, pipeline_run_id to every row."""
        self.logger.info(
            "adding metadata: source_file=%s  pipeline_run_id=%s",
            source_file,
            self.run_id,
        )
        return (
            df.withColumn("ingest_ts", F.current_timestamp())
            .withColumn("source_file", F.lit(source_file))
            .withColumn("pipeline_run_id", F.lit(self.run_id))
        )


class BronzeIngester(PipelineBase):
    """Ingests raw source files into the Bronze Delta Lake layer."""

    PREFIX = "bronze"

    def __init__(
        self,
        source_dir: str | Path = "a_data_generator/outputs",
        output_dir: str | Path | None = None,
    ) -> None:
        super().__init__(config_file=Path(__file__).parent / "bronze_config.yaml")
        self.source_dir = Path(source_dir).resolve()
        if output_dir is not None:
            self.output_dir = Path(output_dir)
        else:
            s3_path, _ = self._resolve_s3_config("bronze")
            self.output_dir = s3_path          # string — S3A path
        self.spark = self._build_spark()
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
            self._ingest_offline_table(table)
        self._ingest_events()
        self.spark.stop()

    def _build_spark(self):
        """Hook for injecting a test SparkSession via patch.object."""
        return self.spark

    def _add_ingest_metadata(self, df, source_file: str):
        """Stamp ingest_ts, source_file, pipeline_run_id onto every row."""
        return self.meta.add_processing_metadata(df, source_file)

    def _ingest_offline_table(self, table: str) -> None:
        """Ingest one offline Parquet table into Bronze Delta."""
        source_file = str(self.source_dir / "offline" / f"{table}.parquet")
        self._ingest(table, source_file, self.reader.read_parquet)

    def _ingest_events(self) -> None:
        """Ingest the streaming events NDJSON file into Bronze Delta."""
        source_file = str(self.source_dir / "streaming" / "events.json")
        self._ingest("events", source_file, self.reader.read_json)

    # ── private ───────────────────────────────────────────────────────────────

    def _table_path(self, table: str) -> str:
        """Build the output path for a table, handling both Path and S3A string."""
        if isinstance(self.output_dir, Path):
            return str(self.output_dir / table)
        return f"{self.output_dir}/{table}"

    def _relative_source(self, source_file: str) -> str:
        """Path relative to source_dir (e.g. "offline/order_items.parquet"),
        stored in the source_file column instead of the absolute path.

        The same physical file resolves to a different absolute path
        depending on whether this script runs on the host
        (/mnt/d/fsds-ecommerce/...) or inside the Airflow container
        (/opt/project/...) -- a real bug, found live: append_if_new_source's
        "already ingested" check compares source_file by exact string, so
        those two absolute paths for the identical file were never
        recognized as a duplicate, and order_items/events each ended up
        ingested twice (once from each context). A relative path is stable
        across both.
        """
        return str(Path(source_file).relative_to(self.source_dir))

    def _ingest(self, table: str, source_file: str, read_fn) -> None:
        """Core ingestion logic shared by offline tables and events.

        `source_file` is the real, resolvable path used to actually read the
        file; the canonical (relative) form is what gets stamped into the
        source_file column and compared for idempotency -- see
        `_relative_source`'s docstring for why they need to differ.
        """
        start_ts = datetime.now()
        input_rows = 0
        canonical_source = self._relative_source(source_file)
        try:
            self.log_table_start(table, canonical_source)
            if not self.reader.exists(source_file):
                raise FileNotFoundError(f"source file not found: {source_file}")
            df = read_fn(source_file)
            df.cache()
            input_rows = df.count()
            df = self._add_ingest_metadata(df, canonical_source)
            self._check_quality(df, table, input_rows)
            if table in APPEND_IF_NEW_SOURCE_TABLES:
                rows_written = self.writer.append_if_new_source(
                    df,
                    self._table_path(table),
                    table,
                    source_file=canonical_source,
                    row_count=input_rows,
                )
            else:
                rows_written = self.writer.merge(
                    df,
                    self._table_path(table),
                    table,
                    key_columns=self._PRIMARY_KEYS.get(table, []),
                )
            df.unpersist()
            print(json.dumps({"status": "success", "table": table, "rows": rows_written}))
            self.log_run(table, start_ts, datetime.now(), input_rows, rows_written, "ok")
        except Exception as exc:
            self.log_run(table, start_ts, datetime.now(), input_rows, 0, "error", str(exc))
            raise

    def _check_quality(self, df, table: str, row_count: int) -> None:
        """Quality gates: non-empty volume, schema presence, PK null-free."""
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


# ── helpers ───────────────────────────────────────────────────────────────────

def wait_for_minio(retries: int = 15, delay: int = 2) -> None:
    url = "http://localhost:9000/minio/health/live"
    for i in range(retries):
        try:
            urllib.request.urlopen(url, timeout=2)
            print("  MinIO is ready.")
            return
        except Exception:
            print(f"  Waiting for MinIO... ({i + 1}/{retries})")
            time.sleep(delay)
    print("  MinIO health check timed out — proceeding anyway.")


# ── entrypoint ────────────────────────────────────────────────────────────────

def main() -> None:
    print("=== [0] Waiting for MinIO ===")
    wait_for_minio()

    print("\n=== [1] Creating buckets ===")
    minio_client = MinioClient()
    cfg = load_config("b_schema_pipelines/pipelines/pipeline_config.yaml")
    buckets = [layer["bucket"] for layer in cfg["layers"].values()]
    for bucket in buckets:
        minio_client.create_bucket(bucket)

    print("\n=== [2] Bronze — Delta table → MinIO ===")
    BronzeIngester().run()


if __name__ == "__main__":
    main()
