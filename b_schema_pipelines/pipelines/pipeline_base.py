from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession
from config.logging import setup_logger
from utils.config import load_config


class PipelineBase(ABC):
    """
    Common functionality shared by every pipeline stage.

    Provides:
        - run_id generation (PREFIX_YYYYMMDD_HHMMSS)
        - config loading
        - logger (console + rotating file via config.logging.setup_logger)
        - log_table_start / log_run for per-table start/end telemetry
        - log_delta_write for Delta Lake commit metrics
    """

    PREFIX: str = "BasePipeline"

    _SHARED_CONFIG = Path(__file__).parent / "pipeline_config.yaml"

    def __init__(self, config_file: str | None = None) -> None:
        self.run_id: str = self._generate_run_id()

        self.logger = setup_logger(
            name=self.run_id,
            log_dir=f"logs/{self.PREFIX}",
            filename=f"{self.run_id}.log",
        )

        self.shared_cfg: dict = load_config(self._SHARED_CONFIG)
        self.cfg: dict = load_config(config_file) if config_file else {}
        self.logger.info("Starting spark...")
        self.spark = self._create_spark_session(app_name=self.run_id)
        self.logger.info("Spark started.")
        self.logger.info(
            "pipeline=%s  run_id=%s  phase=start",
            self.PREFIX,
            self.run_id,
        )

    def _create_spark_session(self, app_name: str) -> SparkSession:
        Path("/tmp/spark-events").mkdir(parents=True, exist_ok=True)
        builder = (
            SparkSession.builder.master("local[*]")
            .appName(app_name)
            # Delta Lake extensions
            .config("spark.sql.extensions",
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config(
                "spark.sql.catalog.spark_catalog",
                "org.apache.spark.sql.delta.catalog.DeltaCatalog",
            )
            # Minio / S3A
            .config(
                "spark.hadoop.fs.s3a.endpoint",
                self.shared_cfg["minio"]["endpoint"]
            )
            .config(
                "spark.hadoop.fs.s3a.access.key",
                self.shared_cfg["minio"]["access_key"]
            )
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                self.shared_cfg["minio"]["secret_key"]
            )
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config(
                "spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            # Increase connection pool for concurrent parallel reads (16+ parquet files)
            .config("spark.hadoop.fs.s3a.connection.maximum", "200")
            .config("spark.hadoop.fs.s3a.connection.timeout", "200000")
            .config("spark.hadoop.fs.s3a.socket.timeout", "200000")
            .config("spark.jars.packages",
                    ",".join(self.shared_cfg["packages"]))
            # History Server event logging
            .config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", "file:///tmp/spark-events")
            .config("spark.history.fs.logDirectory",
                    "file:///tmp/spark-events")
            .getOrCreate()
        )
        return builder

    def _resolve_s3_config(self, layer: str) -> tuple[Path, dict]:
        """
        Returns the output directory and S3 config for the given layer.

        Args:
            layer (str): The layer name (e.g., "bronze", "silver", "gold").

        Returns:
            tuple[Path, dict]: A tuple containing the output directory
            as a Path object and the S3 configuration as a dictionary.
        """
        if layer not in self.shared_cfg["layers"]:
            raise ValueError(f"Layer '{layer}' not found in shared config.")

        layer_config = self.shared_cfg["layers"][layer]
        bucket_name = layer_config["bucket"]
        folder_name = layer_config["prefix"]

        # Construct the output directory path
        output_dir = f"s3a://{bucket_name}/{folder_name}"

        # Return the output directory and S3 config
        return output_dir, {
            "endpoint": self.shared_cfg["minio"]["endpoint"],
            "access_key": self.shared_cfg["minio"]["access_key"],
            "secret_key": self.shared_cfg["minio"]["secret_key"],
        }

    def _generate_run_id(self) -> str:
        return f"{self.PREFIX}_{datetime.now():%Y%m%d_%H%M%S}"

    # logging helpers

    def log_table_start(self, table: str, source: str = "") -> None:
        """Log the beginning of a per-table processing step."""
        self.logger.info(
            "[%s] phase=start  source=%s",
            table,
            source or "n/a",
        )

    def log_run(
        self,
        table: str,
        start_ts: datetime,
        end_ts: datetime,
        input_rows: int,
        output_rows: int,
        status: str,
        error: str = "",
    ) -> None:
        """Emit a structured run-log entry for one table pass.

        Logs at ERROR level when status == 'error', INFO otherwise, so
        Grafana/Loki alert rules fire on the correct severity.

        JSON key names match the CLAUDE.md spec:
            run_id, pipeline_name, table, start_ts, end_ts,
            duration_s, input_rows, output_rows, status, error_summary
        """
        duration = round((end_ts - start_ts).total_seconds(), 3)
        log_fn = self.logger.error if status == "error" else self.logger.info

        # Human-readable summary line — easy to grep in terminal or Loki
        log_fn(
            "[%s] phase=end  status=%s  rows_in=%d  rows_out=%d  duration_s=%.3f%s",
            table,
            status,
            input_rows,
            output_rows,
            duration,
            f"  error={error!r}" if error else "",
        )

        # Structured JSON line — ingested by Loki, queried in Grafana
        entry = {
            "run_id": self.run_id,
            "pipeline_name": self.PREFIX,
            "table": table,
            "start_ts": start_ts.isoformat(),
            "end_ts": end_ts.isoformat(),
            "duration_s": duration,
            "input_rows": input_rows,
            "output_rows": output_rows,
            "status": status,
            "error_summary": error,
        }
        self.logger.debug("STRUCTURED %s", json.dumps(entry))

    def log_delta_write(
        self,
        table: str,
        version: int | str,
        rows_added: int | str,
        files_added: int | str,
        bytes_written: int | str,
        duration_ms: int | str,
    ) -> None:
        """Log Delta Lake commit metrics after a write_deltalake call."""
        self.logger.info(
            "[%s] delta_commit  version=%s  rows_added=%s  files=%s  bytes=%s  duration_ms=%s",
            table,
            version,
            rows_added,
            files_added,
            bytes_written,
            duration_ms,
        )

    @abstractmethod
    def run(self) -> None:
        """Run the pipeline. Must be implemented by subclasses."""
