import logging
import logging.handlers
from pathlib import Path


LEVEL_COLORS = {
    "DEBUG":    "\033[36m",    # cyan
    "INFO":     "\033[32m",    # green
    "WARNING":  "\033[33m",    # yellow
    "ERROR":    "\033[31m",    # red
    "CRITICAL": "\033[41m",    # red background
}
RESET = "\033[0m"


class ColoredConsoleFormatter(logging.Formatter):
    """Adds ANSI color to the level name on console output only."""

    def format(self, record: logging.LogRecord) -> str:
        color = LEVEL_COLORS.get(record.levelname, "")
        record.levelname = f"{color}{record.levelname:<8}{RESET}"
        return super().format(record)


class PlainFileFormatter(logging.Formatter):
    """Plain formatter for file — no ANSI codes."""
    pass


def setup_logger(
    name: str = "LOGGING_NAME",
    log_dir: str | Path = "logs",
    filename: str = "LOGGING_NAME.log",
    console_level: int = logging.DEBUG,
    file_level: int = logging.DEBUG,
    max_bytes: int = 5 * 1024 * 1024,
    backup_count: int = 3,
) -> logging.Logger:
    """Create and return a logger with console + rotating file handlers.

    Both handlers default to DEBUG so every level is captured.
    Console output is colour-coded by level; the file is plain text.

    Args:
        name:          Logger name.
        log_dir:       Directory for log files. Created if absent.
        filename:      Log file name inside log_dir.
        console_level: Minimum level for the console handler.
        file_level:    Minimum level for the file handler.
        max_bytes:     Rotate file after this many bytes.
        backup_count:  Number of rotated files to keep.

    Returns:
        Configured logger. Safe to call multiple times — handlers are not
        duplicated.
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)   # master gate: let handlers decide

    fmt = "%(asctime)s  %(levelname)-8s  %(name)s  —  %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    # ── Console handler (coloured) ────────────────────────────────────────
    console = logging.StreamHandler()
    console.setLevel(console_level)
    console.setFormatter(ColoredConsoleFormatter(fmt=fmt, datefmt=datefmt))

    # ── Rotating file handler (plain) ─────────────────────────────────────
    log_path = Path(log_dir)
    log_path.mkdir(parents=True, exist_ok=True)

    file_handler = logging.handlers.RotatingFileHandler(
        filename=log_path / filename,
        maxBytes=max_bytes,
        backupCount=backup_count,
        encoding="utf-8",
    )
    file_handler.setLevel(file_level)
    file_handler.setFormatter(PlainFileFormatter(fmt=fmt, datefmt=datefmt))

    logger.addHandler(console)
    logger.addHandler(file_handler)

    return logger
