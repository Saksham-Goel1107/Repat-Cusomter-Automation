"""
logger.py — Structured logging.
Outputs JSON lines in production (Docker / log aggregators love it).
Falls back to human-readable text when LOG_FORMAT=text.
"""

import json
import logging
import sys
from datetime import datetime, timezone


class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single-line JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        # Merge any extra kwargs passed to logger.info(..., extra={...})
        for key, val in record.__dict__.items():
            # Skip common logging fields and any extras that are None to avoid
            # emitting null-valued keys like taskName: null in the JSON output.
            if val is None:
                continue
            if key not in {
                "args", "asctime", "created", "exc_info", "exc_text",
                "filename", "funcName", "id", "levelname", "levelno",
                "lineno", "module", "msecs", "message", "msg", "name",
                "pathname", "process", "processName", "relativeCreated",
                "stack_info", "thread", "threadName",
            }:
                payload[key] = val
        return json.dumps(payload, default=str)


def setup_logger(
    name: str = "customer_analysis",
    level: str = "INFO",
    fmt: str = "json",
) -> logging.Logger:
    """
    Return a configured logger.

    Args:
        name:  Logger name.
        level: "DEBUG" | "INFO" | "WARNING" | "ERROR".
        fmt:   "json" (default, structured) | "text" (human-readable).
    """
    logger = logging.getLogger(name)

    if logger.handlers:
        # Already configured (e.g. module re-imported) — don't double-add.
        return logger

    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    handler = logging.StreamHandler(sys.stdout)
    if fmt.lower() == "json":
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(
            logging.Formatter(
                fmt="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S",
            )
        )

    logger.addHandler(handler)
    logger.propagate = False
    return logger
