"""Structured logging for ztract — JSON formatter and logger setup."""
from __future__ import annotations

import logging
import logging.handlers
from datetime import datetime, timezone
from pathlib import Path

# Extra fields that are promoted to top-level JSON keys when present on a record.
_EXTRA_FIELDS = (
    "job",
    "step",
    "event",
    "records_read",
    "records_written",
    "records_rejected",
    "source",
    "target",
    "duration_ms",
)

# Standard LogRecord attributes to exclude from the "extra" scan.
_STD_ATTRS = frozenset(logging.LogRecord(
    "", logging.DEBUG, "", 0, "", (), None
).__dict__.keys()) | {"message", "asctime"}


class JSONFormatter(logging.Formatter):
    """Format log records as single-line JSON."""

    def format(self, record: logging.LogRecord) -> str:
        import json

        record.message = record.getMessage()

        data: dict = {
            "timestamp": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.message,
        }

        # Promote known extra fields.
        for field in _EXTRA_FIELDS:
            if hasattr(record, field):
                data[field] = getattr(record, field)

        # Also include any other non-standard attributes attached to the record.
        for key, val in record.__dict__.items():
            if key not in _STD_ATTRS and key not in data and not key.startswith("_"):
                try:
                    json.dumps(val)  # only include JSON-serialisable extras
                    data[key] = val
                except (TypeError, ValueError):
                    pass

        if record.exc_info:
            data["exc_info"] = self.formatException(record.exc_info)

        return json.dumps(data)


def setup_logging(log_dir: Path, debug: bool = False, quiet: bool = False) -> None:
    """Configure the ``ztract`` root logger.

    Parameters
    ----------
    log_dir:
        Directory where JSON log files are written.  Created if absent.
    debug:
        When ``True`` set the log level to DEBUG; otherwise INFO.
    quiet:
        When ``True`` suppress the console handler entirely.
    """
    log_dir = Path(log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("ztract")
    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    # Avoid adding duplicate handlers on repeated calls (e.g. in tests).
    logger.handlers.clear()
    logger.propagate = False

    # --- File handler (rotating daily, JSON) ---
    log_file = log_dir / "ztract.log"
    file_handler = logging.handlers.TimedRotatingFileHandler(
        filename=log_file,
        when="midnight",
        backupCount=30,
        encoding="utf-8",
    )
    file_handler.setLevel(logging.DEBUG if debug else logging.INFO)
    file_handler.setFormatter(JSONFormatter())
    logger.addHandler(file_handler)

    # --- Console handler (plain text, suppressed if quiet) ---
    if not quiet:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.DEBUG if debug else logging.INFO)
        console_handler.setFormatter(
            logging.Formatter("%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
        )
        logger.addHandler(console_handler)
