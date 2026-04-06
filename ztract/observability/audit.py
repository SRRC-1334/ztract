"""Audit trail — records a structured summary of each ztract run."""
from __future__ import annotations

import getpass
import json
import socket
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ztract import __version__ as _DEFAULT_VERSION


@dataclass
class StepAudit:
    """Audit record for a single pipeline step."""

    step: str
    action: str
    source: str
    targets: list[str]
    records_read: int
    records_written: int
    records_rejected: int
    reject_file: str | None
    status: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "step": self.step,
            "action": self.action,
            "source": self.source,
            "targets": self.targets,
            "records_read": self.records_read,
            "records_written": self.records_written,
            "records_rejected": self.records_rejected,
            "reject_file": self.reject_file,
            "status": self.status,
        }


@dataclass
class AuditEntry:
    """Top-level audit record for a complete ztract job."""

    job_file: str
    jre_version: str
    job_file_hash: str
    overall_status: str
    exit_code: int
    ztract_version: str = field(default_factory=lambda: _DEFAULT_VERSION)
    steps: list[StepAudit] = field(default_factory=list)
    timestamp_start: str = field(
        default_factory=lambda: datetime.now(tz=timezone.utc).isoformat()
    )

    def add_step(self, step: StepAudit) -> None:
        """Append a completed step audit to this entry."""
        self.steps.append(step)

    def to_dict(self) -> dict[str, Any]:
        """Serialise to a dictionary suitable for JSON output.

        ``audit_id``, ``timestamp_end``, ``user`` and ``machine`` are
        generated at serialisation time so each call produces a fresh snapshot.
        """
        try:
            user = getpass.getuser()
        except Exception:
            user = "unknown"

        try:
            machine = socket.gethostname()
        except Exception:
            machine = "unknown"

        return {
            "audit_id": str(uuid.uuid4()),
            "job_file": self.job_file,
            "ztract_version": self.ztract_version,
            "jre_version": self.jre_version,
            "job_file_hash": self.job_file_hash,
            "overall_status": self.overall_status,
            "exit_code": self.exit_code,
            "timestamp_start": self.timestamp_start,
            "timestamp_end": datetime.now(tz=timezone.utc).isoformat(),
            "user": user,
            "machine": machine,
            "steps": [s.to_dict() for s in self.steps],
        }


class AuditWriter:
    """Appends :class:`AuditEntry` records to a JSONL audit file."""

    def __init__(self, audit_file: Path) -> None:
        self._audit_file = Path(audit_file)

    def write(self, entry: AuditEntry) -> None:
        """Append *entry* to the audit file, creating directories as needed."""
        self._audit_file.parent.mkdir(parents=True, exist_ok=True)
        with self._audit_file.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(entry.to_dict()) + "\n")
