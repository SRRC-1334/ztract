"""Tests for the ztract.observability package — all four modules.

TDD order: tests written before implementations.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# RejectHandler (rejects.py)
# ---------------------------------------------------------------------------

class TestRejectHandler:
    """Tests for RejectHandler."""

    def test_reject_writes_jsonl_entry(self, tmp_path):
        from ztract.observability.rejects import RejectHandler

        rfile = tmp_path / "rejects" / "run.jsonl"
        handler = RejectHandler(rfile)
        handler.open()
        handler.reject(
            record_num=1,
            byte_offset=0,
            step="decode",
            error_type="UnicodeDecodeError",
            error_msg="invalid byte",
            target="output.parquet",
        )
        handler.close()

        assert rfile.exists()
        lines = rfile.read_text().strip().splitlines()
        assert len(lines) == 1
        entry = json.loads(lines[0])
        assert entry["record_num"] == 1
        assert entry["byte_offset"] == 0
        assert entry["step"] == "decode"
        assert entry["error_type"] == "UnicodeDecodeError"
        assert entry["error_msg"] == "invalid byte"
        assert entry["target"] == "output.parquet"
        assert "timestamp" in entry

    def test_reject_includes_optional_decoded_and_raw_hex(self, tmp_path):
        from ztract.observability.rejects import RejectHandler

        rfile = tmp_path / "rejects.jsonl"
        with RejectHandler(rfile) as handler:
            handler.reject(
                record_num=5,
                byte_offset=100,
                step="parse",
                error_type="ValueError",
                error_msg="bad value",
                target="table",
                decoded={"field": "val"},
                raw_hex="deadbeef",
            )

        entry = json.loads(rfile.read_text().strip())
        assert entry["decoded"] == {"field": "val"}
        assert entry["raw_hex"] == "deadbeef"

    def test_count_property_reflects_rejects(self, tmp_path):
        from ztract.observability.rejects import RejectHandler

        rfile = tmp_path / "rejects.jsonl"
        with RejectHandler(rfile) as handler:
            assert handler.count == 0
            handler.reject(1, 0, "s", "E", "msg", "t")
            assert handler.count == 1
            handler.reject(2, 8, "s", "E", "msg", "t")
            assert handler.count == 2

    def test_no_file_created_if_no_rejects(self, tmp_path):
        from ztract.observability.rejects import RejectHandler

        rfile = tmp_path / "no_rejects.jsonl"
        with RejectHandler(rfile):
            pass

        assert not rfile.exists()

    def test_context_manager_closes_properly(self, tmp_path):
        from ztract.observability.rejects import RejectHandler

        rfile = tmp_path / "cm_rejects.jsonl"
        with RejectHandler(rfile) as handler:
            handler.reject(1, 0, "s", "E", "msg", "t")

        # File should be closed and readable after context exit
        entry = json.loads(rfile.read_text().strip())
        assert entry["record_num"] == 1

    def test_multiple_rejects_each_own_line(self, tmp_path):
        from ztract.observability.rejects import RejectHandler

        rfile = tmp_path / "multi.jsonl"
        with RejectHandler(rfile) as handler:
            for i in range(5):
                handler.reject(i, i * 10, "step", "Err", "msg", "tgt")

        lines = rfile.read_text().strip().splitlines()
        assert len(lines) == 5
        for i, line in enumerate(lines):
            assert json.loads(line)["record_num"] == i

    def test_parent_dirs_created_lazily(self, tmp_path):
        from ztract.observability.rejects import RejectHandler

        rfile = tmp_path / "a" / "b" / "c" / "rejects.jsonl"
        assert not rfile.parent.exists()
        with RejectHandler(rfile) as handler:
            handler.reject(1, 0, "s", "E", "m", "t")

        assert rfile.exists()


# ---------------------------------------------------------------------------
# JSONFormatter + setup_logging (logging.py)
# ---------------------------------------------------------------------------

class TestJSONFormatter:
    """Tests for JSONFormatter."""

    def test_format_produces_valid_json(self):
        from ztract.observability.logging import JSONFormatter

        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="ztract.test",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="hello world",
            args=(),
            exc_info=None,
        )
        output = formatter.format(record)
        data = json.loads(output)
        assert data["level"] == "INFO"
        assert data["message"] == "hello world"
        assert "timestamp" in data

    def test_format_includes_logger_name(self):
        from ztract.observability.logging import JSONFormatter

        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="ztract.pipeline",
            level=logging.WARNING,
            pathname="",
            lineno=0,
            msg="watch out",
            args=(),
            exc_info=None,
        )
        data = json.loads(formatter.format(record))
        assert data["logger"] == "ztract.pipeline"
        assert data["level"] == "WARNING"

    def test_format_includes_extra_fields(self):
        from ztract.observability.logging import JSONFormatter

        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="ztract",
            level=logging.INFO,
            pathname="",
            lineno=0,
            msg="processing",
            args=(),
            exc_info=None,
        )
        record.job = "myjob"
        record.step = "decode"
        record.records_read = 1000
        data = json.loads(formatter.format(record))
        assert data["job"] == "myjob"
        assert data["step"] == "decode"
        assert data["records_read"] == 1000

    def test_format_level_debug(self):
        from ztract.observability.logging import JSONFormatter

        formatter = JSONFormatter()
        record = logging.LogRecord(
            name="ztract",
            level=logging.DEBUG,
            pathname="",
            lineno=0,
            msg="debug msg",
            args=(),
            exc_info=None,
        )
        data = json.loads(formatter.format(record))
        assert data["level"] == "DEBUG"
        assert data["message"] == "debug msg"


class TestSetupLogging:
    """Tests for setup_logging."""

    def test_creates_log_directory(self, tmp_path):
        from ztract.observability.logging import setup_logging

        log_dir = tmp_path / "logs" / "subdir"
        assert not log_dir.exists()
        setup_logging(log_dir)
        assert log_dir.exists()

    def test_ztract_logger_exists_after_setup(self, tmp_path):
        from ztract.observability.logging import setup_logging

        log_dir = tmp_path / "logs2"
        setup_logging(log_dir)
        logger = logging.getLogger("ztract")
        assert logger is not None

    def test_quiet_suppresses_console(self, tmp_path, capsys):
        from ztract.observability.logging import setup_logging

        log_dir = tmp_path / "logs3"
        setup_logging(log_dir, quiet=True)
        logger = logging.getLogger("ztract.quiet_test")
        logger.info("should not appear on console")
        captured = capsys.readouterr()
        # In quiet mode, console output should be absent
        assert "should not appear on console" not in captured.out
        assert "should not appear on console" not in captured.err

    def test_file_handler_writes_json(self, tmp_path):
        from ztract.observability.logging import setup_logging

        log_dir = tmp_path / "logs4"
        setup_logging(log_dir, quiet=True)
        logger = logging.getLogger("ztract")
        logger.info("file handler test")
        # Flush all handlers
        for h in logger.handlers:
            h.flush()
        # Find the log file
        log_files = list(log_dir.glob("*.log*"))
        assert len(log_files) >= 1
        content = log_files[0].read_text()
        # Should contain a valid JSON line with our message
        found = False
        for line in content.strip().splitlines():
            if not line:
                continue
            try:
                data = json.loads(line)
                if data.get("message") == "file handler test":
                    found = True
                    break
            except json.JSONDecodeError:
                pass
        assert found, f"Message not found in log. Content: {content!r}"


# ---------------------------------------------------------------------------
# AuditWriter (audit.py)
# ---------------------------------------------------------------------------

class TestStepAudit:
    """Tests for StepAudit dataclass."""

    def test_to_dict_returns_expected_keys(self):
        from ztract.observability.audit import StepAudit

        sa = StepAudit(
            step="decode",
            action="extract",
            source="input.bin",
            targets=["output.parquet"],
            records_read=100,
            records_written=95,
            records_rejected=5,
            reject_file="rejects.jsonl",
            status="success",
        )
        d = sa.to_dict()
        assert d["step"] == "decode"
        assert d["records_read"] == 100
        assert d["records_rejected"] == 5
        assert d["targets"] == ["output.parquet"]


class TestAuditEntry:
    """Tests for AuditEntry dataclass."""

    def test_timestamp_start_auto_set(self):
        from ztract.observability.audit import AuditEntry

        entry = AuditEntry(
            job_file="job.yaml",
            ztract_version="0.1.0",
            jre_version="17",
            job_file_hash="abc123",
            overall_status="success",
            exit_code=0,
        )
        assert entry.timestamp_start is not None

    def test_add_step_appends(self):
        from ztract.observability.audit import AuditEntry, StepAudit

        entry = AuditEntry(
            job_file="job.yaml",
            ztract_version="0.1.0",
            jre_version="17",
            job_file_hash="abc",
            overall_status="success",
            exit_code=0,
        )
        sa = StepAudit("s1", "extract", "src", [], 10, 10, 0, None, "success")
        entry.add_step(sa)
        assert len(entry.steps) == 1
        assert entry.steps[0].step == "s1"

    def test_to_dict_includes_audit_id_and_timestamps(self):
        from ztract.observability.audit import AuditEntry

        entry = AuditEntry(
            job_file="job.yaml",
            ztract_version="0.1.0",
            jre_version="17",
            job_file_hash="abc",
            overall_status="success",
            exit_code=0,
        )
        d = entry.to_dict()
        assert "audit_id" in d
        assert "timestamp_start" in d
        assert "timestamp_end" in d
        assert "user" in d
        assert "machine" in d

    def test_to_dict_steps_serialized(self):
        from ztract.observability.audit import AuditEntry, StepAudit

        entry = AuditEntry(
            job_file="j.yaml",
            ztract_version="0.1.0",
            jre_version="17",
            job_file_hash="h",
            overall_status="success",
            exit_code=0,
        )
        entry.add_step(StepAudit("s", "a", "src", ["t"], 1, 1, 0, None, "success"))
        d = entry.to_dict()
        assert isinstance(d["steps"], list)
        assert d["steps"][0]["step"] == "s"


class TestAuditWriter:
    """Tests for AuditWriter."""

    def _make_entry(self):
        from ztract.observability.audit import AuditEntry

        return AuditEntry(
            job_file="job.yaml",
            ztract_version="0.1.0",
            jre_version="17",
            job_file_hash="deadbeef",
            overall_status="success",
            exit_code=0,
        )

    def test_write_creates_file_and_parent_dirs(self, tmp_path):
        from ztract.observability.audit import AuditWriter

        audit_file = tmp_path / "audits" / "run.jsonl"
        writer = AuditWriter(audit_file)
        writer.write(self._make_entry())
        assert audit_file.exists()

    def test_write_appends_jsonl(self, tmp_path):
        from ztract.observability.audit import AuditWriter

        audit_file = tmp_path / "audit.jsonl"
        writer = AuditWriter(audit_file)
        writer.write(self._make_entry())
        writer.write(self._make_entry())
        writer.write(self._make_entry())
        lines = audit_file.read_text().strip().splitlines()
        assert len(lines) == 3

    def test_each_entry_has_unique_audit_id(self, tmp_path):
        from ztract.observability.audit import AuditWriter

        audit_file = tmp_path / "audit_ids.jsonl"
        writer = AuditWriter(audit_file)
        writer.write(self._make_entry())
        writer.write(self._make_entry())
        lines = audit_file.read_text().strip().splitlines()
        ids = [json.loads(l)["audit_id"] for l in lines]
        assert ids[0] != ids[1]

    def test_written_entry_has_required_fields(self, tmp_path):
        from ztract.observability.audit import AuditWriter

        audit_file = tmp_path / "audit_fields.jsonl"
        writer = AuditWriter(audit_file)
        writer.write(self._make_entry())
        data = json.loads(audit_file.read_text().strip())
        for field in ("audit_id", "job_file", "ztract_version", "jre_version",
                      "job_file_hash", "overall_status", "exit_code",
                      "timestamp_start", "timestamp_end", "user", "machine"):
            assert field in data, f"Missing field: {field}"

    def test_default_ztract_version_from_package(self, tmp_path):
        from ztract import __version__
        from ztract.observability.audit import AuditEntry, AuditWriter

        entry = AuditEntry(
            job_file="j.yaml",
            jre_version="17",
            job_file_hash="h",
            overall_status="success",
            exit_code=0,
        )
        audit_file = tmp_path / "audit_ver.jsonl"
        writer = AuditWriter(audit_file)
        writer.write(entry)
        data = json.loads(audit_file.read_text().strip())
        assert data["ztract_version"] == __version__


# ---------------------------------------------------------------------------
# ProgressTracker (progress.py)
# ---------------------------------------------------------------------------

class TestProgressTracker:
    """Tests for ProgressTracker."""

    def test_add_step_returns_task_id(self):
        from ztract.observability.progress import ProgressTracker

        tracker = ProgressTracker(quiet=True)
        task_id = tracker.add_step("loading", total=100)
        assert task_id is not None

    def test_update_advances_count(self):
        from ztract.observability.progress import ProgressTracker

        tracker = ProgressTracker(quiet=True)
        tid = tracker.add_step("processing", total=50)
        tracker.update(tid, advance=10)
        tracker.update(tid, advance=5)
        assert tracker.get_count(tid) == 15

    def test_default_advance_is_one(self):
        from ztract.observability.progress import ProgressTracker

        tracker = ProgressTracker(quiet=True)
        tid = tracker.add_step("writing")
        tracker.update(tid)
        tracker.update(tid)
        assert tracker.get_count(tid) == 2

    def test_multiple_steps_tracked_independently(self):
        from ztract.observability.progress import ProgressTracker

        tracker = ProgressTracker(quiet=True)
        t1 = tracker.add_step("step1", total=10)
        t2 = tracker.add_step("step2", total=20)
        tracker.update(t1, advance=3)
        tracker.update(t2, advance=7)
        tracker.update(t1, advance=2)
        assert tracker.get_count(t1) == 5
        assert tracker.get_count(t2) == 7

    def test_quiet_mode_does_not_crash(self):
        from ztract.observability.progress import ProgressTracker

        tracker = ProgressTracker(quiet=True)
        tid = tracker.add_step("task", total=100)
        for _ in range(10):
            tracker.update(tid)
        tracker.finish()
        assert tracker.get_count(tid) == 10

    def test_finish_callable_multiple_times(self):
        from ztract.observability.progress import ProgressTracker

        tracker = ProgressTracker(quiet=True)
        tracker.add_step("x", total=1)
        tracker.finish()
        tracker.finish()  # Should not raise

    def test_no_total_step_tracks_count(self):
        from ztract.observability.progress import ProgressTracker

        tracker = ProgressTracker(quiet=True)
        tid = tracker.add_step("unbounded")  # no total
        tracker.update(tid, advance=100)
        assert tracker.get_count(tid) == 100

    def test_non_quiet_mode_does_not_crash(self, monkeypatch):
        """Non-quiet mode should work even if not a real TTY."""
        from ztract.observability.progress import ProgressTracker

        # Simulate non-TTY environment so rich falls back gracefully
        monkeypatch.setattr(sys.stdout, "isatty", lambda: False, raising=False)
        tracker = ProgressTracker(quiet=False)
        tid = tracker.add_step("task", total=10)
        tracker.update(tid, advance=5)
        tracker.finish()
        assert tracker.get_count(tid) == 5
