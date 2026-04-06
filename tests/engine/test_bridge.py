"""Tests for ZtractBridge - Java Engine Bridge.

All subprocess calls are mocked. Tests are written first (TDD).
"""

import json
import subprocess
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

from ztract.engine.bridge import (
    EngineError,
    JREError,
    ValidationReport,
    ZtractBridge,
)

JAR = Path("/fake/ztract-engine.jar")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_bridge(**kwargs) -> ZtractBridge:
    return ZtractBridge(jar_path=JAR, **kwargs)


def _completed(stdout: str = "", stderr: str = "", returncode: int = 0):
    """Return a fake CompletedProcess."""
    result = MagicMock()
    result.stdout = stdout
    result.stderr = stderr
    result.returncode = returncode
    return result


# ---------------------------------------------------------------------------
# TestCheckJRE
# ---------------------------------------------------------------------------

class TestCheckJRE:
    """check_jre() parses 'java -version' stderr and returns major version."""

    def test_jre_17_returns_17(self):
        stderr = 'openjdk version "17.0.2" 2022-01-18\nOpenJDK Runtime Environment ...'
        with patch("subprocess.run", return_value=_completed(stderr=stderr)) as mock_run:
            bridge = _make_bridge()
            version = bridge.check_jre()
        assert version == "17"
        mock_run.assert_called_once()

    def test_jre_11_returns_11(self):
        stderr = 'openjdk version "11.0.20" 2023-07-18\nOpenJDK Runtime Environment ...'
        with patch("subprocess.run", return_value=_completed(stderr=stderr)):
            bridge = _make_bridge()
            assert bridge.check_jre() == "11"

    def test_jre_21_returns_21(self):
        stderr = 'openjdk version "21.0.1" 2023-10-17\nOpenJDK Runtime Environment ...'
        with patch("subprocess.run", return_value=_completed(stderr=stderr)):
            bridge = _make_bridge()
            assert bridge.check_jre() == "21"

    def test_jre_8_old_format_raises_jre_error(self):
        stderr = 'java version "1.8.0_301"\nJava(TM) SE Runtime Environment ...'
        with patch("subprocess.run", return_value=_completed(stderr=stderr)):
            bridge = _make_bridge()
            with pytest.raises(JREError, match="Java 11 or later"):
                bridge.check_jre()

    def test_java_not_found_raises_jre_error(self):
        with patch("subprocess.run", side_effect=FileNotFoundError):
            bridge = _make_bridge()
            with pytest.raises(JREError, match="Java not found"):
                bridge.check_jre()

    def test_error_message_contains_adoptium_link(self):
        stderr = 'java version "1.8.0_301"\nJava(TM) SE Runtime Environment ...'
        with patch("subprocess.run", return_value=_completed(stderr=stderr)):
            bridge = _make_bridge()
            with pytest.raises(JREError) as exc_info:
                bridge.check_jre()
            assert "adoptium.net" in str(exc_info.value)

    def test_caches_version_on_second_call(self):
        stderr = 'openjdk version "17.0.2" 2022-01-18\nOpenJDK Runtime Environment ...'
        with patch("subprocess.run", return_value=_completed(stderr=stderr)) as mock_run:
            bridge = _make_bridge()
            bridge.check_jre()
            bridge.check_jre()
        # subprocess.run should only be called once (second call uses cache)
        assert mock_run.call_count == 1


# ---------------------------------------------------------------------------
# TestGetSchema
# ---------------------------------------------------------------------------

class TestGetSchema:
    """get_schema() returns parsed JSON dict from engine stdout."""

    def test_returns_parsed_schema_dict(self):
        schema = {"fields": [{"name": "ID", "type": "PIC 9(5)"}]}
        completed = _completed(stdout=json.dumps(schema))
        with patch("subprocess.run", return_value=completed):
            bridge = _make_bridge()
            result = bridge.get_schema(copybook=Path("/fake/test.cpy"))
        assert result == schema

    def test_returns_schema_with_recfm_and_lrecl(self):
        schema = {"fields": [], "recfm": "FB", "lrecl": 80}
        completed = _completed(stdout=json.dumps(schema))
        with patch("subprocess.run", return_value=completed) as mock_run:
            bridge = _make_bridge()
            result = bridge.get_schema(
                copybook=Path("/fake/test.cpy"), recfm="FB", lrecl=80
            )
        assert result == schema
        cmd = mock_run.call_args[0][0]
        assert "--recfm" in cmd
        assert "FB" in cmd
        assert "--lrecl" in cmd
        assert "80" in cmd

    def test_engine_error_raises_engine_error(self):
        completed = _completed(stdout="", returncode=1, stderr="ERROR: bad copybook")
        with patch("subprocess.run", return_value=completed):
            bridge = _make_bridge()
            with pytest.raises(EngineError):
                bridge.get_schema(copybook=Path("/fake/test.cpy"))

    def test_schema_only_flag_in_command(self):
        schema = {"fields": []}
        completed = _completed(stdout=json.dumps(schema))
        with patch("subprocess.run", return_value=completed) as mock_run:
            bridge = _make_bridge()
            bridge.get_schema(copybook=Path("/fake/test.cpy"))
        cmd = mock_run.call_args[0][0]
        assert "--schema-only" in cmd


# ---------------------------------------------------------------------------
# TestDecode
# ---------------------------------------------------------------------------

class TestDecode:
    """decode() yields dicts from JSON Lines stdout."""

    def _make_popen(self, lines: list[str]):
        """Build a mock Popen context manager yielding the given lines."""
        mock_proc = MagicMock()
        # stdout is an iterator of byte-encoded lines
        mock_proc.stdout = iter(
            (line.encode() + b"\n") for line in lines
        )
        mock_proc.wait.return_value = 0
        mock_proc.returncode = 0
        mock_proc.__enter__ = lambda s: s
        mock_proc.__exit__ = MagicMock(return_value=False)
        return mock_proc

    def test_yields_records_from_stdout(self):
        records = [{"ID": 1, "NAME": "ALICE"}, {"ID": 2, "NAME": "BOB"}]
        lines = [json.dumps(r) for r in records]
        mock_proc = self._make_popen(lines)

        with patch("subprocess.Popen", return_value=mock_proc):
            bridge = _make_bridge()
            result = list(
                bridge.decode(
                    copybook=Path("/fake/test.cpy"),
                    input_path=Path("/fake/data.bin"),
                    recfm="FB",
                    lrecl=80,
                    codepage="cp037",
                )
            )
        assert result == records

    def test_yields_empty_when_no_records(self):
        mock_proc = self._make_popen([])
        with patch("subprocess.Popen", return_value=mock_proc):
            bridge = _make_bridge()
            result = list(
                bridge.decode(
                    copybook=Path("/fake/test.cpy"),
                    input_path=Path("/fake/data.bin"),
                    recfm="FB",
                    lrecl=80,
                    codepage="cp037",
                )
            )
        assert result == []

    def test_yields_single_record(self):
        record = {"FIELD": "VALUE"}
        mock_proc = self._make_popen([json.dumps(record)])
        with patch("subprocess.Popen", return_value=mock_proc):
            bridge = _make_bridge()
            result = list(
                bridge.decode(
                    copybook=Path("/fake/test.cpy"),
                    input_path=Path("/fake/data.bin"),
                    recfm="FB",
                    lrecl=80,
                    codepage="cp037",
                )
            )
        assert result == [record]


# ---------------------------------------------------------------------------
# TestEncode
# ---------------------------------------------------------------------------

class TestEncode:
    """encode() writes JSON Lines to stdin and returns the record count."""

    def _make_popen(self, returncode: int = 0):
        mock_proc = MagicMock()
        mock_proc.stdin = MagicMock()
        mock_proc.wait.return_value = returncode
        mock_proc.returncode = returncode
        mock_proc.__enter__ = lambda s: s
        mock_proc.__exit__ = MagicMock(return_value=False)
        return mock_proc

    def test_returns_record_count(self):
        records = [{"ID": 1}, {"ID": 2}, {"ID": 3}]
        mock_proc = self._make_popen()

        with patch("subprocess.Popen", return_value=mock_proc):
            bridge = _make_bridge()
            count = bridge.encode(
                copybook=Path("/fake/test.cpy"),
                output_path=Path("/fake/out.bin"),
                recfm="FB",
                lrecl=80,
                codepage="cp037",
                records=iter(records),
            )
        assert count == 3

    def test_writes_json_lines_to_stdin(self):
        records = [{"ID": 1}, {"ID": 2}]
        mock_proc = self._make_popen()

        with patch("subprocess.Popen", return_value=mock_proc):
            bridge = _make_bridge()
            bridge.encode(
                copybook=Path("/fake/test.cpy"),
                output_path=Path("/fake/out.bin"),
                recfm="FB",
                lrecl=80,
                codepage="cp037",
                records=iter(records),
            )

        written_calls = mock_proc.stdin.write.call_args_list
        written_bytes = b"".join(c[0][0] for c in written_calls)
        lines = [l for l in written_bytes.splitlines() if l]
        assert len(lines) == 2
        assert json.loads(lines[0]) == {"ID": 1}
        assert json.loads(lines[1]) == {"ID": 2}

    def test_returns_zero_for_empty_records(self):
        mock_proc = self._make_popen()
        with patch("subprocess.Popen", return_value=mock_proc):
            bridge = _make_bridge()
            count = bridge.encode(
                copybook=Path("/fake/test.cpy"),
                output_path=Path("/fake/out.bin"),
                recfm="FB",
                lrecl=80,
                codepage="cp037",
                records=iter([]),
            )
        assert count == 0


# ---------------------------------------------------------------------------
# TestStderrClassification
# ---------------------------------------------------------------------------

class TestStderrClassification:
    """_classify_stderr() categorises JVM output lines."""

    def setup_method(self):
        self.bridge = _make_bridge()

    def test_exception_in_thread_is_fatal(self):
        assert self.bridge._classify_stderr("Exception in thread main") == "fatal"

    def test_out_of_memory_error_is_fatal(self):
        assert self.bridge._classify_stderr("OutOfMemoryError: Java heap space") == "fatal"

    def test_error_prefix_is_fatal(self):
        assert self.bridge._classify_stderr("ERROR: bad copybook") == "fatal"

    def test_warn_prefix_is_warning(self):
        assert self.bridge._classify_stderr("WARN: truncated record") == "warning"

    def test_empty_line_is_ignore(self):
        assert self.bridge._classify_stderr("") == "ignore"

    def test_jvm_noise_is_ignore(self):
        assert (
            self.bridge._classify_stderr("Picked up JAVA_TOOL_OPTIONS: -Xmx512m")
            == "ignore"
        )

    def test_generic_info_line_is_ignore(self):
        assert self.bridge._classify_stderr("OpenJDK 17.0.2 running") == "ignore"


# ---------------------------------------------------------------------------
# TestValidate
# ---------------------------------------------------------------------------

class TestValidate:
    """validate() returns a ValidationReport from engine output."""

    def test_returns_validation_report(self):
        report_data = {
            "records_decoded": 500,
            "records_warnings": 10,
            "records_errors": 2,
            "field_stats": {"ID": {"nulls": 0}},
        }
        completed = _completed(stdout=json.dumps(report_data))
        with patch("subprocess.run", return_value=completed):
            bridge = _make_bridge()
            report = bridge.validate(
                copybook=Path("/fake/test.cpy"),
                input_path=Path("/fake/data.bin"),
                recfm="FB",
                lrecl=80,
                codepage="cp037",
            )
        assert isinstance(report, ValidationReport)
        assert report.records_decoded == 500
        assert report.records_warnings == 10
        assert report.records_errors == 2
        assert report.field_stats == {"ID": {"nulls": 0}}

    def test_validate_mode_flag_in_command(self):
        report_data = {"records_decoded": 0, "records_warnings": 0, "records_errors": 0, "field_stats": {}}
        completed = _completed(stdout=json.dumps(report_data))
        with patch("subprocess.run", return_value=completed) as mock_run:
            bridge = _make_bridge()
            bridge.validate(
                copybook=Path("/fake/test.cpy"),
                input_path=Path("/fake/data.bin"),
                recfm="FB",
                lrecl=80,
                codepage="cp037",
                sample=500,
            )
        cmd = mock_run.call_args[0][0]
        assert "--mode" in cmd
        assert "validate" in cmd
        assert "--sample" in cmd
        assert "500" in cmd


# ---------------------------------------------------------------------------
# TestShutdown
# ---------------------------------------------------------------------------

class TestShutdown:
    """shutdown() terminates any active subprocess."""

    def test_shutdown_terminates_process(self):
        bridge = _make_bridge()
        mock_proc = MagicMock()
        mock_proc.wait.return_value = 0
        bridge._active_proc = mock_proc

        bridge.shutdown()

        mock_proc.terminate.assert_called_once()
        mock_proc.wait.assert_called_once()
