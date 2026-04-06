"""Tests for ZoweConnector — all subprocess calls are mocked."""
from __future__ import annotations

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ztract.connectors.zowe import ZoweConnector, ZoweError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def connector():
    """ZoweConnector with check_zowe bypassed."""
    with patch.object(ZoweConnector, "check_zowe", return_value="3"):
        return ZoweConnector("default")


# ---------------------------------------------------------------------------
# ZoweError
# ---------------------------------------------------------------------------

class TestZoweError:
    def test_is_runtime_error(self):
        err = ZoweError("something went wrong")
        assert isinstance(err, RuntimeError)

    def test_message_preserved(self):
        err = ZoweError("zowe not found")
        assert "zowe not found" in str(err)


# ---------------------------------------------------------------------------
# check_zowe
# ---------------------------------------------------------------------------

class TestCheckZowe:
    def test_version_detection_parses_major(self):
        """'3.0.0' should yield major version '3'."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="@zowe-cli/core/3.0.0 linux-x64 node-v18.0.0\n",
                returncode=0,
            )
            conn = ZoweConnector.__new__(ZoweConnector)
            conn.profile = "default"
            version = conn.check_zowe()
        assert version == "3"

    def test_version_detection_parses_major_v2(self):
        """'2.17.0' should yield major version '2'."""
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="@zowe-cli/core/2.17.0 linux-x64 node-v18.0.0\n",
                returncode=0,
            )
            conn = ZoweConnector.__new__(ZoweConnector)
            conn.profile = "default"
            version = conn.check_zowe()
        assert version == "2"

    def test_zowe_not_found_raises_zowe_error(self):
        with patch("subprocess.run", side_effect=FileNotFoundError("zowe")):
            conn = ZoweConnector.__new__(ZoweConnector)
            conn.profile = "default"
            with pytest.raises(ZoweError, match="not found"):
                conn.check_zowe()

    def test_version_below_v2_raises_zowe_error(self):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="@zowe-cli/core/1.27.0 linux-x64 node-v14.0.0\n",
                returncode=0,
            )
            conn = ZoweConnector.__new__(ZoweConnector)
            conn.profile = "default"
            with pytest.raises(ZoweError, match="v2"):
                conn.check_zowe()

    def test_subprocess_called_error_raises_zowe_error(self):
        with patch(
            "subprocess.run",
            side_effect=subprocess.CalledProcessError(1, "zowe"),
        ):
            conn = ZoweConnector.__new__(ZoweConnector)
            conn.profile = "default"
            with pytest.raises(ZoweError):
                conn.check_zowe()


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------

class TestZoweConnectorDownload:
    def test_download_calls_zowe_with_binary_flag(self, connector, tmp_path):
        """zowe zos-files download must include --binary."""
        dest = tmp_path / "output.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            connector.download("HLQ.DATASET.NAME", str(dest))
        args = mock_run.call_args[0][0]
        assert "--binary" in args

    def test_download_includes_dataset_name(self, connector, tmp_path):
        dest = tmp_path / "output.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            connector.download("HLQ.DATASET.NAME", str(dest))
        args = mock_run.call_args[0][0]
        assert "HLQ.DATASET.NAME" in args

    def test_download_returns_path(self, connector, tmp_path):
        dest = tmp_path / "output.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            result = connector.download("HLQ.DS", str(dest))
        assert isinstance(result, Path)
        assert result == dest

    def test_download_uses_zos_files_subcommand(self, connector, tmp_path):
        dest = tmp_path / "output.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            connector.download("DS.NAME", str(dest))
        args = mock_run.call_args[0][0]
        assert "zos-files" in args
        assert "download" in args

    def test_download_failure_raises_zowe_error(self, connector, tmp_path):
        dest = tmp_path / "output.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="Error!")
            with pytest.raises(ZoweError):
                connector.download("MISSING.DS", str(dest))


# ---------------------------------------------------------------------------
# upload
# ---------------------------------------------------------------------------

class TestZoweConnectorUpload:
    def test_upload_calls_zowe_upload(self, connector, tmp_path):
        src = tmp_path / "input.dat"
        src.write_bytes(b"data")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            connector.upload(str(src), "HLQ.DEST.DS")
        args = mock_run.call_args[0][0]
        assert "upload" in args
        assert "zos-files" in args

    def test_upload_includes_src_and_destination(self, connector, tmp_path):
        src = tmp_path / "input.dat"
        src.write_bytes(b"data")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            connector.upload(str(src), "HLQ.DEST.DS")
        args = mock_run.call_args[0][0]
        assert str(src) in args
        assert "HLQ.DEST.DS" in args

    def test_upload_failure_raises_zowe_error(self, connector, tmp_path):
        src = tmp_path / "input.dat"
        src.write_bytes(b"data")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1, stderr="Upload failed")
            with pytest.raises(ZoweError):
                connector.upload(str(src), "HLQ.DEST.DS")


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------

class TestZoweConnectorExists:
    def test_exists_true_when_command_succeeds(self, connector):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            assert connector.exists("HLQ.PRESENT.DS") is True

    def test_exists_false_when_command_fails(self, connector):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=1)
            assert connector.exists("HLQ.MISSING.DS") is False


# ---------------------------------------------------------------------------
# list_datasets
# ---------------------------------------------------------------------------

class TestZoweConnectorListDatasets:
    def test_list_datasets_returns_list(self, connector):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0,
                stdout="HLQ.DS.ONE\nHLQ.DS.TWO\n",
            )
            result = connector.list_datasets("HLQ.DS.*")
        assert isinstance(result, list)
        assert "HLQ.DS.ONE" in result
        assert "HLQ.DS.TWO" in result

    def test_list_datasets_empty_output(self, connector):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            result = connector.list_datasets("NOTHING.*")
        assert result == []


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------

class TestZoweConnectorClose:
    def test_close_is_noop(self, connector):
        connector.close()  # must not raise

    def test_close_multiple_times(self, connector):
        connector.close()
        connector.close()
