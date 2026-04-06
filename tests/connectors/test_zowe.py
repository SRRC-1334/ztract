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


def _make_connector(
    profile: str = "default",
    backend: str = "zosmf",
    transfer_mode: str = "binary",
    encoding: str | None = None,
) -> ZoweConnector:
    """Build a ZoweConnector with check_zowe bypassed."""
    with patch.object(ZoweConnector, "check_zowe", return_value={"zowe_version": "3", "backend": backend}):
        return ZoweConnector(
            profile=profile,
            backend=backend,
            transfer_mode=transfer_mode,
            encoding=encoding,
        )


@pytest.fixture()
def connector():
    """Default zosmf/binary connector."""
    return _make_connector()


@pytest.fixture()
def zftp_connector():
    """zftp backend connector."""
    return _make_connector(backend="zftp")


# ---------------------------------------------------------------------------
# ZoweError
# ---------------------------------------------------------------------------


class TestZoweError:
    def test_is_runtime_error(self):
        assert isinstance(ZoweError("x"), RuntimeError)

    def test_message_preserved(self):
        assert "zowe not found" in str(ZoweError("zowe not found"))


# ---------------------------------------------------------------------------
# check_zowe
# ---------------------------------------------------------------------------


class TestCheckZowe:
    def test_version_detection_parses_major(self):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="@zowe-cli/core/3.0.0 linux-x64 node-v18.0.0\n",
                returncode=0,
            )
            conn = ZoweConnector.__new__(ZoweConnector)
            conn.profile = "default"
            conn.backend = "zosmf"
            conn.transfer_mode = "binary"
            conn.encoding = None
            conn._zowe_version = None
            info = conn.check_zowe()
        assert info["zowe_version"] == "3"
        assert info["backend"] == "zosmf"

    def test_version_detection_parses_major_v2(self):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                stdout="@zowe-cli/core/2.17.0 linux-x64 node-v18.0.0\n",
                returncode=0,
            )
            conn = ZoweConnector.__new__(ZoweConnector)
            conn.profile = "default"
            conn.backend = "zosmf"
            conn.transfer_mode = "binary"
            conn.encoding = None
            conn._zowe_version = None
            info = conn.check_zowe()
        assert info["zowe_version"] == "2"

    def test_zowe_not_found_raises_zowe_error(self):
        with patch("subprocess.run", side_effect=FileNotFoundError("zowe")):
            conn = ZoweConnector.__new__(ZoweConnector)
            conn.profile = "default"
            conn.backend = "zosmf"
            conn.transfer_mode = "binary"
            conn.encoding = None
            conn._zowe_version = None
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
            conn.backend = "zosmf"
            conn.transfer_mode = "binary"
            conn.encoding = None
            conn._zowe_version = None
            with pytest.raises(ZoweError, match="v2"):
                conn.check_zowe()

    def test_check_zowe_detects_missing_zftp_plugin(self):
        """When backend=zftp, check_zowe must verify the plugin is installed."""
        with patch("subprocess.run") as mock_run:
            # First call: --version succeeds
            # Second call: plugins list has no zos-ftp
            mock_run.side_effect = [
                MagicMock(stdout="3.0.0\n", returncode=0),
                MagicMock(stdout="@zowe/cli\n@zowe/secure\n", returncode=0),
            ]
            conn = ZoweConnector.__new__(ZoweConnector)
            conn.profile = "default"
            conn.backend = "zftp"
            conn.transfer_mode = "binary"
            conn.encoding = None
            conn._zowe_version = None
            with pytest.raises(ZoweError, match="zos-ftp plugin"):
                conn.check_zowe()

    def test_check_zowe_accepts_installed_zftp_plugin(self):
        with patch("subprocess.run") as mock_run:
            mock_run.side_effect = [
                MagicMock(stdout="3.0.0\n", returncode=0),
                MagicMock(stdout="@zowe/cli\n@zowe/zos-ftp-for-zowe-cli\n", returncode=0),
            ]
            conn = ZoweConnector.__new__(ZoweConnector)
            conn.profile = "default"
            conn.backend = "zftp"
            conn.transfer_mode = "binary"
            conn.encoding = None
            conn._zowe_version = None
            info = conn.check_zowe()
        assert info["zftp_plugin"] == "installed"


# ---------------------------------------------------------------------------
# download (zosmf)
# ---------------------------------------------------------------------------


class TestZoweConnectorDownload:
    def test_download_calls_zowe_with_binary_flag(self, connector, tmp_path):
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
# upload (zosmf)
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
# exists / list / close
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


class TestZoweConnectorListDatasets:
    def test_list_datasets_returns_list(self, connector):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout="HLQ.DS.ONE\nHLQ.DS.TWO\n"
            )
            result = connector.list_datasets("HLQ.DS.*")
        assert "HLQ.DS.ONE" in result
        assert "HLQ.DS.TWO" in result

    def test_list_datasets_empty_output(self, connector):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="")
            result = connector.list_datasets("NOTHING.*")
        assert result == []


class TestZoweConnectorClose:
    def test_close_is_noop(self, connector):
        connector.close()

    def test_close_multiple_times(self, connector):
        connector.close()
        connector.close()


# ---------------------------------------------------------------------------
# Transfer modes
# ---------------------------------------------------------------------------


class TestTransferModes:
    def test_binary_adds_binary_flag(self, tmp_path):
        conn = _make_connector(transfer_mode="binary")
        dest = tmp_path / "out.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            conn.download("DS.NAME", str(dest))
        args = mock_run.call_args[0][0]
        assert "--binary" in args

    def test_text_mode_no_flag(self, tmp_path):
        conn = _make_connector(transfer_mode="text")
        dest = tmp_path / "out.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            conn.download("DS.NAME", str(dest))
        args = mock_run.call_args[0][0]
        assert "--binary" not in args
        assert "--encoding" not in args
        assert "--rdw" not in args

    def test_encoding_mode_adds_encoding_flag(self, tmp_path):
        conn = _make_connector(transfer_mode="encoding", encoding="cp277")
        dest = tmp_path / "out.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            conn.download("DS.NAME", str(dest))
        args = mock_run.call_args[0][0]
        assert "--encoding" in args
        idx = args.index("--encoding")
        assert args[idx + 1] == "cp277"

    def test_encoding_mode_without_encoding_raises(self):
        conn = _make_connector(transfer_mode="encoding", encoding=None)
        with pytest.raises(ValueError, match="encoding"):
            conn._transfer_args("download")

    def test_record_mode_adds_rdw_flag(self, tmp_path):
        conn = _make_connector(backend="zftp", transfer_mode="record")
        dest = tmp_path / "out.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            conn.download("DS.NAME", str(dest))
        args = mock_run.call_args[0][0]
        assert "--rdw" in args

    def test_record_mode_raises_for_zosmf(self):
        conn = _make_connector(backend="zosmf", transfer_mode="record")
        with pytest.raises(ValueError, match="zftp"):
            conn._transfer_args("download")


# ---------------------------------------------------------------------------
# zftp backend
# ---------------------------------------------------------------------------


class TestZftpBackend:
    def test_zftp_uses_zos_ftp_command_group(self, zftp_connector, tmp_path):
        dest = tmp_path / "out.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            zftp_connector.download("DS.NAME", str(dest))
        args = mock_run.call_args[0][0]
        assert "zos-ftp" in args
        assert "zos-files" not in args

    def test_zftp_uses_zftp_profile_flag(self, zftp_connector, tmp_path):
        dest = tmp_path / "out.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            zftp_connector.download("DS.NAME", str(dest))
        args = mock_run.call_args[0][0]
        assert "--zftp-profile" in args
        assert "--zosmf-profile" not in args

    def test_zosmf_uses_zosmf_profile_flag(self, connector, tmp_path):
        dest = tmp_path / "out.dat"
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            connector.download("DS.NAME", str(dest))
        args = mock_run.call_args[0][0]
        assert "--zosmf-profile" in args
        assert "--zftp-profile" not in args

    def test_zftp_upload_with_dcb(self, zftp_connector, tmp_path):
        src = tmp_path / "data.dat"
        src.write_bytes(b"content")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            zftp_connector.upload(str(src), "HLQ.OUT", dcb="RECFM=FB LRECL=500")
        args = mock_run.call_args[0][0]
        assert "--dcb" in args
        idx = args.index("--dcb")
        assert args[idx + 1] == "RECFM=FB LRECL=500"

    def test_zosmf_upload_ignores_dcb(self, connector, tmp_path):
        src = tmp_path / "data.dat"
        src.write_bytes(b"content")
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            connector.upload(str(src), "HLQ.OUT", dcb="RECFM=FB LRECL=500")
        args = mock_run.call_args[0][0]
        assert "--dcb" not in args

    def test_zftp_exists_uses_zos_ftp(self, zftp_connector):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0)
            zftp_connector.exists("DS.NAME")
        args = mock_run.call_args[0][0]
        assert "zos-ftp" in args

    def test_zftp_list_uses_zos_ftp(self, zftp_connector):
        with patch("subprocess.run") as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout="DS.A\nDS.B\n")
            zftp_connector.list_datasets("DS.*")
        args = mock_run.call_args[0][0]
        assert "zos-ftp" in args
