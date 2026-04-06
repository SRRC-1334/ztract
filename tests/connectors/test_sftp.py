"""Tests for SFTPConnector — all network I/O is mocked via paramiko."""
from __future__ import annotations

import warnings
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ztract.connectors.sftp import SFTPConnector


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_sftp():
    """Return a MagicMock standing in for paramiko.SFTPClient."""
    return MagicMock()


@pytest.fixture()
def mock_transport():
    """Return a MagicMock standing in for paramiko.Transport."""
    return MagicMock()


@pytest.fixture()
def connector(mock_sftp, mock_transport):
    """SFTPConnector with _connect patched to inject mocks."""
    conn = SFTPConnector.__new__(SFTPConnector)
    conn.host = "sftp.example.com"
    conn.user = "testuser"
    conn.password = "testpass"
    conn.key_path = None
    conn.port = 22
    conn._sftp = mock_sftp
    conn._transport = mock_transport
    return conn


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestSFTPConnectorInit:
    def test_defaults(self):
        with patch("paramiko.Transport"), patch("paramiko.SFTPClient.from_transport"):
            conn = SFTPConnector.__new__(SFTPConnector)
            conn.host = "h"
            conn.user = "u"
            conn.password = None
            conn.key_path = None
            conn.port = 22
            conn._sftp = None
            conn._transport = None
            assert conn.port == 22
            assert conn.key_path is None


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------

class TestSFTPConnectorDownload:
    def test_download_calls_sftp_get(self, connector, mock_sftp, tmp_path):
        dest = tmp_path / "output.dat"
        connector.download("/mainframe/SOME.DATASET", str(dest))
        mock_sftp.get.assert_called_once_with("/mainframe/SOME.DATASET", str(dest))

    def test_download_returns_path(self, connector, mock_sftp, tmp_path):
        dest = tmp_path / "output.dat"
        result = connector.download("/ds/NAME", str(dest))
        assert isinstance(result, Path)
        assert result == dest

    def test_download_creates_parent_dirs(self, connector, mock_sftp, tmp_path):
        dest = tmp_path / "a" / "b" / "out.dat"
        connector.download("/ds/NAME", str(dest))
        assert dest.parent.exists()


# ---------------------------------------------------------------------------
# upload
# ---------------------------------------------------------------------------

class TestSFTPConnectorUpload:
    def test_upload_calls_sftp_put(self, connector, mock_sftp, tmp_path):
        src = tmp_path / "input.dat"
        src.write_bytes(b"data")
        connector.upload(str(src), "/remote/DEST.DS")
        mock_sftp.put.assert_called_once_with(str(src), "/remote/DEST.DS")

    def test_upload_warns_if_site_commands_provided(
        self, connector, mock_sftp, tmp_path
    ):
        """SFTPConnector does not support SITE commands — must warn."""
        src = tmp_path / "input.dat"
        src.write_bytes(b"data")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            connector.upload(
                str(src), "/remote/DS", site_commands={"recfm": "FB"}
            )
        messages = [str(w.message) for w in caught]
        assert any("site_commands" in m.lower() or "site" in m.lower() for m in messages)

    def test_upload_no_warning_without_site_commands(
        self, connector, mock_sftp, tmp_path
    ):
        src = tmp_path / "input.dat"
        src.write_bytes(b"data")
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            connector.upload(str(src), "/remote/DS")
        # No UserWarning about site_commands
        site_warnings = [
            w for w in caught
            if "site" in str(w.message).lower()
        ]
        assert len(site_warnings) == 0


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------

class TestSFTPConnectorExists:
    def test_exists_true_when_stat_succeeds(self, connector, mock_sftp):
        mock_sftp.stat.return_value = MagicMock()
        assert connector.exists("/remote/DATASET") is True

    def test_exists_false_when_stat_raises_file_not_found(
        self, connector, mock_sftp
    ):
        mock_sftp.stat.side_effect = FileNotFoundError
        assert connector.exists("/remote/MISSING") is False

    def test_exists_false_when_stat_raises_ioerror(
        self, connector, mock_sftp
    ):
        import paramiko
        mock_sftp.stat.side_effect = IOError("No such file")
        assert connector.exists("/remote/MISSING") is False


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------

class TestSFTPConnectorClose:
    def test_close_closes_sftp_and_transport(self, connector, mock_sftp, mock_transport):
        connector.close()
        mock_sftp.close.assert_called_once()
        mock_transport.close.assert_called_once()

    def test_close_noop_when_no_connection(self):
        conn = SFTPConnector.__new__(SFTPConnector)
        conn._sftp = None
        conn._transport = None
        conn.close()  # must not raise
