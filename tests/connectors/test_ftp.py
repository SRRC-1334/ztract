"""Tests for FTPConnector — all network I/O is mocked via ftplib."""
from __future__ import annotations

import ftplib
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from ztract.connectors.ftp import FTPConnector


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def mock_ftp():
    """Return a MagicMock that stands in for ftplib.FTP."""
    ftp = MagicMock(spec=ftplib.FTP)
    ftp.__enter__ = lambda s: s
    ftp.__exit__ = MagicMock(return_value=False)
    return ftp


@pytest.fixture()
def connector(mock_ftp):
    """FTPConnector with _connect patched to return mock_ftp."""
    conn = FTPConnector.__new__(FTPConnector)
    conn.host = "mainframe.example.com"
    conn.user = "testuser"
    conn.password = "testpass"
    conn.port = 21
    conn.transfer_mode = "binary"
    conn.ftp_mode = "passive"
    conn.timeout = 30
    conn.retries = 3
    conn._ftp = mock_ftp
    return conn


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

class TestFTPConnectorInit:
    def test_defaults_are_set(self):
        """Constructor defaults are stored correctly without a real connection."""
        c = FTPConnector.__new__(FTPConnector)
        c.host = "h"
        c.user = "u"
        c.password = "p"
        c.port = 21
        c.transfer_mode = "binary"
        c.ftp_mode = "passive"
        c.timeout = 30
        c.retries = 3
        c._ftp = None
        assert c.port == 21
        assert c.transfer_mode == "binary"
        assert c.ftp_mode == "passive"
        assert c.timeout == 30
        assert c.retries == 3


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------

class TestFTPConnectorDownload:
    def test_download_binary_calls_retrbinary(self, connector, mock_ftp, tmp_path):
        """Binary transfer mode must use retrbinary."""
        dest = tmp_path / "output.dat"
        connector.download("SOME.DATASET", str(dest))
        mock_ftp.retrbinary.assert_called_once()
        cmd_arg = mock_ftp.retrbinary.call_args[0][0]
        assert "SOME.DATASET" in cmd_arg

    def test_download_text_calls_retrlines(self, connector, mock_ftp, tmp_path):
        """Text transfer mode must use retrlines."""
        connector.transfer_mode = "text"
        dest = tmp_path / "output.txt"
        connector.download("SOME.DATASET", str(dest))
        mock_ftp.retrlines.assert_called_once()

    def test_download_returns_path(self, connector, mock_ftp, tmp_path):
        dest = tmp_path / "out.dat"
        result = connector.download("DATASET.NAME", str(dest))
        assert isinstance(result, Path)
        assert result == dest

    def test_download_creates_parent_dirs(self, connector, mock_ftp, tmp_path):
        dest = tmp_path / "subdir" / "nested" / "out.dat"
        connector.download("DS.NAME", str(dest))
        assert dest.parent.exists()


# ---------------------------------------------------------------------------
# upload / SITE commands
# ---------------------------------------------------------------------------

class TestFTPConnectorUpload:
    def test_upload_calls_storbinary(self, connector, mock_ftp, tmp_path):
        src = tmp_path / "input.dat"
        src.write_bytes(b"EBCDIC data")
        connector.upload(str(src), "SOME.DATASET")
        mock_ftp.storbinary.assert_called_once()

    def test_upload_sends_site_commands(self, connector, mock_ftp, tmp_path):
        src = tmp_path / "input.dat"
        src.write_bytes(b"data")
        connector.upload(
            str(src),
            "SOME.DATASET",
            site_commands={"recfm": "FB", "lrecl": "80"},
        )
        sendcmd_calls = [str(c) for c in mock_ftp.sendcmd.call_args_list]
        site_calls = [c for c in sendcmd_calls if "SITE" in c.upper()]
        assert any("RECFM" in c.upper() for c in site_calls)
        assert any("LRECL" in c.upper() for c in site_calls)

    def test_site_command_order_recfm_before_lrecl_before_blksize(
        self, connector, mock_ftp, tmp_path
    ):
        """RECFM must come before LRECL, which must come before BLKSIZE."""
        src = tmp_path / "input.dat"
        src.write_bytes(b"data")
        # Pass dict with keys in reverse order to prove ordering is enforced
        connector.upload(
            str(src),
            "SOME.DATASET",
            site_commands={"blksize": "27920", "lrecl": "80", "recfm": "FB"},
        )
        site_calls = [
            str(c)
            for c in mock_ftp.sendcmd.call_args_list
            if "SITE" in str(c).upper()
        ]
        # Identify positions
        recfm_idx = next(i for i, c in enumerate(site_calls) if "RECFM" in c.upper())
        lrecl_idx = next(i for i, c in enumerate(site_calls) if "LRECL" in c.upper())
        blksize_idx = next(i for i, c in enumerate(site_calls) if "BLKSIZE" in c.upper())
        assert recfm_idx < lrecl_idx < blksize_idx

    def test_site_command_order_enforced_regardless_of_dict_order(
        self, connector, mock_ftp, tmp_path
    ):
        """Ordering must hold even when site_commands is given in arbitrary order."""
        src = tmp_path / "input.dat"
        src.write_bytes(b"data")
        # secondary before primary, storclas before mgmtclas
        connector.upload(
            str(src),
            "SOME.DS",
            site_commands={
                "secondary": "5",
                "primary": "10",
                "storclas": "SCLASS",
                "mgmtclas": "MCLASS",
                "space_unit": "CYL",
            },
        )
        site_calls = [
            str(c)
            for c in mock_ftp.sendcmd.call_args_list
            if "SITE" in str(c).upper()
        ]
        primary_idx = next(i for i, c in enumerate(site_calls) if "PRIMARY" in c.upper())
        secondary_idx = next(i for i, c in enumerate(site_calls) if "SECONDARY" in c.upper())
        mgmtclas_idx = next(i for i, c in enumerate(site_calls) if "MGMTCLAS" in c.upper())
        storclas_idx = next(i for i, c in enumerate(site_calls) if "STORCLAS" in c.upper())
        assert primary_idx < secondary_idx
        assert mgmtclas_idx < storclas_idx

    def test_space_unit_sent_as_keyword_only(self, connector, mock_ftp, tmp_path):
        """space_unit sends 'SITE CYLINDERS' (no = sign)."""
        src = tmp_path / "input.dat"
        src.write_bytes(b"data")
        connector.upload(str(src), "DS", site_commands={"space_unit": "CYL"})
        site_calls = [
            str(c)
            for c in mock_ftp.sendcmd.call_args_list
            if "SITE" in str(c).upper()
        ]
        space_call = next(c for c in site_calls if "CYL" in c.upper())
        assert "=" not in space_call

    def test_upload_no_site_commands_does_not_call_sendcmd(
        self, connector, mock_ftp, tmp_path
    ):
        src = tmp_path / "input.dat"
        src.write_bytes(b"data")
        connector.upload(str(src), "DS")
        mock_ftp.sendcmd.assert_not_called()


# ---------------------------------------------------------------------------
# list_datasets
# ---------------------------------------------------------------------------

class TestFTPConnectorListDatasets:
    def test_list_datasets_returns_list(self, connector, mock_ftp):
        mock_ftp.nlst.return_value = ["MY.DS.ONE", "MY.DS.TWO"]
        result = connector.list_datasets("MY.DS.*")
        assert isinstance(result, list)
        assert "MY.DS.ONE" in result

    def test_list_datasets_passes_pattern(self, connector, mock_ftp):
        mock_ftp.nlst.return_value = []
        connector.list_datasets("HLQ.DATA.*")
        mock_ftp.nlst.assert_called_once_with("HLQ.DATA.*")


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------

class TestFTPConnectorExists:
    def test_exists_true_when_size_succeeds(self, connector, mock_ftp):
        mock_ftp.size.return_value = 1024
        assert connector.exists("SOME.DATASET") is True

    def test_exists_false_when_size_raises(self, connector, mock_ftp):
        mock_ftp.size.side_effect = ftplib.error_perm("550 Not found")
        assert connector.exists("MISSING.DS") is False


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------

class TestFTPConnectorClose:
    def test_close_calls_quit(self, connector, mock_ftp):
        connector.close()
        mock_ftp.quit.assert_called_once()

    def test_close_noop_when_no_connection(self):
        conn = FTPConnector.__new__(FTPConnector)
        conn._ftp = None
        conn.close()  # must not raise
