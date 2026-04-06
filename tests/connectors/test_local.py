"""Tests for the LocalConnector."""
from __future__ import annotations

from pathlib import Path

import pytest

from ztract.connectors.local import LocalConnector


@pytest.fixture()
def connector() -> LocalConnector:
    return LocalConnector()


# ---------------------------------------------------------------------------
# download
# ---------------------------------------------------------------------------

class TestLocalConnectorDownload:
    """download() returns the same path, or raises on invalid input."""

    def test_download_returns_same_path(self, tmp_path: Path, connector: LocalConnector) -> None:
        data_file = tmp_path / "data.bin"
        data_file.write_bytes(b"\x00" * 100)
        result = connector.download(str(data_file), str(tmp_path / "ignored"))
        assert result == data_file

    def test_download_returns_path_object(self, tmp_path: Path, connector: LocalConnector) -> None:
        data_file = tmp_path / "sample.dat"
        data_file.write_bytes(b"some content")
        result = connector.download(str(data_file), "irrelevant")
        assert isinstance(result, Path)

    def test_download_missing_file_raises_file_not_found(
        self, tmp_path: Path, connector: LocalConnector
    ) -> None:
        missing = tmp_path / "nonexistent.dat"
        with pytest.raises(FileNotFoundError):
            connector.download(str(missing), str(tmp_path / "out"))

    def test_download_empty_file_raises_value_error(
        self, tmp_path: Path, connector: LocalConnector
    ) -> None:
        empty = tmp_path / "empty.dat"
        empty.write_bytes(b"")
        with pytest.raises(ValueError, match="empty"):
            connector.download(str(empty), str(tmp_path / "out"))

    def test_download_ignores_local_path_argument(
        self, tmp_path: Path, connector: LocalConnector
    ) -> None:
        """local_path is unused for LocalConnector — source is returned directly."""
        data_file = tmp_path / "real.dat"
        data_file.write_bytes(b"data")
        # Pass a nonsensical local_path — should have no effect
        result = connector.download(str(data_file), "/does/not/matter")
        assert result == data_file


# ---------------------------------------------------------------------------
# upload
# ---------------------------------------------------------------------------

class TestLocalConnectorUpload:
    """upload() copies the source file to the destination."""

    def test_upload_copies_file_content(self, tmp_path: Path, connector: LocalConnector) -> None:
        src = tmp_path / "source.bin"
        src.write_bytes(b"\xAB\xCD\xEF")
        dest = tmp_path / "out" / "destination.bin"
        connector.upload(str(src), str(dest))
        assert dest.read_bytes() == b"\xAB\xCD\xEF"

    def test_upload_creates_parent_directories(
        self, tmp_path: Path, connector: LocalConnector
    ) -> None:
        src = tmp_path / "input.dat"
        src.write_bytes(b"hello")
        deep_dest = tmp_path / "a" / "b" / "c" / "output.dat"
        connector.upload(str(src), str(deep_dest))
        assert deep_dest.exists()

    def test_upload_site_commands_ignored(
        self, tmp_path: Path, connector: LocalConnector
    ) -> None:
        src = tmp_path / "f.dat"
        src.write_bytes(b"x")
        dest = tmp_path / "copy.dat"
        # site_commands must be accepted without error
        connector.upload(str(src), str(dest), site_commands={"RECFM": "FB", "LRECL": "80"})
        assert dest.exists()


# ---------------------------------------------------------------------------
# exists
# ---------------------------------------------------------------------------

class TestLocalConnectorExists:
    """exists() returns True for present paths and False for absent ones."""

    def test_exists_returns_true_for_present_file(
        self, tmp_path: Path, connector: LocalConnector
    ) -> None:
        f = tmp_path / "present.dat"
        f.write_bytes(b"data")
        assert connector.exists(str(f)) is True

    def test_exists_returns_false_for_absent_file(
        self, tmp_path: Path, connector: LocalConnector
    ) -> None:
        absent = tmp_path / "absent.dat"
        assert connector.exists(str(absent)) is False

    def test_exists_returns_true_for_directory(
        self, tmp_path: Path, connector: LocalConnector
    ) -> None:
        assert connector.exists(str(tmp_path)) is True


# ---------------------------------------------------------------------------
# close
# ---------------------------------------------------------------------------

class TestLocalConnectorClose:
    """close() is a no-op and must not raise."""

    def test_close_is_noop(self, connector: LocalConnector) -> None:
        connector.close()  # must not raise

    def test_close_can_be_called_multiple_times(self, connector: LocalConnector) -> None:
        connector.close()
        connector.close()  # still must not raise
