"""Tests for ztract.engine.download_engine — JAR auto-download."""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from ztract.engine.download_engine import ensure_jar, get_jar_path


# ---------------------------------------------------------------------------
# TestGetJarPath
# ---------------------------------------------------------------------------

class TestGetJarPath:
    def test_returns_path_object(self):
        path = get_jar_path()
        assert isinstance(path, Path)

    def test_filename_is_ztract_engine_jar(self):
        path = get_jar_path()
        assert path.name == "ztract-engine.jar"

    def test_path_is_inside_engine_package(self):
        path = get_jar_path()
        # Should be in the same directory as download_engine.py
        assert path.parent.name == "engine"


# ---------------------------------------------------------------------------
# TestEnsureJar
# ---------------------------------------------------------------------------

class TestEnsureJar:
    def test_returns_existing_jar_without_downloading(self, tmp_path):
        fake_jar = tmp_path / "ztract-engine.jar"
        fake_jar.write_bytes(b"PK fake jar")

        with patch("ztract.engine.download_engine.get_jar_path", return_value=fake_jar):
            result = ensure_jar()

        assert result == fake_jar

    def test_does_not_call_urlretrieve_when_jar_exists(self, tmp_path):
        fake_jar = tmp_path / "ztract-engine.jar"
        fake_jar.write_bytes(b"PK fake jar")

        with patch("ztract.engine.download_engine.get_jar_path", return_value=fake_jar):
            with patch("urllib.request.urlretrieve") as mock_retrieve:
                ensure_jar()

        mock_retrieve.assert_not_called()

    def test_downloads_jar_when_missing(self, tmp_path):
        fake_jar = tmp_path / "ztract-engine.jar"
        # JAR does not exist yet

        def fake_urlretrieve(url, dest):
            # Simulate download by writing a file
            Path(dest).write_bytes(b"PK downloaded")

        with patch("ztract.engine.download_engine.get_jar_path", return_value=fake_jar):
            with patch("urllib.request.urlretrieve", side_effect=fake_urlretrieve) as mock_retrieve:
                result = ensure_jar(version="0.1.0")

        mock_retrieve.assert_called_once()
        assert result == fake_jar

    def test_download_uses_correct_url(self, tmp_path):
        fake_jar = tmp_path / "ztract-engine.jar"

        def fake_urlretrieve(url, dest):
            Path(dest).write_bytes(b"PK")

        with patch("ztract.engine.download_engine.get_jar_path", return_value=fake_jar):
            with patch("urllib.request.urlretrieve", side_effect=fake_urlretrieve) as mock_retrieve:
                ensure_jar(version="1.2.3")

        called_url = mock_retrieve.call_args[0][0]
        assert "1.2.3" in called_url
        assert "ztract-engine.jar" in called_url
        assert "github.com" in called_url

    def test_download_uses_package_version_when_no_version_given(self, tmp_path):
        fake_jar = tmp_path / "ztract-engine.jar"

        def fake_urlretrieve(url, dest):
            Path(dest).write_bytes(b"PK")

        import ztract
        expected_version = ztract.__version__

        with patch("ztract.engine.download_engine.get_jar_path", return_value=fake_jar):
            with patch("urllib.request.urlretrieve", side_effect=fake_urlretrieve) as mock_retrieve:
                ensure_jar()  # no version argument

        called_url = mock_retrieve.call_args[0][0]
        assert expected_version in called_url

    def test_raises_runtime_error_on_download_failure(self, tmp_path):
        fake_jar = tmp_path / "ztract-engine.jar"

        with patch("ztract.engine.download_engine.get_jar_path", return_value=fake_jar):
            with patch("urllib.request.urlretrieve", side_effect=OSError("connection refused")):
                with pytest.raises(RuntimeError, match="Failed to download"):
                    ensure_jar(version="0.1.0")

    def test_runtime_error_message_contains_url(self, tmp_path):
        fake_jar = tmp_path / "ztract-engine.jar"

        with patch("ztract.engine.download_engine.get_jar_path", return_value=fake_jar):
            with patch("urllib.request.urlretrieve", side_effect=OSError("timeout")):
                with pytest.raises(RuntimeError) as exc_info:
                    ensure_jar(version="2.0.0")

        assert "github.com" in str(exc_info.value)

    def test_runtime_error_message_mentions_releases_page(self, tmp_path):
        fake_jar = tmp_path / "ztract-engine.jar"

        with patch("ztract.engine.download_engine.get_jar_path", return_value=fake_jar):
            with patch("urllib.request.urlretrieve", side_effect=OSError("timeout")):
                with pytest.raises(RuntimeError) as exc_info:
                    ensure_jar(version="2.0.0")

        assert "releases" in str(exc_info.value).lower()

    def test_returns_path_to_jar(self, tmp_path):
        fake_jar = tmp_path / "ztract-engine.jar"

        def fake_urlretrieve(url, dest):
            Path(dest).write_bytes(b"PK")

        with patch("ztract.engine.download_engine.get_jar_path", return_value=fake_jar):
            with patch("urllib.request.urlretrieve", side_effect=fake_urlretrieve):
                result = ensure_jar(version="0.1.0")

        assert result == fake_jar
