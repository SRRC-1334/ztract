"""Tests for ztract.config.loader and ztract.config.schema."""
from __future__ import annotations

import os
import textwrap

import pytest

from ztract.config.loader import interpolate_env_vars, load_job_config
from ztract.config.schema import ConfigError, validate_job_config


# ---------------------------------------------------------------------------
# interpolate_env_vars
# ---------------------------------------------------------------------------


class TestInterpolateEnvVars:
    def test_no_vars_unchanged(self):
        assert interpolate_env_vars("hello world") == "hello world"

    def test_single_var_replaced(self, monkeypatch):
        monkeypatch.setenv("MY_VAR", "replaced")
        assert interpolate_env_vars("value: ${MY_VAR}") == "value: replaced"

    def test_multiple_vars(self, monkeypatch):
        monkeypatch.setenv("FOO", "foo_value")
        monkeypatch.setenv("BAR", "bar_value")
        result = interpolate_env_vars("${FOO} and ${BAR}")
        assert result == "foo_value and bar_value"

    def test_missing_var_raises(self, monkeypatch):
        monkeypatch.delenv("MISSING_VAR", raising=False)
        with pytest.raises(ValueError, match="MISSING_VAR"):
            interpolate_env_vars("value: ${MISSING_VAR}")

    def test_same_var_twice(self, monkeypatch):
        monkeypatch.setenv("DUPE", "x")
        assert interpolate_env_vars("${DUPE}-${DUPE}") == "x-x"

    def test_empty_string_unchanged(self):
        assert interpolate_env_vars("") == ""


# ---------------------------------------------------------------------------
# load_job_config
# ---------------------------------------------------------------------------


class TestLoadJobConfig:
    def test_loads_simple_yaml(self, tmp_path):
        job_file = tmp_path / "job.yaml"
        job_file.write_text(
            textwrap.dedent("""\
                job:
                  name: test-job
                  steps: []
            """),
            encoding="utf-8",
        )
        config = load_job_config(job_file)
        assert config["job"]["name"] == "test-job"

    def test_interpolates_env_var(self, tmp_path, monkeypatch):
        monkeypatch.setenv("COPYBOOK_PATH", "/data/my.cpy")
        job_file = tmp_path / "job.yaml"
        job_file.write_text(
            "job:\n  name: ev-job\n  steps:\n    - name: s1\n      action: convert\n      copybook: ${COPYBOOK_PATH}\n",
            encoding="utf-8",
        )
        config = load_job_config(job_file)
        assert config["job"]["steps"][0]["copybook"] == "/data/my.cpy"

    def test_missing_env_var_raises(self, tmp_path, monkeypatch):
        monkeypatch.delenv("NO_SUCH_VAR", raising=False)
        job_file = tmp_path / "job.yaml"
        job_file.write_text("job:\n  name: ${NO_SUCH_VAR}\n", encoding="utf-8")
        with pytest.raises(ValueError, match="NO_SUCH_VAR"):
            load_job_config(job_file)

    def test_dotenv_file_auto_loaded(self, tmp_path):
        dotenv = tmp_path / ".env"
        dotenv.write_text("DOTENV_HOST=mainframe.example.com\n", encoding="utf-8")
        job_file = tmp_path / "job.yaml"
        job_file.write_text(
            "job:\n  name: dotenv-job\n  steps:\n    - name: s\n      action: convert\n      copybook: /cb.cpy\n      host: ${DOTENV_HOST}\n",
            encoding="utf-8",
        )
        # Ensure the var isn't already in the environment
        os.environ.pop("DOTENV_HOST", None)
        try:
            config = load_job_config(job_file)
            assert config["job"]["steps"][0]["host"] == "mainframe.example.com"
        finally:
            os.environ.pop("DOTENV_HOST", None)

    def test_dotenv_does_not_overwrite_existing_env(self, tmp_path, monkeypatch):
        monkeypatch.setenv("PRIORITY_VAR", "from_env")
        dotenv = tmp_path / ".env"
        dotenv.write_text("PRIORITY_VAR=from_dotenv\n", encoding="utf-8")
        job_file = tmp_path / "job.yaml"
        job_file.write_text(
            "job:\n  name: priority-job\n  steps:\n    - name: s\n      action: convert\n      copybook: ${PRIORITY_VAR}\n",
            encoding="utf-8",
        )
        config = load_job_config(job_file)
        # env var should win over .env file
        assert config["job"]["steps"][0]["copybook"] == "from_env"


# ---------------------------------------------------------------------------
# validate_job_config
# ---------------------------------------------------------------------------


class TestValidateJobConfig:
    def _make_config(self, **overrides) -> dict:
        cfg: dict = {
            "job": {
                "name": "my-job",
                "steps": [],
            }
        }
        cfg["job"].update(overrides)
        return cfg

    def test_valid_minimal_config_passes(self):
        validate_job_config(self._make_config())  # no exception

    def test_missing_job_name_raises(self):
        config = {"job": {"steps": []}}
        with pytest.raises(ConfigError, match="job.name"):
            validate_job_config(config)

    def test_no_job_key_raises(self):
        with pytest.raises(ConfigError, match="'job'"):
            validate_job_config({"other": 1})

    def test_non_dict_raises(self):
        with pytest.raises(ConfigError):
            validate_job_config("not a dict")  # type: ignore[arg-type]

    # --- convert step ---

    def test_valid_convert_step_passes(self):
        config = self._make_config(
            steps=[
                {
                    "name": "extract",
                    "action": "convert",
                    "copybook": "CUSTMAST.cpy",
                    "input": "CUST.bin",
                    "output": "out.csv",
                }
            ]
        )
        validate_job_config(config)  # no exception

    def test_convert_missing_copybook_raises(self):
        config = self._make_config(
            steps=[{"name": "extract", "action": "convert", "input": "CUST.bin"}]
        )
        with pytest.raises(ConfigError, match="copybook"):
            validate_job_config(config)

    def test_convert_invalid_codepage_raises(self):
        config = self._make_config(
            steps=[
                {
                    "name": "s",
                    "action": "convert",
                    "copybook": "cb.cpy",
                    "codepage": "INVALID_CP",
                }
            ]
        )
        with pytest.raises(ConfigError, match="codepage"):
            validate_job_config(config)

    def test_convert_valid_codepage_passes(self):
        config = self._make_config(
            steps=[
                {
                    "name": "s",
                    "action": "convert",
                    "copybook": "cb.cpy",
                    "codepage": "cp037",
                }
            ]
        )
        validate_job_config(config)  # no exception

    def test_convert_invalid_recfm_raises(self):
        config = self._make_config(
            steps=[
                {
                    "name": "s",
                    "action": "convert",
                    "copybook": "cb.cpy",
                    "recfm": "BADFORMAT",
                }
            ]
        )
        with pytest.raises(ConfigError, match="recfm"):
            validate_job_config(config)

    # --- diff step ---

    def test_diff_missing_copybook_raises(self):
        config = self._make_config(
            steps=[{"name": "d", "action": "diff", "before": "a.bin", "after": "b.bin"}]
        )
        with pytest.raises(ConfigError, match="copybook"):
            validate_job_config(config)

    def test_valid_diff_step_passes(self):
        config = self._make_config(
            steps=[
                {"name": "d", "action": "diff", "copybook": "cb.cpy", "before": "a.bin", "after": "b.bin"}
            ]
        )
        validate_job_config(config)

    # --- generate step ---

    def test_generate_missing_copybook_raises(self):
        config = self._make_config(
            steps=[{"name": "g", "action": "generate", "output": "out.bin", "rows": 100}]
        )
        with pytest.raises(ConfigError, match="copybook"):
            validate_job_config(config)

    def test_valid_generate_step_passes(self):
        config = self._make_config(
            steps=[
                {"name": "g", "action": "generate", "copybook": "cb.cpy", "output": "out.bin", "rows": 100}
            ]
        )
        validate_job_config(config)

    # --- unknown action ---

    def test_unknown_action_raises(self):
        config = self._make_config(
            steps=[{"name": "x", "action": "magic", "copybook": "cb.cpy"}]
        )
        with pytest.raises(ConfigError, match="unknown action"):
            validate_job_config(config)

    def test_missing_action_raises(self):
        config = self._make_config(steps=[{"name": "x", "copybook": "cb.cpy"}])
        with pytest.raises(ConfigError, match="action"):
            validate_job_config(config)
