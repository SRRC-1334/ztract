"""YAML job config loader with .env support and environment variable interpolation."""
from __future__ import annotations

import os
import re
from pathlib import Path

import yaml  # type: ignore[import-untyped]

__all__ = ["interpolate_env_vars", "load_job_config"]

_ENV_VAR_PATTERN = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _load_dotenv(directory: Path) -> None:
    """Load key=value pairs from *directory*/.env into os.environ.

    Uses setdefault semantics — existing environment variables are not
    overwritten.  Lines beginning with ``#`` and blank lines are ignored.
    """
    dotenv_path = directory / ".env"
    if not dotenv_path.is_file():
        return
    with dotenv_path.open(encoding="utf-8") as fh:
        for raw_line in fh:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue
            if "=" not in line:
                continue
            key, _, value = line.partition("=")
            key = key.strip()
            value = value.strip()
            # Strip optional surrounding quotes
            if len(value) >= 2 and value[0] == value[-1] and value[0] in ('"', "'"):
                value = value[1:-1]
            os.environ.setdefault(key, value)


def interpolate_env_vars(text: str) -> str:
    """Replace ``${VAR_NAME}`` placeholders in *text* with their environment values.

    Parameters
    ----------
    text:
        Raw string that may contain ``${VAR_NAME}`` tokens.

    Returns
    -------
    str
        String with all tokens substituted.

    Raises
    ------
    ValueError
        If any referenced variable is not set in the environment.
    """

    def _replace(match: re.Match) -> str:
        var_name = match.group(1)
        value = os.environ.get(var_name)
        if value is None:
            raise ValueError(
                f"Environment variable {var_name!r} is not set "
                "(referenced via ${{{}}} in job config)".format(var_name)
            )
        return value

    return _ENV_VAR_PATTERN.sub(_replace, text)


def load_job_config(path: Path) -> dict:
    """Load and parse a YAML job config file.

    Steps performed:

    1. Load a ``.env`` file from the same directory as *path* (if present),
       using setdefault semantics so existing env vars are not overwritten.
    2. Read the raw YAML text from *path*.
    3. Interpolate ``${VAR_NAME}`` placeholders with environment values.
    4. Parse the interpolated text with :func:`yaml.safe_load`.

    Parameters
    ----------
    path:
        Absolute or relative path to a ``.yaml`` / ``.yml`` job file.

    Returns
    -------
    dict
        Parsed job configuration.

    Raises
    ------
    ValueError
        If any ``${VAR_NAME}`` placeholder references an unset variable.
    yaml.YAMLError
        If the (post-interpolation) YAML is malformed.
    """
    path = Path(path)
    _load_dotenv(path.parent)
    raw_text = path.read_text(encoding="utf-8")
    interpolated = interpolate_env_vars(raw_text)
    return yaml.safe_load(interpolated)
