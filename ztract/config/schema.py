"""YAML job config schema validation."""
from __future__ import annotations

from ztract.codepages import CodepageError, resolve_codepage
from ztract.connectors.dataset_format import DatasetFormatError, RecordFormat

__all__ = ["ConfigError", "validate_job_config"]

# Action names that are currently recognised by the orchestrator.
_KNOWN_ACTIONS = {"convert", "diff", "generate"}


class ConfigError(ValueError):
    """Raised when a job config fails schema validation."""


def validate_job_config(config: dict) -> None:
    """Validate a parsed job configuration dictionary.

    Parameters
    ----------
    config:
        Parsed job config (as returned by :func:`~ztract.config.loader.load_job_config`).

    Raises
    ------
    ConfigError
        If the config is structurally invalid or contains bad field values.
    """
    if not isinstance(config, dict):
        raise ConfigError("Job config must be a YAML mapping at the top level.")

    job = config.get("job")
    if not isinstance(job, dict):
        raise ConfigError("Config must contain a top-level 'job' mapping.")

    if not job.get("name"):
        raise ConfigError("'job.name' is required and must be a non-empty string.")

    steps = job.get("steps")
    if steps is None:
        return  # no steps is allowed (empty job)

    if not isinstance(steps, list):
        raise ConfigError("'job.steps' must be a YAML sequence.")

    for step in steps:
        if not isinstance(step, dict):
            raise ConfigError("Each step must be a YAML mapping.")
        step_name = step.get("name", "<unnamed>")
        _validate_step(step, step_name)


def _validate_step(step: dict, step_name: str) -> None:
    """Dispatch validation to the action-specific validator."""
    action = step.get("action")
    if not action:
        raise ConfigError(f"Step {step_name!r}: 'action' is required.")

    action = action.lower()
    if action == "convert":
        _validate_convert_step(step, step_name)
    elif action == "diff":
        _validate_diff_step(step, step_name)
    elif action == "generate":
        _validate_generate_step(step, step_name)
    else:
        raise ConfigError(
            f"Step {step_name!r}: unknown action {action!r}. "
            f"Known actions: {', '.join(sorted(_KNOWN_ACTIONS))}."
        )


def _validate_convert_step(step: dict, step_name: str) -> None:
    """Validate a *convert* step.

    Required fields: ``copybook``.
    Optional validated fields: ``codepage``, ``recfm``.
    """
    if not step.get("copybook"):
        raise ConfigError(f"Step {step_name!r}: 'copybook' is required for action 'convert'.")

    codepage = step.get("codepage")
    if codepage is not None:
        try:
            resolve_codepage(str(codepage))
        except CodepageError as exc:
            raise ConfigError(f"Step {step_name!r}: invalid 'codepage' — {exc}") from exc

    recfm = step.get("recfm")
    if recfm is not None:
        try:
            RecordFormat.from_str(str(recfm))
        except DatasetFormatError as exc:
            raise ConfigError(f"Step {step_name!r}: invalid 'recfm' — {exc}") from exc


def _validate_diff_step(step: dict, step_name: str) -> None:
    """Validate a *diff* step.

    Required fields: ``copybook``.
    """
    if not step.get("copybook"):
        raise ConfigError(f"Step {step_name!r}: 'copybook' is required for action 'diff'.")


def _validate_generate_step(step: dict, step_name: str) -> None:
    """Validate a *generate* step.

    Required fields: ``copybook``.
    """
    if not step.get("copybook"):
        raise ConfigError(f"Step {step_name!r}: 'copybook' is required for action 'generate'.")
