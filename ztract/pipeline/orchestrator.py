"""PipelineOrchestrator — sequential step execution for YAML-defined jobs."""
from __future__ import annotations

import logging

from ztract.pipeline.step_context import StepContext

__all__ = ["PipelineOrchestrator"]

logger = logging.getLogger(__name__)

# Exit-code constants
_EXIT_SUCCESS = 0
_EXIT_FAILURE = 1
_EXIT_REJECTS = 2


class PipelineOrchestrator:
    """Run the steps defined in a parsed job config sequentially.

    Parameters
    ----------
    config:
        Parsed and validated job configuration dict (as returned by
        :func:`~ztract.config.loader.load_job_config`).
    step_filter:
        When supplied, only the step whose ``name`` matches this string
        is executed; all others are skipped.
    dry_run:
        When ``True``, step names are logged but no actual work is done.
        :meth:`run` returns exit-code 0 immediately.
    """

    def __init__(
        self,
        config: dict,
        step_filter: str | None = None,
        dry_run: bool = False,
    ) -> None:
        self._config = config
        self._step_filter = step_filter
        self._dry_run = dry_run
        self._ctx = StepContext()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def run(self) -> int:
        """Execute the pipeline and return an exit code.

        Returns
        -------
        int
            * ``0`` — success (no errors, no rejects).
            * ``1`` — one or more steps failed.
            * ``2`` — pipeline succeeded but at least one record was rejected.
        """
        job = self._config.get("job", {})
        job_name = job.get("name", "<unnamed>")
        steps: list[dict] = job.get("steps") or []
        continue_on_error: bool = bool(job.get("continue_on_error", False))

        if self._dry_run:
            logger.info("Dry-run: job=%r  steps=%d", job_name, len(steps))
            for step in steps:
                step_name = step.get("name", "<unnamed>")
                action = step.get("action", "<unknown>")
                logger.info("  [dry-run] step=%r  action=%r", step_name, action)
            return _EXIT_SUCCESS

        logger.info("Starting job %r  (%d steps)", job_name, len(steps))
        failed = False

        try:
            for step in steps:
                step_name = step.get("name", "<unnamed>")

                if self._step_filter is not None and step_name != self._step_filter:
                    logger.debug("Skipping step %r (filter=%r)", step_name, self._step_filter)
                    continue

                logger.info("Running step %r", step_name)
                self._ctx.start_step(step_name)
                try:
                    self._execute_step(step)
                except Exception as exc:  # noqa: BLE001
                    logger.error("Step %r failed: %s", step_name, exc)
                    failed = True
                    if not continue_on_error:
                        return _EXIT_FAILURE
                finally:
                    self._ctx.end_step(step_name)

                elapsed = self._ctx.get_elapsed(step_name)
                logger.info("Step %r completed in %.3fs", step_name, elapsed)
        finally:
            self._ctx.close()

        if failed:
            return _EXIT_FAILURE
        if self._ctx.total_rejects > 0:
            logger.warning(
                "Job %r finished with %d rejected record(s).",
                job_name,
                self._ctx.total_rejects,
            )
            return _EXIT_REJECTS
        return _EXIT_SUCCESS

    # ------------------------------------------------------------------
    # Internal dispatch
    # ------------------------------------------------------------------

    def _execute_step(self, step: dict) -> None:
        """Dispatch a step dict to its action handler."""
        action = (step.get("action") or "").lower()
        if action == "convert":
            self._run_convert(step)
        elif action == "diff":
            self._run_diff(step)
        elif action == "generate":
            self._run_generate(step)
        else:
            raise ValueError(f"Unknown action {action!r} in step {step.get('name', '<unnamed>')!r}.")

    # ------------------------------------------------------------------
    # Action stubs
    # ------------------------------------------------------------------

    def _run_convert(self, step: dict) -> None:
        """Stub: log convert step parameters."""
        step_name = step.get("name", "<unnamed>")
        logger.info(
            "convert  step=%r  copybook=%r  input=%r  output=%r",
            step_name,
            step.get("copybook"),
            step.get("input"),
            step.get("output"),
        )

    def _run_diff(self, step: dict) -> None:
        """Stub: log diff step parameters."""
        step_name = step.get("name", "<unnamed>")
        logger.info(
            "diff  step=%r  copybook=%r  before=%r  after=%r",
            step_name,
            step.get("copybook"),
            step.get("before"),
            step.get("after"),
        )

    def _run_generate(self, step: dict) -> None:
        """Stub: log generate step parameters."""
        step_name = step.get("name", "<unnamed>")
        logger.info(
            "generate  step=%r  copybook=%r  output=%r  rows=%r",
            step_name,
            step.get("copybook"),
            step.get("output"),
            step.get("rows"),
        )
