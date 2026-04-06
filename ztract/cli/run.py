"""ztract run — execute a YAML job file through the pipeline orchestrator."""
from __future__ import annotations

import sys
from pathlib import Path

import click

from ztract.config.loader import load_job_config
from ztract.config.schema import ConfigError, validate_job_config
from ztract.pipeline.orchestrator import PipelineOrchestrator


@click.command()
@click.argument("job_file", type=click.Path(exists=True))
@click.option(
    "--dry-run",
    is_flag=True,
    default=False,
    help="Log step names without executing any work.",
)
@click.option(
    "--step",
    "step_name",
    default=None,
    metavar="STEP",
    help="Run only the named step.",
)
@click.pass_context
def run(
    ctx: click.Context,
    job_file: str,
    dry_run: bool,
    step_name: str | None,
) -> None:
    """Execute a YAML job file.

    JOB_FILE is the path to a .yaml or .yml job definition file.
    """
    try:
        config = load_job_config(Path(job_file))
    except ValueError as exc:
        raise click.ClickException(str(exc)) from exc
    except Exception as exc:
        raise click.ClickException(f"Failed to load job file: {exc}") from exc

    try:
        validate_job_config(config)
    except ConfigError as exc:
        raise click.ClickException(f"Job config error: {exc}") from exc

    orchestrator = PipelineOrchestrator(config, step_filter=step_name, dry_run=dry_run)
    exit_code = orchestrator.run()
    sys.exit(exit_code)
