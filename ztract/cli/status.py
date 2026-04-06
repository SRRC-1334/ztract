"""ztract status — show recent job execution history from the audit log."""
from __future__ import annotations

import json
from pathlib import Path

import click
from rich.console import Console
from rich.table import Table


def _status_style(status: str) -> str:
    """Return a Rich-styled status string."""
    if status == "SUCCESS":
        return "[green]SUCCESS ✓[/green]"
    if status == "PARTIAL_SUCCESS":
        return "[yellow]PARTIAL_SUCCESS ⚠[/yellow]"
    if status == "FAILED":
        return "[red]FAILED ✗[/red]"
    return status


@click.command()
@click.option("--job", "job_name", default=None, help="Filter by job name (substring match).")
@click.option("--last", "count", type=int, default=10, show_default=True, help="Number of most recent entries to show.")
def status(job_name: str | None, count: int) -> None:
    """Show recent job execution history from the audit log."""
    console = Console()

    audit_file = Path("audit") / "ztract_audit.log"
    if not audit_file.exists():
        console.print(f"[yellow]No audit log found at {audit_file}[/yellow]")
        console.print("Run at least one job to generate audit history.")
        return

    entries = []
    with audit_file.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                entry = json.loads(line)
            except json.JSONDecodeError:
                continue
            entries.append(entry)

    # Filter by job_name if specified
    if job_name is not None:
        entries = [e for e in entries if job_name in e.get("job_file", "")]

    # Take the last N entries
    entries = entries[-count:]

    if not entries:
        filter_note = f" matching '{job_name}'" if job_name else ""
        console.print(f"[yellow]No audit entries found{filter_note}.[/yellow]")
        return

    table = Table(title="Job Execution History", show_header=True, header_style="bold cyan")
    table.add_column("Job", style="bold", no_wrap=True)
    table.add_column("Time", no_wrap=True)
    table.add_column("Records", justify="right")
    table.add_column("Rejects", justify="right")
    table.add_column("Status")

    for entry in entries:
        job = Path(entry.get("job_file", "")).name or entry.get("job_file", "")
        timestamp = entry.get("timestamp_start", "")[:19].replace("T", " ")
        steps = entry.get("steps", [])
        total_records = sum(s.get("records_written", 0) for s in steps)
        total_rejects = sum(s.get("records_rejected", 0) for s in steps)
        overall_status = entry.get("overall_status", "UNKNOWN")

        table.add_row(
            job,
            timestamp,
            str(total_records),
            str(total_rejects),
            _status_style(overall_status),
        )

    console.print(table)
