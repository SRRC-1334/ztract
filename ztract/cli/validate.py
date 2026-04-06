"""ztract validate — pre-flight check: decode sample records and report stats."""
from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console

from ztract.codepages import CodepageError, resolve_codepage
from ztract.engine.bridge import ZtractBridge


@click.command()
@click.option("--copybook", required=True, type=click.Path(exists=True), help="Path to the COBOL copybook file.")
@click.option("--input", "input_path", required=True, type=click.Path(exists=True), help="Path to the EBCDIC dataset to validate.")
@click.option("--recfm", required=True, help="Dataset record format (e.g. FB, VB).")
@click.option("--lrecl", type=int, default=None, help="Logical record length.")
@click.option("--codepage", default="cp037", show_default=True, help="EBCDIC codepage name or alias.")
@click.option("--sample", type=int, default=1000, show_default=True, help="Number of records to sample.")
@click.pass_context
def validate(
    ctx: click.Context,
    copybook: str,
    input_path: str,
    recfm: str,
    lrecl: int | None,
    codepage: str,
    sample: int,
) -> None:
    """Pre-flight check: decode sample records and report stats."""
    console = Console()

    try:
        resolved_codepage = resolve_codepage(codepage)
    except CodepageError as exc:
        raise click.BadParameter(str(exc), param_hint="'--codepage'") from exc

    jar_path = Path(__file__).parent.parent / "engine" / "ztract-engine.jar"
    bridge = ZtractBridge(jar_path=jar_path)

    try:
        bridge.check_jre()
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    try:
        report = bridge.validate(
            copybook=Path(copybook),
            input_path=Path(input_path),
            recfm=recfm,
            lrecl=lrecl,
            codepage=resolved_codepage,
            sample=sample,
        )
    except Exception as exc:
        raise click.ClickException(f"Validation failed: {exc}") from exc

    console.print()
    console.print(f"[bold]Validation Report[/bold] — {Path(input_path).name}")
    console.print()
    console.print(f"  [green]Decoded:  {report.records_decoded:>6}[/green]")
    console.print(f"  [yellow]Warnings: {report.records_warnings:>6}[/yellow]")
    console.print(f"  [red]Errors:   {report.records_errors:>6}[/red]")

    if report.field_stats:
        console.print()
        console.print("[bold]Field Stats:[/bold]")
        for field_name, stats in report.field_stats.items():
            console.print(f"  {field_name}: {stats}")

    console.print()
    if report.records_errors == 0 and report.records_warnings == 0:
        console.print("[green bold]Result: OK — no issues detected.[/green bold]")
    elif report.records_errors == 0:
        console.print("[yellow bold]Result: WARNINGS — review before proceeding.[/yellow bold]")
    else:
        console.print("[red bold]Result: ERRORS — dataset may not decode correctly.[/red bold]")
