"""ztract inspect — display copybook field layout as a formatted table."""
from __future__ import annotations

from pathlib import Path

import click
from rich.console import Console
from rich.table import Table

from ztract.engine.bridge import ZtractBridge


@click.command()
@click.option("--copybook", required=True, type=click.Path(exists=True), help="Path to the COBOL copybook file.")
@click.pass_context
def inspect(ctx: click.Context, copybook: str) -> None:
    """Display copybook field layout as a formatted table."""
    jar_path = Path(__file__).parent.parent / "engine" / "ztract-engine.jar"
    bridge = ZtractBridge(jar_path=jar_path)

    try:
        bridge.check_jre()
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    try:
        schema = bridge.get_schema(copybook=Path(copybook))
    except Exception as exc:
        raise click.ClickException(f"Failed to read copybook schema: {exc}") from exc

    console = Console()

    table = Table(title=f"Copybook: {copybook}", show_header=True, header_style="bold cyan")
    table.add_column("Field", style="bold")
    table.add_column("Level")
    table.add_column("PIC")
    table.add_column("Offset", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Usage")

    fields = schema.get("fields", [])
    total_length = schema.get("record_length", 0)

    for field_def in fields:
        name = field_def.get("name", "")
        level = str(field_def.get("level", ""))
        pic = field_def.get("pic", "")
        offset = str(field_def.get("offset", ""))
        size = field_def.get("size", "")
        usage = field_def.get("usage", "DISPLAY")

        if usage and usage != "DISPLAY":
            size_str = f"{size} ({usage})"
        else:
            size_str = str(size)

        table.add_row(name, level, pic, offset, size_str, usage)

    console.print(table)
    console.print(f"Total record length: {total_length} bytes")
