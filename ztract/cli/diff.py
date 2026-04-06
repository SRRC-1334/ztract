"""ztract diff — compare two EBCDIC datasets field-by-field."""
from __future__ import annotations

import csv
import json
import sys
import tempfile
from pathlib import Path

import click

from ztract.codepages import CodepageError, resolve_codepage
from ztract.diff.differ import Differ
from ztract.engine.bridge import ZtractBridge


def _print_console(result, quiet: bool) -> None:
    """Emit a human-readable summary to stdout."""
    summary = (
        f"{result.added} added  ·  "
        f"{result.deleted} deleted  ·  "
        f"{result.changed} changed  ·  "
        f"{result.unchanged} unchanged"
    )
    click.echo(summary)

    if not quiet and result.changes:
        click.echo("")
        click.echo("Changed records:")
        for change in result.changes:
            before = change.get("_before", {})
            after  = change.get("_after", {})
            key_parts = {k: v for k, v in change.items() if k not in ("_before", "_after", "_index")}
            if key_parts:
                click.echo(f"  key={key_parts}")
            else:
                click.echo(f"  index={change.get('_index')}")
            for field_name in before:
                click.echo(f"    {field_name}: {before[field_name]!r} → {after.get(field_name)!r}")


def _print_json(result) -> None:
    """Emit full diff result as JSON to stdout."""
    data = {
        "summary": {
            "added":         result.added,
            "deleted":       result.deleted,
            "changed":       result.changed,
            "unchanged":     result.unchanged,
            "total_before":  result.total_before,
            "total_after":   result.total_after,
        },
        "changes":   result.changes,
        "additions": result.additions,
        "deletions": result.deletions,
    }
    click.echo(json.dumps(data, indent=2))


def _print_csv(result) -> None:
    """Emit changed records as CSV to stdout."""
    rows = []
    for change in result.changes:
        key_parts = {k: v for k, v in change.items() if k not in ("_before", "_after", "_index")}
        before = change.get("_before", {})
        after  = change.get("_after",  {})
        for field_name in before:
            row = dict(key_parts)
            row["field"]  = field_name
            row["before"] = before[field_name]
            row["after"]  = after.get(field_name)
            rows.append(row)

    if not rows:
        return

    fieldnames = list(rows[0].keys())
    writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)


@click.command()
@click.option("--copybook", required=True, type=click.Path(exists=True), help="Path to the COBOL copybook file.")
@click.option("--before",   required=True, type=click.Path(exists=True), help="Path to the 'before' EBCDIC dataset.")
@click.option("--after",    required=True, type=click.Path(exists=True), help="Path to the 'after' EBCDIC dataset.")
@click.option("--key", "key_fields", multiple=True, help="Field name(s) that uniquely identify a record (repeatable).")
@click.option("--recfm",    required=True, help="Dataset record format (F, FB, V, VB, FBA, VBA).")
@click.option("--lrecl",    type=int, default=None, help="Logical record length (required for F/FB/FBA).")
@click.option("--codepage", default="cp037", show_default=True, help="EBCDIC codepage name or alias.")
@click.option(
    "--format", "fmt",
    default="console",
    show_default=True,
    type=click.Choice(["console", "csv", "json"], case_sensitive=False),
    help="Output format for diff results.",
)
@click.pass_context
def diff(
    ctx: click.Context,
    copybook: str,
    before: str,
    after: str,
    key_fields: tuple[str, ...],
    recfm: str,
    lrecl: int | None,
    codepage: str,
    fmt: str,
) -> None:
    """Compare two EBCDIC datasets field-by-field using a COBOL copybook."""
    quiet: bool = ctx.obj.get("quiet", False) if ctx.obj else False

    # ------------------------------------------------------------------
    # 1. Resolve codepage
    # ------------------------------------------------------------------
    try:
        resolved_codepage = resolve_codepage(codepage)
    except CodepageError as exc:
        raise click.BadParameter(str(exc), param_hint="'--codepage'") from exc

    # ------------------------------------------------------------------
    # 2. Locate JAR and verify JRE
    # ------------------------------------------------------------------
    jar_path = Path(__file__).parent.parent / "engine" / "ztract-engine.jar"
    bridge = ZtractBridge(jar_path=jar_path)
    try:
        bridge.check_jre()
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    # ------------------------------------------------------------------
    # 3. Decode both files to temp JSONL
    # ------------------------------------------------------------------
    with tempfile.TemporaryDirectory() as tmpdir:
        before_jsonl = Path(tmpdir) / "before.jsonl"
        after_jsonl  = Path(tmpdir) / "after.jsonl"

        for src_path, dest_path in [
            (before, before_jsonl),
            (after,  after_jsonl),
        ]:
            try:
                records = bridge.decode(
                    copybook=Path(copybook),
                    input_path=Path(src_path),
                    recfm=recfm,
                    lrecl=lrecl if lrecl is not None else 0,
                    codepage=resolved_codepage,
                )
                with open(dest_path, "w", encoding="utf-8") as fh:
                    for record in records:
                        fh.write(json.dumps(record) + "\n")
            except Exception as exc:
                raise click.ClickException(f"Failed to decode {src_path!r}: {exc}") from exc

        # ------------------------------------------------------------------
        # 4. Run the differ
        # ------------------------------------------------------------------
        key_list = list(key_fields) if key_fields else None
        result = Differ(key_fields=key_list).diff_jsonl(before_jsonl, after_jsonl)

    # ------------------------------------------------------------------
    # 5. Output
    # ------------------------------------------------------------------
    if fmt == "json":
        _print_json(result)
    elif fmt == "csv":
        _print_csv(result)
    else:
        _print_console(result, quiet=quiet)
