"""ztract convert — decode a mainframe EBCDIC dataset to one or more output formats."""
from __future__ import annotations

from pathlib import Path

import click

from ztract.codepages import CodepageError, resolve_codepage
from ztract.connectors.dataset_format import DatasetFormatError, RecordFormat, validate_record_format
from ztract.connectors.local import LocalConnector
from ztract.engine.bridge import ZtractBridge
from ztract.observability.progress import ProgressTracker
from ztract.pipeline.fanout import FanOut
from ztract.writers.csv import CSVWriter
from ztract.writers.jsonl import JSONLWriter
from ztract.writers.parquet import ParquetWriter


def _infer_format(path: str, explicit_fmt: str | None) -> str:
    """Return the output format string inferred from *path*'s extension.

    Falls back to *explicit_fmt* when provided, otherwise 'csv'.
    """
    if explicit_fmt:
        return explicit_fmt.lower()
    ext = Path(path).suffix.lower()
    if ext == ".jsonl":
        return "jsonl"
    if ext == ".parquet":
        return "parquet"
    return "csv"


def _make_writer(output_path: str, fmt: str, delimiter: str):
    """Instantiate the appropriate writer for *fmt*."""
    fmt = fmt.lower()
    if fmt == "jsonl":
        return JSONLWriter(output_path)
    if fmt == "parquet":
        return ParquetWriter(output_path)
    # Default: CSV (also handles 'csv' explicitly)
    return CSVWriter(output_path, delimiter=delimiter)


def _format_from_extension(path: str) -> str | None:
    """Return the format implied by *path*'s extension, or None if ambiguous."""
    ext = Path(path).suffix.lower()
    mapping = {
        ".csv": "csv",
        ".jsonl": "jsonl",
        ".parquet": "parquet",
    }
    return mapping.get(ext)


@click.command()
@click.option("--copybook", required=True, type=click.Path(exists=True), help="Path to the COBOL copybook file.")
@click.option("--input", "input_path", required=True, help="Path to the EBCDIC dataset to convert.")
@click.option("--output", "output_paths", required=True, multiple=True, help="Output file path(s). Repeat for multiple outputs.")
@click.option(
    "--recfm",
    required=True,
    type=click.Choice(["F", "FB", "V", "VB", "FBA", "VBA"], case_sensitive=False),
    help="Dataset record format.",
)
@click.option("--lrecl", type=int, default=None, help="Logical record length (required for F/FB/FBA).")
@click.option("--codepage", default="cp037", show_default=True, help="EBCDIC codepage name or alias.")
@click.option("--format", "fmt", default=None, help="Output format override (csv, jsonl, parquet).")
@click.option("--delimiter", default=",", show_default=True, help="CSV field delimiter.")
@click.option(
    "--encoding",
    default="ebcdic",
    show_default=True,
    type=click.Choice(["ebcdic", "ascii"], case_sensitive=False),
    help="Source file encoding.",
)
@click.pass_context
def convert(
    ctx: click.Context,
    copybook: str,
    input_path: str,
    output_paths: tuple[str, ...],
    recfm: str,
    lrecl: int | None,
    codepage: str,
    fmt: str | None,
    delimiter: str,
    encoding: str,
) -> None:
    """Decode a mainframe EBCDIC dataset to one or more output files.

    The output format is inferred from the file extension (.csv, .jsonl,
    .parquet) unless overridden with --format.
    """
    quiet: bool = ctx.obj.get("quiet", False) if ctx.obj else False

    # ------------------------------------------------------------------
    # 1. Resolve codepage
    # ------------------------------------------------------------------
    try:
        resolved_codepage = resolve_codepage(codepage)
    except CodepageError as exc:
        raise click.BadParameter(str(exc), param_hint="'--codepage'") from exc

    # ------------------------------------------------------------------
    # 2. Parse and validate record format
    # ------------------------------------------------------------------
    try:
        record_format = RecordFormat.from_str(recfm)
        validate_record_format(record_format, lrecl)
    except DatasetFormatError as exc:
        raise click.BadParameter(str(exc), param_hint="'--recfm' / '--lrecl'") from exc

    # ------------------------------------------------------------------
    # 3. Warn if --format conflicts with output extension
    # ------------------------------------------------------------------
    if fmt is not None:
        for out_path in output_paths:
            ext_fmt = _format_from_extension(out_path)
            if ext_fmt is not None and ext_fmt != fmt.lower():
                click.echo(
                    f"Warning: --format={fmt!r} conflicts with extension of {out_path!r} "
                    f"(expected {ext_fmt!r}). Using --format value.",
                    err=True,
                )

    # ------------------------------------------------------------------
    # 4. Locate the engine JAR
    # ------------------------------------------------------------------
    jar_path = Path(__file__).parent.parent / "engine" / "ztract-engine.jar"

    # ------------------------------------------------------------------
    # 5. Build bridge and verify JRE / schema
    # ------------------------------------------------------------------
    bridge = ZtractBridge(jar_path=jar_path)
    try:
        bridge.check_jre()
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    try:
        schema = bridge.get_schema(
            copybook=Path(copybook),
            recfm=record_format.value,
            lrecl=lrecl,
        )
    except Exception as exc:
        raise click.ClickException(f"Failed to read copybook schema: {exc}") from exc

    # ------------------------------------------------------------------
    # 6. Build writers
    # ------------------------------------------------------------------
    writers = []
    for out_path in output_paths:
        effective_fmt = _infer_format(out_path, fmt)
        writers.append(_make_writer(out_path, effective_fmt, delimiter))

    # ------------------------------------------------------------------
    # 7. Download (local pass-through) input file
    # ------------------------------------------------------------------
    connector = LocalConnector()
    try:
        local_input = connector.download(input_path, input_path)
    except FileNotFoundError as exc:
        raise click.ClickException(str(exc)) from exc
    except ValueError as exc:
        raise click.ClickException(f"Input file is empty: {input_path!r}") from exc
    finally:
        connector.close()

    # ------------------------------------------------------------------
    # 8. Decode → progress → fan-out
    # ------------------------------------------------------------------
    tracker = ProgressTracker(quiet=quiet)
    task_id = tracker.add_step("Decoding records")

    records_iter = bridge.decode(
        copybook=Path(copybook),
        input_path=local_input,
        recfm=record_format.value,
        lrecl=lrecl if lrecl is not None else 0,
        codepage=resolved_codepage,
        encoding=encoding,
    )

    def _tracked_records():
        for record in records_iter:
            tracker.update(task_id)
            yield record

    fan_out = FanOut(writers=writers, schema=schema)
    try:
        total = fan_out.run(_tracked_records())
    finally:
        tracker.finish()

    # ------------------------------------------------------------------
    # 9. Summary
    # ------------------------------------------------------------------
    click.echo(f"Done. {total} records written.")
