"""ztract generate — synthesise a mainframe EBCDIC dataset from a copybook."""
from __future__ import annotations

from pathlib import Path

import click

from ztract.codepages import CodepageError, resolve_codepage
from ztract.engine.bridge import ZtractBridge
from ztract.generate.generator import generate_records
from ztract.observability.progress import ProgressTracker
from ztract.writers.ebcdic import EBCDICWriter


@click.command()
@click.option(
    "--copybook",
    required=True,
    type=click.Path(exists=True),
    help="Path to the COBOL copybook file.",
)
@click.option(
    "--records",
    "record_count",
    required=True,
    type=int,
    help="Number of mock records to generate.",
)
@click.option(
    "--output",
    required=True,
    type=click.Path(),
    help="Destination path for the generated EBCDIC binary file.",
)
@click.option(
    "--codepage",
    default="cp037",
    show_default=True,
    help="Target EBCDIC codepage name or alias.",
)
@click.option(
    "--recfm",
    required=True,
    help="Dataset record format (e.g. FB, VB).",
)
@click.option(
    "--lrecl",
    type=int,
    default=None,
    help="Logical record length (required for F/FB/FBA).",
)
@click.option(
    "--seed",
    type=int,
    default=None,
    help="Random seed for reproducible output.",
)
@click.option(
    "--edge-cases",
    is_flag=True,
    default=False,
    help="Include boundary values every 100th record.",
)
@click.pass_context
def generate(
    ctx: click.Context,
    copybook: str,
    record_count: int,
    output: str,
    codepage: str,
    recfm: str,
    lrecl: int | None,
    seed: int | None,
    edge_cases: bool = False,
) -> None:
    """Generate a synthetic mainframe EBCDIC binary file from a copybook.

    The field names in the copybook drive the generated content: fields
    named NAME, ADDR, DATE, AMT etc. receive contextually appropriate
    values.  Use --seed for deterministic output.
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
    # 2. Locate the engine JAR
    # ------------------------------------------------------------------
    jar_path = Path(__file__).parent.parent / "engine" / "ztract-engine.jar"

    # ------------------------------------------------------------------
    # 3. Build bridge, verify JRE, fetch schema
    # ------------------------------------------------------------------
    bridge = ZtractBridge(jar_path=jar_path)
    try:
        bridge.check_jre()
    except Exception as exc:
        raise click.ClickException(str(exc)) from exc

    try:
        schema = bridge.get_schema(
            copybook=Path(copybook),
            recfm=recfm,
            lrecl=lrecl,
        )
    except Exception as exc:
        raise click.ClickException(f"Failed to read copybook schema: {exc}") from exc

    # ------------------------------------------------------------------
    # 4. Build EBCDIC writer
    # ------------------------------------------------------------------
    writer = EBCDICWriter(
        output_path=output,
        bridge=bridge,
        copybook=Path(copybook),
        recfm=recfm,
        lrecl=lrecl,
        codepage=resolved_codepage,
    )
    writer.open(schema)

    # ------------------------------------------------------------------
    # 5. Generate records → buffer → encode
    # ------------------------------------------------------------------
    tracker = ProgressTracker(quiet=quiet)
    task_id = tracker.add_step("Generating records", total=record_count)

    batch: list[dict] = []
    batch_size = writer.batch_size

    try:
        for record in generate_records(
            schema=schema,
            count=record_count,
            codepage=resolved_codepage,
            seed=seed,
            edge_cases=edge_cases,
        ):
            batch.append(record)
            tracker.update(task_id)
            if len(batch) >= batch_size:
                writer.write_batch(batch)
                batch.clear()

        if batch:
            writer.write_batch(batch)

        stats = writer.close()
    finally:
        tracker.finish()

    # ------------------------------------------------------------------
    # 6. Summary
    # ------------------------------------------------------------------
    click.echo(f"Done. {stats.records_written} records written to {output!r}.")
