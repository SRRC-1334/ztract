"""ztract init — scaffold a new Ztract project directory."""
from __future__ import annotations

from pathlib import Path

import click

_DIRS = ["copybooks", "jobs", "output", "logs", "audit", "rejects", "testdata", ".ztract"]

_DEFAULT_CONFIG = """\
# Ztract machine-level configuration
engine:
  jvm_max_heap: 512m
  jvm_args: []
logging:
  retention_days: 30
defaults:
  codepage: cp037
  record_format: FB
"""

_GITIGNORE_ENTRIES = """
# Ztract
.ztract/
.ztract_tmp/
logs/
rejects/
"""


@click.command()
@click.option("--dir", "target_dir", default=".", show_default=True, help="Directory to initialise.")
def init(target_dir: str) -> None:
    """Scaffold a new Ztract project directory."""
    root = Path(target_dir).resolve()

    # Create all project directories
    for dir_name in _DIRS:
        dir_path = root / dir_name
        dir_path.mkdir(parents=True, exist_ok=True)
        click.echo(f"  created  {dir_path.relative_to(root)}/")

    # Write .ztract/config.yaml if it doesn't already exist
    config_path = root / ".ztract" / "config.yaml"
    if not config_path.exists():
        config_path.write_text(_DEFAULT_CONFIG, encoding="utf-8")
        click.echo(f"  created  .ztract/config.yaml")
    else:
        click.echo(f"  exists   .ztract/config.yaml (skipped)")

    # Append to .gitignore if .ztract_tmp is not already present
    gitignore_path = root / ".gitignore"
    should_append = True
    if gitignore_path.exists():
        existing = gitignore_path.read_text(encoding="utf-8")
        if ".ztract_tmp" in existing:
            should_append = False

    if should_append:
        with gitignore_path.open("a", encoding="utf-8") as fh:
            fh.write(_GITIGNORE_ENTRIES)
        action = "updated" if gitignore_path.exists() else "created"
        click.echo(f"  {action}  .gitignore")
    else:
        click.echo(f"  exists   .gitignore (Ztract entries already present, skipped)")

    click.echo()
    click.echo("Ztract project initialised. Next steps:")
    click.echo("  1. Add your COBOL copybooks to copybooks/")
    click.echo("  2. Add your EBCDIC data files to testdata/")
    click.echo("  3. Create a job YAML in jobs/ (see ztract run --help)")
    click.echo("  4. Run: ztract validate --copybook copybooks/<file>.cpy ...")
