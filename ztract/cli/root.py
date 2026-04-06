"""Root CLI group for Ztract."""
import click

from ztract import __version__
from ztract.cli.convert import convert
from ztract.cli.diff import diff
from ztract.cli.generate import generate
from ztract.cli.init import init
from ztract.cli.inspect import inspect
from ztract.cli.run import run
from ztract.cli.status import status
from ztract.cli.validate import validate


@click.group()
@click.version_option(version=__version__, prog_name="ztract")
@click.option("--debug", is_flag=True, default=False, help="Enable debug output.")
@click.option("--quiet", is_flag=True, default=False, help="Suppress non-essential output.")
@click.pass_context
def cli(ctx: click.Context, debug: bool, quiet: bool) -> None:
    """Ztract — Extract mainframe EBCDIC data using COBOL copybooks."""
    ctx.ensure_object(dict)
    ctx.obj["debug"] = debug
    ctx.obj["quiet"] = quiet


cli.add_command(convert)
cli.add_command(diff)
cli.add_command(generate)
cli.add_command(init)
cli.add_command(inspect)
cli.add_command(run)
cli.add_command(status)
cli.add_command(validate)
