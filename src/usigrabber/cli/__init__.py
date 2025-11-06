import typer

app = typer.Typer()

# import commands to register them with the app
from usigrabber.cli import build, search  # noqa
