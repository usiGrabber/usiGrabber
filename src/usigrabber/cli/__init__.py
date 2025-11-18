import typer

from usigrabber.db.cli import app as db_app
from usigrabber.utils.setup import system_setup

app = typer.Typer()

# import commands to register them with the app
from usigrabber.cli import build, search  # noqa

system_setup()

app.add_typer(db_app, name="db")
