import typer

from usigrabber.db.cli import app as db_app

app = typer.Typer()

# import commands to register them with the app
from usigrabber.cli import build, search  # noqa


app.add_typer(db_app, name="db")
