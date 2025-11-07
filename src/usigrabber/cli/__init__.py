import logging

import typer

from usigrabber.db.cli import app as db_app
from usigrabber.utils import logger

app = typer.Typer()

# import commands to register them with the app
from usigrabber.cli import build, search  # noqa

app.add_typer(db_app, name="db")


class TyperLoggerHandler(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        fg = None
        bg = None
        if record.levelno == logging.DEBUG:
            fg = typer.colors.BLACK
        elif record.levelno == logging.INFO:
            fg = typer.colors.BRIGHT_BLUE
        elif record.levelno == logging.WARNING:
            fg = typer.colors.BRIGHT_MAGENTA
        elif record.levelno == logging.CRITICAL:
            fg = typer.colors.BRIGHT_RED
        elif record.levelno == logging.ERROR:
            fg = typer.colors.BRIGHT_WHITE
            bg = typer.colors.RED
        typer.secho(self.format(record), bg=bg, fg=fg)


# remove default logger
logger.handlers.clear()

# add typer logger handler
logger.addHandler(TyperLoggerHandler())
