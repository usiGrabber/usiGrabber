import json
import logging
from datetime import datetime

import better_exceptions
from better_exceptions.formatter import THEME

DEFAULT_LOG_FORMAT = (
    "%(asctime)s.%(msecs)03d | %(name)s | "
    + "{level_color}%(levelname)-s{reset_color} | %(message)s [%(filename)s:%(lineno)s]"
)


class CustomColorFormatter(logging.Formatter):
    """
    A custom log formatter that adds color and consistent formatting,
    including rich tracebacks for exceptions.
    """

    # Color codes
    LEVEL_COLORS = {
        logging.DEBUG: "\033[90m",  # Grey
        logging.INFO: "\033[34m",  # Blue
        logging.WARNING: "\033[33m",  # Yellow
        logging.ERROR: "\033[31m",  # Red
        logging.CRITICAL: "\033[1;31m",  # Bold Red
    }
    RESET_COLOR = "\033[0m"

    MAX_LEVEL_NAME_LENGTH = 8

    def __init__(self, use_colors: bool, format: str = DEFAULT_LOG_FORMAT):
        super().__init__()
        self.use_colors = use_colors
        self._exception_formatter = better_exceptions.ExceptionFormatter(
            colored=self.use_colors,
            theme=THEME,
            max_length=better_exceptions.MAX_LENGTH,
            pipe_char=better_exceptions.PIPE_CHAR,
            cap_char=better_exceptions.CAP_CHAR,
        )
        self._format = format

    def format(self, record: logging.LogRecord) -> str:
        # Create a consistent format string

        # Get the color for the level
        level_color = self.LEVEL_COLORS.get(record.levelno, "") if self.use_colors else ""
        reset_color = self.RESET_COLOR if self.use_colors and level_color else ""

        # Apply the colors to the format string
        formatter = logging.Formatter(
            self._format.format(level_color=level_color, reset_color=reset_color),
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Format the main part of the log record
        output = formatter.format(record)

        # Handle exceptions with rich.traceback and then clear exc_info
        if record.exc_info:
            exception_type, exception, traceback_type = record.exc_info
            lines = self._exception_formatter.format_exception(
                exception_type, exception, traceback_type
            )
            error_string = "".join(lines)

            # Remove exec & stack info from record such that they dont get displayed twice!
            duplicte_record = logging.LogRecord(
                record.name,
                record.levelno,
                record.pathname,
                record.lineno,
                record.msg,
                record.args,
                None,  # This is the exc info
            )
            simplified_output = formatter.format(duplicte_record)
            return simplified_output + "\n" + error_string
        return output


class JsonFormatter(logging.Formatter):
    def __init__(
        self,
    ) -> None:
        self._exception_formatter = better_exceptions.ExceptionFormatter(
            colored=False,
            theme=THEME,
            max_length=better_exceptions.MAX_LENGTH,
            pipe_char=better_exceptions.PIPE_CHAR,
            cap_char=better_exceptions.CAP_CHAR,
        )

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(record.created)

        log_object = {
            "timestamp": timestamp.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3],
            "level": record.levelname,
            "message": record.getMessage(),
            "name": record.name,
            "module": record.module,
            "iso_timestamp": timestamp.isoformat(),
        }

        # These are the standard attributes of a LogRecord. We iterate through the record's
        # __dict__ to find any extra attributes that have been passed.
        standard_attributes = {
            "args",
            "asctime",
            "created",
            "exc_info",
            "exc_text",
            "filename",
            "funcName",
            "levelname",
            "levelno",
            "lineno",
            "module",
            "msecs",
            "message",
            "msg",
            "name",
            "pathname",
            "process",
            "processName",
            "relativeCreated",
            "stack_info",
            "thread",
            "threadName",
        }

        for key, value in record.__dict__.items():
            if key not in standard_attributes:
                log_object[key] = value

        if record.exc_info:
            log_object["exception"] = "".join(
                self._exception_formatter.format_exception(
                    record.exc_info[0], record.exc_info[1], record.exc_info[2]
                )
            )
        return json.dumps(log_object)
