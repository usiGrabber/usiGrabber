import logging
import os
from typing import Any
from urllib.parse import urlparse

from sqlalchemy.engine.base import Engine
from sqlmodel import create_engine
from sqlmodel.pool import StaticPool

logger = logging.getLogger(__name__)

DEFAULT_DB_URL = "sqlite:///database.db"


def build_postgres_url() -> str:
    """
    Build PostgreSQL connection URL from environment variables.
    Information from DB_URL takes precedence over individual components.
    """

    url = urlparse(os.environ.get("DB_URL", ""))
    assert url.scheme in ("postgresql", "postgres", "postgresql+psycopg"), (
        "DB_URL must start with postgresql://, postgres://, or postgresql+psycopg://."
    )

    # user and pwd can also be empty if connection is anonymous
    user = url.username or os.environ.get("POSTGRES_USER")
    pwd = url.password or os.environ.get("POSTGRES_PASSWORD")
    assert user is not None, "Postgres user must be set via DB_URL or POSTGRES_USER"
    assert pwd is not None, "Postgres password must be set via DB_URL or POSTGRES_PASSWORD"

    host = url.hostname or os.environ.get("POSTGRES_HOST", "localhost")
    port = url.port or os.environ.get("POSTGRES_PORT", "5432")
    name = url.path.lstrip("/") or os.environ.get("POSTGRES_DB", "usigrabber")

    # Construct the final URL
    # we want to force psycopg(3) as the driver, as the default is the predecessor (psycopg2)
    return f"postgresql+psycopg://{user}:{pwd}@{host}:{port}/{name}"


def load_db_engine(debug_sql: bool = False) -> Engine:
    kwargs: dict[str, Any] = {"url": os.getenv("DB_URL", DEFAULT_DB_URL)}

    if kwargs["url"].startswith("sqlite://"):
        kwargs |= {"connect_args": {"check_same_thread": False}, "poolclass": StaticPool}
        logger.info("Using SQLite database at %s", kwargs["url"])
    elif (
        kwargs["url"].startswith("postgresql://")
        or kwargs["url"].startswith("postgres://")
        or kwargs["url"].startswith("postgresql+psycopg://")
    ):
        kwargs["url"] = build_postgres_url()
        logger.info("Using PostgreSQL database at %s", urlparse(kwargs["url"]).hostname)
    else:
        raise ValueError("Unsupported DB_URL scheme. Use sqlite:// or postgresql://")

    # Use DB_ECHO_SQL from environment if not explicitly set
    echo_sql = debug_sql or os.getenv("DB_ECHO_SQL")
    if echo_sql:
        logger.info("SQL echo is enabled.")
        kwargs["echo"] = True

    return create_engine(**kwargs)
