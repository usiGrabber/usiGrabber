import logging
import os
from urllib.parse import urlparse

from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

logger = logging.getLogger(__name__)


def build_postgres_url() -> str:
    """
    Build PostgreSQL connection URL from environment variables.
    Information from DB_URL takes precedence over individual components.
    """

    url = urlparse(os.environ.get("DB_URL", ""))
    assert url.scheme in ("postgresql", "postgres"), (
        "DB_URL must start with postgresql:// or postgres://"
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
    db_url = os.getenv("DB_URL", "")

    if not db_url:
        raise ValueError("DB_URL must be set")

    # Use DB_ECHO_SQL from environment if not explicitly set
    echo_sql = debug_sql or os.getenv("DB_ECHO_SQL")
    if echo_sql:
        logger.info("SQL echo is enabled.")

    if db_url.startswith("sqlite"):
        logger.info("Using SQLite database: %s", db_url)
        return create_engine(db_url, echo=bool(echo_sql))

    if db_url.startswith("postgres"):
        url = build_postgres_url()
        logger.info("Using PostgreSQL database at %s", urlparse(url).hostname)
        return create_engine(url, echo=bool(echo_sql))

    raise ValueError(f"DB_URL must start with postgres or sqlite. Got: {db_url[:20]}...")
