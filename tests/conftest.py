import logging
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from dotenv import load_dotenv
from sqlalchemy import Engine
from sqlmodel import Session, create_engine
from sqlmodel.pool import StaticPool

from usigrabber.db.schema import create_db_and_tables
from usigrabber.db.seed import seed_minimal_data

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def load_env(request):
    """Load environment variables from a .env file for tests."""

    env_path = Path(".env.test")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        logger.warning(
            "Environment file .env.test not found. Skipping loading environment variables."
        )


@pytest.fixture(name="engine")
def engine_fixture() -> Generator[Engine, Any, None]:
    # in-memory SQLite database for testing
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    create_db_and_tables(engine)
    yield engine


@pytest.fixture(name="session")
def seeded_session(engine: Engine) -> Generator[Session, Any, None]:
    create_db_and_tables(engine)
    seed_minimal_data(engine)
    logger.warning("Using seeded in-memory sqlite database for testing.")
    with Session(engine) as session:
        yield session
