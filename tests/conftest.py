import logging
import os
from collections.abc import Generator
from pathlib import Path
from typing import Any

import pytest
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from usigrabber.db.schema import Base, create_db_and_tables
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
    """Create a PostgreSQL test database engine."""
    db_url = os.getenv("TEST_DB_URL") or os.getenv("DB_URL")
    if not db_url:
        pytest.skip("No database URL configured (TEST_DB_URL or DB_URL)")

    engine = create_engine(db_url)

    # Drop and recreate all tables for a clean test environment
    Base.metadata.drop_all(engine)
    create_db_and_tables(engine)

    yield engine

    # Cleanup after tests
    Base.metadata.drop_all(engine)


@pytest.fixture(name="session")
def seeded_session(engine: Engine) -> Generator[Session, Any, None]:
    seed_minimal_data(engine)
    logger.info("Using seeded PostgreSQL database for testing.")
    with Session(engine) as session:
        yield session
