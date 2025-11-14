import logging
from collections.abc import Generator
from typing import Any

import pytest
from sqlmodel import Session, create_engine, func, select
from sqlmodel.pool import StaticPool

from usigrabber.db.schema import Project, create_db_and_tables
from usigrabber.db.seed import seed_minimal_data

logger = logging.getLogger(__name__)


@pytest.fixture(name="session")
def session_fixture() -> Generator[Session, Any, None]:
    # in-memory SQLite database for testing
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    create_db_and_tables(engine)
    seed_minimal_data(engine)
    logger.warning("Using seeded in-memory sqlite database for testing.")
    with Session(engine) as session:
        yield session


def test_db(session: Session) -> None:
    statement = select(func.count()).select_from(Project)
    count = session.exec(statement).one()
    print("test message")
    assert count > 0
