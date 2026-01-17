import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from usigrabber.db import Project
from usigrabber.db.schema import Base, create_db_and_tables


@pytest.fixture
def engine():
    """Create a sqlite in memory test database engine."""

    engine = create_engine("sqlite:///:memory:")

    # Drop and recreate all tables for a clean test environment
    Base.metadata.drop_all(engine)
    create_db_and_tables(engine)

    yield engine

    # Cleanup after tests
    Base.metadata.drop_all(engine)


@pytest.fixture
def sample_project(engine: Engine):
    """Create a sample project for testing."""
    project = Project(
        accession="PXD000001",
        title="Test Project",
        submission_type="COMPLETE",
    )
    with Session(engine) as session:
        session.add(project)
        session.commit()
        session.refresh(project)
        # Make sure the project is detached from this session before returning
        session.expunge(project)
    return project
