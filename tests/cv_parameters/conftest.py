import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session, create_engine
from sqlmodel.pool import StaticPool

from usigrabber.db import Project
from usigrabber.db.schema import create_db_and_tables


@pytest.fixture
def engine():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    create_db_and_tables(engine)
    yield engine


@pytest.fixture
def sample_project(engine: Engine):
    """Create a sample project for testing."""
    project = Project(
        accession="PXD000001",
        title="Test Project",
        submissionType="COMPLETE",
    )
    with Session(engine) as session:
        session.add(project)
        session.commit()
        session.refresh(project)
        # Make sure the project is detached from this session before returning
        session.expunge(project)
    return project
