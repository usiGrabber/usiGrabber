import pytest
from sqlmodel import Session, create_engine
from sqlmodel.pool import StaticPool

from usigrabber.db import Project
from usigrabber.db.schema import create_db_and_tables


@pytest.fixture
def session():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    create_db_and_tables(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture
def sample_project(session: Session):
    """Create a sample project for testing."""
    project = Project(
        accession="PXD000001",
        title="Test Project",
        submissionType="COMPLETE",
    )
    session.add(project)
    session.commit()
    session.refresh(project)
    return project
