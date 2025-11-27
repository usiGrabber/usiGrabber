"""Tests for CVInjector class that manages cv_param insertion and linking."""

import asyncio

from sqlmodel import Session, select

from usigrabber.cv_parameters.cv_engine import (
    CVInjector,
    CVParam,
    CVTuple,
)
from usigrabber.db import CvParam as DBCVParam
from usigrabber.db import Project


def run_async(coro):
    """Helper to run async code in sync tests."""
    return asyncio.run(coro)


def test_cv_injector_creates_cv_params(session: Session, sample_project: Project):
    """Test that CVInjector creates new cv_params."""

    async def test_impl():
        async with CVInjector(sample_project.accession, session) as injector:
            await injector.add(CVParam(accession="MS:1000463", value="Orbitrap"))
            await injector.add(CVParam(accession="MS:1000031", value="Mascot"))

    run_async(test_impl())

    # Verify cv_params were created
    statement = select(DBCVParam)
    cv_params = session.exec(statement).all()
    assert len(cv_params) == 2

    # Verify values
    cv_param_dict = {cv.accession: cv.value for cv in cv_params}
    assert cv_param_dict["MS:1000463"] == "Orbitrap"
    assert cv_param_dict["MS:1000031"] == "Mascot"


def test_cv_injector_links_params_to_project(session: Session, sample_project: Project):
    """Test that CVInjector links cv_params to the project."""

    async def test_impl():
        async with CVInjector(sample_project.accession, session) as injector:
            await injector.add(CVParam(accession="MS:1000463", value="Orbitrap"))

    run_async(test_impl())
    session.commit()

    # Refresh project to get updated relationships
    session.refresh(sample_project)

    # Verify cv_param is linked to project
    assert len(sample_project.cv_params) == 1
    assert sample_project.cv_params[0].accession == "MS:1000463"
    assert sample_project.cv_params[0].value == "Orbitrap"


def test_cv_injector_reuses_existing_cv_params(session: Session, sample_project: Project):
    """Test that CVInjector reuses existing cv_params instead of duplicating."""

    async def test_impl1():
        async with CVInjector(sample_project.accession, session) as injector:
            await injector.add(CVParam(accession="MS:1000463", value="Orbitrap"))

    async def test_impl2():
        async with CVInjector(sample_project.accession, session) as injector:
            await injector.add(CVParam(accession="MS:1000463", value="Orbitrap"))

    # First injection
    run_async(test_impl1())
    session.commit()

    # Second injection with same cv_param
    run_async(test_impl2())
    session.commit()

    # Verify only one cv_param exists
    statement = select(DBCVParam)
    cv_params = session.exec(statement).all()
    assert len(cv_params) == 1


def test_cv_injector_avoids_duplicate_links(session: Session, sample_project: Project):
    """Test that CVInjector doesn't create duplicate links between project and cv_param."""

    async def test_impl():
        async with CVInjector(sample_project.accession, session) as injector:
            await injector.add(CVParam(accession="MS:1000463", value="Orbitrap"))

    # First injection
    run_async(test_impl())
    session.commit()
    session.refresh(sample_project)
    initial_count = len(sample_project.cv_params)

    # Second injection with same cv_param
    run_async(test_impl())
    session.commit()
    session.refresh(sample_project)

    # Verify no duplicate links were created
    assert len(sample_project.cv_params) == initial_count
    assert len(sample_project.cv_params) == 1


def test_cv_injector_handles_cv_tuples(session: Session, sample_project: Project):
    """Test that CVInjector handles CVTuple (key-value pairs) and only adds the value."""

    async def test_impl():
        key = CVParam(accession="MS:1000031")
        value = CVParam(accession="MS:1000032", value="Mascot")
        async with CVInjector(sample_project.accession, session) as injector:
            await injector.add(CVTuple(key=key, value=value))

    run_async(test_impl())
    session.commit()
    session.refresh(sample_project)

    # According to CVInjector docs, only the value is added
    assert len(sample_project.cv_params) == 1
    assert sample_project.cv_params[0].accession == "MS:1000032"


def test_cv_injector_handles_none_values(session: Session, sample_project: Project):
    """Test that CVInjector handles cv_params with None values."""

    async def test_impl():
        async with CVInjector(sample_project.accession, session) as injector:
            await injector.add(CVParam(accession="MS:1000463", value=None))

    run_async(test_impl())
    session.commit()
    session.refresh(sample_project)

    assert len(sample_project.cv_params) == 1
    assert sample_project.cv_params[0].accession == "MS:1000463"
    assert sample_project.cv_params[0].value is None


def test_cv_injector_buffer_flush(session: Session, sample_project: Project):
    """Test that CVInjector flushes buffer when max size is reached."""

    async def test_impl():
        async with CVInjector(sample_project.accession, session) as injector:
            # Add more than buffer size (200) to trigger flush
            for i in range(250):
                await injector.add(CVParam(accession=f"MS:{i:07d}", value=str(i)))

    run_async(test_impl())
    session.commit()
    session.refresh(sample_project)

    # Verify all cv_params were created and linked
    assert len(sample_project.cv_params) == 250


def test_cv_injector_shared_cv_param_across_projects(session: Session):
    """Test that the same cv_param can be linked to multiple projects."""
    # Create two projects
    project1 = Project(accession="PXD000001", title="Project 1", submissionType="COMPLETE")
    project2 = Project(accession="PXD000002", title="Project 2", submissionType="COMPLETE")
    session.add(project1)
    session.add(project2)
    session.commit()

    async def test_impl1():
        async with CVInjector(project1.accession, session) as injector:
            await injector.add(CVParam(accession="MS:1000463", value="Orbitrap"))

    async def test_impl2():
        async with CVInjector(project2.accession, session) as injector:
            await injector.add(CVParam(accession="MS:1000463", value="Orbitrap"))

    # Add same cv_param to both projects
    run_async(test_impl1())
    run_async(test_impl2())

    session.commit()

    # Verify only one cv_param exists in database
    statement = select(DBCVParam)
    cv_params = session.exec(statement).all()
    assert len(cv_params) == 1

    # Verify both projects link to the same cv_param
    session.refresh(project1)
    session.refresh(project2)
    assert len(project1.cv_params) == 1
    assert len(project2.cv_params) == 1
    assert project1.cv_params[0].id == project2.cv_params[0].id
