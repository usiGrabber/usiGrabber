import logging
from dataclasses import dataclass
from typing import Self, assert_never

from sqlmodel import Session, and_, or_, select

from usigrabber.db import CvParam as DBCVParam
from usigrabber.db.schema import Project

logger = logging.getLogger(__name__)


@dataclass
class CVParam:
    term: str
    """
    Term with a format like: MS:1000463
    """
    cv_label: str | None = None
    """
    Optionally if the CV part of the term is differnt. Can be dangerous to use
    """
    name: str | None = None
    value: str | float | int | None = None


@dataclass
class CVTuple:
    key: CVParam
    value: CVParam


class CVInjector:
    """
    This class takes in CVParams via a defined interface
    and adds them to the DB in a buffer and efficient way.

    Currently, we accept tuples (key, value) but only add the value to the DB
    """

    def __init__(self, project_accession: str, session: Session):
        self._session = session
        self._project_accession = project_accession
        self._cv_param_buffer: list[CVParam] = []
        self._max_buffer_size = 200

    async def __aenter__(
        self,
    ) -> Self:
        # Set up resources asynchronously (e.g. connect to DB, open socket, etc.)
        if len(self._cv_param_buffer) > 0:
            raise ValueError(
                "CV Param Buffer was not really flushed! User or implementation issue.",
                " Please check it",
            )
        self._cv_param_buffer = []
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._flush_data_to_db()
        # Returning False means any exception will be propagated
        return False

    async def _flush_data_to_db(self) -> None:
        assert self._session
        project = self._session.get(Project, self._project_accession)
        if project is None:
            logger.error(
                f"No project found for accession: {self._project_accession}. The project needs to be written to the db before cv params can be added!"
            )
            return

        filters = []
        for cv in self._cv_param_buffer:
            if cv.value is None:
                filters.append(and_(DBCVParam.name == cv.term, DBCVParam.value.is_(None)))  # pyright: ignore[reportAttributeAccessIssue, reportOptionalMemberAccess] (is_ does not have the correct linter error)
            else:
                filters.append(and_(DBCVParam.name == cv.term, DBCVParam.value == str(cv.value)))

        if not filters:
            return

        statement = select(DBCVParam).where(or_(*filters))
        existing = self._session.exec(statement)
        existing_params = list(existing.all())

        # Map existing for fast lookup
        existing_map = {(cv.name, cv.value): cv for cv in existing_params}

        # Step 3: Prepare CvParams to add
        for cv in self._cv_param_buffer:
            name, value = cv.term, cv.value
            db_value = str(value) if value is not None else None
            key = (name, db_value)
            if key not in existing_map:
                cv_param = DBCVParam(name=name, value=str(value) if value is not None else None)
                self._session.add(cv_param)
                existing_map[key] = cv_param

            db_cv_param = existing_map[key]
            if db_cv_param not in project.cv_params:
                project.cv_params.append(db_cv_param)

        self._session.flush()  # Assigns ID
        self._cv_param_buffer = []

    async def add(self, data: CVTuple | CVParam):
        if self._session is None or self._project_accession is None:
            raise ValueError("You must use this as an async context manager!")

        if len(self._cv_param_buffer) > self._max_buffer_size:
            await self._flush_data_to_db()
        if isinstance(data, CVTuple):
            self._cv_param_buffer.append(data.value)
        elif isinstance(data, CVParam):
            self._cv_param_buffer.append(data)
        else:
            assert_never()
