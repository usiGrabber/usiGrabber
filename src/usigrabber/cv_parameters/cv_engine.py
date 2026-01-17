from dataclasses import dataclass
from typing import Self, assert_never

from sqlalchemy import and_, or_, select
from sqlalchemy.engine import Engine
from sqlalchemy.orm import Session

from usigrabber.db import CvParam as DBCVParam
from usigrabber.db import Project


@dataclass
class CVParam:
    accession: str
    """
    Accession with a format like: MS:1000463
    """
    value: str | None = None


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

    def __init__(self, project_accession: str, engine: Engine):
        self._engine = engine
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
        assert self._engine

        # Early return if buffer is empty
        if not self._cv_param_buffer:
            return

        filters = []
        for cv in self._cv_param_buffer:
            if cv.value is None:
                filters.append(and_(DBCVParam.accession == cv.accession, DBCVParam.value.is_(None)))  # pyright: ignore[reportAttributeAccessIssue, reportOptionalMemberAccess] (is_ does not have the correct linter error)
            else:
                filters.append(
                    and_(DBCVParam.accession == cv.accession, DBCVParam.value == cv.value)
                )

        with Session(self._engine) as session:
            project = session.get(Project, self._project_accession)

            if not project:
                raise ValueError(
                    f"Project with accession {self._project_accession} not found while loading ontologies"
                )

            statement = select(DBCVParam).where(or_(*filters))
            existing = session.execute(statement).scalars()
            existing_params = list(existing.all())

            # Map existing for fast lookup
            existing_map = {(cv.accession, cv.value): cv for cv in existing_params}

            # Step 3: Prepare CvParams to add
            new_cvs = []
            for cv in self._cv_param_buffer:
                name, value = cv.accession, cv.value
                db_value = str(value) if value is not None else None
                key = (name, db_value)
                if key not in existing_map:
                    cv_param = DBCVParam(
                        accession=name, value=str(value) if value is not None else None
                    )
                    session.add(cv_param)

                    existing_map[key] = cv_param
                    new_cvs.append(cv_param)
                    session.flush()  # Assigns ID

            # Step 4: Link all cv_params to project
            # Get the project and append cv_params to its cv_params list

            # Get existing cv_param IDs for this project to avoid duplicates
            existing_cv_param_ids = {cv.id for cv in project.cv_params}
            # Append only new cv_params
            for cv_param in existing_map.values():
                if cv_param.id not in existing_cv_param_ids:
                    project.cv_params.append(cv_param)

            session.commit()

        self._cv_param_buffer = []

    async def add(self, data: CVTuple | CVParam):
        if self._project_accession is None:
            raise ValueError("You must use this as an async context manager!")

        if len(self._cv_param_buffer) >= self._max_buffer_size:
            await self._flush_data_to_db()
        if isinstance(data, CVTuple):
            self._cv_param_buffer.append(data.value)
        elif isinstance(data, CVParam):
            self._cv_param_buffer.append(data)
        else:
            assert_never()
