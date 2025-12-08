import logging

from sqlmodel import Session, func, select

from usigrabber.db.schema import Project

logger = logging.getLogger(__name__)


def test_db(session: Session) -> None:
    statement = select(func.count()).select_from(Project)
    count = session.exec(statement).one()
    assert count > 0
