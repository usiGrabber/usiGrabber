import logging

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from usigrabber.db.schema import Project

logger = logging.getLogger(__name__)


def test_db(session: Session) -> None:
    statement = select(func.count()).select_from(Project)
    count = session.execute(statement).scalar_one()
    assert count > 0
