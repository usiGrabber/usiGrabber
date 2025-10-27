import logging
import os

from dotenv import load_dotenv
from sqlalchemy.engine.base import Engine
from sqlmodel import create_engine

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CLUSTER_DB_PATH = "/sc/projects/sci-renard/usi-grabber/.db/database.db"
LOCAL_DB_PATH = "database.db"


def load_db_engine(debug_sql: bool = False) -> Engine:
	if os.path.exists(CLUSTER_DB_PATH) and os.getenv("USE_LOCAL_DB") is None:
		sqlite_file_name = CLUSTER_DB_PATH
	else:
		sqlite_file_name = LOCAL_DB_PATH
	logger.info(f"Using sqlite db at: {sqlite_file_name}")
	sqlite_url = f"sqlite:///{sqlite_file_name}"

	return create_engine(sqlite_url, echo=debug_sql)
