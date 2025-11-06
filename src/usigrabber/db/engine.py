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
	if os.getenv("USE_LOCAL_DB") is not None:
		sqlite_file_name = LOCAL_DB_PATH
		logger.info("USE_LOCAL_DB environment variable is set. Using local database.")
	elif os.path.exists(CLUSTER_DB_PATH):
		sqlite_file_name = CLUSTER_DB_PATH
		logger.info("Using cluster database.")
	else:
		sqlite_file_name = LOCAL_DB_PATH
		logger.warning(f"Cluster DB path '{CLUSTER_DB_PATH}' does not exist. Falling back to local database.")
	logger.info(f"Using sqlite db at: {sqlite_file_name}")
	sqlite_url = f"sqlite:///{sqlite_file_name}"

	# Use DB_ECHO_SQL from environment if not explicitly set
	echo_sql = debug_sql or os.getenv("DB_ECHO_SQL", "0") == "1"

	return create_engine(sqlite_url, echo=echo_sql)
