import logging
from pathlib import Path

import pytest
from dotenv import load_dotenv

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session", autouse=True)
def load_env(request):
    """Load environment variables from a .env file for tests."""

    env_path = Path(".env.test")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
    else:
        logger.warning(
            "Environment file .env.test not found. Skipping loading environment variables."
        )
