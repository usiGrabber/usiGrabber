"""
Project Exchange - Backend-agnostic project lookup.

Determines which backends have a given project accession by racing
all backend queries in parallel.
"""

import asyncio

from usigrabber.backends import BackendEnum
from usigrabber.backends.base import BaseBackend
from usigrabber.utils import logger


class ProjectExchange:
    """
    Determines which backends have a given project accession.

    Queries all available backends in parallel and returns the list of
    backends that successfully found the project.
    """

    @classmethod
    async def get_backends_for_project(cls, project_accession: str) -> list[BackendEnum]:
        """
        Find which backends have the given project accession.

        Queries all backends in parallel using asyncio.gather and returns
        the list of backends that successfully found the project.

        Args:
            project_accession: Project accession to search for

        Returns:
            List of backend enums that have the project

        Example:
            >>> backends = await ProjectExchange.get_backends_for_project("PXD000001")
            >>> print(backends)
            [BackendEnum.PRIDE]
        """

        async def try_backend(backend_enum: BackendEnum) -> BackendEnum | None:
            """Try to get project from a single backend."""
            backend_class: type[BaseBackend] = backend_enum.value
            try:
                logger.debug(f"Checking {backend_enum.name} for project {project_accession}")
                await backend_class.get_project(project_accession)
                logger.info(f"✓ Found project {project_accession} in {backend_enum.name}")
                return backend_enum
            except Exception as e:
                logger.debug(f"Project {project_accession} not found in {backend_enum.name}: {e}")
                return None

        # Race all backends in parallel
        results = await asyncio.gather(
            *[try_backend(backend_enum) for backend_enum in BackendEnum],
            return_exceptions=False,
        )

        # Filter out None results
        available_backends = [backend for backend in results if backend is not None]

        if not available_backends:
            logger.warning(f"Project {project_accession} not found in any backend")

        return available_backends
