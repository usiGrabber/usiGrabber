import json
import logging
import os
from pathlib import Path

from async_http_client import AsyncHttpClient
from pronto.ontology import Ontology

from ontology_resolver.utils import shrink_owl_file

logger = logging.getLogger(__name__)
ONTOLOGIES_TO_SHRINK = ["NCBITaxon"]
ONTOLOGIES_CACHE_DIR_VARIABLE = "ONTOLOGIES_CACHE_DIR"


class OntologyLoader:
    BASE_URL: str = "https://www.ebi.ac.uk/ols4"
    OWL_DOWNLOAD_KEYS = ["iri", "ontologyPurl"]

    def __init__(self):
        fallback_cache_dir = Path(".cache/ontologies")
        cache_dir_env = os.environ.get(ONTOLOGIES_CACHE_DIR_VARIABLE)
        if cache_dir_env:
            cache_dir_env = Path(cache_dir_env)
            if cache_dir_env.is_dir():
                logger.info(f"Using ontology cache dir: {cache_dir_env}")
                self._cache_dir = cache_dir_env
            else:
                logger.warning(
                    f"Ontology cache dir {cache_dir_env} from env variable $ONTOLOGIES_CACHE_DIR",
                    f" does not exist. Falling backt to: {fallback_cache_dir}",
                )
                self._cache_dir = fallback_cache_dir
        else:
            self._cache_dir = fallback_cache_dir
            logger.info(f"Using local ontology cache dir: {self._cache_dir}")

    async def download_ontology(self, onto: str) -> Path:
        async with AsyncHttpClient(retry_attempts=0) as session:
            params = {"lang": "en", "outputOpts": json.dumps({})}
            ontology_info = await session.get(
                self.BASE_URL + f"/api/v2/ontologies/{onto}", params=params
            )
            assert isinstance(ontology_info, dict), (
                f"Ontology info for : {onto} is not of instance dict"
            )
            self._cache_dir.mkdir(exist_ok=True)
            download_file_name = self._cache_dir / f"{onto}.owl"

            for download_key in self.OWL_DOWNLOAD_KEYS:
                if download_key not in ontology_info:
                    logger.warning(f"{download_key} not in ontology data for {onto}")
                    continue
                download_link = ontology_info[download_key]
                try:
                    await session.stream_file(download_link, download_file_name)
                except Exception as e:
                    logger.error(f"Error downloading {onto} from {download_link}: {e}")
                    continue
                break
            else:
                raise ValueError(
                    f"No valid download link found for,"
                    f"{onto}: {self.BASE_URL + f'/api/v2/ontologies/{onto}'}"
                )
            return download_file_name

    async def get_ontology(self, onto: str) -> Ontology:
        file = self._cache_dir / f"{onto}.owl"
        file_shrunk = self._cache_dir / f"{onto}_shrunk.owl"

        if file_shrunk.is_file():
            return Ontology(file_shrunk)
        else:
            if not file.is_file():
                await self.download_ontology(onto)
            if onto in ONTOLOGIES_TO_SHRINK:
                shrink_owl_file(file, file_shrunk)
                return Ontology(file_shrunk)
            return Ontology(file)
