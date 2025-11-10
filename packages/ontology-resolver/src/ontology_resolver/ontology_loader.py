import json
import logging
import os
from pathlib import Path

from async_http_client import AsyncHttpClient
from pronto.ontology import Ontology

from ontology_resolver.utils import shrink_owl_file

CLUSTER_CACHE_DIR = "/sc/projects/sci-renard/usi-grabber/.cache/ontologies"

logger = logging.getLogger(__name__)

ONTOLOGIES_TO_SHRINK = ["NCBITaxon"]


class OntologyLoader:
    BASE_URL: str = "https://www.ebi.ac.uk/ols4"
    OWL_DOWNLOAD_KEYS = ["iri", "ontologyPurl"]
    cache_dir = Path(".cache/ontologies")

    if os.path.isdir(CLUSTER_CACHE_DIR):
        cache_dir = Path(CLUSTER_CACHE_DIR)

    async def download_ontology(self, onto: str) -> Path:
        async with AsyncHttpClient(retry_attempts=0) as session:
            params = {"lang": "en", "outputOpts": json.dumps({})}
            ontology_info = await session.get(
                self.BASE_URL + f"/api/v2/ontologies/{onto}", params=params
            )
            assert isinstance(ontology_info, dict), (
                f"Ontology info for : {onto} is not of instance dict"
            )
            self.cache_dir.mkdir(exist_ok=True)
            download_file_name = self.cache_dir / f"{onto}.owl"

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
        file = self.cache_dir / f"{onto}.owl"
        file_shrunk = self.cache_dir / f"{onto}_shrunk.owl"

        if os.path.isfile(file_shrunk):
            return Ontology(file_shrunk)
        else:
            if not os.path.isfile(file):
                await self.download_ontology(onto)
            if onto in ONTOLOGIES_TO_SHRINK:
                shrink_owl_file(file, file_shrunk)
                return Ontology(file_shrunk)
            return Ontology(file)
