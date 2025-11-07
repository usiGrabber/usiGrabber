import json
import logging
import os
from pathlib import Path

from async_http_client import AsyncHttpClient
from pronto.ontology import Ontology

logger = logging.getLogger(__name__)


class OntologyLoader:
    BASE_URL: str = "https://www.ebi.ac.uk/ols4"
    OWL_DOWNLOAD_KEY = "http://www.w3.org/2002/07/owl#versionIRI"
    CACHE_DIR: Path = Path(".cache/ontologies")

    async def download_ontology(self, onto: str) -> None:
        async with AsyncHttpClient() as session:
            params = {"lang": "en", "outputOpts": json.dumps({})}
            print(params)
            ontology_info = await session.get(
                self.BASE_URL + f"/api/v2/ontologies/{onto}", params=params
            )

            assert isinstance(ontology_info, dict), (
                f"Ontology info for : {onto} is not of instance dict"
            )

            assert self.OWL_DOWNLOAD_KEY in ontology_info, (
                f"{self.OWL_DOWNLOAD_KEY} not in ontology data"
            )
            os.makedirs(self.CACHE_DIR, exist_ok=True)
            download_file_name = self.CACHE_DIR / f"{onto}.owl"
            try:
                download_link = ontology_info[self.OWL_DOWNLOAD_KEY]
                await session.stream_file(download_link, download_file_name)
            except ValueError:
                logger.warning(
                    f"Failed to fetch {ontology_info[self.OWL_DOWNLOAD_KEY]} falling",
                    " back to {ontology_info['iri']}",
                )
                await session.stream_file(ontology_info["iri"], download_file_name)

    async def get_ontology(self, onto: str) -> Ontology:
        file = self.CACHE_DIR / f"{onto}.owl"
        if not os.path.isfile(file):
            await self.download_ontology(onto)

        return Ontology(file)
