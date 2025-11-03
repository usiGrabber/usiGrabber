import json
import logging
import os
from pathlib import Path
from time import time

from async_http_client import AsyncHttpClient
from pronto.ontology import Ontology

logger = logging.getLogger(__name__)


class OntologyLoader:
	BASE_URL: str = "https://www.ebi.ac.uk/ols4"
	OWL_DOWNLOAD_KEYS = ["iri", "ontologyPurl"]
	CACHE_DIR: Path = Path(".cache/ontologies")

	async def download_ontology(self, onto: str) -> None:
		async with AsyncHttpClient(retry_attempts=0) as session:
			params = {"lang": "en", "outputOpts": json.dumps({})}
			start_time = time()
			ontology_info = await session.get(
				self.BASE_URL + f"/api/v2/ontologies/{onto}", params=params
			)
			print(f"API: {time() - start_time}")
			assert isinstance(ontology_info, dict), (
				f"Ontology info for : {onto} is not of instance dict"
			)
			os.makedirs(self.CACHE_DIR, exist_ok=True)
			download_file_name = self.CACHE_DIR / f"{onto}.owl"

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

	async def get_ontology(self, onto: str) -> Ontology:
		file = self.CACHE_DIR / f"{onto}.owl"
		if not os.path.isfile(file):
			await self.download_ontology(onto)

		return Ontology(file)
