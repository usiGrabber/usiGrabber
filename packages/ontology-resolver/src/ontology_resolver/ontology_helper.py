import logging
import time
import warnings

from pronto.ontology import Ontology
from pronto.term import Term

from ontology_resolver.ontology_loader import OntologyLoader

logger = logging.Logger(__name__)


warnings.filterwarnings("ignore", module="pronto")


class OntologyHelperSingletonMeta(type):
	_instance = {}

	def __call__(cls, *args, **kwargs):
		if cls not in cls._instance:
			cls._instance[cls] = super().__call__(*args, **kwargs)
		return cls._instance[cls]


class OntologyHelper(metaclass=OntologyHelperSingletonMeta):
	"""
	This class is a singleton with cached ontologies
	This avoids parsing the same ontology multiple times.
	"""

	def __init__(self):
		self.ontologies: dict[str, Ontology] = {}

	def parse_ontology(self, term: str) -> tuple[str, str]:
		cv, id_number = term.split(":")
		return cv, id_number

	async def get_ontology(self, onto: str) -> Ontology:
		if onto in self.ontologies:
			return self.ontologies[onto]

		# Manually replace wrong/outdated ontology names

		if onto.lower() == "newt":
			onto = "NCBITaxon"
		start_time = time.time()
		self.ontologies[onto] = await OntologyLoader().get_ontology(onto)
		logger.info(f"Loaded {onto} in {time.time() - start_time}s")
		return self.ontologies[onto]

	async def get_superclasses(self, term: str) -> list[Term]:
		"""
		Includes the term itself
		"""
		cv_accession, _ = self.parse_ontology(term)
		ontology = await self.get_ontology(cv_accession)
		return list(iter(ontology[term].superclasses()))  # pyright: ignore[reportAttributeAccessIssue]
