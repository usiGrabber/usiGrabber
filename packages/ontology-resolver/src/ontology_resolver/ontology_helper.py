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
        self._ontology_loader = OntologyLoader()

    def parse_accession(self, accession: str) -> tuple[str, str]:
        prefix, number = accession.split(":")
        # Manually replace wrong/outdated ontology names
        if prefix == "NEWT":
            prefix = "NCBITaxon"
        return prefix, number

    def build_accession(self, cv_prefix: str, number: str) -> str:
        return f"{cv_prefix}:{number}"

    async def get_ontology(self, onto: str) -> Ontology:
        if onto in self.ontologies:
            return self.ontologies[onto]
        start_time = time.time()
        self.ontologies[onto] = await self._ontology_loader.get_ontology(onto)
        logger.info(f"Loaded {onto} in {time.time() - start_time}s")
        return self.ontologies[onto]

    async def get_superclasses(self, accession: str) -> list[Term]:
        """
        Includes the term itself
        """
        # We need to parse and rebuild the term because parse_accession
        # might correct CV prefix of the accession
        cv_prefix, number = self.parse_accession(accession)
        new_accession = self.build_accession(cv_prefix, number)
        ontology = await self.get_ontology(cv_prefix)
        return list(iter(ontology[new_accession].superclasses()))  # pyright: ignore[reportAttributeAccessIssue]
