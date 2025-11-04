from pathlib import Path

from ontology_resolver.utils import shrink_owl_file
from pronto.ontology import Ontology
from pronto.term import Term


def test_shrinking_mod_ontology():
	MOD_OWL = Path(__file__).parent / "fixtures/mod.owl"
	TARGET_OWL = Path(__file__).parent / "temp_test_data/mod_shrunk.owl"
	shrink_owl_file(MOD_OWL, TARGET_OWL)

	onto = Ontology(TARGET_OWL)

	term = onto["MOD:00983"]

	parents: list[Term] = list(iter(term.superclasses()))  # pyright: ignore[reportAttributeAccessIssue]

	assert parents[0].id == "MOD:00983"
	assert parents[1].id == "MOD:00947"
	assert parents[2].id == "MOD:00032"
	assert parents[3].id == "MOD:00000"
