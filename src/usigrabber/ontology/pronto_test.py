import asyncio

from ontology_resolver.ontology_loader import OntologyLoader

# # Load the PSI-MS ontology (downloaded from GitHub or EBI)
# ontology = Ontology(
# 	"https://raw.githubusercontent.com/HUPO-PSI/psi-ms-CV/master/psi-ms.obo"
# )

# term: Term | Relationship = ontology["MS:1001742"]
# print(term.name)  # LTQ Orbitrap Velos
# print(term.definition)  # A Thermo Fisher Scientific hybrid linear ion trap-Orbitrap.
# # print(term.other["is_a"])
# print(term.superclasses().to_set())


async def main():
	ontology = await OntologyLoader().get_ontology("mod")
	term = ontology["MS:1001742"]

	print(ontology)
	print(term.superclasses().to_set())
	print(term)


if __name__ == "__main__":
	asyncio.run(main())
