import asyncio

from ontology_resolver.ontology_loader import OntologyLoader


async def main():
	t = OntologyLoader()
	await t.get_ontology("NCBITaxon")


if __name__ == "__main__":
	asyncio.run(main())
