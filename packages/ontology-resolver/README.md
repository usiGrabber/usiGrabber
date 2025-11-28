# ontology-resolver

A Python package for downloading and resolving biological ontology terms from the EBI OLS4 API.

## Features

- Downloads OWL ontology files from [EBI OLS4](https://www.ebi.ac.uk/ols4)
- Caches ontologies locally to avoid redundant downloads
- Parses and queries ontology terms using [Pronto](https://github.com/althonos/pronto)
- Resolves ontology term hierarchies and superclasses
- Singleton pattern for efficient ontology reuse

## Installation

```bash
uv add ontology-resolver
```

## Usage

```python
from ontology_resolver import OntologyHelper

# Create helper instance (singleton)
helper = OntologyHelper()

# Get superclasses for a term (includes the term itself)
superclasses = await helper.get_superclasses("NCBITaxon:9606")

# Download and cache an ontology
ontology = await helper.get_ontology("UNIMOD")
```

## How It Works

1. **OntologyLoader**: Downloads `.owl` files from EBI OLS4 API and caches them locally
2. **OntologyHelper**: Singleton class that manages cached ontologies and provides term resolution
3. **Automatic caching**: Downloaded ontologies are stored in the cache directory
4. **Shrinking**: Large ontologies (e.g., NCBITaxon) are automatically shrunk for faster loading

## Supported Ontologies

Any ontology available in the [EBI OLS4](https://www.ebi.ac.uk/ols4) can be downloaded and used, including:

- UNIMOD (protein modifications)
- NCBITaxon (organism taxonomy)
- MS (mass spectrometry ontology)
- And many more...

## Development

```bash
# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=src tests/
```
