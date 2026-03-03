# usiGrabber

This project provides the tools to build the usiGrabber database which stores proteomics data from PRIDE and allows to query for USIs and download associated raw spectra.

## Setup

1. Install prerequisites

- uv: https://docs.astral.sh/uv/getting-started/installation/
- Install recommended VS Code extensions (see `.vscode/extensions.json`)
- Install Docker: https://docs.docker.com/get-docker/ (optional, but recommended)

2. Install dependencies and setup pre-commit hooks

```bash
uv sync

uv run pre-commit install
```

3. Configure environment variables

- Copy `.env.sample` to `.env` and modify as needed or described in the sections below.

## Packages

The project is organized into two packages. Together they can be used to construct machine learning datasets from proteomics data.

### [usiGrabber](./src/usigrabber)

This is the main package that builds the database containing peptide spectrum matches (PSMs) and associated metadata.

### [spectrum-toolkit](./src/spectrum_toolkit)

This package provides generic tools to build datasets from the usiGrabber database:

- export PSMs to CSV/Parquet via custom SQL queries
- download raw spectra files from PRIDE and store them as Parquet or MGF output

The `queries/` folder inside the package contains example queries (e.g. phosphorylation modification prediction). Write your own queries to support different use cases.

## License
This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
