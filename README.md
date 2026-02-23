# usi grabber

This project provides the tools to build the usigrabber database which stores proteomics data from PRIDE and allows to query for USIs and download associated raw spectra.

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

### [usigrabber](./src/usigrabber)
This is the main package that builds the database containing peptide spectrum matches (PSMs) and associated metadata.

### [modification-prediction](./src/mod_prediction)
This package provides tools to
- export PSMs as csv files, which can be used to
- download raw spectra files from PRIDE and store them as mgf or parquet output
