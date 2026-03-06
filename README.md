# usiGrabber

usiGrabber is a tool for building large-scale mass spectrometry-based proteomics datasets from publicly available data. It parses peptide spectrum matches (PSMs) and associated metadata from mzIdentML files from [PRIDE](https://www.ebi.ac.uk/pride/), stores them in a queryable database, and provides tools to download and export the corresponding raw spectra for downstream machine learning use.

## Repository Overview

This repository contains two packages that together implement the full dataset construction pipeline:

| Package              | Description                                                                      | Documentation                                                      |
| -------------------- | -------------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| **usiGrabber**       | Extracts PSMs and metadata from PRIDE into a PostgreSQL database                 | [src/usigrabber/README.md](./src/usigrabber/README.md)             |
| **Spectrum Toolkit** | Queries the database and downloads raw spectra for use in machine learning tasks | [src/spectrum_toolkit/README.md](./src/spectrum_toolkit/README.md) |

## Proof-of-Concept: Curating a dataset for retraining a binary phosphorylation classifier

As a proof of concept, we used usiGrabber to curate a dataset for retraining a binary phosphorylation classifier based on the AHLF model architecture. Adjustments to the original code and the weights of our retrained model are available in the [AHLF-fork](https://github.com/usiGrabber/AHLF-fork) repository.

## General Setup

1. Install prerequisites
   - [uv](https://docs.astral.sh/uv/getting-started/installation/)
   - [Docker](https://docs.docker.com/get-docker/) (optional, recommended for PostgreSQL)

2. Install dependencies and set up pre-commit hooks

   ```bash
   uv sync
   uv run pre-commit install
   ```

3. Configure environment variables
   - Copy `.env.sample` to `.env` and adjust as needed (database URL, credentials).

For detailed database setup and build commands see [src/usigrabber/README.md](./src/usigrabber/README.md).
