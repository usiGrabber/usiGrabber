[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.18853258.svg)](https://doi.org/10.5281/zenodo.18853258)

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

For detailed database setup and build commands, see [src/usigrabber/README.md](./src/usigrabber/README.md).

## Data Availability

The data is available on Zenodo:

- Main record containing the training dataset and model weights for trained [AHLF](https://github.com/usiGrabber/AHLF-fork) model: [https://zenodo.org/records/18853258](https://zenodo.org/records/18853258)
- The usiGrabber database tables are split across multiple records: 
  - peptide_spectrum_matches table: [https://zenodo.org/records/18890370](https://zenodo.org/records/18890370)
  - psm_peptide_evidence table: [https://zenodo.org/records/18864164](https://zenodo.org/records/18864164)
  - Other, smaller tables: [https://zenodo.org/records/18873214](https://zenodo.org/records/18873214)
 
Refer to our [database import/export guide](https://github.com/usiGrabber/usiGrabber/tree/main/src/usigrabber/db_export) on how to reassemble the database from these records.

## Citation

**usiGrabber: Automating the curation of proteomics spectra data at scale, making large datasets ready for use in machine learning systems**
Georg Auge, Matthis Clausen, Konstantin Ketterer, Jacob Schaefer, Nils Schmitt, Tom Altenburg, Yannick Hartmaring, Hendrik Raetz, Christoph N. Schlaffner,  Bernhard Y. Renard
doi: https://doi.org/10.64898/2026.03.15.711873 
