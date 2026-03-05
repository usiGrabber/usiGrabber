# Spectrum Toolkit

## Overview

This package provides generic tools to build datasets from the usigrabber database. Write a SQL query to select the PSMs you care about, download the corresponding raw spectra from PRIDE, and export everything as Parquet or MGF for downstream analysis.

The `queries/` folder contains **example queries** for a modification prediction use case (phosphorylation). Use them as a starting point and write your own queries for different experiments.

## Workflow

1. **Write a SQL query**: Select PSMs from the usigrabber database that match your use case (see `queries/` for examples).
2. **Extract PSMs**: Run the query and export results to CSV and Parquet.
3. **Download Spectra via raw files**: Download raw files directly from PRIDE and extract spectra using ThermoRawFileParser.
4. **Optional MGF Export**: Convert the enriched Parquet to MGF format for tools that require it.

## Tools

### 1. Database Query Export (SQL → CSV & Parquet)

The `db_fetcher.py` tool efficiently exports large SQL query results to **both CSV and Parquet formats simultaneously** using memory-efficient chunked processing. This ensures you have both formats available for different use cases.

#### Usage

```bash
uv run fetch-psms <sql_file> [options]
```

#### Options

- `sql_file`: Path to SQL file containing the query (required)
- `-o, --output`: Base output path (extensions `.csv` and `.parquet` will be added automatically, default: `<sql_filename>`)
- `-c, --chunksize`: Number of rows per chunk (default: 100,000)

#### Examples

```bash
# Export to both formats with default naming
uv run fetch-psms src/spectrum_toolkit/queries/psms_example.sql

# Custom output base path (creates output/psm_data.csv and output/psm_data.parquet)
uv run fetch-psms src/spectrum_toolkit/queries/psms_example.sql -o output/psm_data

# Use your own query
uv run fetch-psms path/to/my_query.sql -o output/my_dataset
```

#### Required Database Columns

The SQL query must return these columns:

- `psm_id`, `project_accession`, `charge_state`, `ms_run`, `index_type`, `index_number`, `peptide_sequence`

### 2. Spectrum Download & Enrichment (CSV → Parquet)

The `download_raw_spectra.py` tool downloads raw files directly from PRIDE and extracts spectra data using ThermoRawFileParser, outputting results as Parquet format. This is an alternative to the PROXI API approach and is useful for getting raw MS data directly from the instrument files. We validate that charge state from raw files matches the PSM data. In case of a mismatch we skip the entire raw file and write to `output_dir/charge_mismatches.csv`.

#### Setup: ThermoRawFileParser

The `download_raw_spectra.py` script requires ThermoRawFileParser to extract spectra from Thermo raw files. Follow these steps to set it up:

1. Download the latest ThermoRawFileParser release from [GitHub releases](https://github.com/CompOmics/ThermoRawFileParser/releases)
2. Extract it to the `thermo/` directory in the project root:

```bash
# Create the thermo directory
mkdir -p thermo

# Extract the downloaded archive
unzip ThermoRawFileParser-v.X.X.X-linux.zip -d thermo
```

3. Verify the installation:

Note: If you are running this locally on a mac, you might need to bypass security settings to run the executable. We recommend to only run this in a sand-boxed linux environment.

```bash
./thermo/ThermoRawFileParser --help
```

The executable should be at `./thermo/ThermoRawFileParser` (relative to the project root).

#### Usage

```bash
uv run download-raw-spectra <input_file> <output_dir> [options]
```

#### Arguments

- `input_file`: Path to CSV or Parquet file with PSM data (must have `project_accession`, `index_type`, `index_number`, and `ms_run` columns, required)
- `output_dir`: Path to output dir with extracted spectra (required)

#### Options

- `-t, --temp-dir`: Temporary directory for downloaded raw files (default: `./pride_raw_files`)
- `-l, --limit`: Limit number of rows to process (useful for testing)
- `-k, --keep-temp-files`: Keep downloaded raw files and extraction directories after processing
- `--convert-to-mgf`: After each raw file is processed, also export the enriched spectra as an MGF file. One MGF file is written per `project_accession`/`ms_run` combination into `<output_dir>/../mgf_output/`.

#### Examples

```bash
# Download and extract spectra, cleanup temp files automatically
uv run download-raw-spectra output/psm_data.csv output/spectra

# Keep raw files for inspection or reuse
uv run download-raw-spectra output/psm_data.csv output/spectra --keep-temp-files

# Test with first 10 rows
uv run download-raw-spectra output/psm_data.csv output/spectra_test --limit 10 --keep-temp-files

# Use custom temp directory on large storage mount
uv run download-raw-spectra output/psm_data.csv output/spectra --temp-dir /large_storage/downloads

# Also export per-run MGF files alongside the Parquet output (written to output/mgf_output/)
uv run download-raw-spectra output/psm_data.csv output/spectra --convert-to-mgf
```

#### What This Does

1. Reads PSM data from the input CSV or Parquet file
2. Identifies unique `project_accession` / `ms_run` combinations
3. Queries PRIDE API to find matching raw files
4. Downloads raw files via FTP (with caching to avoid re-downloads)
5. Uses ThermoRawFileParser to extract spectra to intermediate JSON files
6. Adds source metadata (`ms_run`, `project_accession`, ...) to each spectrum and save it as Parquet
7. Optionally cleans up temporary files

#### Output Parquet Schema

The output contains metadata and spectrum data extracted from raw files. Among others:

- `scan_id`: Scan number/identifier from the raw file
- `mz_array`: Peak m/z values (array)
- `intensity_array`: Peak intensity values (array)
- `ms_run`: MS run name (from input CSV)
- `project_accession`: PRIDE project accession (from input CSV)

#### Requirements

- ThermoRawFileParser must be available at `./thermo/ThermoRawFileParser`
- Input CSV must have `project_accession`, `ms_run`, and `index_number` columns

### 4. Optional MGF Export (Enriched Parquet → MGF)

The `parquet_to_mgf.py` tool converts enriched Parquet files to MGF format for tools that require MGF input. This is optional since the Parquet file contains all the data. Also, we recommend using the `--convert-to-mgf` option in `download_raw_spectra.py` to export MGF files on a per-run basis.

#### Usage

```bash
uv run export-mgf <enriched.parquet> <output.mgf> [options]
```

#### Arguments

- `input_parquet`: Path to enriched Parquet directory or file from psm_to_parquet.py (required)
- `output_mgf`: Path to output MGF file (required)

#### Options

- `-n, --nrows`: Number of rows to convert (default: all rows)
- `-b, --batch-size`: Number of spectra per batch (default: 1000)

#### Examples

```bash
# Convert all spectra to MGF
uv run export-mgf output/spectra.parquet output/spectra.mgf

# Convert first 5000 spectra
uv run export-mgf output/spectra.parquet output/spectra.mgf -n 5000
```

## Running on a SLURM Cluster

If you have access to a SLURM-based HPC cluster, a ready-to-use batch script is provided in [`slurm/spectrum-toolkit/`](../../slurm/spectrum-toolkit/).

### `download-raw-spectra.sh` — raw spectra download via ThermoRawFileParser

Uses the `download-raw-spectra` command to pull raw files from PRIDE and extract spectra with ThermoRawFileParser.

```bash
sbatch slurm/spectrum-toolkit/download-raw-spectra.sh
```

Default resources: 8 CPUs, 32 GB RAM, 24 h wall time.

Before submitting either script, open it and update:

- `--account` and `--partition` to match your cluster's configuration
- Input/output paths to point to your actual data
- `-w` / `--workers` flag to match `--cpus-per-task`

## Example End-to-End Workflows

The example below uses the example query from `queries/` as an illustration. Substitute any SQL file that selects PSMs you care about.

```bash
# Step 1: Export PSM data from database to both CSV and Parquet
# (use your own SQL file or one of the examples in src/spectrum_toolkit/queries/)
uv run fetch-psms src/spectrum_toolkit/queries/psms_example.sql -o output/psm_data

# Step 2: Download raw files and extract spectra directly
uv run download-raw-spectra output/psm_data.csv output --convert-to-mgf
```
