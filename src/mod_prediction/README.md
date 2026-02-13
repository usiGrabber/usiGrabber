# Dataset for modification prediction

## Overview

This project provides tools to extract PSM (Peptide-Spectrum Match) data from a PostgreSQL database, download spectra from PRIDE, and preserve all metadata in Parquet format for modification prediction tasks. The enriched Parquet files can optionally be exported to MGF format.

## Workflow

1. **Extract PSMs**: Run a SQL query to extract PSMs with modifications from the USI grabber database and save the results as a Parquet or CSV file.
2. **Download Spectra** (choose one approach):
   - **Via PROXI API** (default): Use the enrichment tool to download spectra from PRIDE and merge them with PSM metadata
   - **Via Raw Files**: Download raw files directly from PRIDE and extract spectra using ThermoRawFileParser
3. **Optional MGF Export**: If needed, convert the enriched Parquet to MGF format for tools that require it.
4. **Use for Prediction**: The enriched Parquet (or MGF) file can then be used for modification prediction tasks.

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
# Export to both formats with default naming (e.g., psms_with_phospho_mod.csv and .parquet)
uv run fetch-psms queries/psms_with_phospho_mod.sql

# Custom output base path (creates output/psm_data.csv and output/psm_data.parquet)
uv run fetch-psms queries/psms_with_phospho_mod.sql -o output/psm_data
```

#### Environment Setup

Create a `.env` file with your database connection:

```
DATABASE_URL=postgresql://user:password@host:port/database
```

#### Required Database Columns

The SQL query must return these columns:

- `psm_id`, `project_accession`, `spectrum_id`, `charge_state`
- `experimental_mz`, `calculated_mz`, `pass_threshold`, `rank`
- `ms_run`, `index_type`, `index_number`
- `peptide_sequence`, `modified_peptide_id`
- `unimod_id`, `location`, `modified_residue` (modification data, may have multiple rows per PSM)

### 2. Spectrum Download & Enrichment (Parquet → Enriched Parquet)

The `psm_to_parquet.py` tool downloads spectra from PRIDE and merges them with PSM metadata, preserving everything in an enriched Parquet file.

#### Usage

```bash
uv run download-spectra <input.file> <output.parquet> [options]
```

#### Arguments

- `input_file`: Path to input Parquet file or input csv file from db_fetcher.py (required)
- `output_parquet`: Path to output enriched Parquet directory or file (required)

#### Options

- `-n, --nrows`: Number of rows to process (default: all rows)
- `-b, --batch-size`: Number of USIs to process per batch (default: 100)
- `-w, --workers`: Number of parallel download threads (default: 5)

#### Examples

```bash
# Download spectra for all PSMs
uv run download-spectra output/psm_data.parquet output/enriched_psm_data

# Process first 1000 rows with 10 parallel workers
uv run download-spectra output/psm_data.parquet output/enriched_psm_data -n 1000 -w 10
```

#### What This Does

1. Reads PSM data from the input Parquet or csv file
2. Aggregates modifications per `psm_id` (combines multiple rows into arrays)
3. Creates USIs (Universal Spectrum Identifiers) for downloading
4. Downloads spectra from PRIDE in parallel with retry logic
5. Merges spectrum data (mz_array, intensity_array) with all PSM metadata
6. Writes enriched Parquet with complete metadata preservation

#### Enriched Parquet Schema

The output contains all original PSM columns plus:

- `unimod_ids`: list[int] - Aggregated modification IDs per PSM
- `locations`: list[int] - Aggregated modification positions per PSM
- `modified_residues`: list[str] - Aggregated modified residues per PSM
- `mz_array`: list[float] - Peak m/z values from spectrum
- `intensity_array`: list[float] - Peak intensity values from spectrum

### Setup: ThermoRawFileParser

The `download_raw_spectra.py` script requires ThermoRawFileParser to extract spectra from Thermo raw files. Follow these steps to set it up:

#### Installation

1. Download the latest ThermoRawFileParser release from [GitHub releases](https://github.com/CompOmics/ThermoRawFileParser/releases)
2. Extract it to the `thermo/` directory in the project root:

```bash
# Create the thermo directory
mkdir -p thermo

# Extract the downloaded archive
unzip ThermoRawFileParser-v.X.X.X-linux.zip -d thermo/
```

3. Verify the installation:

```bash
./thermo/ThermoRawFileParser --help
```

The executable should be at `./thermo/ThermoRawFileParser` (relative to the project root).

### 3. Download Raw Files & Extract Spectra (CSV → Parquet)

The `download_raw_spectra.py` tool downloads raw files directly from PRIDE and extracts spectra data using ThermoRawFileParser, outputting results as Parquet format. This is an alternative to the PROXI API approach and is useful for getting raw MS data directly from the instrument files. We validate that charge state from raw files matches the PSM data. In case of a mismatch we skip the entire raw file and write to `output_dir/charge_mismatches.csv`.

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

#### Examples

```bash
# Download and extract spectra, cleanup temp files automatically
uv run download-raw-spectra data/psm_data.csv raw_spectra.parquet

# Keep raw files for inspection or reuse
uv run download-raw-spectra data/psm_data.csv raw_spectra.parquet --keep-temp-files

# Test with first 10 rows
uv run download-raw-spectra data/psm_data.csv raw_spectra_test.parquet --limit 10 --keep-temp-files

# Use custom temp directory on large storage mount
uv run download-raw-spectra data/psm_data.csv raw_spectra.parquet --temp-dir /large_storage/downloads
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
- Input CSV must have `project_accession` and `ms_run` columns
- PRIDE API must be accessible for querying raw files
- FTP access to PRIDE server required for downloads

### 4. Optional MGF Export (Enriched Parquet → MGF)

The `parquet_to_mgf.py` tool converts enriched Parquet files to MGF format for tools that require MGF input. This is optional since the Parquet file contains all the data.

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
uv run export-mgf output/enriched_psm_data.parquet output/spectra.mgf

# Convert first 5000 spectra
uv run export-mgf output/enriched_psm_data.parquet output/spectra.mgf -n 5000
```

## Running on SLURM Cluster

We recommend using the SLURM batch system to run the spectrum download and enrichment pipeline, which includes time-intensive downloads from PRIDE.

1. Copy the example SLURM script:

   ```bash
   cp download_spectra.slurm.example download_spectra.slurm
   ```

2. Edit `download_spectra.slurm` to customize:

   - Job parameters (`--time`, `--mem`, `--cpus-per-task`)
   - Input Parquet path (from db_fetcher.py)
   - Output enriched Parquet path
   - Number of workers (`-w` flag) to match `--cpus-per-task`
   - Account and partition settings for your cluster

3. Submit the job:
   ```bash
   sbatch download_spectra.slurm
   ```

## Example End-to-End Workflows

### Workflow A: Using PROXI API (Default)

```bash
# Step 1: Export PSM data from database to both CSV and Parquet
uv run fetch-psms queries/psms_with_phospho_mod.sql -o data/psm_data

# Step 2: Download spectra via PROXI API and create enriched Parquet
uv run download-spectra data/psm_data.parquet data/enriched_psm_data

# Step 3 (Optional): Export to MGF if needed
uv run export-mgf data/enriched_psm_data data/spectra.mgf
```

### Workflow B: Using Raw Files from PRIDE

```bash
# Step 1: Export PSM data from database to both CSV and Parquet
uv run fetch-psms queries/psms_with_phospho_mod.sql -o data/psm_data

# Step 2: Download raw files and extract spectra directly
uv run download-raw-spectra data/psm_data.csv data/raw_spectra.parquet

# Step 3 (Optional): Export to MGF if needed
uv run export-mgf data/raw_spectra.parquet data/spectra.mgf
```
