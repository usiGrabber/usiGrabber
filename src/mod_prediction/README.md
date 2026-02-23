# Dataset for modification prediction

## Overview

This project provides tools to extract PSM (Peptide-Spectrum Match) data from a PostgreSQL database, download spectra from PRIDE, and preserve all metadata in Parquet format for modification prediction tasks. The enriched Parquet files can optionally be exported to MGF format.

## Workflow

1. **Extract PSMs**: Run a SQL query to extract PSMs with modifications from the USI grabber database and save the results as a Parquet or CSV file.
2. **Download Spectra via raw files**: Download raw files directly from PRIDE and extract spectra using ThermoRawFileParser
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
unzip ThermoRawFileParser-v.X.X.X-linux.zip -d thermo/
```

3. Verify the installation:

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
- Input CSV must have `project_accession`, `ms_run`, and `index_number` columns

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

```bash
# Step 1: Export PSM data from database to both CSV and Parquet
uv run fetch-psms queries/psms_with_phospho_mod.sql -o data/psm_data

# Step 2: Download raw files and extract spectra directly
uv run download-raw-spectra data/psm_data.csv data/raw_spectra.parquet

# Step 3 (Optional): Export to MGF if needed
uv run export-mgf data/raw_spectra.parquet data/spectra.mgf
```
