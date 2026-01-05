# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

usiGrabber is a proteomics data processing tool that builds a database from PRIDE proteomics projects. It downloads and parses proteomics files (mzIdentML, mzTab, MaxQuant txt.zip) and stores peptide spectrum matches (PSMs), modifications, and protein evidence in PostgreSQL or SQLite.

## Documentation
Use context7 to retrieve documentation for packages.

## Development Commands

### Environment Setup
```bash
# Initial setup (requires uv: https://docs.astral.sh/uv/getting-started/installation/)
./scripts/setup_dev/all.sh

# Sync dependencies
uv sync --locked
```

### Database Operations
```bash
# Initialize database tables
uv run usigrabber db init

# Seed with sample data
uv run usigrabber db seed

# View database info
uv run usigrabber db info

# Reset database (drop + recreate + seed)
uv run usigrabber db reset --force

# Drop all tables (WARNING: deletes all data)
uv run usigrabber db drop --force
```

### Docker Database
```bash
# Start PostgreSQL container
docker compose up -d

# Stop and remove volumes
docker compose down -v

# Connect to database
docker exec -it usigrabber_db psql -d usigrabber -U <username>
```

### Build Database
```bash
# Build the database from PRIDE data
uv run usigrabber build

# Build in debug mode (uses sampled_projects.json from cache dir)
uv run usigrabber build --debug
```

### Testing
```bash
# Run all tests
uv run pytest

# Run tests in specific directory
uv run pytest tests/file_parser

# Run tests with specific markers
uv run pytest -m "not slow"
uv run pytest -m integration

# Run specific test file
uv run pytest tests/file_parser/test_mzid.py
```

### Linting and Type Checking
```bash
# Run ruff linter
uv run ruff check .

# Run ruff formatter
uv run ruff format .

# Run type checker
uv run basedpyright

# Run pre-commit hooks manually
uv run pre-commit run --all-files
```

### Profiling
```bash
# Profile database build
uv run pyinstrument -o db_build_profile.html $(which usigrabber) build
```

## Architecture

### Project Structure

- **`src/usigrabber/`** - Main application code
  - **`backends/`** - Data source backends (currently PRIDE Archive)
    - `base.py` - Abstract `BaseBackend` interface
    - `pride.py` - PRIDE Archive implementation
  - **`cli/`** - Typer-based CLI commands (`build`, `search`, `db`)
  - **`db/`** - Database models and operations
    - `schema.py` - SQLModel database schema
    - `engine.py` - Database engine configuration
    - `cli.py` - Database management CLI commands
  - **`file_parser/`** - File parsers for proteomics formats
    - `base.py` - `BaseFileParser` abstract class with parser registry
    - `mzid/` - mzIdentML parser
    - `mztab/` - mzTab parser
    - `txt_zip/` - MaxQuant txt.zip parser
    - `models.py` - TypedDicts for bulk insertion
    - `helpers.py` - Shared parsing utilities
  - **`parallelism/`** - Multiprocessing infrastructure for parallel project processing
  - **`cv_parameters/`** - Controlled vocabulary (CV) parameter handling
  - **`usi_validation/`** - USI (Universal Spectrum Identifier) validation
  - **`utils/`** - Shared utilities (file operations, logging, environment variables)

- **`packages/`** - UV workspace packages
  - `async-http-client/` - Async HTTP client with caching
  - `ontology-resolver/` - Ontology resolution using Pronto
  - `test-package/` - Example workspace package

### Key Architectural Patterns

**Backend System**: New data sources can be added by implementing `BaseBackend` abstract class:
- `get_project()` - Fetch project metadata
- `get_new_projects()` - Stream new projects as AsyncGenerator
- `dump_project_to_db()` - Persist project to database
- `get_files_for_project()` - Get downloadable files

**File Parser Registry**: Parsers auto-register via `@register_parser` decorator and are selected by file extension. All parsers inherit from `BaseFileParser` with methods:
- `parse_file()` - Parse file to structured data
- `import_to_db()` - Bulk insert to database
- `file_extensions` - Supported extensions
- `format_name` - Human-readable format name

**Parallel Processing**: Uses `ProcessPoolExecutor` with spawn context:
- Main process streams projects from backend
- Worker pool processes projects in parallel
- Each worker has its own database engine (initialized via `init_worker()`)
- Ontology workers have separate pool with `init_ontology_worker()`

**Database Schema**: SQLModel-based schema (`db/schema.py`) with key tables:
- `Project` - PRIDE project metadata with JSON fields for complex attributes
- `PeptideSpectrumMatch` - Core PSM data linking peptides, spectra, and projects
- `ModifiedPeptide` - Peptide sequences with deterministic UUIDs
- `Modification` - UNIMOD modifications linked to peptides via junction table
- `MzidFile` - Per-file metadata for provenance tracking
- `PeptideEvidence` - Peptide-to-protein mappings via `PSMPeptideEvidence` junction

**Bulk Insertion Strategy**: Parsers return TypedDicts matching schema for efficient bulk insertion using `insert().values()` with `on_conflict_do_nothing()`.

### Configuration

Configuration via `.env` file (see `.env.sample`):
- `DB_URL` - Database connection (SQLite or PostgreSQL)
- `POSTGRES_USER`, `POSTGRES_PASSWORD` - PostgreSQL credentials
- `CACHE_DIR` - Cache location for HTTP, ontology data
- `DEBUG` - Enable debug mode with sampled projects
- `PARALLEL_DOWNLOADS` - Number of concurrent FTP downloads (default: 10)
- `NO_ONTOLOGY` - Disable ontology lookup

### UV Workspace

This is a UV workspace project with multiple packages:
- Add workspace package: `uv add --workspace <package-name>`
- Create new package: `uv init --package` in `packages/` directory
- Add `py.typed` file to package for type hints on build

### USI Specification

PSMs include USI fields (`index_type`, `index_number`, `ms_run`) for Universal Spectrum Identifier construction following ProteomeXchange format.

### Testing Notes

- Tests located in `tests/` and `packages/*/tests/`
- pytest markers: `integration`, `slow`
- Test configuration in `pyproject.toml` under `[tool.pytest.ini_options]`
- Example mzIdentML files: https://github.com/HUPO-PSI/mzIdentML/tree/master/examples
- For ontology-dependent tests, create mini OBO files with only relevant terms (see `tests/cv_parameters/fixtures/`) to avoid slow ontology downloads

### Code Quality: Providing Proof of Correctness

Engineers are not code factories - they provide trustable and maintainable code. When submitting PRs for data processing features:

1. **Validate on real data**: Run the code against actual production data (e.g., all PRIDE projects via `experimental/fetch_pride_projects.py`) to measure real-world impact.

2. **Provide statistics**: Include concrete numbers in PR descriptions or comments:
   - How many records/projects are affected
   - What percentage of problematic cases are handled
   - Examples of successful transformations
   - Examples of edge cases that remain unhandled

3. **Use existing data files**: For analysis, use locally cached data files rather than making slow API calls. The `experimental/` directory has scripts for downloading data.

4. **Document limitations**: Be transparent about what the code doesn't handle (e.g., vendor name variations, typos in instrument names).

Example: For instrument cleaning, run analysis on all 36,786 PRIDE projects and report:
- Duplicates removed: X%
- Resolved to specific accessions: Y%
- Kept unresolved: Z% (with examples of why)
