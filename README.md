## Setup

1. Install prerequisites

- uv: https://docs.astral.sh/uv/getting-started/installation/
- Install recommended VS Code extensions

2. Run the dev setup script

```bash
./scripts/setup_dev/all.sh
```

## Build database

You can build the database using the CLI:

```bash
uv run usigrabber build
```

Arguments:
- `--debug`: Enable debug mode. Defaults to `False`. Can also be set via `DEBUG` environment variable.
  - This will attempt to use a smaller sample dataset for faster testing. This sample dataset show be located under the directory specified by the `--data-dir` flag or the `UG_DATA_DIR` environment variable (default: `./data`). The file should be named `pride_sampled_projects.json`.


## Database management

The project uses SQLite with SQLModel for storing PRIDE proteomics data.

### Quick Start

```bash
# Initialize database (create all tables)
uv run python -m usigrabber.db.cli init

# Seed with sample data
uv run python -m usigrabber.db.cli seed

# View database info
uv run python -m usigrabber.db.cli info
```

### Other Commands

```bash

# Reset database (drop + recreate + seed)
uv run python -m usigrabber.db.cli reset --force

# Drop all tables (WARNING: deletes all data)
uv run python -m usigrabber.db.cli drop --force
```

### Configuration

Database settings can be configured via `.env` file:

```bash
# Use local database (default: database.db)
USE_LOCAL_DB=1

# Enable SQL query logging
DB_ECHO_SQL=1

# Enable debug mode by setting this variable
DEBUG=1

# Directory for storing intermediate files
UG_DATA_DIR=./data
```

## Working with mzIdentML Files

Example mzIdentML (.mzid) files for testing and development can be obtained from the HUPO-PSI mzIdentML repository: https://github.com/HUPO-PSI/mzIdentML/tree/master/examples
