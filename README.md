## Setup

1. Install prerequisites

- uv: https://docs.astral.sh/uv/getting-started/installation/
- Install recommended VS Code extensions

2. Install dependencies and setup pre-commit hooks

```bash
uv sync

uv run pre-commit install
```

## Build database

You can build the database using the CLI:

```bash
uv run usigrabber build
```

Arguments:
- `--debug`: Enable debug mode. Defaults to `False`. Can also be set via `DEBUG` environment variable.
  - This will attempt to use a smaller sample dataset for faster testing. This sample dataset should be located under the directory specified by the `--cache-dir` flag or the `CACHE_DIR` environment variable (default: `./cache`). The file should be named `sampled_projects.json`.


## Database management

The project requires a connection to an SQLite or PostgreSQL database for storing PRIDE proteomics data.

### Local docker container (PostgreSQL)
We provide a simple [docker compose file](./compose.yaml) for running a PostgreSQL database in a container. By default, it stores the data in a Docker-managed volume named `pgdata`. The container requires the following environment variables to be set in your `.env` or overriden in the compose file:
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`.

See [Configuration](#configuration) and [`.env.sample`](./.env.sample) for more details.

#### Running locally
For local development, simply run:
```bash
docker compose up -d
```

#### Running on a cluster VM
On the VM, use the override file to bind-mount the postgres data to `/mnt/helix/postgres`:
```bash
docker compose -f compose.yaml -f compose.helix.yaml up -d
```

**Tip:** To avoid typing the `-f` flags every time, you can set an environment variable:
```bash
export COMPOSE_FILE=compose.yaml:compose.helix.yaml
docker compose up -d
```

The container exposes the database on port `5433`.

You can directly interact with the database using
```bash
docker exec -it usigrabber_db psql -d usigrabber -U <ENTER USERNAME HERE>
```
or via the usigraber database CLI commands (see below).

### Quick Start

```bash
# Initialize database (create all tables)
uv run usigrabber db init

# Seed with sample data
uv run usigrabber db seed

# View database info
uv run usigrabber db info
```

### Other Commands

```bash

# Reset database (drop + recreate + seed)
uv run usigrabber db reset --force

# Or use docker if you have schema changes
docker compose down -v
docker compose up -d

# Drop all tables (WARNING: deletes all data)
uv run usigrabber db drop --force

# Profile database build
uv run pyinstrument -o db_build_profile.html $(which usigrabber) build

```

### Configuration

We provide a sample environment file at [`.env.sample`](./.env.sample).
Copy this file to `.env` and modify the settings as needed.

## Working with mzIdentML Files

Example mzIdentML (.mzid) files for testing and development can be obtained from the HUPO-PSI mzIdentML repository: https://github.com/HUPO-PSI/mzIdentML/tree/master/examples
