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
  - This will attempt to use a smaller sample dataset for faster testing. This sample dataset should be located under the directory specified by the `--cache-dir` flag or the `CACHE_DIR` environment variable (default: `./cache`). The file should be named `sampled_projects.json`.


## Database management

The project requires a connection to an SQLite or PostgreSQL database for storing PRIDE proteomics data.

### Local docker container (PostgreSQL)
We provide a simple [docker compose file](./compose.yaml) for running a PostgreSQL database in a container. By default, it stores the data in a local volume named `usigrabber_pgdata`. The container requires the following environment variables to be set in your `.env` or overriden in the compose file:
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`.

See [Configuration](#configuration) and [`.env.sample`](./.env.sample) for more details. In order to start the container, run:
```bash
docker compose up -d
```
in the root of this project. It will expose the database on port `5432`.

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

## Performance Profiling

USI Grabber includes a dedicated profiling command for benchmarking and optimization:

```bash
# Quick start: Profile a single project with all metrics
./profile_with_memray.sh PXD000001

# Or without memray (CPU profiling only)
uv run usigrabber profile PXD000001
```

This generates:
- **CPU profile** (pyinstrument) - identify performance bottlenecks
- **Memory profile** (memray) - track memory allocations
- **Dashboard snapshot** - all metrics from the run
- **JSON metrics** - benchmark data for comparisons

### Compare Optimization Attempts

```bash
# Baseline
./profile_with_memray.sh PXD000001 --output-dir baseline

# Make code changes, then profile again
./profile_with_memray.sh PXD000001 --output-dir optimized

# Compare results
python3 scripts/compare_profiles.py baseline optimized
```

See [PROFILING.md](./PROFILING.md) for detailed documentation and [PROFILING_QUICKSTART.md](./PROFILING_QUICKSTART.md) for quick reference.

### Legacy Memory Profiling
```bash
memray run -follow-fork --output track.bin src/usigrabber/__init__.py build --reset --max-workers 2
```

### Multiprocessing
- Kill old python processes: `kill -9 $(pgrep -f python3)`
