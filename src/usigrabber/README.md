# usiGrabber tool

## Database setup

We recommend using a postgres database for local development and production deployments. However, you can also skip to ["Using sqlite for quick testing"](#using-sqlite-for-quick-testing-not-recommended-for-production) if you want a super quick testing setup.

### Setup and start postgres database

Note: The default settings assume at least 32GB of RAM available. For local setups, you might need to decrease the `shared_buffers` in the `compose.yaml` file.

1. Adjust the following environment variables in your `.env` file or use the defaults:
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `DB_URL`

2. Start the database using Docker Compose.
- `docker compose up -d`

3. Initialize the database:
- `uv run usigrabber db init`

4. Use tools like pgAdmin to interact with the database if needed or use the command line instructions below for some simple commands.

### Using sqlite for quick testing (not recommended for production)

1. Adjust the `DB_URL` environment variable in your `.env` file to use sqlite, e.g.:
```
DB_URL=sqlite:///./database.db
```
2. Initialize the database:
- `uv run usigrabber db init`

## Commands for building the usigrabber database

Use the following command to start building the usigrabber with ten sample projects:
```bash
uv run usigrabber build --projects-file ./sample_projects.json
```

Remove the argument to use all available projects from PRIDE or provide your own list of projects in a JSON file. You can get one from the [PRIDE API](https://www.ebi.ac.uk/pride/ws/archive/v3/webjars/swagger-ui/index.html#/projects/getProjects).

Check out the help message for more options like resetting the database before building, skipping ontology downloads, or adjusting the number of parallel workers:
```bash
uv run usigrabber build --help
```

### Other useful commands

```bash

# Seed with sample data
uv run usigrabber db seed

# View database info
uv run usigrabber db info

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

## Miscellaneous

### Obtaining example mzIdentML files

Example mzIdentML (.mzid) files for testing and development can be obtained from the HUPO-PSI mzIdentML repository: https://github.com/HUPO-PSI/mzIdentML/tree/master/examples
