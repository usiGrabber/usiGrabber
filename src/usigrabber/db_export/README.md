# Database Export & Import Guide

Because this database is extremely large, the data has been exported into chunked, highly compressed Parquet files.

The architecture is decoupled into three parts:
1. **Schema:** The empty tables ([`00_schema.sql`](./schema/00_schema.sql))
2. **Data:** The Parquet chunks (loaded via Python)
3. **Constraints:** The primary/foreign keys and indices ([`99_indices.sql`](./schema/99_indices.sql))

## Import the database

Follow these steps in exact order to rebuild the database on your local machine.

### 1. Create the database and load the empty schema
This creates the tables without strict constraints, allowing the data to be loaded in any order.

```bash
createdb -U your_user usigrabber
psql -U your_user -d usigrabber -f 00_schema.sql
```

**Note**: If you use docker for the database, you have to run these commands, as well as the ones in step 3 inside the container. In this case, the schema files need to be available inside the container as well. To enter the container, use:
```bash
docker exec -it usigrabber_db bash
```

### 2. Import the data
Run the import script. This uses DuckDB to rapidly stream all Parquet chunks directly into your PostgreSQL tables.

```bash
uv run import-db
```

This script expects the Parquet files to be in the `parquet_exports/` directory in the root of the project. Alternatively, you can specify a different path by supplying the `--input-dir` argument:
```bash
uv run import-db --input-dir /path/to/parquet/files
```

### 3. Build indices and foreign keys
Now that the data is fully loaded, apply the constraints and build the performance indices.

```bash
psql -U your_user -d usigrabber -f 99_indices.sql
```
**Note**: Depending on your hardware, building indices for massive tables may take a few hours.

## Export the database

If you modify the database and need to generate a new export for the repository, follow these steps:

### 1. Extract the schema (Pre-data)

```bash
pg_dump -U your_user -d usigrabber --section=pre-data -f 00_schema.sql
```

**Note**: Again, these commands must be run inside the container, if you are using docker.

### 2. Export the data to Parquet chunks
Run the export script. This will stream the tables into compressed Parquet files, automatically chunking them to stay under repository file size limits.

```bash
uv run export-db
```

### 3. Extract the indices and constraints (Post-data)

```bash
pg_dump -U your_user -d usigrabber --section=post-data -f 99_indices.sql
```

## Update schema files
If you make changes to the database schema, you need to update the SQL files in the `schema/` folder. Follow these steps:

- Connect to a new, empty database
- Run the db scripts to initialize the schema
- Export the schema using `pg_dump` as shown above
