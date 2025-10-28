## Setup

1. Install prerequisites

- uv: https://docs.astral.sh/uv/getting-started/installation/
- Install recommended VS Code extensions

2. Run the dev setup script

```bash
./scripts/setup_dev/all.sh
```

3. _Optional_: Create a .env file in the root directory by copying from .env.sample:

```bash
cp .env.sample .env
```

Then add environment variables to shell using:

```bash
export $(cat .env | xargs)
```

## Database

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
```
