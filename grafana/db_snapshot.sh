#!/bin/bash

# This script connects to the PostgreSQL database and records
# the current row count of the specified table along with a timestamp.
# It creates a new table to store these snapshots if it doesn't already exist.
# It should be run periodically (e.g., via cron) to track the growth of the target table over time.
# We recommend running once per hour:
#   0 * * * * /path/to/db_snapshot.sh

# --- Configuration ---
DB_NAME="usigrabber"
DB_USER="user"
DB_PORT=5432
TARGET_TABLE="peptide_spectrum_matches"
STATS_TABLE="psm_growth_stats"

# --- Execution ---
# 1. Create table if it doesn't exist
# 2. Insert current timestamp and count
# docker exec -i usigrabber_db psql -U $DB_USER -d $DB_NAME -c "
psql -U $DB_USER -d $DB_NAME -c "
CREATE TABLE IF NOT EXISTS $STATS_TABLE (
    measured_at TIMESTAMPTZ DEFAULT now(),
    row_count BIGINT
);

INSERT INTO $STATS_TABLE (row_count)
SELECT COUNT(*) FROM $TARGET_TABLE;"

echo "Snapshot recorded at $(date --iso-8601=seconds)"
