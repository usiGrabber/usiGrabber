#!/bin/bash
# Wrapper script to run usigrabber profile with memray memory profiling
#
# Usage: ./profile_with_memray.sh PROJECT_ACCESSION [additional usigrabber options]
#
# Example:
#   ./profile_with_memray.sh PXD000001
#   ./profile_with_memray.sh PXD000001 --no-ontology --debug

set -e

if [ -z "$1" ]; then
    echo "Error: PROJECT_ACCESSION is required"
    echo "Usage: $0 PROJECT_ACCESSION [additional usigrabber options]"
    exit 1
fi

PROJECT_ACCESSION="$1"
shift  # Remove first argument, rest are passed to usigrabber

# Create output directory
OUTPUT_DIR="profile_results"
mkdir -p "$OUTPUT_DIR"

# Run with memray
echo "Running profile with memray for project: $PROJECT_ACCESSION"
echo "Output directory: $OUTPUT_DIR"
echo ""

MEMRAY_BIN="${OUTPUT_DIR}/${PROJECT_ACCESSION}_memray.bin"

# Run usigrabber profile with memray
memray run \
    --output "$MEMRAY_BIN" \
    --force \
    -m usigrabber \
    profile "$PROJECT_ACCESSION" \
    --output-dir "$OUTPUT_DIR" \
    "$@"

# Generate memray HTML reports
echo ""
echo "Generating memray reports..."

# Flamegraph
MEMRAY_FLAMEGRAPH="${OUTPUT_DIR}/${PROJECT_ACCESSION}_memray_flamegraph.html"
memray flamegraph \
    --output "$MEMRAY_FLAMEGRAPH" \
    --force \
    "$MEMRAY_BIN"

echo "Memray flamegraph: $MEMRAY_FLAMEGRAPH"

# Table view
MEMRAY_TABLE="${OUTPUT_DIR}/${PROJECT_ACCESSION}_memray_table.html"
memray table \
    --output "$MEMRAY_TABLE" \
    --force \
    "$MEMRAY_BIN"

echo "Memray table: $MEMRAY_TABLE"

# Extract peak memory from memray stats
echo ""
echo "Extracting memory statistics..."
MEMRAY_STATS=$(memray stats "$MEMRAY_BIN")
echo "$MEMRAY_STATS"

# Parse peak memory and update JSON
# The value is on the line after "Peak memory usage:"
PEAK_MEMORY=$(echo "$MEMRAY_STATS" | grep -A1 "Peak memory usage:" | tail -n1 | awk '{print $1}')

if [ -n "$PEAK_MEMORY" ]; then
    METRICS_JSON="${OUTPUT_DIR}/${PROJECT_ACCESSION}_metrics.json"
    if [ -f "$METRICS_JSON" ]; then
        # Update the JSON with peak memory using Python
        python3 -c "
import json
import sys

metrics_file = '$METRICS_JSON'
peak_memory = '$PEAK_MEMORY'

with open(metrics_file, 'r') as f:
    data = json.load(f)

# Parse memory value (e.g., '123.45MB' or '123.45 MiB' -> 123.45)
if 'MB' in peak_memory or 'MiB' in peak_memory:
    data['total_memory_mb'] = float(peak_memory.replace('MB', '').replace('MiB', '').strip())
elif 'GB' in peak_memory or 'GiB' in peak_memory:
    data['total_memory_mb'] = float(peak_memory.replace('GB', '').replace('GiB', '').strip()) * 1024
elif 'KB' in peak_memory or 'KiB' in peak_memory:
    data['total_memory_mb'] = float(peak_memory.replace('KB', '').replace('KiB', '').strip()) / 1024

with open(metrics_file, 'w') as f:
    json.dump(data, f, indent=2)

print(f'Updated {metrics_file} with peak memory: {peak_memory}')
"
    fi
fi

echo ""
echo "=========================================="
echo "Profile complete!"
echo "=========================================="
echo "Results in: $OUTPUT_DIR"
echo "  - Metrics JSON: ${OUTPUT_DIR}/${PROJECT_ACCESSION}_metrics.json"
echo "  - Pyinstrument: ${OUTPUT_DIR}/${PROJECT_ACCESSION}_pyinstrument.html"
echo "  - Dashboard:    ${OUTPUT_DIR}/${PROJECT_ACCESSION}_dashboard.html"
echo "  - Memray bin:   $MEMRAY_BIN"
echo "  - Memray flame: $MEMRAY_FLAMEGRAPH"
echo "  - Memray table: $MEMRAY_TABLE"
echo "=========================================="
