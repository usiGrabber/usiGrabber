#!/bin/bash

# Path to your global .env file
ENV_FILE=".env"

# Make sure the file exists
touch "$ENV_FILE"

# Check argument
if [ $# -ne 1 ]; then
    echo "Usage: $0 [local|shared]"
    exit 1
fi

case "$1" in
    local)
        # Remove any existing USE_LOCAL_DB line, then add it
        grep -v '^USE_LOCAL_DB=' "$ENV_FILE" > "$ENV_FILE.tmp"
        echo "USE_LOCAL_DB=1" >> "$ENV_FILE.tmp"
        mv "$ENV_FILE.tmp" "$ENV_FILE"
        echo "USE_LOCAL_DB set to 1 in $ENV_FILE"
        ;;
    shared)
        # Remove any existing USE_LOCAL_DB line
        grep -v '^USE_LOCAL_DB=' "$ENV_FILE" > "$ENV_FILE.tmp"
        mv "$ENV_FILE.tmp" "$ENV_FILE"
        echo "USE_LOCAL_DB removed from $ENV_FILE"
        ;;
    *)
        echo "Error: Invalid argument. Use 'local' or 'shared'."
        exit 1
        ;;
esac
