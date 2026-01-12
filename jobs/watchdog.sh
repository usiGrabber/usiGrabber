#!/bin/bash

# --- Configuration ---
JOB_NAME="usigrabber_build_big"
SUBMIT_SCRIPT="${HOME}/usiGrabber/jobs/build_big.sh"
LOG_FILE="${HOME}/usiGrabber/jobs/logs/watchdog.log"
SLEEP_TIME="15m"  # Can be seconds (s), minutes (m), or hours (h)

echo "Watchdog started for job: $JOB_NAME. Logging to: $LOG_FILE"

while true; do
    # Get current ISO 8601 timestamp
    TIMESTAMP=$(date --iso-8601=seconds)

    # Check for Running (R), Pending (PD), or Configuring (CF) states
    STATUS=$(squeue -h -n "$JOB_NAME" -t R,PD,CF 2>/dev/null)

    if [ -z "$STATUS" ]; then
        echo "[$TIMESTAMP] Job '$JOB_NAME' not found. Restarting..."
        
        # Submit and log the output (captures JobID)
        SUBMIT_OUTPUT=$(sbatch "$SUBMIT_SCRIPT" 2>&1)
        echo "[$TIMESTAMP] $SUBMIT_OUTPUT"
    else
        # Optional: Log heartbeat to verify the watchdog itself hasn't died
        echo "[$TIMESTAMP] Job '$JOB_NAME' is active."
    fi

    # Wait before the next check
    sleep "$SLEEP_TIME"
done