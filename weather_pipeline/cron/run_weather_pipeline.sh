#!/bin/bash

# ----------------------------------------
# Environment Capture
# ----------------------------------------
env > /weather_pipeline/logs/env_from_cron.log

# ----------------------------------------
# Variables / Paths Setup
# ----------------------------------------
BASE_DIR="/weather_pipeline"
LOG_DIR="$BASE_DIR/logs"
VENV_PATH="$BASE_DIR/venv/bin"
SCRIPT_DIR="$BASE_DIR/scripts"

# ------------- Not in use ---------------
# Add project root to PYTHONPATH (so weather_pipeline is discoverable)
# export PYTHONPATH=""
# ----------------------------------------
# Isolate PYTHONPATH for this project only
PROJECT_ROOT="/projects"
if [[ ":$PYTHONPATH:" != *":$PROJECT_ROOT:"* ]]; then
    export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"
fi
# ----------------------------------------

# ----------------------------------------
# Timestamp & Batch ID
# ----------------------------------------
timestamp=$(date '+%Y-%m-%d_%H-%M-%S')
batch_id=$(date '+%Y%m%d_%H%M%S')

# Log files for this run
log_file="$LOG_DIR/weather_pipeline_batch_run_$timestamp.log"
cron_log="$LOG_DIR/weather_pipeline_cron_output_$timestamp.log"

# ----------------------------------------
# Error Handling
# ----------------------------------------
# Exit immediatley if any command exists with a non-zero status
set -e

# Define error handler function for logging failures
error_handler() {
    echo "[ERROR] Pipeline failed at $(date '+%Y-%m-%d %H:%M:%S')" >> "$log_file"
    echo "[ERROR] See above logs for details." >> "$log_file"
    exit 1
}

# Set trap  to catch errors and invoke error_handler
trap 'error_handler' ERR

# ----------------------------------------
# Environment Setup
# ----------------------------------------
# Setup JAVA_HOME to java17 explicitly
export JAVA_HOME=$(/usr/libexec/java_home -v 17)

# Add JAVA_HOME/bin bin to PATH (if not already there, also avoid duplicates)
if [[ ":$PATH:" != *":$JAVA_HOME/bin:"* ]]; then
  export PATH="$JAVA_HOME/bin:$PATH"
fi

# Add Homebrew to PATH if missing (needed for cron environment)
if [[ ":$PATH" != *":/opt/homebrew/bin:"* ]]; then
    export PATH="/opt/homebrew/bin:$PATH"
fi

#Add python virutal environment to PATH if missing
if [[ ":$PATH:" != *":$VENV_PATH:"* ]]; then
    export PATH="$VENV_PATH:$PATH"
fi

# ----------------------------------------
# Logging Start
# ----------------------------------------
echo "[CRON] Script triggered at $(date)" >> "$cron_log"
echo "[INFO] ===> Starting pipeline run: $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$log_file"
echo "[INFO] Using batch_id: $batch_id" >> "$log_file"

# ----------------------------------------
# Pipeline Execution
# ----------------------------------------
{
    # Print java versions
    echo "[INFO] JAVA version: "
    java -version
    echo "[INFO] JAVA_HOME: $JAVA_HOME"

    # Activate virtual environment (redirect output to log)
    echo "[INFO] Activating Python virtual environment"
    source "$VENV_PATH/activate" >> "$log_file" 2>&1

    # Run each pipeline scipt with the batch_id
    echo "[INFO] Running ingest_weather.py with batch_id: $batch_id"
    python3 "$SCRIPT_DIR/ingest_weather.py" "$batch_id"

    # test for debugging in dev only
    echo "PYTHONPATH=$PYTHONPATH" >> "$log_file"
    
    echo "[INFO] Running process_weather.py with batch_id: $batch_id"
    python3 "$SCRIPT_DIR/process_weather.py" "$batch_id"

    echo "[INFO] Running dq_weather.py with batch_id: $batch_id"
    python3 "$SCRIPT_DIR/dq_weather.py" "$batch_id"

    echo "[INFO] Running load_weather.py with batch_id: $batch_id"
    python3 "$SCRIPT_DIR/load_weather.py" "$batch_id"

} >> "$log_file" 2>&1

echo "[INFO] ===> Pipeline run completed: $(date '+%Y-%m-%d %H:%M:%S')" >> "$log_file"