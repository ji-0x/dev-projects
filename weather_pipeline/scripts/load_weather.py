#!/usr/bin/env python3

import os
import sys
import duckdb
import logging
from datetime import datetime
from weather_pipeline.utils.metadata_utils import log_metadata

# -----------------------------
# Paths & Files
# -----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
LOG_DIR = os.path.join(BASE_DIR, 'logs')
DUCKDB_FILE = os.path.join(BASE_DIR, "db", 'weather.duckdb')

# Timestamp for log file naming
log_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_file_path = os.path.join(LOG_DIR, f'load_weather_{log_time}.log')

# Get batch id from command line arguement
batch_id = sys.argv[1]

os.makedirs(LOG_DIR, exist_ok=True)

# -----------------------------
# Logging
# -----------------------------
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# -----------------------------
# Start metadata timing
# -----------------------------
start_time = datetime.now()
status = 'success'
rows_processed = 0
error_message = None

# -----------------------------
# Connect to DuckDB database
# -----------------------------
con = duckdb.connect(DUCKDB_FILE)

try:
    # Create schema and final ''public.weather' if it doesn't already exist
    # Table created empty but with same schema as 'staging.valid_weather'
    con.execute("""
        CREATE SCHEMA IF NOT EXISTS public;

        CREATE TABLE IF NOT EXISTS public.weather AS
        SELECT * FROM staging.valid_weather WHERE FALSE;
    """)

    # Count records for the batch
    row_count = con.execute("""
    SELECT COUNT(*) 
    FROM staging.valid_weather
    WHERE batch_id = ?
    """, (batch_id,)).fetchone()[0]

    if row_count == 0:
        # No data to load for this batch
        logger.info(f"No rows to load for batch: {batch_id} public.weather.")
    else:
        # Insert valid rows for this batch into the final table
        con.execute("""
            INSERT INTO public.weather
            SELECT * FROM staging.valid_weather
            WHERE batch_id = ?
        """, (batch_id,))
        rows_processed = row_count
    logger.info(f"{row_count} rows successfully loaded into public.weather for batch {batch_id}.")

except Exception as e:
    status = 'failed'
    error_message = str(e)
    logger.error(f"Failed to load data into public.weather: {e}")

finally:
    end_time = datetime.now()

    # Log metadata
    log_metadata(
        con=con,
        batch_id=batch_id,
        phase="load",
        start_time=start_time,
        end_time=end_time,
        status=status,
        rows_processed=rows_processed,
        dq_passed=None,
        error_message=error_message
    )

    con.close()
