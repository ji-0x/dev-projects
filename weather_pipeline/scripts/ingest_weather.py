#!/usr/bin/env python3

import requests
from datetime import datetime
import json
import logging
import os
import sys
import duckdb
from weather_pipeline.utils.metadata_utils import log_metadata

# print("PYTHONPATH is:", sys.path)

# -----------------------------
# Paths
# -----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
CONFIG_PATH = os.path.join(BASE_DIR, 'config', 'settings.json')
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
LOG_DIR = os.path.join(BASE_DIR, "logs")

# os.makedirs( ,exist_ok=True)

log_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_file_path = os.path.join(LOG_DIR, f'ingest_weather_{log_time}.log')


# -----------------------------
# Setup Logging
# -----------------------------
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# -----------------------------
# Load API Key and Cities
# -----------------------------
def load_config(path):
    try:
        with open(path, 'r') as f:
            config = json.load(f)
        return config
    except FileNotFoundError:
        logger.error(f"Config file not found at {path}")
        sys.exit(1)
    except json.JSONDecodeError:
        logger.error("Config file is not valid JSON")
        sys.exit(1)

# -----------------------------
# Fetch Weather Data from API
# -----------------------------
def fetch_weather(api_key, query):
    url = f"https://api.weatherapi.com/v1/current.json?key={api_key}&q={query}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"Failed to fetch weather for {query}: {e}")
        return None

# -----------------------------
# Save Raw Weather Data to File
# -----------------------------
def save_raw_data(city, data, batch_id):
    # timestamp is ISO 8601 format
    timestamp = datetime.now().isoformat(timespec='seconds').replace(':', '-')
    safe_city = city.lower().replace(' ', '_')
    filename = f"{safe_city}_weather_{batch_id}.json"
    filepath = os.path.join(RAW_DATA_DIR, filename)

    try:
        with open(filepath, 'w') as f:
            json.dump(data, f, indent=4)
        logger.info(f"Saved weather data for {city} to {filepath}")
        return True
    except Exception as e:
        logger.error(f"Failed to write file for {city}: {e}")
        return False

# -----------------------------
# Main Ingestion Function
# -----------------------------
def main():
    
    # Start metadata timing
    start_time = datetime.now()
    status = 'success'
    rows_processed = 0
    error_message = None
    dq_passed = None 

    batch_id = None

    try:
        # Allow batch_id to be passed as an argument
        if len(sys.argv) > 1:
            batch_id = sys.argv[1]
            logger.info(f"Using provided batch_id: {batch_id}")
        else:
            batch_id = datetime.now().strftime('%Y%m%d_%H%M%S')
            logger.info(f"No batch_id provided, generated: {batch_id}")

        # logger.info("Starting weather ingestion job...")

        # Load config
        config = load_config(CONFIG_PATH)
        api_key = config.get('api_key')
        cities = config.get('cities', {})

        if not api_key or not cities:
            logger.error("Missing API key or city list in config.")
            status = "failed"
            error_message = "Missing config values"
            return

        # Ensure raw data directory exists
        os.makedirs(RAW_DATA_DIR, exist_ok=True)

        # --- Fix: 21/08/2025 ---
        # Added coordinates to config file
        # 400 Client Error: Bad Request for url:
        # {"error":{"code":1006,"message":"No matching location found."}}
        for city, coords in cities.items():
            data = fetch_weather(api_key, coords)
            if data:
                save_raw_data(city, data, batch_id)
                rows_processed += 1
        logger.info(f"Weather ingestion job completed. Rows Processed: {rows_processed}")

    except Exception as e:
        status = "failed"
        error_message = str(e)
        logger.error(f"Error durring weather ingestion: {e}", exc_info=True)

    finally:
        # capture endtoime for log metadata
        end_time = datetime.now()

        # only log metadata if batch_id was set
        if batch_id:
            # Open DuckDB connection so can write metadata to log table
            DUCKDB_FILE = os.path.join(BASE_DIR, "db", "weather.duckdb")
            con = duckdb.connect(DUCKDB_FILE)

            # Log metadata
            try:
                log_metadata(
                    con=con,
                    batch_id=batch_id,
                    phase="ingest",
                    start_time=start_time,
                    end_time=end_time,
                    status=status,
                    rows_processed=rows_processed,
                    dq_passed=None,
                    error_message=error_message
                )
                logger.info("Metadata logged successfully.")
            except Exception as log_err:
                logger.error(f"Failed to log metadata : {log_err}") 
            finally:
                con.close()   

# -----------------------------
# Run as Script
# -----------------------------
if __name__ == '__main__':
    main()
