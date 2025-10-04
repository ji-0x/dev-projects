#!/usr/bin/env python3



import os
import sys
import duckdb
import logging
import pandas as pd
from datetime import datetime
from weather_pipeline.utils.metadata_utils import log_metadata

# -----------------------------
# Paths & Files
# -----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')
QUALITY_DIR = os.path.join(BASE_DIR, 'reports', 'quality')
LOG_DIR = os.path.join(BASE_DIR, 'logs')

duckdb_file = os.path.join(BASE_DIR, "db", 'weather.duckdb')
dq_flag = os.path.join(BASE_DIR, "reports", 'dq_pass.flag')

os.makedirs(QUALITY_DIR, exist_ok=True)
os.makedirs(LOG_DIR, exist_ok=True)

log_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_file_path = os.path.join(LOG_DIR, f'dq_weather_{log_time}.log')

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
dq_passed = True # assume it passes unless proven otherwise

# -----------------------------
# Batch ID and paths
# -----------------------------
batch_id = sys.argv[1]
batch_dir = os.path.join(PROCESSED_DATA_DIR, batch_id)
parquet_path = os.path.join(batch_dir, "*.parquet")

if not os.path.exists(batch_dir):
    logger.error(f"Batch directory does not exist: {batch_dir}")
    # sys.exit(1)
    status = "failed"
    error_message = f"Missing processed data for batch_id {batch_id}"
    dq_passed = False
    #Log metadata even in failure
    end_time = datetime.now()
    con = duckdb.connect(duckdb_file)
    try:
        log_metadata(
        con=con,
        batch_id=batch_id,
        phase="dq",
        start_time=start_time,
        end_time=end_time,
        status=status,
        rows_processed=rows_processed,
        dq_passed=dq_passed,
        error_message=error_message
    )
    except Exception as log_err:
        logger.error(f"Failed to log metadata: {log_err}")
    finally:
        con.close()
        sys.exit(1)

# -----------------------------
# Save DQ Report
# -----------------------------
def save_dq_report(df, filename_prefix):
    if df.empty:
        logger.info(f"No invalid records to save for rule: {filename_prefix}")
        return None
    timestamp = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    filename = f'{filename_prefix}_{timestamp}.csv'
    path = os.path.join(QUALITY_DIR, filename)
    df.to_csv(path, index=False)
    return path

# -----------------------------
# Connect to DuckDB
# -----------------------------
con = duckdb.connect(duckdb_file)

# Create a table, load only batch specific files
con.execute(f"""
    CREATE OR REPLACE TABLE weather_data AS
    SELECT * FROM read_parquet('{parquet_path}')
""")
logger.info(f"Loaded parquet files from {parquet_path} into DuckDB table weather_data")

# -----------------------------
# Identify VALID rows
# -----------------------------
valid_data_query = """
    SELECT * 
    FROM weather_data
    WHERE city IS NOT NULL
        AND local_time IS NOT NULL
        AND last_updated IS NOT NULL
        AND temperature_c IS NOT NULL
        AND condition_desc IS NOT NULL
        AND wind_kph IS NOT NULL
        AND wind_dir IS NOT NULL
        AND pressure_mb IS NOT NULL
        AND precip_mm IS NOT NULL
        AND humidity IS NOT NULL
        AND feelslike_c IS NOT NULL
        AND windchill_c IS NOT NULL
        AND dewpoint_c IS NOT NULL
        AND gust_kph IS NOT NULL
        AND local_time > '1900-01-01';
"""

valid_data = con.execute(valid_data_query).fetchdf()
logger.info(f"Valid rows count: {len(valid_data)}")

# -----------------------------
# Build INVALID data per rule with dq_type
# -----------------------------
invalid_conditions = {
    "null_fields": """
        SELECT *, 'null_fields' AS dq_type
        FROM weather_data
        WHERE city IS NULL
            OR local_time IS NULL
            OR last_updated IS NULL
            OR temperature_c IS NULL
            OR condition_desc IS NULL
            OR wind_kph IS NULL
            OR wind_dir IS NULL
            OR pressure_mb IS NULL
            OR precip_mm IS NULL
            OR humidity IS NULL
            OR feelslike_c IS NULL
            OR windchill_c IS NULL
            OR dewpoint_c IS NULL
            OR gust_kph IS NULL;
    """,
    "bad_timestamps": """
        SELECT *, 'bad_timestamps' AS dq_type
        FROM weather_data
        WHERE TRY_CAST(local_time AS TIMESTAMP) IS NULL
            OR TRY_CAST(last_updated AS TIMESTAMP) IS NULL
            OR TRY_CAST(last_updated AS TIMESTAMP) < TIMESTAMP '1900-01-01';
    """,
    "duplicate_fields": """
        SELECT *, 'duplicate_fields' AS dq_type
        FROM (
            SELECT *,
                COUNT(*) OVER (
                    PARTITION BY
                        city
                        ,local_time
                        ,last_updated
                        ,temperature_c
                        ,condition_desc
                        ,wind_kph
                        ,wind_dir
                        ,pressure_mb
                        ,precip_mm
                        ,humidity
                        ,feelslike_c
                        ,windchill_c
                        ,dewpoint_c
                        ,gust_kph
                ) AS dup_count
            FROM weather_data
        ) t
        WHERE dup_count > 1;
    """,
    "bad_datatypes": """
        SELECT *, 'bad_datatypes' AS dq_type
        FROM weather_data
        WHERE (TRY_CAST(temperature_c AS DOUBLE) IS NULL AND temperature_c IS NOT NULL)
            OR (TRY_CAST(wind_kph AS DOUBLE) IS NULL AND wind_kph IS NOT NULL)
            OR (TRY_CAST(pressure_mb AS DOUBLE) IS NULL AND pressure_mb IS NOT NULL)
            OR (TRY_CAST(precip_mm AS DOUBLE) IS NULL AND precip_mm IS NOT NULL)
            OR (TRY_CAST(humidity AS INTEGER) IS NULL AND humidity IS NOT NULL)
            OR (TRY_CAST(feelslike_c AS DOUBLE) IS NULL AND feelslike_c IS NOT NULL)
            OR (TRY_CAST(windchill_c AS DOUBLE) IS NULL AND windchill_c IS NOT NULL)
            OR (TRY_CAST(dewpoint_c AS DOUBLE) IS NULL AND dewpoint_c IS NOT NULL)
            OR (TRY_CAST(gust_kph AS DOUBLE) IS NULL AND gust_kph IS NOT NULL)
            OR (TRY_CAST(local_time AS TIMESTAMP) IS NULL AND local_time IS NOT NULL);
    """
}

invalid_frames = []

for rule, query in invalid_conditions.items():
    df = con.execute(query).fetchdf()
    # check if df has any rows
    if not df.empty:
        logger.warning(f"Invalid rows found for rule: {rule}, count: {len(df)}")
        invalid_frames.append(df)

# Combine invalid rows or create empty df to continue
if invalid_frames:
    # Combine and deduplicate invalid rows
    invalid_data = pd.concat(invalid_frames, ignore_index=True).drop_duplicates()
else:
    # create empty df, with same columns as valid_data,creating this DF ensure code can continue
    invalid_data = pd.DataFrame(columns=valid_data.columns.tolist() + ['dq_type']) 

# -----------------------------
# Write to DuckDB tables
# -----------------------------
con.execute("CREATE SCHEMA IF NOT EXISTS staging;")
con.execute("CREATE SCHEMA IF NOT EXISTS quarantine;")

# DuckDB sql 
con.register("valid_df", valid_data)
con.execute("""
    CREATE OR REPLACE TABLE staging.valid_weather AS
        SELECT *
        FROM valid_df
""")

con.register("invalid_df", invalid_data)
con.execute("""
    CREATE OR REPLACE TABLE quarantine.invalid_weather AS
        SELECT *
        FROM invalid_df
""")

# -----------------------------
# Save DQ reports & flag file
# -----------------------------
dq_failed = not invalid_data.empty

if dq_failed:
    report_path = save_dq_report(invalid_data, 'invalid_records')
    logger.warning(f"Data Quality issues found — saved invalid rows to: {report_path}")
    if os.path.exists(dq_flag):
        os.remove(dq_flag)
        logger.info("DQ failed - flag not written.")
else:
    with open(dq_flag, "w") as f:
        f.write('DQ passed')
    logger.info("DQ passed - flag written.")

# -----------------------------
# Final Metadata Logging
# -----------------------------
end_time = datetime.now()

dq_passed = not dq_failed
status = "success" if dq_passed else "failed"
rows_processed = len(valid_data) if dq_passed else 0

try:
    log_metadata(
        con=con,
        batch_id=batch_id,
        phase="dq",
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
