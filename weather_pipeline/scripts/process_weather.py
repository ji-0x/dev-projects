#!/usr/bin/env python3

import os
import sys
import json
import logging
import duckdb
from datetime import datetime
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit
from weather_pipeline.utils.metadata_utils import log_metadata



# -----------------------------
# Directory paths
# -----------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__) ,'..'))
RAW_DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')
PROCESSED_DATA_DIR = os.path.join(BASE_DIR, 'data', 'processed')
LOG_DIR = os.path.join(BASE_DIR, "logs")

log_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
log_file_path = os.path.join(LOG_DIR, f'process_weather_{log_time}.log')

# -----------------------------
# Setup logging configuration
# -----------------------------
logging.basicConfig(
    filename=log_file_path,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# -----------------------------
# Main Spark Processing Script
# -----------------------------
def main():

    # Start metadata timing
    start_time = datetime.now()
    status = 'success'
    rows_processed = 0
    error_message = None
    batch_id = None

    if len(sys.argv) < 2:
        logger.error("Missing batch_id arguement. Usage: python3 process_weather.py <batch_id>")
        status = "failed"
        error_message = "Missing batch_id argument"
    else:
        batch_id = sys.argv[1]
        logger.info(f"Starting weather processing job for batch: {batch_id}")

        try:
            # Find raw files for the batch
            raw_files = list(Path(RAW_DATA_DIR).glob(f"*_{batch_id}.json"))
            # Check if raw JSON files exist
            if not raw_files:
                status = "failed" 
                error_message = f"No raw files found for batch {batch_id} in {RAW_DATA_DIR}"
                logger.warning(error_message)
                return

            # Start Spark session
            spark = SparkSession.builder \
                .appName("WeatherDataProcessor") \
                .config("spark.driver.memory", "4g") \
                .getOrCreate()
            
            # Spark tunning, reduce shuffle partitions and parquet block size, saves memory per writter
            spark.conf.set("spark.sql.shuffle.partitions", "4")
            spark.conf.set("parquet.block.size", "67108864") # 64MB

            logger.info("Spark session started.")

            # Read all raw JSON files 
            raw_files_path = os.path.join(RAW_DATA_DIR, f"*_{batch_id}.json")
            df = spark.read.option("multiline", "true").json(raw_files_path)
            logger.info(f"Read raw JSON files from: {raw_files_path}")

            # Select and rename relevant fields
            processed_df = df.select(
                col("location.name").alias("city"),
                col("location.localtime").alias("local_time"),
                col("current.last_updated").alias("last_updated"),
                col("current.temp_c").alias("temperature_c"),
                col("current.condition.text").alias("condition_desc"),
                col("current.wind_kph").alias("wind_kph"),
                col("current.wind_dir").alias("wind_dir"),
                col("current.pressure_mb").alias("pressure_mb"),
                col("current.precip_mm").alias("precip_mm"),
                col("current.humidity").alias("humidity"),
                col("current.feelslike_c").alias("feelslike_c"),
                col("current.windchill_c").alias("windchill_c"),
                col("current.dewpoint_c").alias("dewpoint_c"),
                col("current.gust_kph").alias("gust_kph")
            ).withColumn("batch_id", lit(batch_id))

            # get count of processede records in dataframe
            rows_processed = processed_df.count()
            logger.info(f"Selected relevant fields and added batch_id. Rows processed: {rows_processed}")

            # Ensure processed output directory exists
            os.makedirs(PROCESSED_DATA_DIR, exist_ok=True)

            # write to batch specific directory
            batch_output_dir = os.path.join(PROCESSED_DATA_DIR, batch_id)
            processed_df.write.mode("overwrite").parquet(batch_output_dir)

            logger.info(f"Wrote processed data to: {batch_output_dir}")

        except Exception as e:
            status = "failed" 
            error_message = str(e)
            logger.error(f"Error during weather processing: {e}", exc_info=True)

        finally:
            end_time = datetime.now()
            logger.info("Weather processing job completed.")

            if batch_id:
                DUCKDB_FILE = os.path.join(BASE_DIR, "db", "weather.duckdb")
                con = duckdb.connect(DUCKDB_FILE)

                try:
                    # Log metadata
                    log_metadata(
                        con=con,
                        batch_id=batch_id,
                        phase="process",
                        start_time=start_time,
                        end_time=end_time,
                        status=status,
                        rows_processed=rows_processed,
                        dq_passed=None,
                        error_message=error_message
                    )
                    logger.info("Metadata logged successfully.")
                except Exception as log_err:
                    logger.error(f"Failed to log metadata: {log_err}")
                finally:
                    con.close()

if __name__ == "__main__":
    main()
