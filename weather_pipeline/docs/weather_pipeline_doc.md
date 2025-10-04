# weather_pipeline

## Table of Contents
1. Purpose
2. Architecture / Components
3. Folder Structure
4. Configuration
5. Execution Flow
6. Data Output (Files, DB, Reports)
7. Scheduling
8. Schema & Metadata
9. Logging & Error Handling
10. Testing & Validation
11. Dependencies
12. Maintainers
13. Future Improvements
14. Notes and Comments

## Quick Start
- git clone https://github.com/ji-0x/dev
- cd weather_pipeline
- python3 -m venv venv
- source venv/bin/activate
- pip install -r requirements.txt
- bash/zsh ./run_weather_pipeline.sh
- Or manual run: PYTHONPATH=/ <project-root-dir> python3 scripts/ <script-name> .py <batch_id>

## 1. Purpose / Overview
This pipeline fetches current weather data from [WeatherAPI.com](https://www.weatherapi.com), processes raw JSON responses into Parquet format, 
performs data quality and validation checks, and loads the cleaned data into a DuckDB database.

## 2. Architecture / Components
|  Stage          | Tools Used        | Description                                                              |
|-----------------|-------------------|--------------------------------------------------------------------------|
| ingestion       | Python + Requests | API call to fetch live weather data from WeatherAPI.com                  |
| processing      | Python + PySpark  | Processes raw JSON files, flattens nested structure to parquet DataFrame |
| data quality    | Python + DuckDB   | DQ checks to identify valid and invalid data                             |
| loading         | Python + DuckDB   | Load validated records to DuckDB                                         |
| logging         | Python + Logging  | Logs all pipeline actions and errors                                     |
| log metadata    | Python + DuckDB   | Logs pipeline metdata to DuckDB table for a given stage during the run   |

## 3. Folder Structure
Folder structure is as follows:
```text
    -weather_pipeline/
        |-- config/
        |-- cron/
        |-- data/
        |       |-- processed/
        |       |-- raw/
        |-- db/
        |-- logs/
        |-- docs/
        |-- reports/
        |       |-- quality/
        |-- scripts/
        |-- utils/
        |-- venv/
        |-- requirements.txt
        |-- README.md
```

## 4. Configuration
A config file has been setup in config/settings.json. This is where the API Key, Cities and Coordinates are defined.

## 5. Execution Flow
```text
    ingest_weather.py      ---------|
            ↓                       |
    process_weather.py              |
            ↓                       |--- run_weather_pipeline.sh
    dq_weather.py                   |
            ↓                       |
    load_weather.py        ---------|     
```
## 6. Data Output (DB, Files, Reports)
- Data loaded into DuckDB database in db/weather.duckdb
- Raw JSON files written to data/raw/
- Structured parquet files saved to data/processed/YYYYMMDD_HHMMSS/
- Data quality report written to reports/quality/invalid_records_yyyy-mm-dd_hh-mm-ss.csv

## 7. Scheduling
Scheduled via cron job every 1 hour.

## 8. Schema & Metadata
| Schema Name   | Table Name        | Description                                                           |
|---------------|-------------------|-----------------------------------------------------------------------|
| staging       | valid_weather     | Pre dq check, valid records only                                      |
| quarantine    | invalid_weather   | Invalid records, nulls, duplicates, bad datatypes, and bad timestamps |
| public        | weather           | Validated records from staging.valid_weather                          |
| metadata      | pipeline_metadata | Metadata log table, batch_id, phase, start/end_time, status, etc      |

| Column Name       | Data Type | Description                               |
|-------------------|-----------|-------------------------------------------|
| city              | object    | City Name                                 |
| local_time        | object    | Local time                                |
| last_updated      | object    | Last updated time                         |
| temperature_c     | float64   | Temperature (degrees celsius)             |
| condition_desc    | object    | Condition description                     |
| wind_kph          | float64   | Wind speed (kilometers per hour)          |
| wind_dir          | object    | Wind direction                            |
| pressure_mb       | float64   | Pressure (millibar)                       |
| precip_mm         | float64   | Precipitation (millimeters)               |
| humidity          | int64     | Humidity                                  |
| feelslike_c       | float64   | Feels like temperature (degrees celsius)  |
| windchill_c       | float64   | Wind chill (degrees celsius)              |
| dewpoint_c        | float64   | Dew point (degrees celsius)               |
| gust_kph          | float64   | Wind gust (kilometers per hour)           |
| batch_id          | object    | Unique batch identifier                   |

## 9. Error Handling & Logging
All logs written to logs/
- ingest_weather_yyyy-mm-dd_hh-mm-ss.log
- process_weather_yyyy-mm-dd_hh-mm-ss.log
- dq_weather_yyyy-mm-dd_hh-mm-ss.log
- load_weather_yyyy-mm-dd-hh-mm-ss.log
- weather_pipeline_batch_run_yyyy-mm-dd_hh-mm-ss.log
- weather_pipeline_cron_output_yyyy-mm-dd_hh-mm-ss.log
- env_from_cron.log
- Other:   
    - Errors are logged with stack traces
    - If API fails, the script logs and skips that city

## 10. Testing / Validation
- Unit tests for individual scripts (pending or under development)
- Automated pre-check validation of loaded data in DuckDB
- Spot checks of logs for failure handling

## 11. Dependencies
See requirements.txt file for full list of dependencies.
- python venv
- requests
- duckdb
- pyspark
- pandas
- cron
    
## 12. Owners / Maintainers
- Owner: ji-0x
- Contact:

## 13. Future Improvements
- Change scheduler for improved scheduling and monitoring. Reason being, limitations with cron on mac when in sleep mode. Possible options to consider:
    - Airflow DAG
    - Launchd
    - Github actions (using existing cron job)
- Testing
    - Develop formal unit tests. Testing was done during development, however is was unclean and adhoc. The intention here would be to create a testing folder with purpose built functions.
- Add more locations
    - Scalability, load and batch partitioning during processing.
    - Rate limiting, due to free plan and limited calls.
- Weather History
- Forecasts
- Shell script to clean up logs > x days
- Build reports and/or some analysis

## 14. Notes and Comments
- This Project was used as a learning exercise to get more familiar with data engineering concepts, tools and techniques. Using PySpark for this project and initial number of locations is clearly overkill, but i still wanted to experiment with it for the sake of hands on experience.
- Why DuckDB(OLAP) and not SQLite(OLTP)? 
    - easy to use for local dev work and playing around
    - free and serverless
    - Fast performance with parquet
    - Good SQL support, including window functions and CTEs, 
