#!/usr/bin/env python


import duckdb
from datetime import datetime

# -----------------------------
# Metadata logging helper function
# -----------------------------

def log_metadata(
        con,
        batch_id, 
        phase,
        start_time, 
        end_time, 
        status, 
        rows_processed=None, 
        dq_passed=None, 
        error_message=None
):
    """
    Logs pipeline metadata to the DuckDB metadata table for a given phase

    Parameters:
        - con (duckdb.DuckDBPyConnection): Active DuckDB connection.
        - batch_id (str): Unique batch identifier
        - phase (str): Phase of the pipeline for the pipeline run
        - start_time (datetime): Pipeline start time
        - end_time (datetime): Pipeline end time
        - status (str): Pipeline status ('success' or 'failed')
        - rows_processed (int): Number of rows handled
        - dq_passed (bool): Whether DQ checks passed
        - error_message (str): Optional error description if failed
    """
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS metadata;")
        # Ensure table exists
        con.execute("""
            CREATE TABLE IF NOT EXISTS metadata.pipeline_metadata (
                batch_id TEXT, 
                phase TEXT,
                start_time TIMESTAMP, 
                end_time TIMESTAMP, 
                status TEXT, 
                rows_processed INTEGER, 
                dq_passed BOOLEAN, 
                error_message TEXT,
                inserted_at TIMESTAMP DEFAULT current_timestamp
            );
        """)

        # Insert metadata
        con.execute("""
            INSERT INTO metadata.pipeline_metadata (
                batch_id, 
                phase,
                start_time, 
                end_time, 
                status, 
                rows_processed, 
                dq_passed, 
                error_message
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (batch_id, 
                phase,
                start_time, 
                end_time, 
                status, 
                rows_processed, 
                dq_passed, 
                error_message or None
        ))

    except Exception as e:
        print(f"[ERROR] Failed to log metadata: {e}")