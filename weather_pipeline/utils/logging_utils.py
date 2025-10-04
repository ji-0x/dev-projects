#!/usr/bin/env python3

import os
import logging
from datetime import datetime

def setup_logger(name: str, log_dir: str, prefix: str) -> logging.Logger:
    os.makedirs(log_dir ,exist_ok=True)
    log_time = datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    log_file_path = os.path.join(LOG_DIR, f"{prefix}_{log_time}.log")

    logging.basicConfig(
        filename=log_file_path,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    return logging.getLogger(name)
     

"""
To call the functon in each script:

from utils import setup_logger

LOG_DIR = os.path.join(BASE_DIR, "logs")
logger = setup_logger(__name__, LOG_DIR, 'process_weather')

Just change 'process_weather' to whatever prefix fits your script ('ingest_weather', 'dq_weather', etc.).

"""