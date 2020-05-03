import json
import uuid
import random
import logging
import numpy as np
import pandas as pd
from time import sleep
from kafka import KafkaProducer
from datetime import datetime, timedelta

def create_logger(name, level=logging.INFO):
    # Create  logging format
    format = logging.Formatter(
        '%(asctime)s - %(name)s %(levelname)-8s [%(filename)s:%(lineno)d] %(message)s'
    )
    # Get logger
    logger = logging.getLogger(name)
    # Set logger level
    logger.setLevel(level)
    # Create logger console stream handler
    c_handler = logging.StreamHandler()
    # Set console logger level
    c_handler.setLevel(level)
    # Set console logger format
    c_handler.setFormatter(format)
    # Add console logger into main logger
    logger.addHandler(c_handler)

    return logger

logger = create_logger("Generator", logging.DEBUG)

broker_url = 'localhost:9092'
topic_name = 'telemetry_csv_3'
date_start = datetime(2020,1,1)
date_end = datetime(2021,1,1)
assets_count = 20
rows_count = 24 * 60
measure_count = 1000

producer = KafkaProducer(bootstrap_servers=broker_url)

logger.info('Start generating data.')

# Generate uid of asset
asset_uid_list = [str(uuid.uuid4()) for i in range(assets_count)]

# Generate uid of measure
measure_uid_list = [str(uuid.uuid4()) for i in range(measure_count)]

for date in pd.date_range(start=date_start, end=date_end, freq='D'):
    logger.debug("Send date: %s" % date)

    for i, asset_id in enumerate(asset_uid_list):

        for ts in pd.date_range(start=date, end=date + timedelta(days=1), periods=rows_count):
            
            df = pd.DataFrame({
                    'value': np.random.random_sample(len(measure_uid_list)),
                    'ts': [int(ts.timestamp())] * len(measure_uid_list),
                    'asset_uid': [asset_id] * len(measure_uid_list),
                    'measure_uid': measure_uid_list
                })

            # Send
            producer.send(topic_name, df.to_csv().encode())

        logger.debug('Send messages for asset: %d/%d' % (i + 1, len(asset_uid_list)))

        # sleep(5)
    
logger.info('End generating data.')