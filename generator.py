import io
import json
import uuid
import random
import numpy as np
import pandas as pd
from time import sleep
from kafka import KafkaProducer
from datetime import datetime, timedelta

broker_url = 'localhost:9092'
topic_name = 'telemetry_2'
date_start = datetime(2020,1,1)
date_end = datetime(2020,1,2)
assets_count = 2
rows_count = 24 * 60
measure_count = 20

producer = KafkaProducer(bootstrap_servers=broker_url)

print('Start generating data.')

# Generate uid of asset
asset_uid_list = [str(uuid.uuid4()) for i in range(assets_count)]

# Generate uid of measure
measure_uid_list = [str(uuid.uuid4()) for i in range(measure_count)]


for date in pd.date_range(start=date_start, end=date_end, freq='D'):
    print("Send date: %s" % date)

    for i, asset_id in enumerate(asset_uid_list):
        for measure_id in measure_uid_list:
            buf_df = pd.DataFrame({
                    'ts': pd.date_range(start=date, end=date + timedelta(days=1), periods=rows_count),
                    'value': np.random.random_sample(rows_count)
                })
            buf_df['ts'] = buf_df['ts'].astype('int64')
            buf_df['asset_uid'] = asset_id
            buf_df['measure_uid'] = measure_id

            buffer = io.BytesIO()
            buf_df.to_parquet(buffer, engine='pyarrow')

            # Send
            producer.send(topic_name, buffer.getvalue())
        
        print('Send messages for asset: %d/%d' % (i + 1, len(asset_uid_list)))
    
    # sleep(5)