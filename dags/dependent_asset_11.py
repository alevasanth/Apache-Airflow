from airflow.sdk import dag, task, asset
import pendulum
import os
from assets_11 import data_extract

import pendulum

@asset(
    schedule=data_extract,
    # This is the path where the output of the asset will be stored. It can be a local path or a remote storage path and OPTIONAL yet Good Practice (like S3, GCS, etc.)
    uri = 'opt/airflow/logs/data/data_process.txt',
    name = 'process_data'
)
def process_data(self):
    # Ensure the directory exists before writing the file
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    
    # Simulate data extraction logic here
    with open(self.uri, 'w') as f:
        f.write(f"Data processed successfully! on {pendulum.now('Canada/Eastern')}\n")

    print(f"Data processed and stored at: {self.uri}")

    return self.uri