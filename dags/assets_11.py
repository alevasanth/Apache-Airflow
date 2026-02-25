from airflow.sdk import dag, task, asset
import pendulum
import os

import pendulum

@asset(
    schedule="@daily",
    # This is the path where the output of the asset will be stored. It can be a local path or a remote storage path and OPTIONAL yet Good Practice (like S3, GCS, etc.)
    uri = '/opt/airflow/logs/data/data_extract.txt',
    name = 'data_extract'
)
def data_extract(self):
    # Ensure the directory exists before writing the file
    os.makedirs(os.path.dirname(self.uri), exist_ok=True)
    
    # Simulate data extraction logic here
    with open(self.uri, 'w') as f:
        f.write(f"Data extracted successfully! on {pendulum.now('Canada/Eastern')}\n")

    print(f"Data extracted and stored at: {self.uri}")

    return self.uri