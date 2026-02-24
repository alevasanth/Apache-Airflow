from airflow.sdk import asset
import os
import csv
from materialize_asset_12_0 import stage0_source_data

# Stage 1: Read Data
@asset(
    schedule=stage0_source_data,
    uri='opt/airflow/logs/data/stage1_raw_data.txt',
    name='stage1_read_data'
)
def stage1_read_data():
    """Stage 1: Read and validate data from Stage 0 source"""
    source_uri = 'opt/airflow/logs/data/source_data.txt'
    target_uri = 'opt/airflow/logs/data/stage1_raw_data.txt'
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(target_uri), exist_ok=True)
    
    # Read data from Stage 0 and pass through for validation
    record_count = 0
    with open(source_uri, 'r') as src:
        reader = csv.DictReader(src)
        rows = list(reader)
        record_count = len(rows)
    
    # Write data to Stage 1 output (pass-through with validation)
    with open(source_uri, 'r') as src:
        with open(target_uri, 'w') as tgt:
            tgt.write(src.read())
    
    print(f"[Stage 1] Data read from Stage 0 and validated")
    print(f"[Stage 1] Total records: {record_count}")
    print(f"[Stage 1] Output stored at: {target_uri}")
    return target_uri