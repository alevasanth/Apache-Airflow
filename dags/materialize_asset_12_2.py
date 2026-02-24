from airflow.sdk import asset
import pendulum
import os
import csv
from materialize_asset_12_1 import stage1_read_data

# Stage 2: Deduplication
@asset(
    schedule=stage1_read_data,  # Depends on stage1_read_data upstream
    uri='opt/airflow/logs/data/stage2_deduplicated_data.txt',
    name='stage2_deduplicate_data'
)
def stage2_deduplicate_data():
    """Stage 2: Remove duplicate records from raw data"""
    source_uri = 'opt/airflow/logs/data/stage1_raw_data.txt'
    target_uri = 'opt/airflow/logs/data/stage2_deduplicated_data.txt'
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(target_uri), exist_ok=True)
    
    # Read raw data and remove duplicates
    seen_records = set()
    deduplicated_data = []
    
    with open(source_uri, 'r') as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        
        for row in reader:
            # Create a tuple of values to check for duplicates
            record_tuple = tuple(row.values())
            
            if record_tuple not in seen_records:
                seen_records.add(record_tuple)
                deduplicated_data.append(row)
    
    # Write deduplicated data to target file
    with open(target_uri, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(deduplicated_data)
    
    duplicate_count = len([line for line in open(source_uri).readlines()]) - 1 - len(deduplicated_data)
    print(f"[Stage 2] Deduplication complete at: {target_uri}")
    print(f"[Stage 2] Removed {duplicate_count} duplicate records")
    print(f"[Stage 2] Final record count: {len(deduplicated_data)}")
    
    return target_uri