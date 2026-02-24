from airflow.sdk import asset
import pendulum
import os
import csv
import shutil
from materialize_asset_12_2 import stage2_deduplicate_data

# Stage 3: Load to Target
@asset(
    schedule=stage2_deduplicate_data,  # Depends on stage2_deduplicate_data upstream
    uri='opt/airflow/logs/data/stage3_target_loaded.txt',
    name='stage3_load_to_target'
)
def stage3_load_to_target():
    """Stage 3: Load deduplicated data to target storage"""
    source_uri = 'opt/airflow/logs/data/stage2_deduplicated_data.txt'
    target_uri = 'opt/airflow/logs/data/stage3_target_loaded.txt'
    target_backup = 'opt/airflow/logs/data/target_backup.txt'
    
    # Ensure the directory exists
    os.makedirs(os.path.dirname(target_uri), exist_ok=True)
    
    # If target exists, create backup
    if os.path.exists(target_uri):
        shutil.copy(target_uri, target_backup)
        print(f"[Stage 3] Backup created at: {target_backup}")
    
    # Load deduplicated data to target
    with open(source_uri, 'r') as src:
        with open(target_uri, 'w') as tgt:
            tgt.write(src.read())
    
    # Read and validate loaded data
    record_count = 0
    with open(target_uri, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            record_count += 1
    
    # Create a summary report
    summary_file = 'opt/airflow/logs/data/load_summary.txt'
    os.makedirs(os.path.dirname(summary_file), exist_ok=True)
    
    with open(summary_file, 'w') as f:
        f.write(f"DATA LOAD SUMMARY\n")
        f.write(f"================\n")
        f.write(f"Timestamp: {pendulum.now('Canada/Eastern')}\n")
        f.write(f"Source: {source_uri}\n")
        f.write(f"Target: {target_uri}\n")
        f.write(f"Records Loaded: {record_count}\n")
        f.write(f"Status: SUCCESS\n")
    
    print(f"[Stage 3] Data loaded to target at: {target_uri}")
    print(f"[Stage 3] Total records loaded: {record_count}")
    print(f"[Stage 3] Load summary: {summary_file}")
    
    return target_uri