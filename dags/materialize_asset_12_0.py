from airflow.sdk import asset
import pendulum
import os
import csv

@asset(
    schedule="@daily",
    uri='opt/airflow/logs/data/source_data.txt',
    name='stage0_source_data'
)
def stage0_source_data():
    """Stage 0: Source data"""
    uri = 'opt/airflow/logs/data/source_data.txt'
    os.makedirs(os.path.dirname(uri), exist_ok=True)
    
    # Simulate data extraction logic here
    with open(uri, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['id', 'name', 'email', 'value'])
        writer.writerow(['1', 'John', 'john@example.com', '100'])  # duplicate
        writer.writerow(['2', 'Jane', 'jane@example.com', '200'])
        writer.writerow(['1', 'John', 'john@example.com', '100'])  # duplicate
        writer.writerow(['3', 'Bob', 'bob@example.com', '300'])
        writer.writerow(['4', 'Alice', 'alice@example.com', '400'])
        writer.writerow(['3', 'Bob', 'bob@example.com', '300'])  # duplicate

    print(f"[Stage 0] Raw data extracted and stored at: {uri}")
    return uri