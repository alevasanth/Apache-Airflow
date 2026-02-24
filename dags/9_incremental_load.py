from  airflow.sdk import dag, task
from pendulum import datetime
from datetime import datetime as dt
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    schedule=CronDataIntervalTimetable("@daily", timezone="Canada/Eastern"),
    start_date=datetime(year=2026, month=2, day=20, tz="Canada/Eastern"),
    end_date=datetime(year=2026, month=2, day=28, tz="Canada/Eastern"),
    catchup=True
)
def incremental_load_dag():

    @task.python
    def incremental_data_extract(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Extracting data for the interval: {date_interval_start} to {date_interval_end}")
        
        # Simulate data extraction logic - querying a database with date filters
        # Example: SELECT * FROM users WHERE created_at BETWEEN start_date AND end_date
        simulated_database = [
            {"id": 1, "name": "Alice", "created_at": "2026-02-20"},
            {"id": 2, "name": "Bob", "created_at": "2026-02-21"},
            {"id": 3, "name": "Charlie", "created_at": "2026-02-22"},
            {"id": 4, "name": "David", "created_at": "2026-02-23"},
            {"id": 5, "name": "Eve", "created_at": "2026-02-24"},
        ]
        
        # Filter data based on date interval
        extracted_data = [
            record for record in simulated_database 
            if date_interval_start.date() <= dt.strptime(record['created_at'], '%Y-%m-%d').date() < date_interval_end.date()
        ]
        
        print(f"Extracted {len(extracted_data)} records: {extracted_data}")
        return extracted_data
    
    @task.bash
    def incremental_data_processing():
        return "echo 'Processing Incremental Data from {{ data_interval_start }} to {{ data_interval_end }}'"
    
    # Set task dependencies
    incremental_data_extract() >> incremental_data_processing()

# Generate/Initiate the DAG
incremental_load_dag()