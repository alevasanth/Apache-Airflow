from  airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.interval import CronDataIntervalTimetable

@dag(
    schedule=CronDataIntervalTimetable("@daily", timezone="Canada/Eastern"),
    start_date=datetime(year=2026, month=2, day=20, tz="Canada/Eastern"),
    end_date=datetime(year=2026, month=2, day=28, tz="Canada/Eastern"),
    catchup=True
)
def incremental_dag():

    @task.python
    def incremental_extract(**kwargs):
        date_interval_start = kwargs['data_interval_start']
        date_interval_end = kwargs['data_interval_end']
        print(f"Extracting data for the interval: {date_interval_start} to {date_interval_end}")
        #Simulate data extraction logic here, e.g., querying a database with date filters  
        extracted_data = f"Data Extraction from {date_interval_start} to {date_interval_end}"
        return extracted_data
    
    @task.bash
    def incremental_processing():
        return "echo 'Processing Incremental Data from {{ data_interval_start }} to {{ data_interval_end }}'"
    
    # Set task dependencies
    incremental_extract() >> incremental_processing()

# Generate/Initiate the DAG
incremental_dag()