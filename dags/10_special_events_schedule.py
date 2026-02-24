from  airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.events import EventsTimetable

special_dates = EventsTimetable(
    event_dates=[
        datetime(year=2026, month=2, day=5, tz="Canada/Eastern"),
        datetime(year=2026, month=2, day=15, tz="Asia/Kolkata"),
        datetime(year=2026, month=2, day=19, tz="Europe/London"),
        datetime(year=2026, month=2, day=23, tz="America/Chicago"),
        datetime(year=2026, month=2, day=25, tz="Asia/Kolkata"),
        datetime(year=2026, month=2, day=26, tz="America/New_York"),
    ]
)

@dag(
    schedule=special_dates,
    start_date=datetime(year=2026, month=1, day=30, tz="Canada/Eastern"),
    end_date=datetime(year=2026, month=2, day=28, tz="Canada/Eastern"),
    catchup=True
)
def special_events_dag():

    @task.python
    def special_event(**kwargs):
        event_date = kwargs['logical_date']
        print(f"Running task for special event on: {event_date}")

    # Set task dependencies
    special_event()

# Generate/Initiate the DAG
special_events_dag()