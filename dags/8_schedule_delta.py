from airflow.sdk import dag, task
from pendulum import datetime, duration
from airflow.timetables.trigger import DeltaTriggerTimetable

@dag(
        dag_id="schedule_delta_dag",
        start_date= datetime(year=2026, month=2, day=15, tz="Canada/Eastern"),
        end_date= datetime(year=2026, month=2, day=28, tz="Canada/Eastern"),
        schedule=DeltaTriggerTimetable(duration(days=3)),  # Every 3 days
        is_paused_upon_creation=True,
        catchup=True
)

def schedule_delta_dag():
    @task.python
    def first_task():
        print("This is the first task")

    @task.python
    def second_task():
        print("This is the second task")
        
    @task.python
    def third_task():
        print("This is the third task, Dag Completed")

    # Set task dependencies
    first_task() >> second_task() >> third_task()

# Generate/Initiate the DAG
schedule_delta_dag()