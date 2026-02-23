from airflow.sdk import dag, task
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

@dag(
        dag_id="schedule_cron_dag",
        start_date= datetime(year=2026, month=2, day=15, tz="Canada/Eastern"),
        end_date= datetime(year=2026, month=2, day=28, tz="Canada/Eastern"),
        schedule=CronTriggerTimetable("0 17 * * 1-5", timezone="Canada/Eastern"),  # Every 5 minutes Monday
        is_paused_upon_creation=False,
        catchup=True
)

def schedule_cron_dag():
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
schedule_cron_dag()