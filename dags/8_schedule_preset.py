from airflow.sdk import dag, task
from pendulum import datetime

@dag(
        dag_id="schedule_preset_dag",
        start_date= datetime(year=2026, month=1, day=1, tz="Canada/Eastern"),
        schedule="@daily",
        is_paused_upon_creation=False
)

def schedule_preset_dag():
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
schedule_preset_dag()