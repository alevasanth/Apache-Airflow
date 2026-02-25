from airflow.sdk import dag, task


@dag(
        dag_id="third_orchestration_dag"
)
def third_orchestration_dag():
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
third_orchestration_dag()