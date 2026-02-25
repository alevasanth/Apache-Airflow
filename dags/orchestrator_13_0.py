from airflow.sdk import dag, task


@dag(
        dag_id="first_orchestration_dag"
)
def first_orchestration_dag():
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
first_orchestration_dag()