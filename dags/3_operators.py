from airflow.sdk import dag, task
from airflow.operators.bash import BashOperator

@dag(
        dag_id="operators_dag"
)
def operators_dag():
    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")
        
    @task
    def third_task():
        print("This is the third task")

    @task.python
    def my_function(p_x):
        if (p_x > 10):
            print("X is greater than 10")
        else:
            print("X is less than 10")

    @task.bash
    def bash_task_latest():
        return "echo https://airflow.apache.org/"

    bash_task_oldway = BashOperator(
        task_id="bash_task_oldway",
        bash_command="echo Hello DAG Completed!"
    )

    # Defining task dependencies
    first = first_task()
    second = second_task()
    third = third_task()
    fourth = my_function(15)
    bash_latest = bash_task_latest()
    bash_oldway = bash_task_oldway

    # Set task dependencies
    first >> second >> third >> fourth >> bash_latest >> bash_oldway

# Generate/Initiate the DAG
operators_dag()