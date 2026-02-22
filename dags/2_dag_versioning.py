from airflow.sdk import dag, task


@dag(
        dag_id="versioned_dag"
)
def versioned_dag():
    @task
    def first_task():
        print("This is the first task")

    @task
    def second_task():
        print("This is the second task")
        
    @task
    def third_task():
        print("This is the third task, Dag Completed")

    @task
    def my_function(p_x):
        if (p_x > 10):
            print("X is greater than 10")
        else:
            print("X is less than 10")

    # Set task dependencies
    first_task() >> second_task() >> third_task() >> my_function(15)

# Generate/Initiate the DAG
versioned_dag()