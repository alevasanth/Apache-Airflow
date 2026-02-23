from airflow.sdk import dag, task


@dag(
        dag_id="xcoms_auto_dag"
)
def xcoms_auto_dag():
    @task
    def first_task():
        print("Extracting data... This is the first task")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        return fetched_data

    @task
    def second_task(data:dict):
        fetched_data = data['data']
        transformed_data = fetched_data*2
        transformed_dict = {"trans_data":transformed_data}
        return transformed_dict
        
    @task
    def third_task(data:dict):
        load_data = data
        return load_data
        print("Load Transformed Data is the third task, Dag Finished")

    # define the task dependencies
    first = first_task()
    second = second_task(first)
    third = third_task(second)

# Generate/Initiate the DAG
xcoms_auto_dag()