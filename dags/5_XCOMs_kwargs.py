from airflow.sdk import dag, task


@dag(
        dag_id="xcoms_kwargs_dag"
)
def xcoms_kwargs_dag():
    @task
    def first_task(**kwargs):

        #Extracting TI (TASK INSTANCE) from Kwargs to push Xcoms manually
        ti = kwargs['ti']
        print("Extracting data... This is the first task")
        fetched_data = {"data": [1, 2, 3, 4, 5]}
        ti.xcom_push(key="return_data", value=fetched_data)

    @task
    def second_task(**kwargs):
    #Extracting TI (TASK INSTANCE) from Kwargs to pull Xcoms manually
        ti = kwargs['ti']
    # Pulling the data pushed by first_task using Xcoms
        fetched_data = ti.xcom_pull(task_ids='first_task',key="return_data")['data']
        print(f"Transforming the Data in second task: {fetched_data}")
        transformed_data = fetched_data*2
        transformed_dict = {"trans_data":transformed_data}
    # Pushing the transformed data to Xcoms to be pulled by third_task
        ti.xcom_push(key="transformed_data", value=transformed_dict)
        
    @task
    def third_task(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='second_task',key="transformed_data")
        load_data = data
        return load_data
        print("Load Transformed Data is the third task, Dag Finished")

    # define the task dependencies
    # first = first_task()
    # second = second_task(first)
    # third = third_task(second)

    # Set task dependencies using bitshift operators
    first_task() >> second_task() >> third_task()

# Generate/Initiate the DAG
xcoms_kwargs_dag() 