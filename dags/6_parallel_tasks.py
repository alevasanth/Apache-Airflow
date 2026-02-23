from airflow.sdk import dag, task

@dag(
        dag_id="parallel_tasks_dag"
)
def parallel_tasks_dag():
    @task.python
    def extract(**kwargs):
        print("Extracting Data from sources")
        ti = kwargs['ti']
        source_data = {"source_api":[1,2,3],
                       "source_db2":[3,4,5],
                       "source_s3":[7,8,9]}
        ti.xcom_push(key='return_value', value=source_data)

    @task.python
    def api_transformation(**kwargs):
        ti = kwargs['ti']
        source_api = ti.xcom_pull(task_ids='extract')['source_api']
        print(f"Transforming API data: {source_api}")
        transformed_api_data = [i*10 for i in source_api]
        ti.xcom_push(key='return_value', value=transformed_api_data)


    @task.python
    def db2_transformation(**kwargs):
        ti = kwargs['ti']
        source_db2 = ti.xcom_pull(task_ids='extract')['source_db2']
        print(f"Transforming db2 data: {source_db2}")
        transformed_db2_data = [i*5 for i in source_db2]
        ti.xcom_push(key='return_value', value=transformed_db2_data)

    @task.python
    def s3_transformation(**kwargs):
        ti = kwargs['ti']
        source_s3 = ti.xcom_pull(task_ids='extract')['source_s3']
        print(f"Transforming S3 data: {source_s3}")
        transformed_s3_data = [i*3 for i in source_s3]
        ti.xcom_push(key='return_value', value=transformed_s3_data)


    @task.bash
    def load(**kwargs):
        print("Loading transformed data to the destination")
        api_load = kwargs['ti'].xcom_pull(task_ids='api_transformation')
        db2_load = kwargs['ti'].xcom_pull(task_ids='db2_transformation')
        s3_load = kwargs['ti'].xcom_pull(task_ids='s3_transformation')
        return f"echo 'Loaded data: {api_load}, {db2_load}, {s3_load}'"
    
        
    # Defining task dependencies
    extract_task = extract()
    api_task = api_transformation()
    db2_task = db2_transformation()
    s3_task = s3_transformation()
    load_task = load()

    # Set task dependencies
    extract_task >> [api_task, db2_task, s3_task] >> load_task

# Generate/Initiate the DAG
parallel_tasks_dag()