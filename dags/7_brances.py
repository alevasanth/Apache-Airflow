from airflow.sdk import dag, task

@dag(
        dag_id="branches_dag"
)
def branches_dag():
    @task.python
    def extract(**kwargs):
        print("Extracting Data from sources")
        ti = kwargs['ti']
        source_data = {"source_api":[1,2,3],
                       "source_db2":[3,4,5],
                       "source_s3":[7,8,9],
                       "weekend_holiday": "False"}
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


    # Creating the Decision Branching task or Node
    @task.branch
    def is_weekend_holiday(**kwargs):
        ti = kwargs['ti']
        source_data = ti.xcom_pull(task_ids='extract')
        if source_data['weekend_holiday'] == "True":
            return "no_load"
        else:
            return "load"



    @task.bash
    def load(**kwargs):
        print("Loading transformed data to the destination")
        api_load = kwargs['ti'].xcom_pull(task_ids='api_transformation')
        db2_load = kwargs['ti'].xcom_pull(task_ids='db2_transformation')
        s3_load = kwargs['ti'].xcom_pull(task_ids='s3_transformation')
        return f"echo 'Loaded data: {api_load}, {db2_load}, {s3_load}'"
    
    @task.bash
    def no_load(**kwargs):
        print("It's a weekend or holiday. No load will be performed.")
        return "echo 'No load performed due to weekend/holiday'"
    
        
    # Defining task dependencies
    extract_task = extract()
    api_task = api_transformation()
    db2_task = db2_transformation()
    s3_task = s3_transformation()
    load_task = load()
    no_load_task = no_load()
    decider_branch = is_weekend_holiday()

    # Set task dependencies
    extract_task >> [api_task, db2_task, s3_task] >> decider_branch >> [load_task, no_load_task]

# Generate/Initiate the DAG
branches_dag()