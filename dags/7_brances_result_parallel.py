from airflow.sdk import dag, task

@dag(
        dag_id="branches_result_parallel_dag"
)
def branches_result_parallel_dag():
    @task.python
    def extract(**kwargs):
        print("Extracting Data from sources")
        ti = kwargs['ti']
        source_data = {"source_api":[1,2,3],
                       "source_db2":[240,260,300],
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
        transformed_db2_data = [i/10 for i in source_db2]
        ti.xcom_push(key='return_value', value=transformed_db2_data)

    @task.python
    def s3_transformation(**kwargs):
        ti = kwargs['ti']
        source_s3 = ti.xcom_pull(task_ids='extract')['source_s3']
        print(f"Transforming S3 data: {source_s3}")
        transformed_s3_data = [i+15 for i in source_s3]
        ti.xcom_push(key='return_value', value=transformed_s3_data)


    # Creating the Decision Branching task or Node
    @task.branch
    def check_data_threshold(**kwargs):
        ti = kwargs['ti']
        api_data = ti.xcom_pull(task_ids='api_transformation')
        db2_data = ti.xcom_pull(task_ids='db2_transformation')
        s3_data = ti.xcom_pull(task_ids='s3_transformation')
        
        all_data = api_data + db2_data + s3_data
        print(f"All transformed data: {all_data}")
        
        # Separate data into two branches
        greater_than_25 = [value for value in all_data if value > 25]
        less_equal_25 = [value for value in all_data if value <= 25]
        
        # Store the separated data
        ti.xcom_push(key='greater_than_25', value=greater_than_25)
        ti.xcom_push(key='less_equal_25', value=less_equal_25)
        
        print(f"Values > 25: {greater_than_25}")
        print(f"Values <= 25: {less_equal_25}")
        
        # Return both branches to run in parallel
        branches = []
        if greater_than_25:
            branches.append("process_greater_than_25")
        if less_equal_25:
            branches.append("process_less_equal_25")
        
        return branches


    @task.python
    def process_greater_than_25(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='check_data_threshold', key='greater_than_25')
        print(f"Processing values > 25: {data}")
        greater_25 = sum(data)
        print(f"Sum of values > 25: {greater_25}")
        ti.xcom_push(key='return_value', value=greater_25)
        return greater_25
    
    @task.python
    def process_less_equal_25(**kwargs):
        ti = kwargs['ti']
        data = ti.xcom_pull(task_ids='check_data_threshold', key='less_equal_25')
        print(f"Processing values <= 25: {data}")
        lessequal_25 = sum(data)
        print(f"Sum of values <= 25: {lessequal_25}")
        ti.xcom_push(key='return_value', value=lessequal_25)
        return lessequal_25

    @task.python
    def final_result(**kwargs):
        from datetime import datetime
        import pytz
        ti = kwargs['ti']
        
        # Pull the two sum values from both branches
        sum_greater_25 = ti.xcom_pull(task_ids='process_greater_than_25')
        sum_less_equal_25 = ti.xcom_pull(task_ids='process_less_equal_25')
        
        # Get current timestamp in EST timezone
        est = pytz.timezone('US/Eastern')
        timestamp = datetime.now(est).strftime("%Y-%m-%d %H:%M:%S %Z")
        
        result = {
            "timestamp": timestamp,
            "sum_greater_than_25": sum_greater_25,
            "sum_less_equal_25": sum_less_equal_25
        }
        
        print(f"\n{'-'*100}")
        print(f"Finised at - {timestamp}")
        print(f"{'-'*100}")
        print(f"Sum of values > 25: {sum_greater_25}")
        print(f"Sum of values <= 25: {sum_less_equal_25}")
        print(f"{'-'*100}\n")
        
        ti.xcom_push(key='return_value', value=result)
        return result
    
        
    # Defining task dependencies
    extract_task = extract()
    api_task = api_transformation()
    db2_task = db2_transformation()
    s3_task = s3_transformation()
    greater_task = process_greater_than_25()
    less_equal_25_task = process_less_equal_25()
    final_task = final_result()
    decider_branch = check_data_threshold()

    # Set task dependencies - both branches run in parallel after decider_branch, then final result
    extract_task >> [api_task, db2_task, s3_task] >> decider_branch >> [greater_task, less_equal_25_task] >> final_task

# Generate/Initiate the DAG
branches_result_parallel_dag()