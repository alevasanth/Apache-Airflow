from airflow.sdk import dag, task

# Import orchestrator DAGs
from orchestrator_13_0 import first_orchestration_dag
from orchestrator_13_1 import second_orchestration_dag
from orchestrator_13_2 import third_orchestration_dag
from orchestrator_13_3 import fourth_orchestration_dag

@dag(
    dag_id='orchestrate_pipeline',
    description='Parent DAG that orchestrates child DAGs'
)

def orchestrate_pipeline():
    
    @task
    def run_first_dag():
        first_orchestration_dag()
    
    @task
    def run_second_dag():
        second_orchestration_dag()

    @task
    def run_third_dag():
        third_orchestration_dag()

    @task
    def run_fourth_dag():
        fourth_orchestration_dag()

    # Set dependencies to run sequentially
    run_first_dag() >> run_second_dag() >> run_third_dag() >> run_fourth_dag()

# Instantiate the DAG
orchestrate_pipeline()