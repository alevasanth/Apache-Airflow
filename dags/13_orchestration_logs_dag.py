import os

from airflow.sdk import dag, task
from airflow.providers.standard.operators.python import PythonOperator
import logging

# Import orchestrator DAGs
from orchestrator_13_0 import first_orchestration_dag
from orchestrator_13_1 import second_orchestration_dag
from orchestrator_13_2 import third_orchestration_dag
from orchestrator_13_3 import fourth_orchestration_dag

logger = logging.getLogger(__name__)

@dag(
    dag_id='orchestrate_pipeline_logs',
    description='Parent DAG that orchestrates child DAGs with logging'
)
def orchestrate_pipeline_logs():
    """Orchestrate execution of child DAGs sequentially with logging"""
    
    @task
    def run_first_dag():
        """Execute first DAG"""
        logger.info("Starting first_orchestration_dag")
        first_orchestration_dag()
        logger.info("Completed first_orchestration_dag")
    
    @task
    def run_second_dag():
        """Execute second DAG"""
        logger.info("Starting second_orchestration_dag")
        second_orchestration_dag()
        logger.info("Completed second_orchestration_dag")

    @task
    def run_third_dag():
        """Execute third DAG"""
        logger.info("Starting third_orchestration_dag")
        third_orchestration_dag()
        logger.info("Completed third_orchestration_dag")

    @task
    def run_fourth_dag():
        """Execute fourth DAG"""
        logger.info("Starting fourth_orchestration_dag")
        fourth_orchestration_dag()
    # Ensure logs are flushed before next DAG starts
        os.makedirs('/opt/airflow/logs/orchestration', exist_ok=True)
        with open('/opt/airflow/logs/orchestration/orchestration_logs.txt', 'w') as log_file:
            log_file.write("Completed fourth_orchestration_dag\n")   
        logger.info("Completed fourth_orchestration_dag")

    # Set dependencies to run sequentially
    run_first_dag() >> run_second_dag() >> run_third_dag() >> run_fourth_dag()

# Instantiate the DAG
orchestration_dag_logs = orchestrate_pipeline_logs()