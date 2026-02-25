from airflow.sdk import dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
import os
from datetime import datetime

# Import orchestrator DAGs
from orchestrator_13_0 import first_orchestration_dag
from orchestrator_13_1 import second_orchestration_dag
from orchestrator_13_2 import third_orchestration_dag
from orchestrator_13_3 import fourth_orchestration_dag

@dag()
def orchestrate_trigger_parent():
    
    trigger_first_dag = TriggerDagRunOperator(
        task_id='trigger_first_dag',
        trigger_dag_id='first_orchestration_dag',
        wait_for_completion=True
    )

    trigger_second_dag = TriggerDagRunOperator(
        task_id='trigger_second_dag',
        trigger_dag_id='second_orchestration_dag',
        wait_for_completion=True
    )

    trigger_third_dag = TriggerDagRunOperator(
        task_id='trigger_third_dag',
        trigger_dag_id='third_orchestration_dag',
        wait_for_completion=True
    )

    trigger_fourth_dag = TriggerDagRunOperator(
        task_id='trigger_fourth_dag',
        trigger_dag_id='fourth_orchestration_dag',
        wait_for_completion=True
    )
    
    @task
    def log_completion():
        """Log completion of all DAGs"""
        log_dir = '/opt/airflow/logs/orchestration'
        log_file = os.path.join(log_dir, 'orchestration_completion_log.txt')
        
        # Ensure directory exists
        os.makedirs(log_dir, exist_ok=True)
        
        # Write completion log
        with open(log_file, 'a') as f:
            f.write(f"[{datetime.now()}] - All child DAGs executed and completed\n")
        
        print(f" Completion log written to: {log_file}")
        return log_file

    # Set dependencies to run sequentially
    trigger_first_dag >> trigger_second_dag >> trigger_third_dag >> trigger_fourth_dag

# Instantiate the DAG
orchestrate_trigger_parent()