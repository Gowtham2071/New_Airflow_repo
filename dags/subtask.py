from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.operators.bash import BashOperator
from datetime import datetime

def process_claims_subdag(parent_dag_id, child_dag_id, args):
    with DAG(
        dag_id=f"{parent_dag_id}.{child_dag_id}",
        default_args=args,
        schedule_interval="@daily"
    ) as subdag:
        validate = BashOperator(
            task_id="validate_files",
            bash_command="echo Validating claims"
        )
        clean = BashOperator(
            task_id="clean_data",
            bash_command="echo Cleaning data"
        )
        enrich = BashOperator(
            task_id="enrich_with_patient_info",
            bash_command="echo Enriching with patient info"
        )
        payouts = BashOperator(
            task_id="calculate_payouts",
            bash_command="echo Calculating payouts"
        )

        validate >> clean >> enrich >> payouts
    return subdag

default_args = {"start_date": datetime(2025, 8, 4)}

with DAG(
    dag_id="claims_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
) as dag:

    start = BashOperator(task_id="start", bash_command="echo Starting pipeline")

    collect = BashOperator(task_id="collect_claims", bash_command="echo Collecting claims")

    process = SubDagOperator(
        task_id="process_claims",
        subdag=process_claims_subdag("claims_pipeline", "process_claims", default_args)
    )

    report = BashOperator(task_id="generate_reports", bash_command="echo Generating reports")

    end = BashOperator(task_id="end", bash_command="echo Pipeline complete")

    start >> collect >> process >> report >> end
