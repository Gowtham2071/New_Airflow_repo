from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime

with DAG(
    dag_id="taskgroup_example",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    with TaskGroup("extract_group") as extract_group:
        t1 = BashOperator(task_id="get_from_s3", bash_command="echo 'Extract from S3'")
        t2 = BashOperator(task_id="get_from_api", bash_command="echo 'Extract from API'")
        t3 = BashOperator(task_id="get_from_db", bash_command="echo 'Extract from DB'")
        [t1, t2, t3]  # Run in parallel

    with TaskGroup("transform_group") as transform_group:
        t4 = BashOperator(task_id="clean_data", bash_command="echo 'Cleaning data'")
        t5 = BashOperator(task_id="remove_duplicates", bash_command="echo 'Removing duplicates'")
        t6 = BashOperator(task_id="enrich_data", bash_command="echo 'Enriching data'")
        [t4, t5, t6]

    with TaskGroup("load_group") as load_group:
        t7 = BashOperator(task_id="load_to_dw", bash_command="echo 'Loading to warehouse'")
        t8 = BashOperator(task_id="quality_check", bash_command="echo 'Running quality checks'")
        t7 >> t8

    extract_group >> transform_group >> load_group
