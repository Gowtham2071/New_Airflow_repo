from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

@dag(
    dag_id="must_succeed_wait_optional",
    start_date=datetime(2025, 8, 8),
    schedule="@daily",
    catchup=False,
)
def d():

    @task
    def must_succeed():
        return "important_output"

    @task
    def optional_task():
        raise Exception("optional failed")  # doesn't matter

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def next_task(x):
        # x only exists if must_succeed succeeded
        print("Next runs, must_succeed output:", x)

    a = must_succeed()
    b = optional_task()

    join = next_task(a)
    b >> join  # ✅ wait for optional to finish, but ALL_DONE allows fail

d()