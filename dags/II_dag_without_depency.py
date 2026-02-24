from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime

@dag(
    dag_id="parallel_fail_continue_taskflow",
    start_date=datetime(2025, 8, 8),
    schedule="@daily",
    catchup=False,
)
def parallel_fail_continue_taskflow():

    @task
    def task_ok():
        print("Task OK running")
        return "ronaldo"   # returned value automatically becomes XCom

    @task
    def task_fail():
        print("Task FAIL running")
        raise Exception("boom")

    # ✅ This task will run even if task_fail fails
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def next_task():
        print("I will run even if one upstream task fails")

    t1 = task_ok()
    t2 = task_fail()

    next_task()  # dependencies set below
    [t1, t2] >> next_task  # ✅ parallel join with ALL_DONE

dag_instance = parallel_fail_continue_taskflow()