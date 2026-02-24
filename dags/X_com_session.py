from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

@dag
def Xcom_manual():
    @task.python
    def first_task(**kwargs):
        ti = kwargs[ti]
        fetched_data = {"data":[1,2,3,4]}
        print("extracting.. the first task")
        ti.xcom_push(key = "return_result", value = fetched_data)
    @task.python
    def Second_task(**kwargs):
        ti = kwargs[ti]
        fetched_data = ti.xcom_pull(task_id = "first_task", key = "return_result")
        print("transforming data.. this is second task")
        transformed_data = fetched_data*2 
        transformed_data_dict = {"trans_dat_dat":transformed_data}
        ti.xcom_push(key ="return_result", value = transformed_data_dict)
    @task.python
    def Third_task(**kwargs):
        ti = kwargs[ti]
        load_data = ti.xcom_pull(key = "second_task", value= "return_result")
        return load_data
        print("this is the third task")
    first = first_task()
    second = Second_task()
    third = Third_task()
    first >> second >> third
# here we dont need to type the dependencies because airflow will unterstand the code
Xcom_manual()