from airflow.decorators import dag, task
from airflow.operators.python import PythonOperator

@dag
def share_dag():
    @task.python
    def first_task():
        fetched_data = {"data":[1,2,3,4]}
        print("extracting.. the first task")
    @task.python
    def Second_task(data:dict):
        fetched_data = data["data"]
        transformed_data = fetched_data*2 
        transformed_data_dict = {"trans_dat_dat":transformed_data}
        return transformed_data_dict
    @task.python
    def Third_task(data:dict):
        Load_data = data
        return Load_data
        print("this is the third task")
    first = first_task()
    second = Second_task(first)
    third = Third_task(second)
    first >> second >> third
# here we dont need to type the dependencies because airflow will unterstand the code
share_dag()