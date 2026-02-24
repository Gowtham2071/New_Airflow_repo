from airflow.decorators import dag,task


@dag
def first_dag():
    @task.python
    def first_task():
        print("this is the first task")
    @task.python
    def Second_task():
        print("this is the second task")
    @task.python
    def Third_task():
        print("this is the third task")
    first = first_task()
    second = Second_task()
    third = Third_task()
    first >> second >> third

first_dag()