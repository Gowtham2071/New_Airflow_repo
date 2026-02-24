from airflow.decorators import dag, task
from datetime import datetime

@dag(dag_id ="my_dag", start_date=datetime(2025,7,3), schedule="@daily")
def my_dag():
    
    @task
    def training_model_a():
        return 1
    @task
    def training_model_b():
        return 2
    
    @task
    def training_model_c():
        return 3
    @task.branch
    def choose_best_model(accuracies: list[int]):
        if max(accuracies) > 2:
            return "accurate"
        return "inaccurate"
    @task.bash
    def accurate():
        return "echo 'accurate'"
    
    @task.bash
    def inaccurate():
        return "echo 'inaccurate'"
    
    accuracies = [training_model_a(), training_model_b(), training_model_c()] 
    choose_best_model(accuracies) >>[ accurate(), inaccurate()]
    
my_dag()