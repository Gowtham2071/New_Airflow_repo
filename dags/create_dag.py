from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
default_args= {
     
    "start_date" : datetime(2025, 8, 8)

}
def python_Love():
    print("MY First Love: Hashu")
def worst_love():
    print("samyu: the danger")

with DAG ( 
    "new_dag",
    default_args = default_args,
    schedule ="@daily", 
    catchup=False,
    
) as dag:
    task1 = PythonOperator(
        task_id = "My_love",
        python_callable=python_Love

    )

    task2 = PythonOperator(
        task_id = "worst_Behaviour",
        python_callable = worst_love
    )


    task1 >> task2