from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# List of cities
cities = ["geo","TX", "CA"]

# Function to run for each task
def fav_city(city, **kwargs):
    print(f"My favorite city is: {city}")

with DAG(
    dag_id="dynamic_tasking_example",
    start_date=datetime(2025, 8, 4),
    schedule="@daily",
    catchup=False
) as dag:

    for citi in cities:
        PythonOperator(
            task_id=f"good_{citi.lower()}",
            python_callable=fav_city,
            op_args=[citi]  # Pass city as an argument
        )
