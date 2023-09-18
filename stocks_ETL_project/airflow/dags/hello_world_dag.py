from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def print_hello():
    print("Hello World !")

# Define your default_args
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1),
}

# Create DAG
dag = DAG(
    'hello_world_dag',
    default_args=default_args,
    schedule=None,  # Set this to None to manually trigger the DAG
)

# Define the PythonOperator task
print_hello_task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)

# Set the task dependencies
print_hello_task
