import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from ..etlClass import StocksETL

# set environment variables
# os.environ['SQL_USERNAME'] = 
# os.environ["SQL_PASSWORD"] = 
# os.environ["SQL_SERVER"] = 
# os.environ["SQL_DATABASE"] = 

def run_etl(stock_list, period, interval):
    x = StocksETL(stock_list)
    x.combined_tables()
    # issue with loop to extract currency for each ticker using .info method
    # x.exchange_rate_table(period=period, interval=interval)

# DAG arguments with default parameters
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1, 22, 0),  # Start at 10 PM
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'params': {
        'stock_list': ['IAG.L'],
        'period': '1y',
        'interval': '1d',
    }
}

# Create the DAG
dag = DAG(
    'stock_etl_dag',
    default_args=default_args,
    start_date=datetime(2023, 8, 1),
    description='My DAG for executing functions once a day at 10 PM',
    schedule="@daily"
)

# Define a task to run the ETL function
task_run_etl = PythonOperator(
    task_id='run_etl_task',
    python_callable=run_etl,
    op_args=[
        '{{ params.stock_list }}',
        '{{ params.period }}',
        '{{ params.interval }}'
    ],
    dag=dag,
)

# Set the task dependencies to define the execution order
task_run_etl
