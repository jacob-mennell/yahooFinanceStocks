import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# set environment variables
# os.environ['SQL_USERNAME'] = 
# os.environ["SQL_PASSWORD"] = 
# os.environ["SQL_SERVER"] = 
# os.environ["SQL_DATABASE"] = 

# Get the directory of the current script (dataExtract_DAG.py)
current_script_dir = os.path.dirname(os.path.abspath(__file__))

# Get the parent directory of the current script (airflow/dags)
parent_dir = os.path.dirname(current_script_dir)

# Get the parent of the parent directory (python_sql_tableau_project)
package_dir = os.path.dirname(parent_dir)

# Add the package directory to the Python path
sys.path.append(package_dir)

from etlClass import ETLClass

def run_etl(stock_list, period, interval):
    x = ETLClass(stock_list)
    x.combined_tables()
    x.exchange_rate_table(period=period, interval=interval)

# DAG arguments with default parameters
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1, 22, 0),  # Start at 10 PM
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'params': {
        'stock_list': ['IAG.L', '0293.HK', 'AF.PA'],
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
    schedule_interval='0 22 * * *',  # Cron expression to run at 10 PM every day
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