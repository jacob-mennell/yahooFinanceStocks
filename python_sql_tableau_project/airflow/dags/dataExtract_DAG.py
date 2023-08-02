import sys
import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# Get the directory of the current script (dataExtract_DAG.py)
current_script_dir = os.path.dirname(os.path.abspath(__file__))

# Get the parent directory of the current script (airflow/dags)
parent_dir = os.path.dirname(current_script_dir)

# Get the parent of the parent directory (python_sql_tableau_project)
package_dir = os.path.dirname(parent_dir)

# Add the package directory to the Python path
sys.path.append(package_dir)

from dataExtract import *

# DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 8, 1, 22, 0),  # Start at 10 PM
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'my_dag',
    default_args=default_args,
    description='My DAG for executing functions once a day at 10 PM',
    schedule_interval='0 22 * * *',  # Cron expression to run at 10 PM every day
)

# Define tasks for each function execution

# Task for sending individual tables to SQL
task_combined_stock = PythonOperator(
    task_id='combined_stock_task',
    python_callable=combined_stock_sql_send,
    op_args=[['AAPL', 'GOOG']],  # Stock list
)

# Task for sending exchange rate table to SQL
task_exchange_rate = PythonOperator(
    task_id='exchange_rate_task',
    python_callable=exchangeRate_table,
    op_args=[['AAPL', 'GOOG'], '1y', '1d'],  # Pass stock_list, period, and interval
    dag=dag,
)

# Set task dependencies to define the execution order
task_combined_stock >> task_exchange_rate