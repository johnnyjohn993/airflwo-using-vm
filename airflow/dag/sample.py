from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

# Default arguments applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Python function for the transform step
def transform_data(**kwargs):
    print("Transforming data...")
    # Add your transformation logic here
    return "Transform complete"

# Instantiate the DAG
with DAG(
    dag_id='basic_sample_dag',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule=timedelta(days=1),  # Or use a cron expression like '0 0 * * *'
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Task 1: Extract (using a Bash operator)
    extract_task = BashOperator(
        task_id='extract',
        bash_command='echo "Extracting data..." && sleep 5',
    )

    # Task 2: Transform (using a Python operator)
    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data,
    )

    # Task 3: Load (using a Bash operator)
    load_task = BashOperator(
        task_id='load',
        bash_command='echo "Loading data into destination..."',
    )

    # Define the task dependencies (Extract -> Transform -> Load)
    extract_task >> transform_task >> load_task