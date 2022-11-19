from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

default_args = {
 'retries': 2,
 'owner': 'airflow',
 'retry_delay': timedelta(minutes=1),
 }

with DAG(
    dag_id="first_bash_dag",
    default_args=default_args,
    description="Echo hello world",
    start_date=datetime(2022, 11, 18),
    schedule_interval='@daily',
) as dag:
    task1 = BashOperator(
        task_id = 'echo_hello',
        bash_command = 'echo "Hello World"'
    )
    
    task2 = BashOperator(
        task_id = 'second_task',
        bash_command='echo "This is the second task"'
    )
    
    task3 = BashOperator(
        task_id='third_task',
        bash_command='echo "This is the third task"'
    )
    
    task1 >> [task2]
    task1 >> [task3]
