"""
This DAG will search ebay for a given keyword and save the results to a json file.

TODO: Add DAG to make api requests to ebay and save the results to a json file.
TODO:   STEP 1: Create a DAG that will check if API is up.
TODO:   STEP 2: Create a search request to ebay.
TODO:   STEP 3: Save the results to a json file.
"""
import json
from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
import toml

#API GET     
# API_KEY = config["ebay_api_keys_prod"]["app_id"]
# DEFAULT ARGS
default_args = {
 'retries': 2,
 'owner': 'airflow',
 'retry_delay': timedelta(minutes=1),
 }
#function to prin api key
def get_api_key():
    with open('dags/config/ebay.toml') as f:
        config = toml.load(f)
    api_key = config["ebay_api_keys_prod"]["app_id"]
    f.close()
    return api_key

def search_ebay(ti):
    api_key = ti.xcom_pull(task_ids=['task1'])
    print(f'successfully pulled api key: {api_key}')
    return api_key
    
    
#DAG PythonOperator
with DAG(
    default_args=default_args,
    dag_id="print_api_key",
    description="Print API Key",
    start_date=datetime(2021, 11, 18),
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id = 'task1',
        python_callable=get_api_key,
    )
    
    task2 = PythonOperator(
        task_id = 'search_ebay',
        python_callable=search_ebay,
    )
    
    task1 >> task2