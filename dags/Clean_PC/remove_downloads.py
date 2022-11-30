import json
from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
import toml
import os

#generate list of files in datadirectory
with open ('dags/config/ebay.toml') as f:
    config = toml.load(f)
    
FILEPATH = config["data_directory"]["filepath"]
FILEPATH_DOWNLOADS = config["data_directory"]["filepath_download"]    


default_arg = {
    'schedule_interval': '@daily',
    'retries': 2,
    'owner': 'airflow',
    'retry_delay': timedelta(minutes=1),
}

@dag(default_args=default_arg,start_date=datetime(2022,11,18),description="Clean PC from old files", dag_id="Clean_PC") 
def taskflow():
    
    @task(task_id='get_file_list',retries=2)
    def get_files(filepath):
        files = os.listdir(filepath)
        return files



    @task(task_id='log_files',retries=2)
    def log_files(files):
        print(files)
        return files
    
    @task(task_id='remove_files',retries=2)
    def remove_files(files):
        os.remove(files)
    
    
    log_files(get_files) >> remove_files(get_files) 

dag = taskflow()    