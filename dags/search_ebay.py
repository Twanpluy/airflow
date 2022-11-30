"""
This DAG will search ebay for a given keyword and save the results to a json file.

"""
import json
from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
import toml
from ebaysdk.finding import Connection 
from ebaysdk.exception import ConnectionError
#replace with airflow connetion tooling
FILEPATH = f"datafiles/"
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
    api_key = ti.xcom_pull(task_ids=['get_api_key'])
    try:
        api = Connection(appid=api_key[0], config_file=None, siteid='EBAY-NL')
        response = api.execute('findItemsByKeywords', {'keywords': 'moog 32',})
        dump = response.dict()
        return dump
    
    except ConnectionError as e:
        print(e)
        print(e.response.dict())
  
def ebay_to_json_file(ti):
    date = datetime.now().strftime("%Y-%m-%d")
    date = date.replace('-','_')
    ebay_data = ti.xcom_pull(task_ids=['search_ebay'])
    dump = json.dumps(ebay_data, indent=4)
    filename = f"{date}_ebay_seach.json"
    filepath = f"datafiles/"
    with open(filepath + filename,'w') as f:
        f.write(dump)
        f.close()

def ebay_to_database(ti):
    ebay_data = ti.xcom_pull(task_ids=['search_ebay'])
    jsonStr = json.dumps(ebay_data)
    print(jsonStr['searchResult']) 
    print(type(jsonStr))
    print('test')
    return ebay_data
    
    
        
#DAG PythonOperator
with DAG(
    default_args=default_args,
    dag_id="Get_ebay_synths",
    description="Search ebay for modular synths",
    start_date=datetime(2021, 11, 18),
    schedule_interval='@daily',
) as dag:
    task1 = PythonOperator(
        task_id = 'get_api_key',
        python_callable=get_api_key,
    )
    
    task2 = PythonOperator(
        task_id = 'search_ebay',
        python_callable=search_ebay,
    )
    
    task3 = PythonOperator(
        task_id = 'ebay_to_json_file',
        python_callable=ebay_to_json_file,
    )
    
    task4 = PythonOperator(
        task_id = 'ebay_to_database',
        python_callable=ebay_to_database,
        )
    
    task1 >> task2 >> [task3, task4] 
    