import json
from datetime import datetime,timedelta
from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.operators.python import PythonOperator
import toml
from ebaysdk.finding import Connection 
from ebaysdk.exception import ConnectionError

with open('dags/config/ebay.toml') as f:
    config = toml.load(f)
    
api_key = config["ebay_api_keys_prod"]["app_id"]
DATE = datetime.now().strftime("%Y-%m-%d")
DATE = DATE.replace('-','_')


def search_ebay(date):
    try:
        api = Connection(appid=api_key, config_file=None, siteid='EBAY-NL')
        response = api.execute('findItemsByKeywords', {'keywords': 'moog 32',})
        dump = json.dumps(response.dict(), indent=4)
        filename = f"{date}_ebay_seach.json"
        filepath = f"datafiles/"
        with open(filepath + filename,'w') as f:
            f.write(dump)
            f.close()

    except ConnectionError as e:
        print(e)
        print(e.response.dict())


def to_databasse(date):
    print("to database")

        
search_ebay(DATE)



