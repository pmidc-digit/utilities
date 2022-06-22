from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests 
import logging


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2017, 1, 24)

}

dag = DAG('curl_execute', default_args=default_args, schedule_interval=None)
log_endpoint = 'kibana/api/console/proxy'
batch_size = 50

def dump():
    url = "http://elasticsearch-data-v1.es-cluster:9200/property-services/_search"
    logging.info(url)
    headers = {"content-type": "application/json", "Accept-Charset": "UTF-8"}
    logging.info(headers)

    r = requests.post(url, data={
    "size": 10,
    "query": {
        "match_all": {}
    },
        "sort": [
        {
        "Data.@timestamp": {
            "order": "desc"
        }
        }
    ]
    }, headers=headers)
    data = r.json()
    logging.info("response"+data)


curl_execute  = PythonOperator(
    task_id='curl_execute',
    python_callable=dump,
    provide_context=True,
    do_xcom_push=True,
    dag=dag)

curl_execute  



