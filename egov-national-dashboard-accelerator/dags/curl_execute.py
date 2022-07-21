import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from hooks.elastic_hook import ElasticHook
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


def elastic_dump():
    hook = ElasticHook('GET', 'es_conn')
    q = """
    {
        "size": 10,
        "query": {
        "match_all": {   }
         },
        "sort": [
        {
        "Data.@timestamp": {
            "order": "desc"
        }
        }
    ]
    }
    """
    logging.info(q)
    resp = hook.search('property-services/_search', q )
    logging.info(resp)
    logging.info(resp['hits']['hits'])
    return resp['hits']['hits']

def dump():
    url = "http://elasticsearch-data-v1.es-cluster:9200/property-services/_search"
    logging.info(url)
    headers = {"content-type": "application/json", "Accept-Charset": "UTF-8"}
    logging.info(headers)

    dict_data = {"size": 10,
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
    }

    json_object = json.dumps(dict_data, indent = 4)
    print(json_object)
    response = requests.post(url, data=json_object, headers=headers)
    data = response.json()
    logging.info(data)


curl_execute  = PythonOperator(
    task_id='curl_execute',
    python_callable=elastic_dump,
    provide_context=True,
    do_xcom_push=True,
    dag=dag)

curl_execute  
