import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from hooks.elastic_hook import ElasticHook
from datetime import datetime, timedelta
import requests 
import logging
from airflow.hooks.base import BaseHook



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


def dump_kibana_pt():
    connection = BaseHook.get_connection('qa-punjab-kibana')
    endpoint = 'kibana/api/console/proxy'

    merged_document = {}
    query = property_details
    url = '{0}://{1}/{2}?path={3}&method=POST'.format('https', connection.host, endpoint, query.get('path'))
    q = query.get('query')
    logging.info(q)
    logging.info(query.get('path'))
    logging.info(url)
    r = requests.post(url, data=q, headers={'kbn-xsrf' : 'true', 'Content-Type' : 'application/json'}, auth=(connection.login, connection.password))
    logging.info(r.request)
    logging.info('>>>>>>>>>>>>>>>>')
    logging.info(r.text)
    response = r.json()
    merged_document[query.get('name')] = response['hits']['hits']

property_details = {
  'path': 'property-services/_search',
  'name': 'property_details',
  'query': """
GET /property-services/_search
{{
  "size":100,
    "query": {{
        "bool": {{
          "must_not": [
            {{
              "term": {{
                "Data.tenantId.keyword": "pb.testing"
              }}
            }}
          ]
        }}
      }}
}}

    """
} 


def elastic_dump():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('/property-services', {
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
    })
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

def readulb():
    ulbs = []
    ulb_json = open("/opt/airflow/dags/json/punjab_ulb.json")
    tenants_array=json.load(ulb_json)["tenants"]
    for tenant in tenants_array:
        ulbs.append(tenant["code"])
    print(len(ulbs))


curl_execute  = PythonOperator(
    task_id='elastic_dump',
    python_callable=elastic_dump,
    provide_context=True,
    do_xcom_push=True,
    dag=dag)
    
es_execute  = PythonOperator(
    task_id='es_dump',
    python_callable=dump,
    provide_context=True,
    do_xcom_push=True,
    dag=dag)

curl_execute
es_execute

