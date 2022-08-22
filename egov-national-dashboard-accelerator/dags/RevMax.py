from airflow import DAG
from hooks.elastic_hook import ElasticHook
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np
import json
import logging
import os

default_args = {
'owner': 'airflow',
'depends_on_past': False,
'retries': 3,
'retry_delay': timedelta(seconds=10),
'start_date': datetime(2017, 1, 24)
}

dag = DAG('rev_max', default_args=default_args, schedule_interval=None)

property_details = {
  'path': 'property-services/_search',
  'name': 'property_details',
  'query': """
{{
  "size":1000,
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

def elastic_dump_pt():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('property-services/_search', {
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
    with open("property_service.json", "w") as outfile:
        json.dump(elastic_dump_pt, outfile)
    return resp['hits']['hits']

flatten_data = PythonOperator(
task_id='flatten_data',
python_callable=elastic_dump_pt,
provide_context=True,
dag=dag)

flatten_data



