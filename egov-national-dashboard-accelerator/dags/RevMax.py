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
 
def elastic_dump_pt():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('property-services/_search', {
    "size": 10,
    "_source": ["Data.propertyId","data.superBuiltUpArea","Data.channel", "Data.tenantId", "Data.ward.name", 
    "Data.ward.code","Data.source", "Data.propertyType", "Data.accountId", "Data.noOfFloors", "Data.@timestamp", 
    "Data.ownershipCategory", "Data.acknowldgementNumber", "Data.usageCategory", "Data.status"],
    "query": {
        "bool": {
        "must_not": [
            {
            "term": {
                "Data.tenantId.keyword": "pb.testing"
            }
            }
        ]
        }
    },
    "sort": [
        {
        "Data.@timestamp": {
            "order": "desc"
        }
        }
    ]
    }
    )
    logging.info(resp['hits']['hits'])
    with open("property_service.json", "w") as outfile:
        outfile.write(resp['hits']['hits'])
    return resp['hits']['hits']

def elastic_dump_tl():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('tlindex-v1-enriched/_search', {
    "size": 10000,
    "_source": [
    "Data.ward.name",
    "Data.ward.code",
    "Data.tradelicense"
  ],
  "query": {
    "bool": {
      "must_not": [
        {
          "term": {
            "Data.tradelicense.tenantId.keyword": "pb.testing"
          }
        }
      ]
    }
  },
  "sort": [
    {
      "Data.tradelicense.@timestamp": {
        "order": "desc"
      }
    }
  ]
    })
    logging.info(resp)
    logging.info(resp['hits']['hits'])
    return resp['hits']['hits']

def elastic_dump_ws():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('wsapplications/_search', {
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

def elastic_dump_collection_pt():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('dss-collection_v2/_search', {
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


def elastic_dump_meter():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('meter-services/_search', {
        "size": 10,
        "query": {
        "match_all": {}
         },
        "sort": [
        {
        "Data.currentReadingDate": {
            "order": "desc"
        }
        }
    ]
    })
    logging.info(resp)
    logging.info(resp['hits']['hits'])
    return resp['hits']['hits']

flatten_data = PythonOperator(
task_id='flatten_data',
python_callable=elastic_dump_pt,
provide_context=True,
dag=dag)

flatten_data



