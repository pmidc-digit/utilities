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
    "size": 3000,
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
    with open("/opt/airflow/dags/property_service.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    return resp['hits']['hits']

def elastic_dump_tl():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('tlindex-v1-enriched/_search', {
    "size": 3000,
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
    }
    )
    logging.info(resp['hits']['hits'])
    with open("/opt/airflow/dags/trade_license.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    return resp['hits']['hits']

def elastic_dump_ws():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('water-services-enriched/_search', {
        "size": 3000,
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
    logging.info(resp['hits']['hits'])
    with open("/opt/airflow/dags/water_service.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    return resp['hits']['hits']

def elastic_dump_collection():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('dss-collection_v2/_search', {
    "size": 3000,
    "_source":["dataObject.paymentMode","dataObject.transactionNumber","dataObject.tenantId","dataObject.tenantData",
    "dataObject.paymentDetails.businessService","dataObject.paymentDetails.totalDue","dataObject.paymentDetails.receiptType",
    "dataObject.paymentDetails.receiptDate","dataObject.paymentDetails.bill.consumerCode","dataObject.paymentDetails.bill.billNumber",
    "dataObject.paymentDetails.bill.status","dataObject.paymentDetails.bill.billDate","dataObject.paymentDetails.bill.billDetails.fromPeriod",
    "dataObject.paymentDetails.bill.billDetails.toPeriod","dataObject.paymentDetails.bill.billDetails.demandId","dataObject.paymentDetails.bill.billDetails.billId", 
    "dataObject.paymentDetails.bill.billDetails.id", "dataObject.paymentDetails.bill.billNumber", 
    "dataObject.paymentDetails.totalAmountPaid","dataObject.paymentDetails.receiptNumber","dataObject.payer.name",
    "dataObject.payer.id","dataObject.paymentStatus","domainObject.ward","domainObject.propertyId",
    "domainObject.usageCategory","domainObject.tradeLicense","domainObject.propertyUsageType"],
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
    with open("/opt/airflow/dags/dss_collection.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    return resp['hits']['hits']

def elastic_dump_meter():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('meter-services/_search', {
        "size": 3000,
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
    with open("/opt/airflow/dags/meter_service.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    logging.info(resp['hits']['hits'])
    return resp['hits']['hits']

def collect_data():
    elastic_dump_pt()
    elastic_dump_tl()
    elastic_dump_ws()
    #elastic_dump_meter()
    elastic_dump_collection()

flatten_data = PythonOperator(
task_id='flatten_data',
python_callable=collect_data,
provide_context=True,
dag=dag)

flatten_data



