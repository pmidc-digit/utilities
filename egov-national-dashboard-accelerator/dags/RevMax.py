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
    "size": 300,
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
        json.dump(resp['hits']['hits'],outfile)
        outfile.close()

    logging.info("absolute path {0}".format(os.path.abspath("property_service.json")))
    return resp['hits']['hits']

def elastic_dump_tl():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('tlindex-v1-enriched/_search', {
    "size": 300,
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
    with open("trade_license.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    
    logging.info("absolute path {0}".format(os.path.abspath("trade_license.json")))
    return resp['hits']['hits']
    
def elastic_dump_ws():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('water-services-enriched/_search', {
        "size": 300,
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
    with open("water_service.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    
    logging.info("absolute path {0}".format(os.path.abspath("water_service.json")))
    return resp['hits']['hits']

def elastic_dump_collection_pt():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('dss-collection_v2/_search', {
    "size": 300,
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
        ],
        "must": [
            {
            "term": {
                "dataObject.paymentDetails.businessService.keyword": "PT"
            }
            }
        ]
        }
    },
    "sort": [
        {
        "dataObject.@timestamp": {
            "order": "desc"
        }
        }
    ] 
    }) 
    logging.info(resp['hits']['hits'])
    with open("dss_collection_pt.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    
    logging.info("absolute path {0}".format(os.path.abspath("dss_collection_pt.json")))
    return resp['hits']['hits']

def elastic_dump_collection_tl():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('dss-collection_v2/_search', {
    "size": 300,
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
        ],
        "must": [
            {
            "term": {
                "dataObject.paymentDetails.businessService.keyword": "TL"
            }
            }
        ]
        }
    },
    "sort": [
        {
        "dataObject.@timestamp": {
            "order": "desc"
        }
        }
    ] 
    }) 
    logging.info(resp['hits']['hits'])
    with open("dss_collection_tl.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    
    logging.info("absolute path {0}".format(os.path.abspath("dss_collection_tl.json")))
    return resp['hits']['hits']

def elastic_dump_collection_ws():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('dss-collection_v2/_search', {
    "size": 300,
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
        ],
       "must": [
        {
          "terms": {
            "dataObject.paymentDetails.businessService.keyword": [
              "WS",
              "WS.ONE_TIME_FEE",
              "SW.ONE_TIME_FEE",
              "SW"
            ]
          }
        }
   ]
        }
    },
    "sort": [
        {
        "dataObject.@timestamp": {
            "order": "desc"
        }
        }
    ] 
    }) 
    logging.info(resp['hits']['hits'])
    with open("dss_collection_ws.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    
    logging.info("absolute path {0}".format(os.path.abspath("dss_collection_ws.json")))
    return resp['hits']['hits']

def elastic_dump_meter():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('meter-services/_search', {
        "size": 300,
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
    with open("meter_service.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    logging.info(resp['hits']['hits'])

    logging.info("absolute path {0}".format(os.path.abspath("meter_service.json")))
    return resp['hits']['hits']

def collect_data():
    elastic_dump_pt()
    elastic_dump_tl()
    elastic_dump_ws()
    #elastic_dump_meter()
    elastic_dump_collection_pt()

    f= open('property_service.json',"r")
    property_service_json = json.loads(f.read())
    f.close()

    f= open('trade_license.json',"r")
    trade_license_json = json.loads(f.read())
    f.close()

    f= open('water_service.json',"r")
    water_service_json = json.loads(f.read())
    f.close()

    f= open('dss_collection_pt.json',"r")
    dss_collection_pt_json = json.loads(f.read())
    f.close()

    f= open('dss_collection_tl.json',"r")
    dss_collection_tl_json = json.loads(f.read())
    f.close()
  
    f= open('dss_collection_ws.json',"r")
    dss_collection_ws_json = json.loads(f.read())
    f.close()
   
    f= open('meter_service.json',"r")
    meter_service_json = json.loads(f.read())
    f.close()
  
    df = get_dataframe_after_flattening(property_service_json)
    convert_dataframe_to_csv(dataframe=df,file_name="property_service")

    # water_service csv
    df = get_dataframe_after_flattening(water_service_json)
    convert_dataframe_to_csv(dataframe=df,file_name="water_service")
    # trade service csv
    df = get_dataframe_after_flattening(trade_license_json)
    convert_dataframe_to_csv(dataframe=df,file_name="trade_license")
    # meter_service csv
    df = get_dataframe_after_flattening(meter_service_json)
    convert_dataframe_to_csv(dataframe=df,file_name="meter_service")
    # dss_collection pt csv
    df = get_dataframe_after_flattening(dss_collection_pt_json)
    convert_dataframe_to_csv(dataframe=df,file_name="dss_collection_pt")
     # dss_collection tl csv
    df = get_dataframe_after_flattening(dss_collection_tl_json)
    convert_dataframe_to_csv(dataframe=df,file_name="dss_collection_tl")
    # dss_collection ws csv
    df = get_dataframe_after_flattening(dss_collection_ws_json)
    convert_dataframe_to_csv(dataframe=df,file_name="dss_collection_ws")



def replace_empty_objects_with_null_value(df):
    df_columns = df.columns.tolist()
    for cols in df_columns:
        try:
            unique_values = df[cols].unique().tolist()
            if (len(unique_values) == 1) and (
                unique_values == "{}" or unique_values == "[]"
            ):
                df[cols] = np.NaN
        except Exception as e:
            df[cols] = np.NaN
    return df

def convert_dataframe_to_csv(dataframe, file_name):
    dataframe.to_csv(
       f"""{file_name}.csv""", index=False
    )
    logging.info(dataframe)

def get_dataframe_after_flattening(json_data):
    logging.info(json_data)  
    df = [flatten_json(d) for d in json_data]  
    df = pd.DataFrame(df)
    df = replace_empty_objects_with_null_value(df)
    return df

def flatten_json(y):
    out = {}
    def flatten(x, name =''):
        if type(x) is dict:            
            for a in x:
                flatten(x[a], name + a + '.')
        elif type(x) is list:          
            i = 0        
            for a in x:                
                flatten(a, name + str(i) + '.')
                i += 1
        else:
            out[name[:-1]] = x
  
    flatten(y)
    return out

def water_and_meter_services(water_services, meter_services):
    water_and_meter = water_services.merge(
        meter_services,
        how="inner",
        left_on="_source.Data.connectionNo",
        right_on="_source.Data.connectionNo",
        suffixes=("_water", "_meter"),
    )
    convert_dataframe_to_csv(dataframe=water_and_meter, file_name="water_and_meter")

def property_and_water_services(water_services, property_services):
    water_and_property = water_services.merge(
        property_services,
        how="inner",
        left_on="_source.Data.propertyId",
        right_on="_source.Data.propertyId",
        suffixes=("_water", "_property"),
    )
    convert_dataframe_to_csv(dataframe=water_and_property, file_name="water_and_property"
    )

def trade_and_property_services(trade_services, property_services):
    trade_and_property = trade_services.merge(
        property_services,
        how="inner",
        left_on="_source.Data.tradelicense.propertyId",
        right_on="_source.Data.propertyId",
        suffixes=("_trade", "_property"),
    )
    convert_dataframe_to_csv(dataframe=trade_and_property, file_name="trade_and_property"
    )

def dss_collection_and_water(dss_collection,water_services):
    collection_and_water = dss_collection.merge(
        water_services,
        how="inner",
        left_on="_source.dataObject.paymentDetails.bill.consumerCode",
        right_on="_source.Data.applicationNo",
        suffixes=("_trade", "_property"),
    )
    convert_dataframe_to_csv(dataframe=collection_and_water, file_name="collection_and_water"
    )

def dss_collection_and_property(dss_collection,property_services):
    collection_and_property = dss_collection.merge(
        property_services,
        how="inner",
        left_on="_source.dataObject.paymentDetails.bill.consumerCode",
        right_on="_source.Data.propertyId",
        suffixes=("_trade", "_property"),
    )
    convert_dataframe_to_csv(
        dataframe=collection_and_property, file_name="collection_and_property"
    )

def dss_collection_and_trade(trade_services, dss_collection):
    collection_and_trade = dss_collection.merge(
        trade_services,
        how="inner",
        left_on="_source.dataObject.paymentDetails.bill.consumerCode",
        right_on="_source.Data.tradelicense.applicationNumber",
        suffixes=("_trade", "_property"),
    )
    convert_dataframe_to_csv(
        dataframe=collection_and_trade, file_name="collection_and_trade"
    )

# property_service_json = open("/opt/airflow/dags/json/property_service.json")
# water_service_json = open("/opt/airflow/dags/json/water_service.json")
# dss_service_json = open("/opt/airflow/dags/dss_collection.json")
# meter_service_json = open("/opt/airflow/dags/json/meter_service.json")
# trade_licence_json = open("/opt/airflow/dags/jso# # property_service csv
#     df = get_dataframe_after_flattening(property_service_json)
#     convert_dataframe_to_csv(dataframe=df,file_name="property_service")
# # water_service csv
#     df = get_dataframe_after_flattening(water_service_json)
#     convert_dataframe_to_csv(dataframe=df,file_name="water_service")
# # trade service csv
#     df = get_dataframe_after_flattening(trade_licence_json)
#     convert_dataframe_to_csv(dataframe=df,file_name="tradetrade_license_license")
# # meter_service csv
#     df = get_dataframe_after_flattening(meter_service_json)
#     convert_dataframe_to_csv(dataframe=df,file_name="meter_service")
# # dss_collection csv
#     df = get_dataframe_after_flattening(dss_service_json)
#     convert_dataframe_to_csv(dataframe=df,file_name="dss_collection")
#     logging.info('end')
# def joindata():
# #join water and meter
#     water_and_meter_services(water_services=water_service_after_flattening,meter_services=meter_services_after_flattening)
# #join trade and property
#     trade_and_property_services(trade_services=trade_licence_after_flattening,property_services=property_service_after_flattening)
# #join water and property
#     property_and_water_services(water_services=water_service_after_flattening,property_services=property_service_after_flattening)
# #join water and collection
#     dss_collection_and_water(water_services=water_service_after_flattening,dss_collection=dss_services_after_flattening)
# #join property and collection
#     dss_collection_and_property(property_services=property_service_after_flattening,dss_collection=dss_services_after_flattening)
# #join trade and collection
#     dss_collection_and_trade(trade_services=trade_licence_after_flattening,dss_collection=dss_services_after_flattening)
# property_service_after_flattening = get_dataframe_after_flattening(
#     json_data=json.load(property_service_json)["hits"]
# )
# water_service_after_flattening = get_dataframe_after_flattening(
#     json_data=json.load(water_service_json)["hits"]
# )
# dss_services_after_flattening = get_dataframe_after_flattening(
#     json_data=json.load(dss_service_json)["hits"]["hits"]
# )
# meter_services_after_flattening = get_dataframe_after_flattening(
#     json_data=json.load(meter_service_json)["hits"]
# )
# trade_licence_after_flattening = get_dataframe_after_flattening(
#     json_data=json.load(trade_licence_json)["hits"]
# )
flatten_data = PythonOperator(
task_id='flatten_data',
python_callable=collect_data,
provide_context=True,
dag=dag)

# join_data = PythonOperator(
# task_id='flatten_data',
# python_callable=join_data,
# provide_context=True,
# dag=dag)

flatten_data
