from heapq import merge
import json
from airflow import DAG
from hooks.elastic_hook import ElasticHook
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import logging
import pandas as pd
import numpy as np
import json
import requests



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2017, 1, 24)

}

dag = DAG('rev_max', default_args=default_args, schedule_interval=None)
log_endpoint = 'kibana/api/console/proxy'
batch_size = 50


def dump_kibana_pt():
    #connection = BaseHook.get_connection('qa-punjab-kibana')
    #endpoint = 'kibana/api/console/proxy'

    merged_document = {}
    # query = property_details
    # url = '{0}://{1}/{2}?path={3}&method=POST'.format('https', connection.host, endpoint, query.get('path'))
    # q = query.get('query')
    # logging.info(q)
    # logging.info(query.get('path'))
    # logging.info(url)
    # r = requests.post(url, data=q, headers={'kbn-xsrf' : 'true', 'Content-Type' : 'application/json'}, auth=(connection.login, connection.password))
    # logging.info(r.request)
    # logging.info('>>>>>>>>>>>>>>>>')
    # logging.info(r.text)
    merged_document['pt'] = elastic_dump_pt
    merged_document['tl'] = elastic_dump_tl
    merged_document['ws'] = elastic_dump_ws
    merged_document['collection'] = elastic_dump_collection
    merged_document['meter'] = elastic_dump_meter



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

def elastic_dump_pt():
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

def elastic_dump_tl():
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

def elastic_dump_ws():
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

def elastic_dump_collection():
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


def elastic_dump_meter():
    hook = ElasticHook('GET', 'es_conn')
    resp = hook.search('/meter-services', {
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
       f"""/opt/airflow/dags/csv/{file_name}.csv""", index=False
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

    convert_dataframe_to_csv(
        dataframe=water_and_property, file_name="water_and_property"
    )


def trade_and_property_services(trade_services, property_services):

    trade_and_property = trade_services.merge(
        property_services,
        how="inner",
        left_on="_source.Data.tradelicense.propertyId",
        right_on="_source.Data.propertyId",
        suffixes=("_trade", "_property"),
    )

    convert_dataframe_to_csv(
        dataframe=trade_and_property, file_name="trade_and_property"
    )

def dss_collection_and_water(dss_collection,water_services):

    collection_and_water = dss_collection.merge(
        water_services,
        how="inner",
        left_on="_source.dataObject.paymentDetails.bill.consumerCode",
        right_on="_source.Data.applicationNo",
        suffixes=("_trade", "_property"),
    )

    convert_dataframe_to_csv(
        dataframe=collection_and_water, file_name="collection_and_water"
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

property_service_json = open("/opt/airflow/dags/json/property_service.json")

water_service_json = open("/opt/airflow/dags/json/water_service.json")

dss_service_json = open("/opt/airflow/dags/dss_collection.json")

meter_service_json = open("/opt/airflow/dags/json/meter_service.json")

trade_licence_json = open("/opt/airflow/dags/json/trade_license.json")


property_service_after_flattening = get_dataframe_after_flattening(
    json_data=json.load(property_service_json)["hits"]
)

water_service_after_flattening = get_dataframe_after_flattening(
    json_data=json.load(water_service_json)["hits"]
)

dss_services_after_flattening = get_dataframe_after_flattening(
    json_data=json.load(dss_service_json)["hits"]["hits"]
)

meter_services_after_flattening = get_dataframe_after_flattening(
    json_data=json.load(meter_service_json)["hits"]
)

trade_licence_after_flattening = get_dataframe_after_flattening(
    json_data=json.load(trade_licence_json)["hits"]
)


def flattendata():
    logging.info('start')
# property_service csv
    df = get_dataframe_after_flattening(property_service_json)
    convert_dataframe_to_csv(dataframe=df,file_name="property_service")

# water_service csv
    df = get_dataframe_after_flattening(water_service_json)
    convert_dataframe_to_csv(dataframe=df,file_name="water_service")

# trade service csv
    df = get_dataframe_after_flattening(trade_licence_json)
    convert_dataframe_to_csv(dataframe=df,file_name="trade_license")

# meter_service csv
    df = get_dataframe_after_flattening(meter_service_json)
    convert_dataframe_to_csv(dataframe=df,file_name="meter_service")

# dss_collection csv
    df = get_dataframe_after_flattening(dss_service_json)
    convert_dataframe_to_csv(dataframe=df,file_name="dss_collection")
    logging.info('end')

def joindata():
#join water and meter
    water_and_meter_services(water_services=water_service_after_flattening,meter_services=meter_services_after_flattening)

#join trade and property
    trade_and_property_services(trade_services=trade_licence_after_flattening,property_services=property_service_after_flattening)

#join water and property
    property_and_water_services(water_services=water_service_after_flattening,property_services=property_service_after_flattening)

#join water and collection
    dss_collection_and_water(water_services=water_service_after_flattening,dss_collection=dss_services_after_flattening)

#join property and collection
    dss_collection_and_property(property_services=property_service_after_flattening,dss_collection=dss_services_after_flattening)

#join trade and collection
    dss_collection_and_trade(trade_services=trade_licence_after_flattening,dss_collection=dss_services_after_flattening)

    

# property_service csv


flatten_data  = PythonOperator(
    task_id='flatten_data',
    python_callable=flattendata,
    provide_context=True,
    do_xcom_push=True,
    dag=dag)

join_data  = PythonOperator(
    task_id='join_data',
    python_callable=joindata,
    provide_context=True,
    do_xcom_push=True,
    dag=dag)

flatten_data >> join_data