from airflow import DAG
from hooks.elastic_hook import ElasticHook
from airflow.hooks.base import BaseHook
from airflow.operators.python_operator import PythonOperator
from elasticsearch import Elasticsearch, helpers
from datetime import datetime, timedelta, timezone
from datetime import date
from pytz import timezone
from pydruid.client import *
import logging
import pandas as pd
import numpy as np
import json
import logging
import os
import csv
import requests
from pytz import timezone



default_args = {
'owner': 'airflow',
'depends_on_past': False,
'retries': 3,
'retry_delay': timedelta(seconds=10),
'start_date': datetime(2017, 1, 24)
}

dag = DAG('rev_max', default_args=default_args, schedule_interval=None)
druid_url = "https://druid-qa.ifix.org.in/druid/indexer/v1/task"

def elastic_dump_pt(start,end):
    hook = ElasticHook('GET', 'es_conn')
    query = """
    {{
    "size": 1000,
    "_source": ["Data.propertyId","data.superBuiltUpArea","Data.channel", "Data.tenantId", "Data.ward.name", 
    "Data.ward.code","Data.source", "Data.propertyType", "Data.accountId", "Data.noOfFloors", "Data.@timestamp", 
    "Data.ownershipCategory", "Data.acknowldgementNumber", "Data.usageCategory", "Data.status"],
    "query": {{
        "bool": {{
        "must_not": [
            {{
            "term": {{
                "Data.tenantId.keyword": "pb.testing"
            }}
            }}
        ],
        "must": [
            {{
                "range": {{
                    "Data.@timestamp": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
            }}
        ]
        }}
    }},
    "sort": [
        {{
        "Data.@timestamp": {{
            "order": "desc"
        }}
        }}
    ]
    }}"""

    resp = hook.search('property-services/_search', json.loads(query.format(start,end)))
    logging.info(resp['hits']['hits'])
    with open("property_service.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    return resp['hits']['hits']

def elastic_dump_tl(start,end):
    hook = ElasticHook('GET', 'es_conn')
    query = """
    {{
    "size": 1000,
    "_source": [
        "Data.ward.name",
        "Data.ward.code",
        "Data.tradelicense.applicationDate",
        "Data.tradelicense.applicationNumber",
        "Data.tradelicense.applicationType",
        "Data.tradelicense.validFrom",
        "Data.tradelicense.validTo",
        "Data.tradelicense.financialYear",
        "Data.tradelicense.licenseType",
        "Data.tradelicense.tradeName",
        "Data.tradelicense.tradeLicenseDetail.auditDetails",
        "Data.tradelicense.action",
        "Data.tradelicense.licenseNumber",
        "data.tradelicence.id",
        "Data.tradelicense.propertyId",
        "Data.history.businessService",
        "Data.tradelicense.workflowCode",
        "Data.tradelicense.accountId",
        "Data.tradelicense.@timestamp",
        "Data.tradelicense.tenantId",
        "Data.tradelicense.applicationDate",
        "Data.tradelicense.status",
        "Data.tradelicense.tradeLicenseDetail.channel",
        "Data.tradelicense.tradeLicenseDetail.adhocExemption",
        "Data.tradelicense.tradeLicenseDetail.adhocExemptionReason",
        "Data.tradelicense.tradeLicenseDetail.adhocPenalty",
        "Data.tradelicense.tradeLicenseDetail.structureType",
        "Data.tradelicense.tradeLicenseDetail.operationalArea"
    ],
    "query": {{
        "bool": {{
        "must_not": [
            {{
            "term": {{
                "Data.tradelicense.tenantId.keyword": "pb.testing"
            }}
            }}
        ],
        "must": [
            {{
                "range": {{
                    "Data.tradelicense.@timestamp": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
            }}
        ]
        }}
    }},
    "sort": [
        {{
        "Data.tradelicense.@timestamp": {{
            "order": "desc"
        }}
        }}
    ]
    }}
    """
    resp = hook.search('tlindex-v1-enriched/_search', json.loads(query.format(start,end)))
    logging.info(resp['hits']['hits'])
    with open("trade_license.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    return resp['hits']['hits']
    
def elastic_dump_ws(start,end):
    hook = ElasticHook('GET', 'es_conn')
    query="""
    {{
        "size": 1000,
        "_source": [
                "Data.applicationType",
                "Data.applicationNo",
                "Data.oldConnectionNo",
                "Data.proposedPipeSize",
                "Data.pipeSize",
                "Data.channel",
                "Data.channel",
                "Data.propertyUsageType",
                "Data.ward.name",
                "Data.ward.code",
                "Data.connectionType",
                "Data.applicationStatus",
                "Data.roadCuttingArea",
                "Data.rainWaterHarvesting",
                "Data.id",
                "Data.dateEffectiveFrom",
                "Data.propertyId",
                "Data.connectionNo",
                "Data.plumberInfo",
                "Data.proposedTaps",
                "Data.noOfTaps",
                "Data.waterSource",
                "Data.connectionCategory",
                "Data.connectionHolders",
                "Data.roadCuttingInfo",
                "Data.roadType",
                "Data.@timestamp",
                "data.meterId",
                "Data.tenantId",
                "Data.status"
            ],
        "query": {{
          "bool" :{{
            "must": [
            {{
            "range": {{
                "Data.@timestamp": {{
                "gte": {0},
                "lte": {1},
                "format": "epoch_millis"
                }}
            }}
            }}
          ]
            }}
            }},
        "sort": [
        {{
        "Data.@timestamp": {{
            "order": "desc"
        }}
        }}
    ]
    }}
    """
    resp = hook.search('water-services-enriched/_search', json.loads(query.format(start,end)))
    logging.info(resp['hits']['hits'])
    with open("water_service.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    return resp['hits']['hits']

def elastic_dump_collection_pt(start,end):
    hook = ElasticHook('GET', 'es_conn')
    query="""
    {{
    "size": 1000,
    "_source":["dataObject.paymentMode","dataObject.transactionNumber","dataObject.tenantId",
    "dataObject.paymentDetails.businessService","dataObject.paymentDetails.totalDue","dataObject.paymentDetails.receiptType",
    "dataObject.paymentDetails.receiptDate","dataObject.paymentDetails.bill.consumerCode","dataObject.paymentDetails.bill.billNumber",
    "dataObject.paymentDetails.bill.status","dataObject.paymentDetails.bill.billDate","dataObject.paymentDetails.bill.billDetails.fromPeriod",
    "dataObject.paymentDetails.bill.billDetails.toPeriod","dataObject.paymentDetails.bill.billDetails.demandId","dataObject.paymentDetails.bill.billDetails.billId", 
    "dataObject.paymentDetails.bill.billDetails.id", "dataObject.paymentDetails.bill.billNumber", 
    "dataObject.paymentDetails.totalAmountPaid","dataObject.paymentDetails.receiptNumber","dataObject.payer.name",
    "dataObject.payer.id","dataObject.paymentStatus","domainObject.ward.code","domainObject.ward.name","domainObject.propertyId",
    "domainObject.usageCategory","domainObject.tradeLicense","domainObject.propertyUsageType"],
    "query": {{
        "bool": {{
        "must_not": [
            {{
            "term": {{
                "Data.tenantId.keyword": "pb.testing"
            }}
            }}
        ],
        "must": [
            {{
            "term": {{
                "dataObject.paymentDetails.businessService.keyword": "PT"
            }}
            }},
            {{
                "range": {{
                    "dataObject.paymentDetails.receiptDate": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
            }}
        ]
        }}
    }},
    "sort": [
        {{
        "dataObject.paymentDetails.receiptDate": {{
            "order": "desc"
        }}
        }}
    ] 
    }}
    """
    resp = hook.search('dss-collection_v2/_search', json.loads(query.format(start,end))) 
    logging.info(resp['hits']['hits'])
    with open("dss_collection_pt.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    return resp['hits']['hits']

def elastic_dump_collection_tl(start,end):
    hook = ElasticHook('GET', 'es_conn')
    query="""
    {{
    "size": 1000,
    "_source":["dataObject.paymentMode","dataObject.transactionNumber","dataObject.tenantId",
    "dataObject.paymentDetails.businessService","dataObject.paymentDetails.totalDue","dataObject.paymentDetails.receiptType",
    "dataObject.paymentDetails.receiptDate","dataObject.paymentDetails.bill.consumerCode","dataObject.paymentDetails.bill.billNumber",
    "dataObject.paymentDetails.bill.status","dataObject.paymentDetails.bill.billDate","dataObject.paymentDetails.bill.billDetails.fromPeriod",
    "dataObject.paymentDetails.bill.billDetails.toPeriod","dataObject.paymentDetails.bill.billDetails.demandId","dataObject.paymentDetails.bill.billDetails.billId", 
    "dataObject.paymentDetails.bill.billDetails.id", "dataObject.paymentDetails.bill.billNumber", 
    "dataObject.paymentDetails.totalAmountPaid","dataObject.paymentDetails.receiptNumber","dataObject.payer.name",
    "dataObject.payer.id","dataObject.paymentStatus","domainObject.ward.code","domainObject.ward.name","domainObject.propertyId",
    "domainObject.usageCategory","domainObject.tradeLicense","domainObject.propertyUsageType"],
    "query": {{
        "bool": {{
        "must_not": [
            {{
            "term": {{
                "Data.tenantId.keyword": "pb.testing"
            }}
            }}
        ],
        "must": [
            {{
            "term": {{
                "dataObject.paymentDetails.businessService.keyword": "TL"
            }}
            }},
            {{
                "range": {{
                    "dataObject.paymentDetails.receiptDate": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
            }}
        ]
        }}
    }},
    "sort": [
        {{
        "dataObject.paymentDetails.receiptDate": {{
            "order": "desc"
        }}
        }}
    ] 
    }}
    """
    resp = hook.search('dss-collection_v2/_search',json.loads(query.format(start,end))) 
    logging.info(resp['hits']['hits'])
    with open("dss_collection_tl.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    return resp['hits']['hits']

def elastic_dump_collection_ws(start,end):
    hook = ElasticHook('GET', 'es_conn')
    query="""
    {{
    "size": 1000,
    "_source":["dataObject.paymentMode","dataObject.transactionNumber","dataObject.tenantId",
    "dataObject.paymentDetails.businessService","dataObject.paymentDetails.totalDue","dataObject.paymentDetails.receiptType",
    "dataObject.paymentDetails.receiptDate","dataObject.paymentDetails.bill.consumerCode","dataObject.paymentDetails.bill.billNumber",
    "dataObject.paymentDetails.bill.status","dataObject.paymentDetails.bill.billDate","dataObject.paymentDetails.bill.billDetails.fromPeriod",
    "dataObject.paymentDetails.bill.billDetails.toPeriod","dataObject.paymentDetails.bill.billDetails.demandId","dataObject.paymentDetails.bill.billDetails.billId", 
    "dataObject.paymentDetails.bill.billDetails.id", "dataObject.paymentDetails.bill.billNumber", 
    "dataObject.paymentDetails.totalAmountPaid","dataObject.paymentDetails.receiptNumber","dataObject.payer.name",
    "dataObject.payer.id","dataObject.paymentStatus","domainObject.ward.code","domainObject.ward.name","domainObject.propertyId",
    "domainObject.usageCategory","domainObject.tradeLicense","domainObject.propertyUsageType"],
    "query": {{
        "bool": {{
        "must_not": [
            {{
            "term": {{
                "Data.tenantId.keyword": "pb.testing"
            }}
            }}
        ],
       "must": [
        {{
          "terms": {{
            "dataObject.paymentDetails.businessService.keyword": [
              "WS",
              "WS.ONE_TIME_FEE",
              "SW.ONE_TIME_FEE",
              "SW"
            ]
          }}
        }},
        {{
                "range": {{
                   "dataObject.paymentDetails.receiptDate": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                }}
              }}
        }}
   ]
        }}
    }},
    "sort": [
        {{
        "dataObject.paymentDetails.receiptDate": {{
            "order": "desc"
        }}
        }}
    ] 
    }}
    """
    resp = hook.search('dss-collection_v2/_search', json.loads(query.format(start,end))) 
    logging.info(resp['hits']['hits'])
    with open("dss_collection_ws.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))
    return resp['hits']['hits']

def elastic_dump_meter(start,end):
    hook = ElasticHook('GET', 'es_conn')
    query="""
    {{
        "size": 1000,
        "query": {{
            "bool": {{
            "must": [
                {{
                "range": {{
                    "Data.currentReadingDate": {{
                    "gte": {0},
                    "lte": {1},
                    "format": "epoch_millis"
                    }}
                }}
                }}
            ]
            }}
         }},
        "sort": [
        {{
        "Data.currentReadingDate": {{
            "order": "desc"
        }}
        }}
    ]
    }}
    """
    resp = hook.search('meter-services/_search', json.loads(query.format(start,end)))
    logging.info(resp['hits']['hits'])
    with open("meter_service.json", "w") as outfile:
        outfile.write(json.dumps(resp['hits']['hits']))

    return resp['hits']['hits']

def collect_data(**kwargs):
    date = kwargs['dag_run'].conf.get('start')
    enddate = kwargs['dag_run'].conf.get('end')
    localtz = timezone('Asia/Kolkata')
    dt_aware = localtz.localize(datetime.strptime(date, "%d-%m-%Y"))
    start = int(dt_aware.timestamp() * 1000)
    dt_aware = localtz.localize(datetime.strptime(enddate, "%d-%m-%Y"))
    end = int(dt_aware.timestamp()*1000) + (24 * 60 * 60 * 1000) - 1000
    logging.info(start)
    logging.info(end)
    elastic_dump_pt(start,end)
    elastic_dump_tl(start,end)
    elastic_dump_ws(start,end)
    #elastic_dump_meter() - not in punjab prod
    elastic_dump_collection_pt(start,end)
    elastic_dump_collection_tl(start,end)
    elastic_dump_collection_ws(start,end)
    return 'done collecting data'

def join_data():
    logging.info("in join")
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
   
    # f= open('meter_service.json',"r")
    # meter_service_json = json.loads(f.read())
    # f.close()
  
    #property_service.csv
    df = get_dataframe_after_flattening(property_service_json)
    convert_dataframe_to_csv(dataframe=df,file_name="property_service")
    # water_service csv
    df = get_dataframe_after_flattening(water_service_json)
    convert_dataframe_to_csv(dataframe=df,file_name="water_service")
    # trade service csv
    df = get_dataframe_after_flattening(trade_license_json)
    convert_dataframe_to_csv(dataframe=df,file_name="trade_license")
    # meter_service csv
    # df = get_dataframe_after_flattening(meter_service_json)
    # convert_dataframe_to_csv(dataframe=df,file_name="meter_service")
    # dss_collection pt csv
    df = get_dataframe_after_flattening(dss_collection_pt_json)
    convert_dataframe_to_csv(dataframe=df,file_name="dss_collection_pt")
     # dss_collection tl csv
    df = get_dataframe_after_flattening(dss_collection_tl_json)
    convert_dataframe_to_csv(dataframe=df,file_name="dss_collection_tl")
    # dss_collection ws csv
    df = get_dataframe_after_flattening(dss_collection_ws_json)
    convert_dataframe_to_csv(dataframe=df,file_name="dss_collection_ws")


    property_service_after_flattening = get_dataframe_after_flattening(property_service_json)

    water_service_after_flattening = get_dataframe_after_flattening(water_service_json)

    dss_collection_pt_after_flattening = get_dataframe_after_flattening(dss_collection_pt_json)

    dss_collection_tl_after_flattening = get_dataframe_after_flattening(dss_collection_tl_json)

    dss_collection_ws_after_flattening = get_dataframe_after_flattening(dss_collection_ws_json)

    # meter_services_after_flattening = get_dataframe_after_flattening(meter_service_json)

    trade_licence_after_flattening = get_dataframe_after_flattening(trade_license_json)

    #join water and meter
    #water_and_meter_services(water_services=water_service_after_flattening,meter_services=meter_services_after_flattening)
    #join trade and property
    trade_and_property_services(trade_services=trade_licence_after_flattening,property_services=property_service_after_flattening)
    #join water and property
    property_and_water_services(water_services=water_service_after_flattening,property_services=property_service_after_flattening)
    #join water and collection
    dss_collection_and_water(water_services=water_service_after_flattening,dss_collection=dss_collection_ws_after_flattening)
    #join property and collection
    dss_collection_and_property(property_services=property_service_after_flattening,dss_collection=dss_collection_pt_after_flattening)
    #join trade and collection
    dss_collection_and_trade(trade_services=trade_licence_after_flattening,dss_collection=dss_collection_tl_after_flattening)
    #join water and property for rule3
    rule3(property_services=property_service_after_flattening,water_services=water_service_after_flattening)

def upload_property_service():
    data = ""
    f= open("property_service.csv","r")
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    for row in spamreader:
        data+=', '.join(row)
        data+='\\n'
    f.close()

    payload =  """
    {{
    "type": "index_parallel",
    "spec":{{
        "ioConfig": {{
        "type": "index_parallel",
        "inputSource": {{
            "type": "inline",
            "data": "_index,_type,_id,_score,_source.Data.channel,_source.Data.source,_source.Data.ward.code,_source.Data.ward.name,_source.Data.accountId,_source.Data.noOfFloors,_source.Data.@timestamp,_source.Data.ownershipCategory,_source.Data.propertyType,_source.Data.tenantId,_source.Data.propertyId,_source.Data.acknowldgementNumber,_source.Data.usageCategory,_source.Data.status,sort.0\nproperty-services,general,PT-1013-1246008pb.jalandhar,,CFC_COUNTER,MUNICIPAL_RECORDS,B5,Block 5,36d1eab8-bde6-40a6-b01c-615e7adb65d1,1,2022-05-31T16:37:43.850Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.jalandhar,PT-1013-1246008,AC-2022-05-30-1377661,Commercial,ACTIVE,1654015063850\nproperty-services,general,PT-1013-1246723pb.jalandhar,,CFC_COUNTER,MUNICIPAL_RECORDS,B5,Block 5,36d1eab8-bde6-40a6-b01c-615e7adb65d1,2,2022-05-31T16:03:30.379Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jalandhar,PT-1013-1246723,AC-2022-05-31-1379938,Mixed,ACTIVE,1654013010379\nproperty-services,general,PT-1910-1171076pb.patiala,,LEGACY_MIGRATION,LEGACY_RECORD,B13,BLOCK - 13,9c76e0df-8e28-4810-8d79-e983d0470a8c,2,2022-05-31T12:57:43.225Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.patiala,PT-1910-1171076,AC-2022-05-31-1379936,Residential,ACTIVE,1654001863225\nproperty-services,general,PT-1201-1246722pb.doraha,,CFC_COUNTER,MUNICIPAL_RECORDS,B6,Block 6,ad373a44-270b-48f2-be6c-7962ded48470,2,2022-05-31T12:37:58.808Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.doraha,PT-1201-1246722,AC-2022-05-31-1379935,Residential,INWORKFLOW,1654000678808\nproperty-services,general,PT-402-1096315pb.jaitu,,CFC_COUNTER,MUNICIPAL_RECORDS,B7,Block 7,d16a10a0-ffc6-464c-bc24-4220e9e9909a,1,2022-05-31T12:33:43.520Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jaitu,PT-402-1096315,AC-2022-05-31-1379934,Commercial,INWORKFLOW,1654000423520\nproperty-services,general,PT-1013-1246721pb.jalandhar,,CFC_COUNTER,MUNICIPAL_RECORDS,B12,Block 12,ee3e28ee-4b97-45b6-b985-3ced29717a6b,2,2022-05-31T12:31:35.350Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jalandhar,PT-1013-1246721,AC-2022-05-31-1379933,Residential,INWORKFLOW,1654000295350\nproperty-services,general,PT-1406-849339pb.nihalsinghwala,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T11:59:31.547Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.nihalsinghwala,PT-1406-849339,AC-2022-05-31-1379932,Residential,ACTIVE,1653998371547\nproperty-services,general,PT-601-695840pb.abohar,,CFC_COUNTER,MUNICIPAL_RECORDS,B27,Block 27,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T11:53:15.312Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.abohar,PT-601-695840,AC-2022-05-31-1379928,Residential,ACTIVE,1653997995312\nproperty-services,general,PT-2204-1246718pb.tarntaran,,CFC_COUNTER,MUNICIPAL_RECORDS,B3,Block 3,a396be2a-e3d3-4a79-95ec-ab02fcb674c4,1,2022-05-31T11:50:35.181Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.tarntaran,PT-2204-1246718,AC-2022-05-31-1379927,Commercial,ACTIVE,1653997835181\nproperty-services,general,PT-601-1246335pb.abohar,,CFC_COUNTER,MUNICIPAL_RECORDS,B49,Block 49,9ca18bd7-fcc3-41ff-a23d-5f9f3c366e87,1,2022-05-31T11:44:20.494Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.abohar,PT-601-1246335,AC-2022-05-31-1379926,Commercial,ACTIVE,1653997460494\nproperty-services,general,PT-1202-1246717pb.jagraon,,CFC_COUNTER,MUNICIPAL_RECORDS,B2,Block 2,016e818a-3e24-43b2-8bee-5728ea85c814,1,2022-05-31T11:43:52.181Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jagraon,PT-1202-1246717,AC-2022-05-31-1379925,Commercial,ACTIVE,1653997432181\nproperty-services,general,PT-1203-1246716pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W27,WARD 27,35b6de9d-2038-4e2e-9901-7d3c9478f49c,1,2022-05-31T11:41:58.435Z,INDIVIDUAL.SINGLEOWNER,VACANT,pb.khanna,PT-1203-1246716,AC-2022-05-31-1379924,Residential,ACTIVE,1653997318435\nproperty-services,general,PT-318-1134051pb.rampuraphul,,CFC_COUNTER,MUNICIPAL_RECORDS,B5,Block 5,ddab1664-8b8b-4e11-8469-43c190336768,2,2022-05-31T11:40:44.594Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.rampuraphul,PT-318-1134051,MT-318-033648,Mixed,ACTIVE,1653997244594\nproperty-services,general,PT-1203-1246715pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W27,WARD 27,35b6de9d-2038-4e2e-9901-7d3c9478f49c,1,2022-05-31T11:39:02.612Z,INDIVIDUAL.SINGLEOWNER,VACANT,pb.khanna,PT-1203-1246715,AC-2022-05-31-1379923,Residential,ACTIVE,1653997142612\nproperty-services,general,PT-1203-1246714pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W27,WARD 27,35b6de9d-2038-4e2e-9901-7d3c9478f49c,1,2022-05-31T11:35:22.141Z,INDIVIDUAL.SINGLEOWNER,VACANT,pb.khanna,PT-1203-1246714,AC-2022-05-31-1379922,Residential,ACTIVE,1653996922141\nproperty-services,general,PT-107-1246694pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-53,WARD-53,f70e5383-7f2d-4291-88e7-91580a7aedc1,2,2022-05-31T11:26:43.265Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-1246694,AC-2022-05-31-1379919,Residential,ACTIVE,1653996403265\nproperty-services,general,PT-1508-1129295pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,5,2022-05-31T11:26:05.439Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.mohali,PT-1508-1129295,AC-2022-05-31-1379918,Commercial,ACTIVE,1653996365439\nproperty-services,general,PT-1505-1246712pb.lalru,,CFC_COUNTER,MUNICIPAL_RECORDS,B4,Block 4,aff7f4c3-1425-4d65-a575-187b7fe61c70,1,2022-05-31T11:25:42.475Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.lalru,PT-1505-1246712,AC-2022-05-31-1379917,Residential,ACTIVE,1653996342475\nproperty-services,general,PT-1202-1246710pb.jagraon,,CFC_COUNTER,MUNICIPAL_RECORDS,B6,Block 6,016e818a-3e24-43b2-8bee-5728ea85c814,2,2022-05-31T11:22:44.517Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jagraon,PT-1202-1246710,AC-2022-05-31-1379914,Commercial,ACTIVE,1653996164517\nproperty-services,general,PT-1506-1246202pb.nayagaon,,CFC_COUNTER,MUNICIPAL_RECORDS,B9b,Block 9b,1cb4fc5f-1681-4d52-af12-c4caf5488ca8,2,2022-05-31T11:20:04.656Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.nayagaon,PT-1506-1246202,AC-2022-05-30-1378227,Residential,INACTIVE,1653996004656\nproperty-services,general,PT-1506-777766pb.nayagaon,,MIGRATION,MUNICIPAL_RECORDS,B9b,Block 9b,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T11:18:38.441Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.nayagaon,PT-1506-777766,PB-AC-2020-01-27-777776,Mixed,ACTIVE,1653995918441\nproperty-services,general,PT-1603-671370pb.malout,,CFC_COUNTER,MUNICIPAL_RECORDS,B7,Block A-7,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T11:16:53.582Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.malout,PT-1603-671370,AC-2022-05-31-1379913,Residential,ACTIVE,1653995813582\nproperty-services,general,PT-502-1246709pb.bassipathana,,CFC_COUNTER,MUNICIPAL_RECORDS,B5,Block 5,20d110f2-129c-4ced-a35b-78933ed6f634,1,2022-05-31T11:16:17.806Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.bassipathana,PT-502-1246709,AC-2022-05-31-1379912,Commercial,ACTIVE,1653995777806\nproperty-services,general,PT-1704-770091pb.rahon,,MIGRATION,MUNICIPAL_RECORDS,Ward1,Ward 1,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T11:14:09.272Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.rahon,PT-1704-770091,MT-1704-033645,Commercial,ACTIVE,1653995649272\nproperty-services,general,PT-1106-1246706pb.sultanpurlodhi,,CFC_COUNTER,MUNICIPAL_RECORDS,B42,Block 42,9979570b-5df2-466c-9e43-a078bc084c4b,1,2022-05-31T11:12:24.859Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.sultanpurlodhi,PT-1106-1246706,AC-2022-05-31-1379908,Residential,ACTIVE,1653995544859\nproperty-services,general,PT-107-1246651pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-51,WARD-51,8760f7a0-1fe6-4a54-abd0-e8c3c2fdefb0,3,2022-05-31T11:06:30.427Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-1246651,AC-2022-05-31-1379906,Residential,ACTIVE,1653995190427\nproperty-services,general,PT-1603-1246705pb.malout,,CFC_COUNTER,MUNICIPAL_RECORDS,B13,Block B-4,b52400b6-a25b-461b-bc97-94f017ceba3e,1,2022-05-31T11:02:25.490Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.malout,PT-1603-1246705,AC-2022-05-31-1379903,Residential,ACTIVE,1653994945490\nproperty-services,general,PT-1508-1108190pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,1,2022-05-31T11:02:25.080Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.mohali,PT-1508-1108190,AC-2022-05-31-1379902,Commercial,ACTIVE,1653994945080\nproperty-services,general,PT-1013-1246704pb.jalandhar,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block 8,dd2e694b-049a-448c-b188-be80eb4eb08b,2,2022-05-31T11:02:08.252Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.jalandhar,PT-1013-1246704,AC-2022-05-31-1379901,Commercial,ACTIVE,1653994928252\nproperty-services,general,PT-107-970937pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-51,WARD-51,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,5,2022-05-31T11:00:18.025Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-970937,AC-2022-05-31-1379893,Commercial,ACTIVE,1653994818025\nproperty-services,general,PT-2110-1246703pb.malerkotla,,CFC_COUNTER,MUNICIPAL_RECORDS,B31,Block31,28d40092-2545-492f-bb8f-97e2ad17a4fb,1,2022-05-31T11:00:08.639Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.malerkotla,PT-2110-1246703,AC-2022-05-31-1379900,Commercial,ACTIVE,1653994808639\nproperty-services,general,PT-107-944657pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-44,WARD-44,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:59:52.677Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-944657,AC-2022-05-31-1379884,Industrial,ACTIVE,1653994792677\nproperty-services,general,PT-107-918003pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-44,WARD-44,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:59:12.843Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-918003,AC-2022-05-31-1379879,Industrial,ACTIVE,1653994752843\nproperty-services,general,PT-1208-1158136pb.raikot,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,3f9c5759-0d74-491e-8f8b-25007552ae7c,1,2022-05-31T10:58:57.040Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.raikot,PT-1208-1158136,MT-1208-033642,Residential,ACTIVE,1653994737040\nproperty-services,general,PT-107-963374pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-44,WARD-44,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:58:56.372Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-963374,AC-2022-05-31-1379876,Industrial,ACTIVE,1653994736372\nproperty-services,general,PT-1508-1130830pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,1,2022-05-31T10:58:46.031Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.mohali,PT-1508-1130830,AC-2022-05-31-1379899,Residential,ACTIVE,1653994726031\nproperty-services,general,PT-107-1204328pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-76,WARD-76,8760f7a0-1fe6-4a54-abd0-e8c3c2fdefb0,1,2022-05-31T10:58:43.196Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-1204328,AC-2022-05-31-1379898,Commercial,ACTIVE,1653994723196\nproperty-services,general,PT-107-942772pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-14,WARD-14,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T10:57:34.676Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-942772,AC-2022-05-31-1379896,Residential,ACTIVE,1653994654676\nproperty-services,general,PT-107-1203836pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-1,WARD-1,8760f7a0-1fe6-4a54-abd0-e8c3c2fdefb0,1,2022-05-31T10:57:26.088Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-1203836,AC-2022-05-31-1379897,Commercial,ACTIVE,1653994646088\nproperty-services,general,PT-2101-644445pb.ahmedgarh,,MIGRATION,MUNICIPAL_RECORDS,B7,Block 7,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T10:54:55.202Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.ahmedgarh,PT-2101-644445,MT-2101-033641,Residential,ACTIVE,1653994495202\nproperty-services,general,PT-1503-1246700pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B16,Block 16,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,1,2022-05-31T10:54:10.079Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-1246700,AC-2022-05-31-1379890,Residential,ACTIVE,1653994450079\nproperty-services,general,PT-909-1246701pb.urmartanda,,CFC_COUNTER,MUNICIPAL_RECORDS,W13,Ward 13,6225b3ab-57a8-407b-82b6-60e322c961d1,1,2022-05-31T10:54:03.739Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.urmartanda,PT-909-1246701,AC-2022-05-31-1379891,Commercial,ACTIVE,1653994443739\nproperty-services,general,PT-2105-1211400pb.dhuri,,CFC_COUNTER,MUNICIPAL_RECORDS,B4,Block 4,996bd707-aba2-438d-a764-1a9c4947bdc4,1,2022-05-31T10:53:30.174Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.dhuri,PT-2105-1211400,AC-2022-05-31-1379889,Commercial,ACTIVE,1653994410174\nproperty-services,general,PT-107-965522pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-55,WARD-55,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T10:52:41.640Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-965522,AC-2022-05-31-1379888,Residential,ACTIVE,1653994361640\nproperty-services,general,PT-1502-099530pb.derabassi,,CFC_COUNTER,MUNICIPAL_RECORDS,W18,Ward 18,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T10:51:12.143Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.derabassi,PT-1502-099530,AC-2022-05-31-1379885,Mixed,ACTIVE,1653994272143\nproperty-services,general,PT-1508-1110961pb.mohali,,LEGACY_MIGRATION,LEGACY_RECORD,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,2,2022-05-31T10:48:27.840Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.SHAREDPROPERTY,pb.mohali,PT-1508-1110961,MT-1508-033638,Residential,ACTIVE,1653994107840\nproperty-services,general,PT-1503-1246699pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B11,Block 11,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T10:47:39.389Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246699,AC-2022-05-31-1379883,Residential,ACTIVE,1653994059389\nproperty-services,general,PT-1508-1129283pb.mohali,,LEGACY_MIGRATION,LEGACY_RECORD,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,2,2022-05-31T10:47:02.977Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.mohali,PT-1508-1129283,MT-1508-033637,Commercial,ACTIVE,1653994022977\nproperty-services,general,PT-1906-1246698pb.rajpura,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,b1915b70-c38f-47cb-a250-2c45c07e0e99,1,2022-05-31T10:46:56.029Z,INDIVIDUAL.SINGLEOWNER,VACANT,pb.rajpura,PT-1906-1246698,AC-2022-05-31-1379882,Residential,ACTIVE,1653994016029\nproperty-services,general,PT-1503-1246697pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B9,Block 9,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T10:46:32.623Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246697,AC-2022-05-31-1379880,Residential,ACTIVE,1653993992623\nproperty-services,general,PT-1508-1113290pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,2,2022-05-31T10:44:04.068Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.mohali,PT-1508-1113290,AC-2022-05-31-1379878,Residential,ACTIVE,1653993844068\nproperty-services,general,PT-1508-1246696pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,a7bb16bc-0ec0-4431-90cf-b8d1f5d1dccc,2,2022-05-31T10:43:18.086Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.SHAREDPROPERTY,pb.mohali,PT-1508-1246696,AC-2022-05-31-1379875,Residential,ACTIVE,1653993798086\nproperty-services,general,PT-1103-1246695pb.dhilwan,,CFC_COUNTER,MUNICIPAL_RECORDS,B5,Block 5,4df54821-f205-435e-82a9-1c22321bb851,1,2022-05-31T10:42:23.057Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.dhilwan,PT-1103-1246695,AC-2022-05-31-1379873,Residential,ACTIVE,1653993743057\nproperty-services,general,PT-107-977295pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-44,WARD-44,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:42:18.874Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-977295,AC-2022-05-31-1379870,Industrial,ACTIVE,1653993738874\nproperty-services,general,PT-316-121547pb.raman,,CFC_COUNTER,MUNICIPAL_RECORDS,B 9,Block 9,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:41:54.272Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.raman,PT-316-121547,AC-2022-05-31-1379872,Commercial,ACTIVE,1653993714272\nproperty-services,general,PT-1503-1246693pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B9,Block 9,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T10:40:33.076Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246693,AC-2022-05-31-1379868,Residential,ACTIVE,1653993633076\nproperty-services,general,PT-316-857124pb.raman,,CFC_COUNTER,MUNICIPAL_RECORDS,B 9,Block 9,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:40:13.243Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.raman,PT-316-857124,AC-2022-05-31-1379869,Commercial,INWORKFLOW,1653993613243\nproperty-services,general,PT-701-1246692pb.ferozepur,,CFC_COUNTER,MUNICIPAL_RECORDS,W14,Ward 14,3b59f9a0-bb4a-41d5-8776-d254136d8361,1,2022-05-31T10:39:18.578Z,INDIVIDUAL.SINGLEOWNER,VACANT,pb.ferozepur,PT-701-1246692,AC-2022-05-31-1379866,Residential,ACTIVE,1653993558578\nproperty-services,general,PT-1508-1108551pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,2,2022-05-31T10:37:29.436Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.SHAREDPROPERTY,pb.mohali,PT-1508-1108551,AC-2022-05-31-1379864,Residential,ACTIVE,1653993449436\nproperty-services,general,PT-1503-1246691pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B11,Block 11,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T10:35:55.260Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246691,AC-2022-05-31-1379862,Residential,ACTIVE,1653993355260\nproperty-services,general,PT-107-1246681pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-10,WARD-10,8f778bdc-6125-487d-9c12-ef325f463c10,2,2022-05-31T10:34:14.113Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-1246681,MT-107-033633,Residential,ACTIVE,1653993254113\nproperty-services,general,PT-1904-1246689pb.nabha,,CFC_COUNTER,MUNICIPAL_RECORDS,B10,Block10,029bbba2-8b8d-4699-bdff-a746f51d5461,2,2022-05-31T10:33:42.617Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.nabha,PT-1904-1246689,AC-2022-05-31-1379858,Residential,ACTIVE,1653993222617\nproperty-services,general,PT-1508-1117181pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,3,2022-05-31T10:33:17.994Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.mohali,PT-1508-1117181,AC-2022-05-31-1379859,Residential,ACTIVE,1653993197994\nproperty-services,general,PT-1208-1246685pb.raikot,,CFC_COUNTER,MUNICIPAL_RECORDS,B2,Block 2,3f9c5759-0d74-491e-8f8b-25007552ae7c,2,2022-05-31T10:32:14.093Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.raikot,PT-1208-1246685,AC-2022-05-31-1379854,Commercial,ACTIVE,1653993134093\nproperty-services,general,PT-1503-1246688pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B2,Block 2,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,1,2022-05-31T10:31:55.386Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-1246688,AC-2022-05-31-1379857,Commercial,ACTIVE,1653993115386\nproperty-services,general,PT-107-1246671pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-32,WARD-32,f70e5383-7f2d-4291-88e7-91580a7aedc1,1,2022-05-31T10:30:35.460Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-1246671,AC-2022-05-31-1379853,Industrial,ACTIVE,1653993035460\nproperty-services,general,PT-107-920125pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-42,WARD-42,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:27:55.292Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-920125,AC-2022-05-31-1379832,NonResidential,ACTIVE,1653992875292\nproperty-services,general,PT-107-1246638pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-51,WARD-51,8760f7a0-1fe6-4a54-abd0-e8c3c2fdefb0,2,2022-05-31T10:27:13.113Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-1246638,MT-107-033630,Residential,ACTIVE,1653992833113\nproperty-services,general,PT-1503-1246684pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B9,Block 9,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T10:26:42.189Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246684,AC-2022-05-31-1379849,Residential,ACTIVE,1653992802189\nproperty-services,general,PT-1508-1212876pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,a7bb16bc-0ec0-4431-90cf-b8d1f5d1dccc,2,2022-05-31T10:26:36.992Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.mohali,PT-1508-1212876,AC-2022-05-31-1379847,Residential,ACTIVE,1653992796992\nproperty-services,general,PT-1401-1246679pb.badhnikalan,,CFC_COUNTER,MUNICIPAL_RECORDS,B6,Ward 6,c7bed31e-77ac-4a47-8065-5cad77faa524,1,2022-05-31T10:25:53.739Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.badhnikalan,PT-1401-1246679,AC-2022-05-31-1379846,Commercial,ACTIVE,1653992753739\nproperty-services,general,PT-1508-1246683pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,a7bb16bc-0ec0-4431-90cf-b8d1f5d1dccc,3,2022-05-31T10:25:50.921Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.mohali,PT-1508-1246683,AC-2022-05-31-1379845,Residential,ACTIVE,1653992750921\nproperty-services,general,PT-2105-1243312pb.dhuri,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,996bd707-aba2-438d-a764-1a9c4947bdc4,1,2022-05-31T10:25:36.900Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.dhuri,PT-2105-1243312,AC-2022-05-31-1379844,Commercial,ACTIVE,1653992736900\nproperty-services,general,PT-107-931739pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-45,WARD-45,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:24:02.682Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-931739,AC-2022-05-31-1379830,Residential,ACTIVE,1653992642682\nproperty-services,general,PT-1401-1246682pb.badhnikalan,,CFC_COUNTER,MUNICIPAL_RECORDS,B6,Ward 6,c7bed31e-77ac-4a47-8065-5cad77faa524,1,2022-05-31T10:23:02.229Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.badhnikalan,PT-1401-1246682,AC-2022-05-31-1379841,Commercial,ACTIVE,1653992582229\nproperty-services,general,PT-1603-1246680pb.malout,,CFC_COUNTER,MUNICIPAL_RECORDS,B27,Block F-1,b52400b6-a25b-461b-bc97-94f017ceba3e,2,2022-05-31T10:20:32.201Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.malout,PT-1603-1246680,AC-2022-05-31-1379839,Residential,ACTIVE,1653992432201\nproperty-services,general,PT-1011-1246604pb.phillaur,,CFC_COUNTER,MUNICIPAL_RECORDS,B11A,Block 11 A,72af516c-864d-41f8-aa10-6de050d36479,3,2022-05-31T10:19:58.706Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.phillaur,PT-1011-1246604,AC-2022-05-31-1379837,Residential,ACTIVE,1653992398706\nproperty-services,general,PT-503-630068pb.khamano,,MIGRATION,MUNICIPAL_RECORDS,B11,Block11,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:19:29.328Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.khamano,PT-503-630068,MT-503-033629,Commercial,ACTIVE,1653992369328\nproperty-services,general,PT-1508-1100605pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,2,2022-05-31T10:17:57.676Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.mohali,PT-1508-1100605,AC-2022-05-31-1379836,Residential,ACTIVE,1653992277676\nproperty-services,general,PT-1508-1103014pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,2,2022-05-31T10:17:00.350Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.mohali,PT-1508-1103014,AC-2022-05-31-1379834,Mixed,ACTIVE,1653992220350\nproperty-services,general,PT-401-1246644pb.faridkot,,CFC_COUNTER,MUNICIPAL_RECORDS,B11,Block 11,da70cf46-31a3-4c1e-aee1-f014d9f76110,1,2022-05-31T10:16:59.035Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.faridkot,PT-401-1246644,AC-2022-05-31-1379738,Residential,ACTIVE,1653992219035\nproperty-services,general,PT-1904-1246678pb.nabha,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block8,029bbba2-8b8d-4699-bdff-a746f51d5461,1,2022-05-31T10:16:51.590Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.nabha,PT-1904-1246678,AC-2022-05-31-1379833,Industrial,ACTIVE,1653992211590\nproperty-services,general,PT-1102-1159694pb.bhulath,,CFC_COUNTER,MUNICIPAL_RECORDS,B7,Block 7,bcdec0a5-2deb-4163-8ba4-ab8497fd6b7c,1,2022-05-31T10:15:48.981Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.bhulath,PT-1102-1159694,MT-1102-033627,Commercial,ACTIVE,1653992148981\nproperty-services,general,PT-1013-1246676pb.jalandhar,,CFC_COUNTER,MUNICIPAL_RECORDS,B3,Block 3,8b8595c3-91e7-4aed-89bf-1e92dd1d64f2,1,2022-05-31T10:13:33.301Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jalandhar,PT-1013-1246676,AC-2022-05-31-1379828,Commercial,ACTIVE,1653992013301\nproperty-services,general,PT-1503-011525pb.kharar,,MIGRATION,MUNICIPAL_RECORDS,B2,Block 2,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:13:11.134Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-011525,AC-2022-05-31-1379827,Residential,INACTIVE,1653991991134\nproperty-services,general,PT-1508-1122923pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,3,2022-05-31T10:12:47.108Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.mohali,PT-1508-1122923,AC-2022-05-31-1379825,Residential,ACTIVE,1653991967108\nproperty-services,general,PT-1013-1246017pb.jalandhar,,CFC_COUNTER,MUNICIPAL_RECORDS,B18,Block 18,834e9327-981f-4448-8679-c9d7f06043bf,3,2022-05-31T10:12:33.456Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jalandhar,PT-1013-1246017,AC-2022-05-30-1377698,Residential,ACTIVE,1653991953456\nproperty-services,general,PT-601-1246675pb.abohar,,CFC_COUNTER,MUNICIPAL_RECORDS,B44,Block 44,9ca18bd7-fcc3-41ff-a23d-5f9f3c366e87,2,2022-05-31T10:12:28.659Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.abohar,PT-601-1246675,AC-2022-05-31-1379824,Commercial,ACTIVE,1653991948659\nproperty-services,general,PT-2110-1246674pb.malerkotla,,CFC_COUNTER,MUNICIPAL_RECORDS,B42,Block42,28d40092-2545-492f-bb8f-97e2ad17a4fb,1,2022-05-31T10:12:28.545Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.malerkotla,PT-2110-1246674,AC-2022-05-31-1379823,Residential,ACTIVE,1653991948545\nproperty-services,general,PT-1703-1245678pb.nawanshahr,,CFC_COUNTER,MUNICIPAL_RECORDS,B13,Block 13,73629555-f356-49e2-8546-59257ec369bd,1,2022-05-31T10:11:01.213Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.nawanshahr,PT-1703-1245678,AC-2022-05-27-1376715,Residential,ACTIVE,1653991861213\nproperty-services,general,PT-503-630035pb.khamano,,MIGRATION,MUNICIPAL_RECORDS,B11,Block11,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:10:13.743Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.khamano,PT-503-630035,MT-503-033623,Commercial,ACTIVE,1653991813743\nproperty-services,general,PT-107-1031794pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-13,WARD-13,f70e5383-7f2d-4291-88e7-91580a7aedc1,1,2022-05-31T10:09:22.003Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-1031794,AC-2022-05-31-1379820,Mixed,ACTIVE,1653991762003\nproperty-services,general,PT-0604-1246132pb.jalalabad,,CFC_COUNTER,MUNICIPAL_RECORDS,WD-9,Ward No 9,8a265fb1-c474-454b-9165-c19ffeded35a,1,2022-05-31T10:09:08.621Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jalalabad,PT-0604-1246132,AC-2022-05-30-1378019,Commercial,ACTIVE,1653991748621\nproperty-services,general,PT-107-939402pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-6,WARD-6,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T10:09:08.111Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-939402,AC-2022-05-31-1379804,Residential,ACTIVE,1653991748111\nproperty-services,general,PT-1503-1246673pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block 8,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T10:08:58.161Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246673,AC-2022-05-31-1379819,Residential,ACTIVE,1653991738161\nproperty-services,general,PT-1503-1246672pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block 8,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T10:07:25.279Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246672,AC-2022-05-31-1379818,Residential,ACTIVE,1653991645279\nproperty-services,general,PT-1203-626547pb.khanna,,MIGRATION,MUNICIPAL_RECORDS,W20,WARD 20,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T10:07:18.761Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.khanna,PT-1203-626547,MT-1203-033622,Residential,ACTIVE,1653991638761\nproperty-services,general,PT-601-1146740pb.abohar,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block 8,9ca18bd7-fcc3-41ff-a23d-5f9f3c366e87,2,2022-05-31T10:06:18.171Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.abohar,PT-601-1146740,AC-2022-05-31-1379809,Residential,ACTIVE,1653991578171\nproperty-services,general,PT-503-630060pb.khamano,,MIGRATION,MUNICIPAL_RECORDS,B11,Block11,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:06:13.294Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.khamano,PT-503-630060,MT-503-033621,Commercial,ACTIVE,1653991573294\nproperty-services,general,PT-601-753741pb.abohar,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block 8,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T10:06:06.611Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.abohar,PT-601-753741,AC-2022-05-31-1379812,Residential,ACTIVE,1653991566611\nproperty-services,general,PT-1508-1110802pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,2,2022-05-31T10:06:00.403Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.mohali,PT-1508-1110802,AC-2022-05-31-1379816,Institutional,ACTIVE,1653991560403\nproperty-services,general,PT-201-032191pb.barnala,,CFC_COUNTER,MUNICIPAL_RECORDS,B-11A,Block 11A,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,3,2022-05-31T10:05:45.368Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.barnala,PT-201-032191,AC-2022-05-31-1379815,Mixed,ACTIVE,1653991545368\nproperty-services,general,PT-1503-1246670pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block 8,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T10:05:39.536Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246670,AC-2022-05-31-1379811,Residential,ACTIVE,1653991539536\nproperty-services,general,PT-2105-1225275pb.dhuri,,CFC_COUNTER,MUNICIPAL_RECORDS,B3,Block 3,996bd707-aba2-438d-a764-1a9c4947bdc4,1,2022-05-31T10:05:07.284Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.dhuri,PT-2105-1225275,MT-2105-033620,Residential,ACTIVE,1653991507284\nproperty-services,general,PT-1003-1246654pb.bhogpur,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,415e187c-824a-4d53-aca3-a5df8f8cb4ef,1,2022-05-31T10:04:39.151Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.bhogpur,PT-1003-1246654,AC-2022-05-31-1379810,Residential,ACTIVE,1653991479151\nproperty-services,general,PT-1508-1130424pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,2,2022-05-31T10:04:22.122Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.SHAREDPROPERTY,pb.mohali,PT-1508-1130424,AC-2022-05-31-1379807,Commercial,ACTIVE,1653991462122\nproperty-services,general,PT-1906-1016016pb.rajpura,,CFC_COUNTER,MUNICIPAL_RECORDS,B18,Block 18,3899b041-8454-42e9-afa7-ca7800e7ad52,1,2022-05-31T10:04:16.706Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.rajpura,PT-1906-1016016,AC-2022-05-31-1379808,Commercial,ACTIVE,1653991456706\nproperty-services,general,PT-1503-1246669pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block 8,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T10:03:29.585Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246669,AC-2022-05-31-1379806,Residential,ACTIVE,1653991409585\nproperty-services,general,PT-403-1246668pb.kotkapura,,CFC_COUNTER,MUNICIPAL_RECORDS,B2,Block 2,af79bd1f-d681-4db8-a02a-b1014e9c14a5,1,2022-05-31T10:02:51.324Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.kotkapura,PT-403-1246668,AC-2022-05-31-1379803,Residential,ACTIVE,1653991371324\nproperty-services,general,PT-1910-1240574pb.patiala,,CFC_COUNTER,MUNICIPAL_RECORDS,B7,BLOCK - 07,bd161016-54d1-4c8a-944f-5b04026c842a,1,2022-05-31T10:02:32.770Z,INDIVIDUAL.MULTIPLEOWNERS,VACANT,pb.patiala,PT-1910-1240574,AC-2022-05-31-1379802,Residential,ACTIVE,1653991352770\nproperty-services,general,PT-107-1139738pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-62,WARD-62,71c01356-d9cd-459d-a8e4-eb09ccb001ee,1,2022-05-31T10:02:22.988Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-1139738,AC-2022-05-31-1379794,Commercial,ACTIVE,1653991342988\nproperty-services,general,PT-1203-1007937pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W24,WARD 24,35b6de9d-2038-4e2e-9901-7d3c9478f49c,2,2022-05-31T10:02:22.697Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.khanna,PT-1203-1007937,AC-2022-05-31-1379801,Residential,ACTIVE,1653991342697\nproperty-services,general,PT-107-1246662pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-48,WARD-48,ed9ef9b4-1039-42af-8c39-adc686d1416a,2,2022-05-31T10:02:13.590Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-1246662,AC-2022-05-31-1379793,Residential,ACTIVE,1653991333590\nproperty-services,general,PT-308-1246665pb.kotfatta,,CFC_COUNTER,MUNICIPAL_RECORDS,B11B,Block11B,64981dce-1c13-4388-af19-89fbc1b01643,1,2022-05-31T10:01:48.841Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.kotfatta,PT-308-1246665,AC-2022-05-31-1379798,Residential,ACTIVE,1653991308841\nproperty-services,general,PT-107-954409pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-25,WARD-25,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,3,2022-05-31T10:01:42.050Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-954409,AC-2022-05-31-1379779,Mixed,ACTIVE,1653991302050\nproperty-services,general,PT-1503-1246667pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block 8,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T10:01:38.436Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246667,AC-2022-05-31-1379800,Residential,ACTIVE,1653991298436\nproperty-services,general,PT-107-926881pb.amritsar,,MIGRATION,MUNICIPAL_RECORDS,W-17,WARD-17,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T10:01:30.741Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-926881,MT-107-033611,Residential,ACTIVE,1653991290741\nproperty-services,general,PT-303-1246666pb.bhairoopa,,CFC_COUNTER,MUNICIPAL_RECORDS,B3,Block 3,0661086e-fd8c-4afc-81c3-2bf3f3fb78ef,1,2022-05-31T10:01:16.617Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.bhairoopa,PT-303-1246666,AC-2022-05-31-1379799,Commercial,ACTIVE,1653991276617\nproperty-services,general,PT-601-1246664pb.abohar,,CFC_COUNTER,MUNICIPAL_RECORDS,B36,Block 36,9ca18bd7-fcc3-41ff-a23d-5f9f3c366e87,1,2022-05-31T09:59:31.553Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.abohar,PT-601-1246664,AC-2022-05-31-1379797,Residential,ACTIVE,1653991171553\nproperty-services,general,PT-1503-1246663pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B7,Block 7,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,3,2022-05-31T09:57:22.416Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-1246663,AC-2022-05-31-1379795,Residential,ACTIVE,1653991042416\nproperty-services,general,PT-503-630072pb.khamano,,CFC_COUNTER,MUNICIPAL_RECORDS,B11,Block11,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:57:18.358Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.khamano,PT-503-630072,MT-503-033615,Residential,ACTIVE,1653991038358\nproperty-services,general,PT-2005-029651pb.nangal,,MIGRATION,MUNICIPAL_RECORDS,ZB9,CORRECT LOCALICTY BLOCK,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:56:32.953Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.nangal,PT-2005-029651,PB-AC-2019-02-13-029654,Residential,ACTIVE,1653990992953\nproperty-services,general,PT-1910-1171931pb.patiala,,LEGACY_MIGRATION,LEGACY_RECORD,B9,BLOCK - 09,9c76e0df-8e28-4810-8d79-e983d0470a8c,1,2022-05-31T09:55:43.768Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.patiala,PT-1910-1171931,MT-1910-033614,Residential,ACTIVE,1653990943768\nproperty-services,general,PT-1503-1246661pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block 8,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T09:55:15.924Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246661,AC-2022-05-31-1379790,Residential,ACTIVE,1653990915924\nproperty-services,general,PT-1910-1240487pb.patiala,,CFC_COUNTER,MUNICIPAL_RECORDS,B20,BLOCK - 20,f974d04b-a78d-4feb-8463-038d3a583f81,3,2022-05-31T09:54:00.249Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.patiala,PT-1910-1240487,AC-2022-05-31-1379789,Mixed,ACTIVE,1653990840249\nproperty-services,general,PT-1201-1246655pb.doraha,,CFC_COUNTER,MUNICIPAL_RECORDS,B3,Block 3,0f0646c6-5c03-4fb4-8992-f882051bb16f,2,2022-05-31T09:53:26.701Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.doraha,PT-1201-1246655,AC-2022-05-31-1379786,Mixed,ACTIVE,1653990806701\nproperty-services,general,PT-1203-118901pb.khanna,,MIGRATION,MUNICIPAL_RECORDS,W32,WARD 32,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:53:14.518Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.khanna,PT-1203-118901,AC-2022-05-31-1379787,Residential,ACTIVE,1653990794518\nproperty-services,general,PT-1503-1246660pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block 8,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T09:52:14.781Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246660,AC-2022-05-31-1379777,Residential,ACTIVE,1653990734781\nproperty-services,general,PT-1906-1246645pb.rajpura,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block 8,b1915b70-c38f-47cb-a250-2c45c07e0e99,2,2022-05-31T09:51:38.361Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.rajpura,PT-1906-1246645,MT-1906-033613,Residential,ACTIVE,1653990698361\nproperty-services,general,PT-1203-615646pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W30,WARD 30,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:51:30.356Z,INDIVIDUAL.MULTIPLEOWNERS,VACANT,pb.khanna,PT-1203-615646,AC-2022-05-31-1379780,Residential,ACTIVE,1653990690356\nproperty-services,general,PT-107-958802pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-4,WARD-4,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:51:28.420Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-958802,MT-107-033607,Residential,ACTIVE,1653990688420\nproperty-services,general,PT-1202-069036pb.jagraon,,MIGRATION,MUNICIPAL_RECORDS,B18,Block 18,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:50:32.461Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jagraon,PT-1202-069036,MT-1202-033612,Commercial,ACTIVE,1653990632461\nproperty-services,general,PT-107-1023483pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-63,WARD-63,8f778bdc-6125-487d-9c12-ef325f463c10,2,2022-05-31T09:50:17.394Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-1023483,AC-2022-05-31-1379756,Residential,ACTIVE,1653990617394\nproperty-services,general,PT-1203-1208240pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W11,WARD 11,35b6de9d-2038-4e2e-9901-7d3c9478f49c,1,2022-05-31T09:48:47.815Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.khanna,PT-1203-1208240,AC-2022-05-31-1379775,Commercial,ACTIVE,1653990527815\nproperty-services,general,PT-2109-097334pb.longowal,,CFC_COUNTER,MUNICIPAL_RECORDS,B4,Block 4,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:47:37.484Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.longowal,PT-2109-097334,MT-2109-033610,Commercial,ACTIVE,1653990457484\nproperty-services,general,PT-201-1205620pb.barnala,,CFC_COUNTER,MUNICIPAL_RECORDS,B-12,Block 12,e273b525-ba81-40e5-8079-aa1a45a827e0,2,2022-05-31T09:44:58.767Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.barnala,PT-201-1205620,MT-201-033609,Residential,ACTIVE,1653990298767\nproperty-services,general,PT-1104-1246659pb.kapurthala,,CFC_COUNTER,MUNICIPAL_RECORDS,B22,Block 22,aa659f4a-f797-4c7e-b7db-ab7be4956c0c,1,2022-05-31T09:44:42.870Z,INDIVIDUAL.SINGLEOWNER,VACANT,pb.kapurthala,PT-1104-1246659,AC-2022-05-31-1379771,Residential,INWORKFLOW,1653990282870\nproperty-services,general,PT-2105-096500pb.dhuri,,CFC_COUNTER,MUNICIPAL_RECORDS,B4,Block 4,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:44:26.297Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.dhuri,PT-2105-096500,AC-2022-05-31-1379715,Residential,ACTIVE,1653990266297\nproperty-services,general,PT-1013-589244pb.jalandhar,,CFC_COUNTER,MUNICIPAL_RECORDS,B5,Block 5,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:43:03.325Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jalandhar,PT-1013-589244,AC-2022-05-31-1379764,Commercial,ACTIVE,1653990183325\nproperty-services,general,PT-1011-639793pb.phillaur,,MIGRATION,MUNICIPAL_RECORDS,B6,Block 6,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:43:00.322Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.phillaur,PT-1011-639793,MT-1011-033608,Residential,ACTIVE,1653990180322\nproperty-services,general,PT-1014-887484pb.phagwara,,MIGRATION,MUNICIPAL_RECORDS,WARD6,WARD_6,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:41:56.574Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.phagwara,PT-1014-887484,AC-2022-05-27-1377493,Commercial,INACTIVE,1653990116574\nproperty-services,general,PT-1503-1240990pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B2,Block 2,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T09:41:49.256Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-1240990,AC-2022-05-31-1379760,Residential,ACTIVE,1653990109256\nproperty-services,general,PT-1503-1246653pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B19,Block 19,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T09:38:54.834Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246653,AC-2022-05-31-1379757,Residential,ACTIVE,1653989934834\nproperty-services,general,PT-201-1246652pb.barnala,,CFC_COUNTER,MUNICIPAL_RECORDS,B-10,Block 10,e273b525-ba81-40e5-8079-aa1a45a827e0,2,2022-05-31T09:37:57.180Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.barnala,PT-201-1246652,AC-2022-05-31-1379755,Mixed,ACTIVE,1653989877180\nproperty-services,general,PT-201-1246050pb.barnala,,CFC_COUNTER,MUNICIPAL_RECORDS,B-11A,Block 11A,e273b525-ba81-40e5-8079-aa1a45a827e0,1,2022-05-31T09:37:41.606Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.barnala,PT-201-1246050,AC-2022-05-30-1377771,Commercial,ACTIVE,1653989861606\nproperty-services,general,PT-1910-1240097pb.patiala,,CFC_COUNTER,MUNICIPAL_RECORDS,B19,BLOCK - 19,fc15af44-de3d-4a21-bda4-d1c8e62a0548,2,2022-05-31T09:36:35.496Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.patiala,PT-1910-1240097,AC-2022-05-31-1379751,Residential,ACTIVE,1653989795496\nproperty-services,general,PT-1503-1148540pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B2,Block 2,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T09:35:53.002Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1148540,MT-1503-033605,Residential,ACTIVE,1653989753002\nproperty-services,general,PT-107-957686pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-45,WARD-45,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:35:52.609Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-957686,AC-2022-05-31-1379746,Residential,ACTIVE,1653989752609\nproperty-services,general,PT-1508-1111807pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,1,2022-05-31T09:35:33.969Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.mohali,PT-1508-1111807,AC-2022-05-31-1379750,Commercial,ACTIVE,1653989733969\nproperty-services,general,PT-1503-1246649pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B21,Block 21,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T09:35:17.100Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-1246649,AC-2022-05-31-1379749,Residential,ACTIVE,1653989717100\nproperty-services,general,PT-107-993965pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-6,WARD-6,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:34:11.665Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-993965,AC-2022-05-31-1379748,Residential,ACTIVE,1653989651665\nproperty-services,general,PT-1014-1246648pb.phagwara,,CFC_COUNTER,MUNICIPAL_RECORDS,WARD17,WARD_17,84bcc12e-de5a-4ae7-a177-bd80d4011127,2,2022-05-31T09:33:58.535Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.phagwara,PT-1014-1246648,AC-2022-05-31-1379747,Industrial,ACTIVE,1653989638535\nproperty-services,general,PT-910-1246521pb.hoshiarpur,,CFC_COUNTER,MUNICIPAL_RECORDS,B-2,Block 2,5c556fc4-2694-427a-a229-69fb79617bfc,1,2022-05-31T09:33:25.452Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.hoshiarpur,PT-910-1246521,AC-2022-05-31-1379745,Residential,ACTIVE,1653989605452\nproperty-services,general,PT-0704-1246647pb.mallanwala,,CFC_COUNTER,MUNICIPAL_RECORDS,B12,Block 12,ef85fac8-52f0-4d05-8be9-4c7fad933265,1,2022-05-31T09:30:57.986Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.mallanwala,PT-0704-1246647,AC-2022-05-31-1379741,Residential,ACTIVE,1653989457986\nproperty-services,general,PT-107-941715pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-49,WARD-49,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:29:52.343Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-941715,AC-2022-05-31-1379661,Industrial,ACTIVE,1653989392343\nproperty-services,general,PT-107-941604pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-49,WARD-49,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:29:43.131Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-941604,AC-2022-05-31-1379659,Mixed,ACTIVE,1653989383131\nproperty-services,general,PT-107-956763pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:29:33.523Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-956763,AC-2022-05-31-1379656,Residential,ACTIVE,1653989373523\nproperty-services,general,PT-107-922340pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:29:22.738Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-922340,AC-2022-05-31-1379662,Residential,ACTIVE,1653989362738\nproperty-services,general,PT-107-972239pb.amritsar,,MIGRATION,MUNICIPAL_RECORDS,NA1,OMCL,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,3,2022-05-31T09:29:09.400Z,INSTITUTIONALPRIVATE,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-972239,MT-107-033588,NonResidential,ACTIVE,1653989349400\nproperty-services,general,PT-2101-1246643pb.ahmedgarh,,CFC_COUNTER,MUNICIPAL_RECORDS,B3,Block 3,06e05885-e703-40ab-a089-8410310e80aa,3,2022-05-31T09:28:59.793Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.ahmedgarh,PT-2101-1246643,AC-2022-05-31-1379737,Commercial,ACTIVE,1653989339793\nproperty-services,general,PT-107-945285pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:28:56.960Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.amritsar,PT-107-945285,AC-2022-05-31-1379677,Residential,ACTIVE,1653989336960\nproperty-services,general,PT-1906-1246642pb.rajpura,,CFC_COUNTER,MUNICIPAL_RECORDS,B9,Block 9,cd7451b8-d5ca-4ed7-a7a3-f674ddf8ad29,1,2022-05-31T09:28:48.569Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.rajpura,PT-1906-1246642,AC-2022-05-31-1379736,Residential,ACTIVE,1653989328569\nproperty-services,general,PT-107-966756pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-30,WARD-30,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,3,2022-05-31T09:28:45.250Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-966756,AC-2022-05-31-1379679,NonResidential,ACTIVE,1653989325250\nproperty-services,general,PT-107-961473pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-9,WARD-9,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:28:35.185Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-961473,AC-2022-05-31-1379681,Residential,ACTIVE,1653989315185\nproperty-services,general,PT-1401-054829pb.badhnikalan,,MIGRATION,MUNICIPAL_RECORDS,B13,Ward 13,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:28:29.034Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.badhnikalan,PT-1401-054829,MT-1401-033604,Residential,ACTIVE,1653989309034\nproperty-services,general,PT-910-707698pb.hoshiarpur,,MIGRATION,MUNICIPAL_RECORDS,B-2,Block 2,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:28:21.802Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.hoshiarpur,PT-910-707698,MT-910-033603,Commercial,ACTIVE,1653989301802\nproperty-services,general,PT-107-971547pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:28:09.003Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-971547,AC-2022-05-31-1379684,Residential,ACTIVE,1653989289003\nproperty-services,general,PT-107-937053pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-63,WARD-63,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,4,2022-05-31T09:27:58.309Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-937053,AC-2022-05-31-1379704,NonResidential,ACTIVE,1653989278309\nproperty-services,general,PT-107-974379pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-62,WARD-62,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:27:46.685Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-974379,AC-2022-05-31-1379709,Mixed,ACTIVE,1653989266685\nproperty-services,general,PT-1013-519530pb.jalandhar,,CFC_COUNTER,MUNICIPAL_RECORDS,B5,Block 5,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:27:32.984Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jalandhar,PT-1013-519530,AC-2022-05-31-1379731,Commercial,ACTIVE,1653989252984\nproperty-services,general,PT-107-976570pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-45,WARD-45,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:27:29.969Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-976570,AC-2022-05-31-1379733,Residential,ACTIVE,1653989249969\nproperty-services,general,PT-1405-1246640pb.kotissekhan,,CFC_COUNTER,MUNICIPAL_RECORDS,BL-4,Block 4,7ce45d35-7e31-46e8-8135-e10ab0ea60da,2,2022-05-31T09:27:29.338Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.kotissekhan,PT-1405-1246640,AC-2022-05-31-1379734,Residential,ACTIVE,1653989249338\nproperty-services,general,PT-107-945444pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-10,WARD-10,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,3,2022-05-31T09:27:18.961Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-945444,AC-2022-05-31-1379716,Mixed,ACTIVE,1653989238961\nproperty-services,general,PT-107-937676pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,3,2022-05-31T09:27:09.743Z,INSTITUTIONALPRIVATE.OTHERSPRIVATEINSTITUITION,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-937676,AC-2022-05-31-1379717,NonResidential,ACTIVE,1653989229743\nproperty-services,general,PT-1014-1246639pb.phagwara,,CFC_COUNTER,MUNICIPAL_RECORDS,WARD3,WARD_3,84bcc12e-de5a-4ae7-a177-bd80d4011127,2,2022-05-31T09:26:23.895Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.phagwara,PT-1014-1246639,AC-2022-05-31-1379729,Residential,ACTIVE,1653989183895\nproperty-services,general,PT-107-956753pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:26:19.065Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-956753,AC-2022-05-31-1379654,Residential,ACTIVE,1653989179065\nproperty-services,general,PT-107-916371pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-28,WARD-28,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:26:12.707Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-916371,AC-2022-05-31-1379725,Residential,ACTIVE,1653989172707\nproperty-services,general,PT-107-977097pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,3,2022-05-31T09:25:43.790Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-977097,AC-2022-05-31-1379651,Residential,ACTIVE,1653989143790\nproperty-services,general,PT-1802-1245890pb.pathankot,,CFC_COUNTER,MUNICIPAL_RECORDS,B15,Sector 15,3230f599-68c5-4bf9-8657-ca3a8e041837,4,2022-05-31T09:25:37.019Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.pathankot,PT-1802-1245890,AC-2022-05-31-1379726,Commercial,ACTIVE,1653989137019\nproperty-services,general,PT-1703-704985pb.nawanshahr,,MIGRATION,MUNICIPAL_RECORDS,B1,Block 1,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:25:33.958Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.nawanshahr,PT-1703-704985,AC-2022-05-31-1379728,Residential,INWORKFLOW,1653989133958\nproperty-services,general,PT-107-921971pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,3,2022-05-31T09:25:33.193Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-921971,AC-2022-05-31-1379652,Residential,ACTIVE,1653989133193\nproperty-services,general,PT-107-974308pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:25:10.581Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-974308,AC-2022-05-31-1379648,Residential,ACTIVE,1653989110581\nproperty-services,general,PT-1203-1246637pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W22,WARD 22,35b6de9d-2038-4e2e-9901-7d3c9478f49c,1,2022-05-31T09:24:46.232Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.khanna,PT-1203-1246637,AC-2022-05-31-1379724,Residential,ACTIVE,1653989086232\nproperty-services,general,PT-107-974325pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:24:44.688Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-974325,AC-2022-05-31-1379647,Residential,ACTIVE,1653989084688\nproperty-services,general,PT-107-945306pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:24:27.192Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-945306,AC-2022-05-31-1379644,Residential,ACTIVE,1653989067192\nproperty-services,general,PT-107-921474pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:24:18.705Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-921474,AC-2022-05-31-1379642,Residential,ACTIVE,1653989058705\nproperty-services,general,PT-107-945296pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:23:30.220Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.amritsar,PT-107-945296,AC-2022-05-31-1379641,Residential,ACTIVE,1653989010220\nproperty-services,general,PT-1401-054813pb.badhnikalan,,MIGRATION,MUNICIPAL_RECORDS,B13,Ward 13,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:23:19.674Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.badhnikalan,PT-1401-054813,AC-2022-05-31-1379722,Residential,INWORKFLOW,1653988999674\nproperty-services,general,PT-107-964148pb.amritsar,,CFC_COUNTER,MUNICIPAL_RECORDS,W-31,WARD-31,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:23:15.491Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.amritsar,PT-107-964148,AC-2022-05-31-1379638,Residential,ACTIVE,1653988995491\nproperty-services,general,PT-1503-1246636pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B2,Block 2,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,1,2022-05-31T09:22:12.881Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-1246636,AC-2022-05-31-1379721,Commercial,ACTIVE,1653988932881\nproperty-services,general,PT-1906-1246635pb.rajpura,,CFC_COUNTER,MUNICIPAL_RECORDS,B5,Block 5,b1915b70-c38f-47cb-a250-2c45c07e0e99,1,2022-05-31T09:22:06.052Z,INDIVIDUAL.SINGLEOWNER,VACANT,pb.rajpura,PT-1906-1246635,AC-2022-05-31-1379719,Residential,ACTIVE,1653988926052\nproperty-services,general,PT-1401-054785pb.badhnikalan,,MIGRATION,MUNICIPAL_RECORDS,B8,Ward 8,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:19:59.272Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.badhnikalan,PT-1401-054785,MT-1401-033600,Commercial,ACTIVE,1653988799272\nproperty-services,general,PT-1904-1246634pb.nabha,,CFC_COUNTER,MUNICIPAL_RECORDS,B8,Block8,029bbba2-8b8d-4699-bdff-a746f51d5461,2,2022-05-31T09:16:53.555Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.nabha,PT-1904-1246634,AC-2022-05-31-1379714,Residential,ACTIVE,1653988613555\nproperty-services,general,PT-1203-1214928pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W22,WARD 22,35b6de9d-2038-4e2e-9901-7d3c9478f49c,1,2022-05-31T09:16:51.599Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.khanna,PT-1203-1214928,AC-2021-12-23-1307925,Commercial,INACTIVE,1653988611599\nproperty-services,general,PT-505-1246633pb.sirhind,,CFC_COUNTER,MUNICIPAL_RECORDS,B5,Block 5,7bdcc69f-9360-4598-89f3-b10a2382e49b,1,2022-05-31T09:16:17.688Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.sirhind,PT-505-1246633,AC-2022-05-31-1379713,Commercial,ACTIVE,1653988577688\nproperty-services,general,PT-1503-1246631pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B17,Block 17,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,1,2022-05-31T09:14:55.199Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-1246631,AC-2022-05-31-1379711,Commercial,INWORKFLOW,1653988495199\nproperty-services,general,PT-701-1144137pb.ferozepur,,CFC_COUNTER,MUNICIPAL_RECORDS,W22,Ward 22,69085ca3-57df-47ae-8208-ffd383481fe5,1,2022-05-31T09:14:26.173Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.ferozepur,PT-701-1144137,AC-2021-08-10-1197345,Commercial,INACTIVE,1653988466173\nproperty-services,general,PT-1503-1246630pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B11,Block 11,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T09:11:48.953Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246630,AC-2022-05-31-1379708,Residential,ACTIVE,1653988308953\nproperty-services,general,PT-1203-884909pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W33,WARD 33,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:10:14.132Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.khanna,PT-1203-884909,MT-1203-033599,Commercial,ACTIVE,1653988214132\nproperty-services,general,PT-1503-1246629pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B2,Block 2,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,1,2022-05-31T09:08:27.662Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-1246629,AC-2022-05-31-1379706,Residential,ACTIVE,1653988107662\nproperty-services,general,PT-1203-884864pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W33,WARD 33,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:07:42.128Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.khanna,PT-1203-884864,MT-1203-033597,Commercial,INWORKFLOW,1653988062128\nproperty-services,general,PT-2110-1246628pb.malerkotla,,CFC_COUNTER,MUNICIPAL_RECORDS,B36,Block36,28d40092-2545-492f-bb8f-97e2ad17a4fb,1,2022-05-31T09:07:22.200Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.malerkotla,PT-2110-1246628,AC-2022-05-31-1379705,Commercial,ACTIVE,1653988042200\nproperty-services,general,PT-204-1246627pb.handiaya,,CFC_COUNTER,MUNICIPAL_RECORDS,B6,Block 6,eefdacf4-a8ce-4dfd-a77a-fcda99f48942,1,2022-05-31T09:06:04.453Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.handiaya,PT-204-1246627,AC-2022-05-31-1379703,Residential,ACTIVE,1653987964453\nproperty-services,general,PT-1203-884857pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W33,WARD 33,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:05:16.510Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.khanna,PT-1203-884857,AC-2021-06-14-1173969,Commercial,INACTIVE,1653987916510\nproperty-services,general,PT-1503-1246626pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T09:04:46.261Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246626,AC-2022-05-31-1379700,Residential,ACTIVE,1653987886261\nproperty-services,general,PT-1508-1108118pb.mohali,,LEGACY_MIGRATION,LEGACY_RECORD,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,2,2022-05-31T09:04:41.073Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.mohali,PT-1508-1108118,AC-2022-05-31-1379701,Residential,ACTIVE,1653987881073\nproperty-services,general,PT-1503-1246625pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B2,Block 2,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T09:03:05.596Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.kharar,PT-1503-1246625,AC-2022-05-31-1379699,Residential,ACTIVE,1653987785596\nproperty-services,general,PT-1202-118585pb.jagraon,,MIGRATION,MUNICIPAL_RECORDS,B3,Block 3,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T09:02:12.617Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jagraon,PT-1202-118585,PB-AC-2019-07-10-118588,Residential,INACTIVE,1653987732617\nproperty-services,general,PT-1203-884845pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W33,WARD 33,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T09:01:57.250Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.khanna,PT-1203-884845,AC-2021-06-14-1173968,Commercial,INACTIVE,1653987717250\nproperty-services,general,PT-1910-1178154pb.patiala,,CFC_COUNTER,MUNICIPAL_RECORDS,B13,BLOCK - 13,9c76e0df-8e28-4810-8d79-e983d0470a8c,1,2022-05-31T08:59:57.746Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.patiala,PT-1910-1178154,AC-2022-05-31-1379696,Commercial,ACTIVE,1653987597746\nproperty-services,general,PT-1203-884792pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W33,WARD 33,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T08:59:42.882Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.khanna,PT-1203-884792,AC-2021-06-14-1173960,Commercial,INACTIVE,1653987582882\nproperty-services,general,PT-1013-568534pb.jalandhar,,MIGRATION,MUNICIPAL_RECORDS,B5,Block 5,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T08:59:37.983Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jalandhar,PT-1013-568534,PB-AC-2019-07-28-568544,NonResidential,INACTIVE,1653987577983\nproperty-services,general,PT-1203-884840pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W33,WARD 33,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T08:58:57.675Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.SHAREDPROPERTY,pb.khanna,PT-1203-884840,AC-2021-06-14-1173965,Commercial,INACTIVE,1653987537675\nproperty-services,general,PT-1503-1246623pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B9,Block 9,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T08:58:10.436Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-1246623,AC-2022-05-31-1379694,Residential,INWORKFLOW,1653987490436\nproperty-services,general,PT-1203-884916pb.khanna,,CFC_COUNTER,MUNICIPAL_RECORDS,W24,WARD 24,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T08:56:35.926Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.khanna,PT-1203-884916,MT-1203-033591,Residential,ACTIVE,1653987395926\nproperty-services,general,PT-1910-1175744pb.patiala,,LEGACY_MIGRATION,LEGACY_RECORD,B13,BLOCK - 13,9c76e0df-8e28-4810-8d79-e983d0470a8c,1,2022-05-31T08:55:29.030Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.patiala,PT-1910-1175744,AC-2021-10-17-1256227,Residential,INACTIVE,1653987329030\nproperty-services,general,PT-2104-627793pb.cheema,,CFC_COUNTER,MUNICIPAL_RECORDS,B5,Block 5,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T08:54:58.430Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.cheema,PT-2104-627793,AC-2022-05-31-1379692,Commercial,ACTIVE,1653987298430\nproperty-services,general,PT-2104-095907pb.cheema,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T08:53:20.191Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.cheema,PT-2104-095907,AC-2022-05-31-1379691,Mixed,ACTIVE,1653987200191\nproperty-services,general,PT-2111-053526pb.moonak,,CFC_COUNTER,MUNICIPAL_RECORDS,W11,WARD NO 11,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,1,2022-05-31T08:53:05.920Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.moonak,PT-2111-053526,AC-2022-05-31-1379690,Commercial,ACTIVE,1653987185920\nproperty-services,general,PT-601-1246622pb.abohar,,CFC_COUNTER,MUNICIPAL_RECORDS,B25,Block 25,9ca18bd7-fcc3-41ff-a23d-5f9f3c366e87,2,2022-05-31T08:52:27.280Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.abohar,PT-601-1246622,AC-2022-05-31-1379688,Mixed,ACTIVE,1653987147280\nproperty-services,general,PT-1508-1150709pb.mohali,,CFC_COUNTER,MUNICIPAL_RECORDS,B1,Block 1,1d5ab5a4-b152-4316-8686-875cfb7fc9a6,4,2022-05-31T08:51:54.266Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.mohali,PT-1508-1150709,AC-2022-05-31-1379687,Commercial,ACTIVE,1653987114266\nproperty-services,general,PT-1503-1244590pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B2,Block 2,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,3,2022-05-31T08:50:50.923Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-1244590,AC-2022-05-31-1379686,Residential,ACTIVE,1653987050923\nproperty-services,general,PT-502-874473pb.bassipathana,,MIGRATION,MUNICIPAL_RECORDS,B5,Block 5,4d9b0e12-6ad2-4d9c-950d-2624c3fc6e65,2,2022-05-31T08:47:35.380Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.bassipathana,PT-502-874473,AC-2022-05-31-1379685,Residential,ACTIVE,1653986855380\nproperty-services,general,PT-1503-1246621pb.kharar,,CFC_COUNTER,MUNICIPAL_RECORDS,B2,Block 2,f2219a32-fc2e-4a23-bf2d-0ac8e850c2ea,2,2022-05-31T08:47:31.689Z,INDIVIDUAL.MULTIPLEOWNERS,BUILTUP.INDEPENDENTPROPERTY,pb.kharar,PT-1503-1246621,AC-2022-05-31-1379683,Residential,ACTIVE,1653986851689\nproperty-services,general,PT-1013-1041347pb.jalandhar,,CFC_COUNTER,MUNICIPAL_RECORDS,B7,Block 7,fd361f3f-f396-4639-a33d-4927aec6fd08,4,2022-05-31T08:46:20.640Z,INDIVIDUAL.SINGLEOWNER,BUILTUP.INDEPENDENTPROPERTY,pb.jalandhar,PT-1013-1041347,AC-2022-05-31-1379672,Commercial,INACTIVE,1653986780640"
        }},
        "inputFormat": {{
            "type": "csv",
            "findColumnsFromHeader": true
        }},
        "appendToExisting": true
        }},
        "tuningConfig": {{
        "type": "index_parallel",
        "partitionsSpec": {{
            "type": "dynamic"
        }}
        }},
        "dataSchema": {{
        "dataSource": "property_service",
        "timestampSpec": {{
            "column": "_source.Data.@timestamp",
            "format": "iso"
        }},
        "dimensionsSpec": {{
            "dimensions": [
            "_id",
            "_index",
            "_score",
            "_source.Data.accountId",
            "_source.Data.acknowldgementNumber",
            "_source.Data.channel",
            {{
                "type": "long",
                "name": "_source.Data.noOfFloors"
            }},
            "_source.Data.ownershipCategory",
            "_source.Data.propertyId",
            "_source.Data.propertyType",
            "_source.Data.source",
            "_source.Data.status",
            "_source.Data.tenantId",
            "_source.Data.usageCategory",
            "_source.Data.ward.code",
            "_source.Data.ward.name",
            "_type",
            {{
                "type": "long",
                "name": "sort.0"
            }}
            ]
        }},
        "granularitySpec": {{
            "queryGranularity": "none",
            "rollup": false,
            "segmentGranularity": "day"
        }}
        }}
    }}
    }}
    """
    q=payload.format(data)
    header = {
    'Content-Type': 'application/json'
    }
    response = requests.request("POST", druid_url, headers=header, data=q)
    logging.info(response.text)

def upload_trade_license():
    data = ""
    f= open("trade_license.csv","r")
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    for row in spamreader:
        data+=', '.join(row)
        data+='\\n'
    f.close()

    payload =  """{{
    "type": "index_parallel",
    "spec": {{
        "ioConfig": {{
        "type": "index_parallel",
        "inputSource": {{
            "type": "inline",
            "data": "{0}"
        }},
        "inputFormat": {{
            "type": "csv",
            "findColumnsFromHeader": true
        }}
        }},
        "tuningConfig": {{
        "type": "index_parallel",
        "partitionsSpec": {{
            "type": "dynamic"
        }}
        }},
        "dataSchema": {{
        "dataSource": "test2",
        "timestampSpec": {{
            "column": "!!!_no_such_column_!!!",
            "missingValue": "2010-01-01T00:00:00Z"
        }},
        "dimensionsSpec": {{
            "dimensions": [
            "additionaldetails",
            "billexpirytime",
            "businessservice",
            "consumercode",
            "consumertype",
            "createdby",
            {{
                "type": "long",
                "name": "createdtime"
            }},
            "fixedbillexpirydate",
            "id",
            "ispaymentcompleted",
            "lastmodifiedby",
            {{
                "type": "long",
                "name": "lastmodifiedtime"
            }},
            {{
                "type": "long",
                "name": "minimumamountpayable"
            }},
            "payer",
            "status",
            {{
                "type": "long",
                "name": "taxperiodfrom"
            }},
            {{
                "type": "long",
                "name": "taxperiodto"
            }},
            "tenantid"
            ]
        }},
        "granularitySpec": {{
            "queryGranularity": "none",
            "rollup": false,
            "segmentGranularity": "day"
        }}
        }}
    }}
    }}"""
    q=payload.format(data)
    header = {
    'Content-Type': 'application/json'
    }
    response = requests.request("POST", druid_url, headers=header, data=q)
    logging.info(response.text)

def upload_water_service():
    data = ""
    f= open("water_service.csv","r")
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    for row in spamreader:
        data+=', '.join(row)
        data+='\\n'
    f.close()

    payload =  """
    {{
    "type": "index_parallel",
    "spec": {{
        "ioConfig": {{
        "type": "index_parallel",
        "inputSource": {{
            "type": "inline",
            "data": "{0}"
        }},
        "inputFormat": {{
            "type": "csv",
            "findColumnsFromHeader": true
        }},
        "appendToExisting": true
        }},
        "tuningConfig": {{
        "type": "index_parallel",
        "partitionsSpec": {{
            "type": "dynamic"
        }}
        }},
        "dataSchema": {{
        "dataSource": "water_service",
        "timestampSpec": {{
            "column": "_source.Data.@timestamp",
            "format": "iso"
        }},
        "dimensionsSpec": {{
            "dimensions": [
            "_id",
            "_index",
            "_score",
            "_source.Data.applicationNo",
            "_source.Data.applicationStatus",
            "_source.Data.applicationType",
            "_source.Data.channel",
            "_source.Data.connectionCategory",
            "_source.Data.connectionHolders",
            {{
                "type": "long",
                "name": "_source.Data.connectionNo"
            }},
            "_source.Data.connectionType",
            {{
                "type": "long",
                "name": "_source.Data.dateEffectiveFrom"
            }},
            "_source.Data.id",
            {{
                "type": "long",
                "name": "_source.Data.noOfTaps"
            }},
            "_source.Data.oldConnectionNo",
            {{
                "type": "double",
                "name": "_source.Data.pipeSize"
            }},
            "_source.Data.plumberInfo",
            "_source.Data.propertyId",
            "_source.Data.propertyUsageType",
            {{
                "type": "double",
                "name": "_source.Data.proposedPipeSize"
            }},
            {{
                "type": "long",
                "name": "_source.Data.proposedTaps"
            }},
            "_source.Data.rainWaterHarvesting",
            {{
                "type": "long",
                "name": "_source.Data.roadCuttingArea"
            }},
            "_source.Data.roadCuttingInfo",
            "_source.Data.roadCuttingInfo.0.auditDetails",
            "_source.Data.roadCuttingInfo.0.id",
            {{
                "type": "long",
                "name": "_source.Data.roadCuttingInfo.0.roadCuttingArea"
            }},
            "_source.Data.roadCuttingInfo.0.roadType",
            "_source.Data.roadCuttingInfo.0.status",
            "_source.Data.roadType",
            "_source.Data.status",
            "_source.Data.tenantId",
            "_source.Data.ward.code",
            "_source.Data.ward.name",
            "_source.Data.waterSource",
            "_type",
            {{
                "type": "long",
                "name": "sort.0"
            }}
            ]
        }},
        "granularitySpec": {{
            "queryGranularity": "none",
            "rollup": false,
            "segmentGranularity": "day"
        }}
        }}
    }}
    }}
    """
    q=payload.format(data)
    header = {
    'Content-Type': 'application/json'
    }
    response = requests.request("POST", druid_url, headers=header, data=q)
    logging.info(response.text)

def upload_water_and_property():
    data = ""
    f= open("water_and_property.csv","r")
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    for row in spamreader:
        data+=', '.join(row)
        data+='\\n'
    f.close()

    payload =  """
    {{
    "type": "index_parallel",
    "spec": {{
        "ioConfig": {{
        "type": "index_parallel",
        "inputSource": {{
            "type": "inline",
            "data": "{0}"
        }},
        "inputFormat": {{
            "type": "csv",
            "findColumnsFromHeader": true
        }},
        "appendToExisting": true
        }},
        "tuningConfig": {{
        "type": "index_parallel",
        "partitionsSpec": {
            "type": "dynamic"
        }}
        }},
        "dataSchema": {{
        "dataSource": "water_and_property",
        "timestampSpec": {{
            "column": "_source.Data.@timestamp_property",
            "format": "iso"
        }},
        "dimensionsSpec": {{
            "dimensions": [
            "_id",
            "_index",
            "_score",
            "_source.Data.applicationNo",
            "_source.Data.applicationStatus",
            "_source.Data.applicationType",
            "_source.Data.channel",
            "_source.Data.connectionCategory",
            "_source.Data.connectionHolders",
            {{
                "type": "long",
                "name": "_source.Data.connectionNo"
            }},
            "_source.Data.connectionType",
            {{
                "type": "long",
                "name": "_source.Data.dateEffectiveFrom"
            }},
            "_source.Data.id",
            {{
                "type": "long",
                "name": "_source.Data.noOfTaps"
            }},
            "_source.Data.oldConnectionNo",
            {{
                "type": "double",
                "name": "_source.Data.pipeSize"
            }},
            "_source.Data.plumberInfo",
            "_source.Data.propertyId",
            "_source.Data.propertyUsageType",
            {{
                "type": "double",
                "name": "_source.Data.proposedPipeSize"
            }},
            {{
                "type": "long",
                "name": "_source.Data.proposedTaps"
            }},
            "_source.Data.rainWaterHarvesting",
            {{
                "type": "long",
                "name": "_source.Data.roadCuttingArea"
            }},
            "_source.Data.roadCuttingInfo",
            "_source.Data.roadCuttingInfo.0.auditDetails",
            "_source.Data.roadCuttingInfo.0.id",
            {{
                "type": "long",
                "name": "_source.Data.roadCuttingInfo.0.roadCuttingArea"
            }},
            "_source.Data.roadCuttingInfo.0.roadType",
            "_source.Data.roadCuttingInfo.0.status",
            "_source.Data.roadType",
            "_source.Data.status",
            "_source.Data.tenantId",
            "_source.Data.ward.code",
            "_source.Data.ward.name",
            "_source.Data.waterSource",
            "_type",
            {{
                "type": "long",
                "name": "sort.0"
            }}
            ]
        }},
        "granularitySpec": {{
            "queryGranularity": "none",
            "rollup": false,
            "segmentGranularity": "day"
        }}
        }}
    }}
    }}
    """
    q=payload.format(data)
    header = {
    'Content-Type': 'application/json'
    }
    response = requests.request("POST", druid_url, headers=header, data=q)
    logging.info(response.text)

def upload_trade_and_property():
    data = ""
    f= open("trade_and_property.csv","r")
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    for row in spamreader:
        data+=', '.join(row)
        data+='\\n'
    f.close()

    payload =  """{{
    "type": "index_parallel",
    "spec": {{
        "ioConfig": {{
        "type": "index_parallel",
        "inputSource": {{
            "type": "inline",
            "data": "{0}"
        }},
        "inputFormat": {{
            "type": "csv",
            "findColumnsFromHeader": true
        }}
        }},
        "tuningConfig": {{
        "type": "index_parallel",
        "partitionsSpec": {{
            "type": "dynamic"
        }}
        }},
        "dataSchema": {{
        "dataSource": "test2",
        "timestampSpec": {{
            "column": "!!!_no_such_column_!!!",
            "missingValue": "2010-01-01T00:00:00Z"
        }},
        "dimensionsSpec": {{
            "dimensions": [
            "additionaldetails",
            "billexpirytime",
            "businessservice",
            "consumercode",
            "consumertype",
            "createdby",
            {{
                "type": "long",
                "name": "createdtime"
            }},
            "fixedbillexpirydate",
            "id",
            "ispaymentcompleted",
            "lastmodifiedby",
            {{
                "type": "long",
                "name": "lastmodifiedtime"
            }},
            {{
                "type": "long",
                "name": "minimumamountpayable"
            }},
            "payer",
            "status",
            {{
                "type": "long",
                "name": "taxperiodfrom"
            }},
            {{
                "type": "long",
                "name": "taxperiodto"
            }},
            "tenantid"
            ]
        }},
        "granularitySpec": {{
            "queryGranularity": "none",
            "rollup": false,
            "segmentGranularity": "day"
        }}
        }}
    }}
    }}"""
    q=payload.format(data)
    header = {
    'Content-Type': 'application/json'
    }
    response = requests.request("POST", druid_url, headers=header, data=q)
    logging.info(response.text)

def upload_rule_3():
    data = ""
    f= open("rule_3.csv","r")
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    for row in spamreader:
        data+=', '.join(row)
        data+='\\n'
    f.close()

    payload =  """
    {{
    "type": "index_parallel",
    "spec": {{
        "ioConfig": {{
        "type": "index_parallel",
        "inputSource": {{
            "type": "inline",
            "data": "{0}"
        }},
        "inputFormat": {{
            "type": "csv",
            "findColumnsFromHeader": true
        }},
        "appendToExisting": true
        }},
        "tuningConfig": {{
        "type": "index_parallel",
        "partitionsSpec": {{
            "type": "dynamic"
        }}
        }},
        "dataSchema": {{
        "dataSource": "rule_3",
        "timestampSpec": {{
            "column": "_source.Data.@timestamp",
            "format": "iso"
        }},
        "dimensionsSpec": {{
            "dimensions": [
            "_id",
            "_index",
            "_score",
            "_source.Data.accountId",
            "_source.Data.acknowldgementNumber",
            "_source.Data.channel",
            {{
                "type": "long",
                "name": "_source.Data.noOfFloors"
            }},
            "_source.Data.ownershipCategory",
            "_source.Data.propertyId",
            "_source.Data.propertyType",
            "_source.Data.source",
            "_source.Data.status",
            "_source.Data.tenantId",
            "_source.Data.usageCategory",
            "_source.Data.ward.code",
            "_source.Data.ward.name",
            "_type",
            {{
                "type": "long",
                "name": "sort.0"
            }}
            ]
        }},
        "granularitySpec": {{
            "queryGranularity": "none",
            "rollup": false,
            "segmentGranularity": "day"
        }}
        }}
    }}
    }}
    """
    q=payload.format(data)
    logging.info(q)
    header = {
    'Content-Type': 'application/json'
    }
    response = requests.request("POST", druid_url, headers=header, data=q)
    logging.info(response.text)

def upload_dss_service():
    data = ""
    header = {
    'Content-Type': 'application/json'
    }
    payload =  """
    {{
    "type": "index_parallel",
    "spec": {{
        "ioConfig": {{
        "type": "index_parallel",
        "inputSource": {{
            "type": "inline",
            "data": "{0}"
        }},
        "inputFormat": {{
            "type": "csv",
            "findColumnsFromHeader": true
        }},
        "appendToExisting": true
        }},
        "tuningConfig": {{
        "type": "index_parallel",
        "partitionsSpec": {
            "type": "dynamic"
        }}
        }},
        "dataSchema": {{
        "dataSource": "dss_service",
        "timestampSpec": {
            "column": "_source.dataObject.paymentDetails.bill.billDate",
            "format": "millis"
        }},
        "dimensionsSpec": {{
            "dimensions": [
            "_id",
            "_index",
            "_score",
            "_source.Data.applicationNo",
            "_source.Data.applicationStatus",
            "_source.Data.applicationType",
            "_source.Data.channel",
            "_source.Data.connectionCategory",
            "_source.Data.connectionHolders",
            {{
                "type": "long",
                "name": "_source.Data.connectionNo"
            }},
            "_source.Data.connectionType",
            {{
                "type": "long",
                "name": "_source.Data.dateEffectiveFrom"
            }},
            "_source.Data.id",
            {{
                "type": "long",
                "name": "_source.Data.noOfTaps"
            }},
            "_source.Data.oldConnectionNo",
            {{
                "type": "double",
                "name": "_source.Data.pipeSize"
            }},
            "_source.Data.plumberInfo",
            "_source.Data.propertyId",
            "_source.Data.propertyUsageType",
            {{
                "type": "double",
                "name": "_source.Data.proposedPipeSize"
            }},
            {{
                "type": "long",
                "name": "_source.Data.proposedTaps"
            }},
            "_source.Data.rainWaterHarvesting",
            {{
                "type": "long",
                "name": "_source.Data.roadCuttingArea"
            }},
            "_source.Data.roadCuttingInfo",
            "_source.Data.roadCuttingInfo.0.auditDetails",
            "_source.Data.roadCuttingInfo.0.id",
            {{
                "type": "long",
                "name": "_source.Data.roadCuttingInfo.0.roadCuttingArea"
            }},
            "_source.Data.roadCuttingInfo.0.roadType",
            "_source.Data.roadCuttingInfo.0.status",
            "_source.Data.roadType",
            "_source.Data.status",
            "_source.Data.tenantId",
            "_source.Data.ward.code",
            "_source.Data.ward.name",
            "_source.Data.waterSource",
            "_type",
            {{
                "type": "long",
                "name": "sort.0"
            }}
            ]
        }},
        "granularitySpec": {{
            "queryGranularity": "none",
            "rollup": false,
            "segmentGranularity": "day"
        }}
        }}
    }}
    }}
    
    """
   

    f= open("dss_collection_ws.csv","r")
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    for row in spamreader:
        data+=', '.join(row)
        data+='\\n'
    f.close()
    q=payload.format(data)
    response = requests.request("POST", druid_url, headers=header, data=q)
    logging.info(response.text)

    data = ""
    f= open("dss_collection_pt.csv","r")
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    for row in spamreader:
        data+=', '.join(row)
        data+='\\n'
    f.close()
    q=payload.format(data)
    response = requests.request("POST", druid_url, headers=header, data=q)
    logging.info(response.text)

    data=""
    f= open("dss_collection_tl.csv","r")
    spamreader = csv.reader(f, delimiter=',', quotechar='"')
    for row in spamreader:
        data+=', '.join(row)
        data+='\\n'
    f.close()
    q=payload.format(data)

    response = requests.request("POST", druid_url, headers=header, data=q)
    logging.info(response.text)

def upload_data():
    logging.info("Upload data to Druid")
    upload_property_service()
    # upload_trade_license()
    upload_water_service()
    #upload_water_and_meter() - data not in prod for punjab
    #upload_meter_service() - data not in prod for punjab
    #upload_demand() - data not in prod for punjab
    upload_water_and_property()
    # upload_trade_and_property() 
    upload_rule_3()   
    upload_dss_service()


    #url = "https://druid-qa.ifix.org.in/druid/indexer/v1/task"
    # data = ""
    # f= open("property_service.csv","r")
    # spamreader = csv.reader(f, delimiter=',', quotechar='"')
    # for row in spamreader:
    #     data+=', '.join(row)
    #     data+='\\n'
    # f.close()

    # payload =  """{{
    # "type": "index_parallel",
    # "spec": {{
    #     "ioConfig": {{
    #     "type": "index_parallel",
    #     "inputSource": {{
    #         "type": "inline",
    #         "data": "{0}"
    #     }},
    #     "inputFormat": {{
    #         "type": "csv",
    #         "findColumnsFromHeader": true
    #     }}
    #     }},
    #     "tuningConfig": {{
    #     "type": "index_parallel",
    #     "partitionsSpec": {{
    #         "type": "dynamic"
    #     }}
    #     }},
    #     "dataSchema": {{
    #     "dataSource": "test2",
    #     "timestampSpec": {{
    #         "column": "!!!_no_such_column_!!!",
    #         "missingValue": "2010-01-01T00:00:00Z"
    #     }},
    #     "dimensionsSpec": {{
    #         "dimensions": [
    #         "additionaldetails",
    #         "billexpirytime",
    #         "businessservice",
    #         "consumercode",
    #         "consumertype",
    #         "createdby",
    #         {{
    #             "type": "long",
    #             "name": "createdtime"
    #         }},
    #         "fixedbillexpirydate",
    #         "id",
    #         "ispaymentcompleted",
    #         "lastmodifiedby",
    #         {{
    #             "type": "long",
    #             "name": "lastmodifiedtime"
    #         }},
    #         {{
    #             "type": "long",
    #             "name": "minimumamountpayable"
    #         }},
    #         "payer",
    #         "status",
    #         {{
    #             "type": "long",
    #             "name": "taxperiodfrom"
    #         }},
    #         {{
    #             "type": "long",
    #             "name": "taxperiodto"
    #         }},
    #         "tenantid"
    #         ]
    #     }},
    #     "granularitySpec": {{
    #         "queryGranularity": "none",
    #         "rollup": false,
    #         "segmentGranularity": "day"
    #     }}
    #     }}
    # }}
    # }}"""


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
    logging.info("absolute path {0}".format(os.path.abspath(f"""{file_name}.csv"""))) 

    
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

# def water_and_meter_services(water_services, meter_services):
#     water_and_meter = water_services.merge(
#         meter_services,
#         how="inner",
#         left_on="_source.Data.connectionNo",
#         right_on="_source.Data.connectionNo",
#         suffixes=("_water", "_meter"),
#     )
#     convert_dataframe_to_csv(dataframe=water_and_meter, file_name="water_and_meter")

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

def rule3(property_services, water_services):
    water_and_property = water_services.merge(
        property_services,
        how="inner",
        left_on="_source.Data.propertyId",
        right_on="_source.Data.propertyId",
        suffixes=("_water", "_property"),
    )

    water_and_property = water_and_property["_source.Data.propertyId"]

    query = f"""(`_source.Data.propertyType`.str.upper() != 'VACANT')"""

    property_services = property_services.query(query)

    property_services = property_services[
        (~(property_services["_source.Data.propertyId"].isin(water_and_property)))
    ]
    convert_dataframe_to_csv(dataframe=property_services, file_name="rule_3")

flattendata = PythonOperator(
task_id='flatten_data',
python_callable=collect_data,
provide_context=True,
dag=dag)

joindata = PythonOperator(
task_id='join_data',
python_callable=join_data,
provide_context=True,
dag=dag)

uploaddata = PythonOperator(
task_id='upload_data',
python_callable=upload_data,
provide_context=True,
dag=dag)


flattendata >> joindata >> uploaddata
