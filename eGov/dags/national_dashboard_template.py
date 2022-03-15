from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta, timezone
from hooks.elastic_hook import ElasticHook
from airflow.operators.http_operator import SimpleHttpOperator
import requests 
from airflow.hooks.base import BaseHook
import logging
import json
from queries.tl import *
from queries.pgr import *
from queries.ws import *
from queries.pt import *
from utils.utils import log
from pytz import timezone

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=10),
    'start_date': datetime(2017, 1, 24)

}

module_map = {
    'TL' : (tl_queries, empty_tl_payload),
    'PGR' : (pgr_queries, empty_pgr_payload),
    'WS' : (ws_queries, empty_ws_payload),
    'PT' : (pt_queries, empty_pt_payload)
}


dag = DAG('national_dashboard_template', default_args=default_args, schedule_interval=None)
log_endpoint = 'kibana/api/console/proxy'


def dump_kibana(**kwargs):
    connection = BaseHook.get_connection('qa-punjab-kibana')
    endpoint = 'kibana/api/console/proxy'
    module = kwargs['module']
    module_config = module_map.get(module)
    queries = module_config[0]
    date = kwargs['dag_run'].conf.get('date')
    localtz = timezone('Asia/Kolkata')
    dt_aware = localtz.localize(datetime.strptime(date, "%d-%m-%Y"))
    start = int(dt_aware.timestamp() * 1000)
    end = start + (24 * 60 * 59 * 1000)

    merged_document = {}
    for query in queries:
        url = '{0}://{1}/{2}?path={3}&method=POST'.format('https', connection.host, endpoint, query.get('path'))
        q = query.get('query').format(start,end)
        logging.info(q)
        r = requests.post(url, data=q, headers={'kbn-xsrf' : 'true', 'Content-Type' : 'application/json'}, auth=(connection.login, connection.password))
        response = r.json()
        merged_document[query.get('name')] = response
        logging.info(json.dumps(response))
        # with open('{0}{1}.json'.format(file_share, query.get('name')), "wt") as outfile:
        #     outfile.write(json.dumps(response))
    ward_list = transform_response_sample(merged_document, date, module)
    kwargs['ti'].xcom_push(key='payload_{0}'.format(module), value=json.dumps(ward_list))
    # logging.info(merged_document)
    return json.dumps(ward_list)


def transform_response_sample(merged_document, date, module):
    module_config = module_map.get(module)
    queries = module_config[0]
    ward_map = {}
    for query in queries:
        single_document = merged_document[query.get('name')]
        single_document = single_document.get('aggregations')
        lambda_function = query.get('lambda')
        ward_map = transform_single(single_document, ward_map, date, lambda_function, module)
    ward_list = [ward_map[k] for k in ward_map.keys()]
    logging.info(json.dumps(ward_list))
    return ward_list

def transform_single(single_document, ward_map, date, lambda_function, module):
    module_config = module_map.get(module)
    empty_lambda = module_config[1]
    ward_agg = single_document.get('ward')
    ward_buckets = ward_agg.get('buckets')
    for ward_bucket in ward_buckets:
        ward = ward_bucket.get('key')
        ulb_agg = ward_bucket.get('ulb')
        ulb_buckets = ulb_agg.get('buckets')
        for ulb_bucket in ulb_buckets:
            ulb = ulb_bucket.get('key')
            region_agg = ulb_bucket.get('region')
            region_buckets = region_agg.get('buckets')
            for region_bucket in region_buckets:
                region = region_bucket.get('key')
                if ward_map.get(ward):
                    ward_payload = ward_map.get(ward)
                else:
                    ward_payload = empty_lambda(region, ulb, ward, date)
                # logging.info(region_bucket)
                metrics = ward_payload.get('metrics')
                metrics = lambda_function(metrics, region_bucket)
                ward_payload['metrics'] = metrics
                # logging.info(ward_payload)
                # logging.info(ward_map)
                ward_map[ward] = ward_payload
    
    return ward_map






def dump(**kwargs):
    ds = kwargs['ds']
    hook = ElasticHook('GET', 'test-es')
    resp = hook.search('/dss-collection_v2', {
        'size': 10000,
         "query": {
            "term": {
            "dataObject.paymentDetails.businessService.keyword": "TL" 
            }
        }
    })
    return resp['hits']['hits']

def get_auth_token(connection):
    endpoint = 'user/oauth/token'
    url = '{0}://{1}/{2}'.format('https', connection.host, endpoint)
    data = {
        'grant_type' : 'password',
        'scope' : 'read',
        'username' : 'amr001',
        'password' : 'eGov@123',
        'tenantId' : 'pb.amritsar',
        'userType' : 'EMPLOYEE'
    }
    # data = "grant_type=password&scope=read&username=amr001&password=eGov@123&tenantId=pb.amritsar&userType=EMPLOYEE"
    r = requests.post(url, data=data, headers={'Authorization' : 'Basic ZWdvdi11c2VyLWNsaWVudDo=', 'Content-Type' : 'application/x-www-form-urlencoded'})
    response = r.json()
    logging.info(response)
    return (response.get('access_token'), response.get('refresh_token'), response.get('UserRequest'))


def call_ingest_api(connection, access_token, user_info, payload, module):
    endpoint = 'national-dashboard/metric/_ingest'
    url = '{0}://{1}/{2}'.format('https', connection.host, endpoint)
    data = {
        "RequestInfo": {
        "apiId": "asset-services",
        "ver": None,
        "ts": None,
        "action": None,
        "did": None,
        "key": None,
        "msgId": "search with from and to values",
        "authToken": access_token,
        "userInfo": user_info
        },
        "Data": json.loads(payload)

    }
    # data = "grant_type=password&scope=read&username=amr001&password=eGov@123&tenantId=pb.amritsar&userType=EMPLOYEE"
    log(module, 'Info', json.dumps(data), BaseHook.get_connection('qa-punjab-kibana'), log_endpoint)
    r = requests.post(url, data=json.dumps(data), headers={'Content-Type' : 'application/json'})
    response = r.json()
    log(module, 'Info', json.dumps(response), BaseHook.get_connection('qa-punjab-kibana'), log_endpoint)
    logging.info(json.dumps(data))
    logging.info(response)
    return response




def load(**kwargs):
    connection = BaseHook.get_connection('digit-auth')
    (access_token, refresh_token, user_info) = get_auth_token(connection)
    module = kwargs['module']

    payload = kwargs['ti'].xcom_pull(key='payload_{0}'.format(module))
    # logging.info('>>>>>>this is the payload')
    logging.info(payload)
    if access_token and refresh_token:
        call_ingest_api(connection, access_token, user_info, payload, module)
    # national-dashboard/metric/_ingest
    #/user/oauth/token
    return None

def transform(**kwargs):
    logging.info('Your transformations go here')
    return 'Post Transformed Data'



extract_tl = PythonOperator(
    task_id='elastic_search_extract_tl',
    python_callable=dump_kibana,
    provide_context=True,
    do_xcom_push=True,
    op_kwargs={ 'module' : 'TL'},
    dag=dag)

transform_tl = PythonOperator(
    task_id='nudb_transform_tl',
    python_callable=transform,
    provide_context=True,
    dag=dag)

load_tl = PythonOperator(
    task_id='nudb_ingest_load_tl',
    python_callable=load,
    provide_context=True,
    op_kwargs={ 'module' : 'TL'},
    dag=dag)


extract_pgr = PythonOperator(
    task_id='elastic_search_extract_pgr',
    python_callable=dump_kibana,
    provide_context=True,
    do_xcom_push=True,
    op_kwargs={ 'module' : 'PGR'},
    dag=dag)

transform_pgr = PythonOperator(
    task_id='nudb_transform_pgr',
    python_callable=transform,
    provide_context=True,
    dag=dag)

load_pgr = PythonOperator(
    task_id='nudb_ingest_load_pgr',
    python_callable=load,
    provide_context=True,
    op_kwargs={ 'module' : 'PGR'},
    dag=dag)

extract_ws = PythonOperator(
    task_id='elastic_search_extract_ws',
    python_callable=dump_kibana,
    provide_context=True,
    do_xcom_push=True,
    op_kwargs={ 'module' : 'WS'},
    dag=dag)

transform_ws = PythonOperator(
    task_id='nudb_transform_ws',
    python_callable=transform,
    provide_context=True,
    dag=dag)

load_ws = PythonOperator(
    task_id='nudb_ingest_load_ws',
    python_callable=load,
    provide_context=True,
    op_kwargs={ 'module' : 'WS'},
    dag=dag)


extract_pt = PythonOperator(
    task_id='elastic_search_extract_pt',
    python_callable=dump_kibana,
    provide_context=True,
    do_xcom_push=True,
    op_kwargs={ 'module' : 'PT'},
    dag=dag)

transform_pt = PythonOperator(
    task_id='nudb_transform_pt',
    python_callable=transform,
    provide_context=True,
    dag=dag)

load_pt = PythonOperator(
    task_id='nudb_ingest_load_pt',
    python_callable=load,
    provide_context=True,
    op_kwargs={ 'module' : 'PT'},
    dag=dag)


# curl --location --request POST 'https://dev.digit.org/user/oauth/token' \
# --header 'Authorization: Basic ZWdvdi11c2VyLWNsaWVudDo=' \
# --header 'Content-Type: application/x-www-form-urlencoded' \
# --data-urlencode 'grant_type=password' \
# --data-urlencode 'scope=read' \
# --data-urlencode 'username=amr001' \
# --data-urlencode 'password=eGov@123' \
# --data-urlencode 'tenantId=pb.amritsar' \
# --data-urlencode 'userType=EMPLOYEE'
# load = SimpleHttpOperator(
#         task_id="auth",
#         http_conn_id="digit-auth",
#         method="POST",
#         endpoint="/user/oauth/token",
#         data="grant_type=password&scope=read&username=amr001&password=eGov@123&tenantId=pb.amritsar&userType=EMPLOYEE",
#         headers={"Content-Type": "application/x-www-form-urlencoded", "Authorization" : "Basic ZWdvdi11c2VyLWNsaWVudDo="},
#         dag=dag,
#         do_xcom_push=True
#     )




extract_tl >> transform_tl >> load_tl
extract_pgr >> transform_pgr >> load_pgr
extract_ws >> transform_ws >> load_ws
extract_pt >> transform_pt >> load_pt