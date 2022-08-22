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

default_args = {
'owner': 'airflow',
'depends_on_past': False,
'retries': 3,
'retry_delay': timedelta(seconds=10),
'start_date': datetime(2017, 1, 24)

}

dag = DAG('rev_max', default_args=default_args, schedule_interval=None)

def test():
    logging.info('testing DAG')

flatten_data = PythonOperator(
task_id='flatten_data',
python_callable=test,
provide_context=True,
dag=dag)

flatten_data



