from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime

import logging

def task_1(ti, ts):
    ti.xcom_push(key="ts_inicial", value = ts)

def task_2(ti):
    ts = ti.xcom_pull(key="ts_inicial")
    logging.info(ts)

args = {
    'owner' : 'sales',
    'email_on_retry' : None,
    'email_on_failure': None,
    'retyr': 2,
    'retry_delay': timedelta(minutes=5)
}

with DAG (
    dag_id="simple_dag",
    schedule_interval= timedelta(days=1),
    catchup=False,
    start_date=datetime(2024,5,1),
    default_args = args,
) as dag:
    
    first_task = PythonOperator(
        task_id="task_1",
        python_callable=task_1
    )
    second_task = PythonOperator(
        task_id="task_2",
        python_callable=task_2
    )
    
    first_task >> second_task