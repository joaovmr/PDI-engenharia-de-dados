from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 4, 1),
    'retries': 3
}

def print_ds(**kwargs):
    ds = kwargs['execution_date'].strftime('%Y-%m-%d')
    print(f'DS: {ds}')
    return ds

def print_ts(**kwargs):
    ts = kwargs['ts']
    print(f'TS: {ts}')
    return ts

def concatenate_ds_ts(ds, ts, **kwargs):
    concatenated_str = f'{ds} -- {ts}'
    print(f'Concatenated: {concatenated_str}')
    return concatenated_str

dag = DAG(
    'multi_task_dag',
    default_args=default_args,
    schedule_interval=None
)

start_task = BashOperator(
    task_id='start_task',
    bash_command='echo "DAG started"',
    dag=dag
)

print_ds_task = PythonOperator(
    task_id='print_ds_task',
    python_callable=print_ds,
    provide_context=True,
    dag=dag
)

print_ts_task = PythonOperator(
    task_id='print_ts_task',
    python_callable=print_ts,
    provide_context=True,
    dag=dag
)

dummy_task = EmptyOperator(
    task_id='dummy_task',
    dag=dag
)

concatenate_task = PythonOperator(
    task_id='concatenate_task',
    python_callable=concatenate_ds_ts,
    op_kwargs={'ds': "{{ task_instance.xcom_pull(task_ids='print_ds_task') }}",
               'ts': "{{ task_instance.xcom_pull(task_ids='print_ts_task') }}"},
    provide_context=True,
    dag=dag
)

start_task >> [print_ds_task, print_ts_task] >> dummy_task >> concatenate_task
