from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
import pandas as pd
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}
airflow_temp_dir = '/tmp'
def extract_data():
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn')
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()
    query = """
    SELECT v.id, c.marca, c.modelo, cl.nome AS cliente_nome, f.nome AS funcionario_nome, v.data_da_venda, v.preco_de_venda
    FROM vendas v
    JOIN carros c ON v.carro_id = c.id
    JOIN clientes cl ON v.cliente_id = cl.id
    JOIN funcionarios f ON v.funcionario_id = f.id
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    df = pd.DataFrame(rows, columns=['id', 'marca', 'modelo', 'cliente_nome', 'funcionario_nome', 'data_da_venda', 'preco_de_venda'])
    df.to_csv(f'{airflow_temp_dir}/resultado_vendas.csv', index=False)

def process_csv():
    df = pd.read_csv(f'{airflow_temp_dir}/resultado_vendas.csv')
    vendedor_faturamento = df.groupby('funcionario_nome')['preco_de_venda'].sum().reset_index()
    vendedor_faturamento = vendedor_faturamento.sort_values(by='preco_de_venda', ascending=False)
    for i, row in enumerate(vendedor_faturamento.iterrows(), start=1):
        nome_vendedor = row[1]['funcionario_nome']
        faturamento = row[1]['preco_de_venda']
        print(f'Vendedor {nome_vendedor} faturou o valor {faturamento} e ficou em {i}º lugar.')

with DAG(
    'consulta_e_processamento_dados',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
) as dag:

    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data,
    )

    process_csv_task = PythonOperator(
        task_id='process_csv',
        python_callable=process_csv,
    )

    extract_data_task >> process_csv_task
