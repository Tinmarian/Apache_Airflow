from airflow.models import DAG

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils import dates
import pandas as pd
import logging

from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    "owner":"Tinmar",
    "start_date": dates.days_ago(1)
}

def get_pandas():
    conn = PostgresHook('redshift_default',region='us-east-1')
    df = conn.get_pandas_df('SELECT * FROM TABLE')
    logging.info('Datos obtenidos de la query')
    print(df.head())
    df.to_csv('s3://bucket/key.csv', index=False)
    logging.info('Guardado en S3')

with DAG(
        'hooks_tercer_dag',
        catchup=False,
        default_args=default_args,
        schedule_interval=None,
        tags=['Curso 2', 'Apache_Airflow']
    ) as dag:

    start = DummyOperator(task_id='start')

    get_pandas_df = PythonOperator(
                                    task_id='get_pandas',
                                    python_callable=get_pandas
                                )

    end = DummyOperator(task_id='end')

    start >> get_pandas_df >>end