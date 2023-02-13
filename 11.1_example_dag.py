from airflow.models import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "Owner":"Tinmar",
    "start_date":datetime(2023,1,13)
}

with DAG(
            'example_dag',
            catchup=False,
            default_args=default_args,
            schedule_interval=None,
            tags=['Curso 2', 'Apache_Airflow']
        ) as dag:

    start = DummyOperator(task_id="start")

    sleep = BashOperator(
                            task_id='sleep',
                            bash_command='sleep 30'   
                        )

    end = DummyOperator(task_id='end')

    start >> sleep >> end