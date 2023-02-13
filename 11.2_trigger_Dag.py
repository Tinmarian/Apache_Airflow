from airflow.models import DAG
from datetime import datetime

from airflow.operators.dagrun_operator import TriggerDagRunOperator
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

    trigger = TriggerDagRunOperator(
                                        task_id='trigger',
                                        trigger_dag_id='example_dag',
                                        wait_for_completion=True   
                                    )

    end = DummyOperator(task_id='end')

    start >> trigger >> end
