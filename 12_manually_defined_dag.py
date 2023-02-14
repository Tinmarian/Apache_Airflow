from airflow.models import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

default_args = {
    "Owner":"Tinmar",
    "start_date":datetime(2023,1,14)
}

def hola(**kwargs):
    print(kwargs['dag_run'].conf)
    print(kwargs['dag_run'].conf['variable'])


with DAG(
            '12_manually_defined_dag',
            catchup=False,
            default_args=default_args,
            schedule_interval=None,
            tags=['Curso 2', 'Apache_Airflow']
        ) as dag:

    start = DummyOperator(task_id="start")

    task_1 = BashOperator(
                            task_id='task_1',
                            bash_command='echo {{ dag_run.conf["variable_prueba"] or 1000 }}'   
                        )

    task_2 = PythonOperator(
                                task_id='task_2',
                                python_callable=hola   
                            )

    end = DummyOperator(task_id='end')

    start >> task_1 >> task_2 >> end
