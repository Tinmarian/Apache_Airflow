from airflow.models import DAG
from datetime import datetime

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator


default_Args = {
    "owner":"Tinmar",
    "start_date":datetime(2023,1,13)
}

with DAG(
            '9_dynamic_tasks_dag',
            catchup=False,
            schedule_interval=None,
            default_args=default_Args,
            tags=['Curso 2', 'Apache_Airflow']
        ) as dag:

    operators_list = list()

    start = DummyOperator(task_id='start')
    operators_list.append(start)

    for i in ['Italia', 'Francia', 'Alemania', 'Suiza']:
        imprimir = BashOperator(
                                    task_id=f'imprimir_{i}',
                                    bash_command=f'echo {i}'
                                )
        operators_list.append(imprimir)

    end = DummyOperator(task_id='end')
    operators_list.append(end)

    for i in range(len(operators_list)-1):
        operators_list[i] >> operators_list[i+1]