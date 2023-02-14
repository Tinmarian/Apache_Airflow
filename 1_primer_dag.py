from airflow.models import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from datetime import datetime

NAME_LIST = ['Tinmar','Alejandra','Kenya','Atocha','Francisco','Paola','Lucy','Ajna']

default_args = {
    "owner":"Tinmar",
    "start_date": datetime(2023, 1, 9)
}

# PythonOperator: prueba_python
def hello_world_loop(*args):
    for palabra in args:
        print(f'Hola {palabra}')
with DAG(
        '1_primer_dag',
        catchup = False,
        default_args=default_args,
        schedule_interval=None,
        tags=['Curso 2', 'Apache_Airflow']
        ) as dag:

    start_task = DummyOperator(task_id='start_task')

    prueba_python = PythonOperator(
                                    task_id='python_op',
                                    python_callable=hello_world_loop,
                                    op_args=NAME_LIST
                                )

    prueba_bash = BashOperator(
                               task_id='bash_op',
                               bash_command='echo Prueba Bash' 
                            )

    end_task = DummyOperator(task_id='end_task')



    start_task >> prueba_python >> prueba_bash >> end_task
