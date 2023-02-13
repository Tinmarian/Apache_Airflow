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
def xcom_push(*args, **context):
    for palabra in args:
        print(f'Hola {palabra}')

    ti = context['ti']
    ti.xcom_push(key='xcom_prueba', value='valor_de_prueba')
    return 'Este es un Xcom'

def xcom_pull(**context):
    ti = context['ti']
    valor_recibido = ti.xcom_pull(task_ids='xcom_push')
    valor_selecciondado = ti.xcom_pull(task_ids='xcom_push',key='xcom_prueba')
    print(valor_recibido, valor_selecciondado)

with DAG(
        'xcoms_cuarto_dag',
        catchup = False,
        default_args=default_args,
        schedule_interval=None,
        tags=['Curso 2', 'Apache_Airflow']
        ) as dag:

    start_task = DummyOperator(task_id='start_task')

    prueba_push = PythonOperator(
                                    task_id='xcom_push',
                                    python_callable=xcom_push,
                                    op_args=NAME_LIST,
                                    provide_context=True,
                                    do_xcom_push=True
                                )

    prueba_pull = PythonOperator(
                                    task_id='xcom_pull',
                                    python_callable=xcom_pull,
                                    provide_context=True,
                                    do_xcom_push=True
                                )

    prueba_bash = BashOperator(
                               task_id='bash_op',
                               bash_command='echo Prueba Bash' 
                            )

    end_task = DummyOperator(task_id='end_task')



    start_task >> prueba_push >> prueba_pull >> prueba_bash >> end_task
