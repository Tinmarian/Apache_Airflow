from airflow.models import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime

NAME_LIST = ['Tinmar','Alejandra','Kenya','Atocha','Francisco','Paola','Lucy','Ajna']

default_args = {
    "owner":"Tinmar",
    "start_date": datetime(2023,1,13)
}

def xcom_push(*args, **context):
    for palabra in args:
        print(f'Hola {palabra}')

    ti = context['ti']
    ti.xcom_push(key='xcom_prueba', value='valor_de_prueba')
    return 'Este es un Xcom'

with DAG(
        'jinja_macros_quinto_dag',
        catchup=False,
        schedule_interval=None,
        default_args=default_args,
        tags=['Curso 2', 'Apache_Airflow']
    ) as dag:

    start = DummyOperator(task_id='start')

    prueba_push = PythonOperator(
                                    task_id='xcom_push',
                                    python_callable=xcom_push,
                                    op_args=NAME_LIST,
                                    provide_context=True,
                                    do_xcom_push=True
                                )

    jinja_1 = BashOperator(
                            task_id='jinja_1',
                            bash_command='echo {{ ds }}'
                        )

    jinja_2 = BashOperator(
                            task_id='jinja_2',
                            bash_command='echo {{ ti.xcom_pull(task_ids="xcom_push") }}'
                        )

    jinja_3 = BashOperator(
                            task_id='jinja_3',
                            bash_command='echo {{ ti.xcom_pull(task_ids="xcom_push",key="xcom_prueba") }}'
                        )

    jinja_4 = BashOperator(
                            task_id='jinja_4',
                            bash_command='echo {{ var.value.jinja_template_var }}'
                        )

    end = DummyOperator(task_id='end')


    start >> prueba_push >> jinja_1 >> jinja_2 >> jinja_3 >> jinja_4 >> end