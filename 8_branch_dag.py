from airflow.models import DAG
from datetime import datetime
import ast

from airflow.operators.dummy import DummyOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner":"Tinmar",
    "start_date":datetime(2023,1,13)
}

def branching(**context):
    ti = context['ti']
    api_response = ti.xcom_pull(task_ids='api_call')
    print(api_response)

    response_list = ast.literal_eval(api_response)
    print(response_list)
    number = response_list[0]['random']
    ti.xcom_push(key='numero',value=number)

    if number > 50:
        return 'mayor_que_50'
    else:
        return 'menor_que_50'

def mayor_50(**context):
    ti = context['ti']
    number = ti.xcom_pull(task_ids="branch",key="numero")
    print(f'El número fue mayor a 50 y fue {number}')

def menor_50(**context):
    ti = context['ti']
    number = ti.xcom_pull(task_ids="branch",key="numero")
    print(f'El número fue menor a 50 y fue {number}')

with DAG(
        '8_branch_dag',
        catchup=False,
        schedule_interval=None,
        default_args=default_args,
        tags=['Curso 2', 'Apache_Airflow']
    ) as dag:

    start = DummyOperator(task_id='start')

    api_call = SimpleHttpOperator(
                                    task_id='api_call',
                                    http_conn_id='random_number',
                                    endpoint='csrng/csrng.php',
                                    method='GET',
                                    data={'min':'0','max':'100'},
                                )

    branch = BranchPythonOperator(
                                    task_id='branch',
                                    python_callable=branching   
                                )

    mayor_que_50 = PythonOperator(
                                        task_id='mayor_que_50',
                                        python_callable=mayor_50   
                                    )

    menor_que_50 = PythonOperator(
                                        task_id='menor_que_50',
                                        python_callable=menor_50   
                                    )

    end = DummyOperator(task_id='end',trigger_rule='all_done')
(
    start >> api_call 
    >> branch 
    >> [mayor_que_50, menor_que_50] 
    >> end
)
