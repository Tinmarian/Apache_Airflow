from airflow.models import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator

default_args = {
    "owner":"Tinmar",
    "start_date": datetime(2023, 1, 13)
}

def hello():
    print('Hola Mundo')

def load_subdag(parent_dag_name,child_dag_name,default_args):
    with DAG(
            child_dag_name,
            default_args=default_args,
            schedule_interval=None
        ) as subdag:

        sub_start = DummyOperator(task_id='sub_start')

        python_task = PythonOperator(
                                        task_id='python_task',
                                        python_callable=hello
                                    )

        sub_end = DummyOperator(task_id='sub_end')

        sub_start >> python_task >>sub_end

    return subdag

with DAG(
        'subdags_septimo_dag',
        catchup=False,
        schedule_interval=None,
        default_args=default_args,
        tags=['Curso 2', 'Apache_Airflow']
    ) as dag:

    start = DummyOperator(task_id='start')

    subdag = SubDagOperator(
                            task_id='subdag',
                            subdag=load_subdag   
                        )

    end = DummyOperator(task_id='end')

    start >> subdag >> end