
from airflow.models import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

default_args = {

}

def create_dag(dag_id,schedule,default_args,pais):

    dag = DAG(
                dag_id,  # 'dynamic_dags_decimo_dag',
                catchup=False,
                schedule_interval=schedule,
                default_args=default_args,
                tags=['Curso 2', 'Apache_Airflow']
            )

    with dag:

        start = DummyOperator(task_id='start')

        imprimir_pais = BashOperator(
                                        task_id=f'imprimir_{pais}',
                                        bash_command=f'echo {pais}'
                                    )

        end = DummyOperator(task_id='end')

        start >> imprimir_pais >> end

    return dag

paises = ['Mexico','Brasil','Nicaragua']

for pais in paises:
    dag_id=f"10_dynamic_dags_{pais}"
    default_args={"owner":f"Tinmar_{pais}","start_date":datetime(2023,1,13)}
    
    globals()[dag_id] = create_dag(dag_id,None,default_args=default_args,pais=pais)