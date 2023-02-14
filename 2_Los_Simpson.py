from airflow.models import DAG

from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

FIRST_SECTION = "El uso de Airflow en la universidad de Springfield"
SECOND_SECTION = "El otro día mi hija me dijo que Airflow no se utilizaba en la universidad de Springfield, y yo le dije: qué no Lisa? qué no?"
THIRD_SECTION = "Púdrete Flanders "

default_args = {
    "owner":"Homero Simpson",
    "start_date": datetime(2023, 1, 9)
}

def remember_homer(first,second,third):
    print(
        f"\n\n\n{first}\n\n{second}\n\n{third*7}\n{third*7}\n{third*7}\n \
        {third*7}\n{third*7}\n{third*7}\n{third*7}\n{third*7}\n{third*7}\n \
        {third*7}\n{third*7}\n{third*7}\n{third*7}\n{third*7}\n{third*7}\n \
        {third*7}\n{third*7}\n{third*7}\n{third*7}\n{third*7}\n{third*7}\n \
        {third*3}\n" 
    )

with DAG(
        '2_Los_Simpson',
        default_args=default_args,
        catchup=False,
        schedule_interval=None,
        tags=['Curso 2', 'Apache_Airflow']
    ) as dag:

    start = DummyOperator(task_id='start')

    Remember_Homer = PythonOperator(
                                        task_id='Remember_Homer',
                                        python_callable=remember_homer,
                                        op_kwargs={
                                                    "first":FIRST_SECTION,
                                                    "second":SECOND_SECTION,
                                                    "third":THIRD_SECTION
                                                }
                                    )

    end = DummyOperator(task_id='end')

start >> Remember_Homer >> end
