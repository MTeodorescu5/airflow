from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator

def _py_func():
    print('hey')


with DAG(dag_id='new_dag', start_date=datetime(2023,2,24), schedule_interval=timedelta(hours=1)) as dag:
    python_task = PythonOperator(
        task_id='python_task',
        python_callable=_py_func
    )
    
