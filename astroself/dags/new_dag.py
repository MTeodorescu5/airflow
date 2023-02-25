from airflow import DAG
from datetime import datetime, timedelta

from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator



def _py_func():
    print('hey')

default_args = {
    'retries': 3,
    'owner': 'Mike',
    'start_date': datetime(2023,2,24)
}

with DAG(dag_id='new_dag', schedule_interval=timedelta(hours=1), default_args=default_args) as dag:
    python_task = PythonOperator(
        task_id='python_task',
        python_callable=_py_func
    )
    
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='exit 0'
    )

    python_task >> bash_task