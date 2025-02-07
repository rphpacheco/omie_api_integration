from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

dag = DAG(
    dag_id="simple_example_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

def print_hello():
    print("Olá Mundo!!! \n Esta é a minha primeira tarefa")
    
task1 = PythonOperator(
    task_id="print_hello",
    python_callable=print_hello,
    dag=dag
)

task2 = BashOperator(
    task_id="print_date",
    bash_command="date && sleep 5 & date",
    dag=dag
)

task1 >> task2