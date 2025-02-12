from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

def get_endpoints():
    from src.endpoints import Endpoints
    return Endpoints().get_all()

def get_cutomers(endpoint: dict):
    from src.controllers.paginations import PaginationController
    
    resource = endpoint.get("resources", None)
    action = endpoint.get("action", None)
    params = endpoint.get("params", None)
    data_source = endpoint.get("data_source", None)
    pagination_type = endpoint.get("pagination_type", "per_page")
    page_label = endpoint.get("page_label", None)
    total_of_pages_label = endpoint.get("total_of_pages_label", None)
    records_label = endpoint.get("records_label", "registros")
    
    pagination = PaginationController()
    pagination = pagination.pagination(
        type=pagination_type,
        resource=resource,
        action=action,
        params=params,
        data_source=data_source,
        page_label=page_label,
        total_of_pages_label=total_of_pages_label,
        records_label=records_label
    )
    

with DAG(
    'execute_entities',
    default_args=default_args,
    description='Execute entities',
    start_date=datetime(2025, 1, 1),
    schedule_interval='0 3 * * *',
    catchup=False,
) as dag:
    
    start = DummyOperator(task_id='start')
    end = DummyOperator(task_id='end')
    
    endpoints = get_endpoints()
    
    for endpoint in endpoints:
        tasks = PythonOperator(
            task_id=f"extract_and_load_{endpoint.get('action', None)}",
            python_callable=get_cutomers,
            op_kwargs={"endpoint": endpoint},
            dag=dag
        )

        start >> tasks >> end