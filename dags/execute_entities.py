from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from loguru import logger

from airflow.utils.task_group import TaskGroup

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
    
    if pagination_type == "date_range":
        depends_on = endpoint.get("depends_on", None)
        
        if depends_on:
            from src.db.database import Database
            db = Database()
            
            try:
                accounts = db.select_from_table(
                    table_name=depends_on,
                    distinct_column="nCodCC"
                )
            except Exception as e:
                logger.error(f"An error occurred while selecting from the table '{depends_on}': {e}")
            
            for account in accounts:
                params["nCodCC"] = account
                
                try:
                    pagination.pagination(
                        type=pagination_type,
                        resource=resource,
                        action=action,
                        params=params,
                        data_source=data_source
                    )
                except Exception as e:
                    logger.error(f"An error occurred while pagination: {e}")
    else:
        try:
            pagination.pagination(
                type=pagination_type,
                resource=resource,
                action=action,
                params=params,
                data_source=data_source,
                page_label=page_label,
                total_of_pages_label=total_of_pages_label,
                records_label=records_label
            )
        except Exception as e:
            logger.error(f"An error occurred while pagination: {e}")
    

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
    
    extract_endpoints = [e for e in endpoints if e.get("action") != "ListarExtrato"]
    excluded_extract_endpoints = [e for e in endpoints if e.get("action") == "ListarExtrato"]
    
    with TaskGroup("extract_and_load_omie_entities") as extract_group:
        for endpoint in extract_endpoints:
            tasks = PythonOperator(
                task_id=f"extract_and_load_{endpoint.get('action', None)}",
                python_callable=get_cutomers,
                op_kwargs={"endpoint": endpoint},
                dag=dag
            )
    
    with TaskGroup("extract_and_load_omie_second_flow") as extract_second_group:
        for second_endpoint in excluded_extract_endpoints:
            second_tasks = PythonOperator(
                task_id=f"extract_and_load_{second_endpoint.get('action', None)}",
                python_callable=get_cutomers,
                op_kwargs={"endpoint": second_endpoint},
                dag=dag
            )
        

    start >> extract_group >> extract_second_group >> end