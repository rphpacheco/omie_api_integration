import json
from typing import Optional

from loguru import logger

from src.config import Settings
from src.api import Api
from src.endpoints import Endpoints
from src.db import Database

settings = Settings()

endpoints = Endpoints()
endpoints = endpoints.get_endpoint(action="ListarContasCorrentes")

base_url = settings.BASE_URL
app_key = settings.APP_KEY
app_secret = settings.APP_SECRET

HEADERS = {
    'Content-Type': 'application/json'
}

def request(resource: str, body: dict, params: dict) -> dict:
    response = Api(
        url=f"{base_url}{resource}",
        headers=HEADERS,
        json=body,
        params=params
    ).post()
    
    if response.status_code == 200:
        json = response.json()
        return json
    elif response.status_code == 500:
        print("RESPONSE", response)
        return response.json()
    else:
        raise Exception (f"Erro: {response.status_code}")

def get_total_of_pages(
    resource: str, 
    action: str, 
    params: dict,
    total_of_pages_label: Optional[str],
    records_label: Optional[str]
    ) -> int:
    
    total_of_pages_label = "total_de_paginas" if total_of_pages_label is None else total_of_pages_label
    records_label = "registros" if records_label is None else records_label
    
    payload = {
        "call": action,
        "app_key": app_key,
        "app_secret": app_secret,
        "param":[params]
    }
    
    response = request(resource, payload, params)
    total_of_pages = response.get(total_of_pages_label, 0)
    
    return total_of_pages

def save_to_file(resource: str, content: dict):
    content = json.dumps(content)
    
    file_name = resource.split("/")[-2]
    with open(f'{file_name}.json', 'w') as file:
        file.write(content)


for endpoint in endpoints:
    resource = endpoint.get("resources", None)
    action = endpoint.get("action", None)
    params = endpoint.get("params", None)
    data_source = endpoint.get("data_source", None)
    total_of_pages_label = endpoint.get("total_of_pages_label", None)
    records_label = endpoint.get("records_label", None)
    
    total_of_pages = get_total_of_pages(
        resource, 
        action, 
        params,
        total_of_pages_label,
        records_label
    )
    
    records_fetched = 0
    for page in range(1, total_of_pages + 1):
        # params["nPagina"] = page
        params["pagina"] = page
        
        body = {
            "call": action,
            "app_key": app_key,
            "app_secret": app_secret,
            "param":[params]
        }
        
        response = request(resource, body, params)

        if action == "ListarMovimentos":            
            records_fetched += response.get("nRegistros", 0)
            contents = response.get(data_source, [])
        else:
            records_fetched += response.get("registros", 0)
            contents = response.get(data_source, [])
        
        black_list = ['tags', 'recomendacoes', 'homepage', 'fax_ddd', 'bloquear_exclusao', 'produtor_rural']
        
        for content in contents:
            for item in black_list:
                if item in content:
                    del content[item]
        
        logger.info(f"Page {page} has been fetched with {records_fetched} records.")
        
        db = Database()
        db.save_into_db(page, resource, contents)