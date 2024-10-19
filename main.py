import json
from src.config import Settings

import pandas as pd
from sqlalchemy import create_engine

from src.api import Api
from src.endpoints import Endpoints

settings = Settings()

endpoints = Endpoints()
endpoints = endpoints.get_all()

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
    else:
        raise Exception (f"Erro: {response.status_code}")

def get_total_of_pages(resource: str, action: str, params: dict) -> int:
    payload = {
        "call": action,
        "app_key": app_key,
        "app_secret": app_secret,
        "param":[params]
    }
    
    response = request(resource, payload, params)
    total_of_pages = response.get("total_de_paginas", 0)
    records = response.get("total_de_registros", 0)
    print(f"Total of pages: {total_of_pages}")
    print(f"Total of records: {records}")
    
    return total_of_pages

def save_to_file(resource: str, content: dict):
    content = json.dumps(content)
    
    file_name = resource.split("/")[-2]
    with open(f'{file_name}.json', 'w') as file:
        file.write(content)
def save_into_db(page: int, resource: str, content: dict):
    connection_string = f"postgresql://{settings.DB_USERNANE}:{settings.DB_PASSWORD}@{settings.DB_HOST}:{settings.DB_PORT}/{settings.DB_NAME}"
    table_name = resource.split("/")[-2]
    
    df = pd.json_normalize(content)
    
    # print(df.head())
    
    engine = create_engine(connection_string)
    
    if page == 1:
        df.to_sql(table_name, engine, if_exists='replace', index=False)
    else:
        df.to_sql(table_name, engine, if_exists='append', index=False)

for endpoint in endpoints:
    resource = endpoint.get("resources", None)
    action = endpoint.get("action", None)
    params = endpoint.get("params", None)
    data_source = endpoint.get("data_source", None)
    
    total_of_pages = get_total_of_pages(resource, action, params)
    
    records_fetched = 0
    for page in range(1, total_of_pages + 1):
        params["pagina"] = page
        
        body = {
            "call": action,
            "app_key": app_key,
            "app_secret": app_secret,
            "param":[params]
        }
        
        response = request(resource, body, params)
        records_fetched += response.get("registros", 0)
        
        contents = response.get(data_source, [])
        
        black_list = ['tags', 'recomendacoes', 'homepage', 'fax_ddd', 'bloquear_exclusao', 'produtor_rural']
        
        for content in contents:
            for item in black_list:
                if item in content:
                    del content[item]
        
        print(f"Page: {page}", f"Records: {records_fetched}")
        
        # print(contents)
        
        save_into_db(page, resource, contents)