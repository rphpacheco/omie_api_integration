from typing import Optional

from src.config import Settings
from src.api import Api
from src.utils.constants import HEADERS

settings = Settings()

def get_body_params_pagination(
    action: str, 
    params: dict, 
    page: int, 
    field_pagination: str
) -> dict:
    params[field_pagination] = page | 1
    return {
        "call": action,
        "app_key": settings.APP_KEY,
        "app_secret": settings.APP_SECRET,
        "param":[params]
    }
    
def get_total_of_pages(
    resource: str, 
    action: str, 
    params: dict,
    page_label: Optional[str] = None,
    total_of_pages_label: Optional[str] = None,
    records_label: Optional[str] = None
    ) -> int:
    
    page_label = "pagina" if page_label is None else page_label
    total_of_pages_label = "total_de_paginas" if total_of_pages_label is None else total_of_pages_label
    records_label = "registros" if records_label is None else records_label
    
    payload = get_body_params_pagination(action, params, 1, page_label)
    
    api = Api(
        url=f"{settings.BASE_URL}{resource}",
        headers=HEADERS,
        json=payload,
        params=params
    )
    response = api.request(api.post)
    total_of_pages = response.get(total_of_pages_label, 0)
    
    return total_of_pages