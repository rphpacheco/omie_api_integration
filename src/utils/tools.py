from datetime import datetime
from typing import Optional

from src.config import Settings
from src.api import Api
from src.utils.constants import HEADERS

settings = Settings()

def get_body_params_pagination(
    action: str, 
    params: dict, 
    page: Optional[int] = None, 
    field_pagination: Optional[str] = None
) -> dict:
    if field_pagination:
        params[field_pagination] = page
        
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

def generate_date_range(start_date_str: str):
    def add_month(data):
        new_month = data.month + 1
        new_year = data.year
        if new_month > 12:
            new_month = 1
            new_year += 1
        
        return data.replace(month=new_month, year=new_year)
    
    start_date = datetime.strptime(start_date_str, "%d/%m/%Y")
    start_date = start_date.replace(day=1)
    # "25/01/2025" -> "01/01/2025"
    # ["01/01/2025", "01/02/2025"]
    
    today = datetime.today()
    
    date_list = []
    
    current_date = start_date
    while current_date <= today:
        date_list.append(current_date.strftime("%d/%m/%Y"))
        current_date = add_month(current_date)
        
    return date_list