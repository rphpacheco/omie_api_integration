import calendar
from datetime import datetime

from typing import Literal
from loguru import logger

from src.api import Api
from src.db import Database
from src.utils.tools import get_total_of_pages, get_body_params_pagination, generate_date_range

from src.utils.constants import HEADERS
from src.config import Settings
settings = Settings()

class PaginationController:
    # TODOS 
    # 1. OTIMIZAR PAGINAÇÃO PERPAGE
    # 2. RESOLVER A BLACK LIST
    # 3. IMPLAMENTAR PAGINAÇÃO POR PERIODO - OK
    def __init__(self) -> None:
        self.page = 1

    def pagination(
        self, 
        type: Literal["per_page", "date_range"],
        resource: str,
        action: str,
        params: dict,
        data_source: str,
        page_label: str = "pagina",
        total_of_pages_label: str = "total_de_paginas",
        records_label: str = "registros",
    ):
        match type:
            case "per_page":
                return self.per_page(
                    resource=resource, 
                    action=action, 
                    params=params, 
                    data_source=data_source,
                    page_label=page_label,
                    total_of_pages_label=total_of_pages_label,
                    records_label=records_label
                )
            case "date_range":
                return self.date_range(
                    resource=resource, 
                    action=action, 
                    params=params, 
                    data_source=data_source,
                    date_init=settings.DATE_INIT
                )
    def per_page(
        self, 
        resource: str,
        action: str,
        params: dict,
        data_source: str,
        page_label: str = "pagina",
        total_of_pages_label: str = "total_de_paginas",
        records_label: str = "registros",
    ):
        total_of_pages = get_total_of_pages(
            resource, 
            action, 
            params,
            page_label,
            total_of_pages_label,
            records_label
        )
        
        records_fetched = 0
        for page in range(1, total_of_pages + 1):
            params[page_label] = page
            
            body = get_body_params_pagination(
                action=action, 
                params=params, 
                page=page, 
                field_pagination=page_label
            )
            
            api = Api(
                url=f"{settings.BASE_URL}{resource}",
                headers=HEADERS,
                json=body,
                params=params
            )
            response = api.request(api.post)
            
            records_fetched += response.get(records_label, 0)
            contents = response.get(data_source, [])
            
            # Issue for development
            black_list = ['tags', 'recomendacoes', 'homepage', 'fax_ddd', 'bloquear_exclusao', 'produtor_rural']
            
            for content in contents:
                for item in black_list:
                    if item in content:
                        del content[item]   
            
            
            logger.info(f"Page {page} has been fetched with {records_fetched} records.")
            
            db = Database()
            db.save_into_db(page, resource, contents)
    
    def date_range(
        self,
        resource: str,
        action: str,
        params: dict,
        data_source: str,
        date_init: str
    ):
        dates = generate_date_range(date_init)
        
        for date in dates:
            date_obj = datetime.strptime(date, "%d/%m/%Y")
            last_day = calendar.monthrange(date_obj.year, date_obj.month)[1]
            end_of_month_date = date_obj.replace(day=last_day)
            end_of_month_date = end_of_month_date.strftime("%d/%m/%Y")
            
            params["dPeriodoInicial"] = date
            params["dPeriodoFinal"] = end_of_month_date
            
            body = get_body_params_pagination(
                    action=action, 
                    params=params,
                )
            
            api = Api(
                    url=f"{settings.BASE_URL}{resource}",
                    headers=HEADERS,
                    json=body
                )
            response = api.request(api.post)
            
            records_fetched = len(response.get(f"{data_source}", 0))
            
            logger.info(f"nCodCC: {params['nCodCC']} - Date {date} at {end_of_month_date} has been fetched with {records_fetched} records.")
            
            db = Database()
            # Verificar este lance do parâmetro page em save_into_db
            db.save_into_db(self.page, resource, response)
            
            self.page += 1
            print(f"PAGE: {self.page}")