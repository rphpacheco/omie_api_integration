from typing import Literal
from loguru import logger

from src.api import Api
from src.db import Database
from src.utils.tools import get_total_of_pages, get_body_params_pagination

from src.utils.constants import HEADERS
from src.config import Settings
settings = Settings()

class PaginationController:
    # TODOS 
    # 1. OTIMIZAR PAGINAÇÃO PERPAGE
    # 2. RESOLVER A BLACK LIST
    # 3. IMPLAMENTAR PAGINAÇÃO POR PERIODO
    def __init__(self) -> None:
        ...

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
                return self.date_range()
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