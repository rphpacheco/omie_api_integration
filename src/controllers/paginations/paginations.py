import calendar
from datetime import datetime
from typing import Literal
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.api import Api
from src.db import Database
from src.utils.tools import get_total_of_pages, get_body_params_pagination, generate_date_range
from src.utils.constants import HEADERS
from src.config import Settings

settings = Settings()

class PaginationController:
    def __init__(self) -> None:
        self.page = 1
        self.batch_size = 10  # Number of pages to process in each batch
        self.max_workers = 5  # Number of concurrent workers

    def fetch_page(self, page: int, resource: str, action: str, params: dict, 
                  page_label: str, data_source: str, records_label: str) -> tuple:
        """Fetch a single page of data from the API"""
        try:
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
            
            records_fetched = response.get(records_label, 0)
            contents = response.get(data_source, [])
            
            # Remove blacklisted fields
            black_list = ['tags', 'recomendacoes', 'homepage', 'fax_ddd', 'bloquear_exclusao', 'produtor_rural']
            for content in contents:
                for item in black_list:
                    if item in content:
                        del content[item]
                        
            logger.info(f"Page {page} has been fetched with {records_fetched} records.")
            return page, contents
            
        except Exception as e:
            logger.error(f"Error fetching page {page}: {e}")
            return page, None

    def process_batch(self, batch_pages: list, resource: str, db: Database) -> None:
        """Process a batch of pages and save to database"""
        try:
            all_contents = []
            for page, contents in batch_pages:
                if contents:
                    all_contents.extend(contents)
            
            if all_contents:
                if batch_pages[0][0] == 1:  # First batch
                    db.save_into_db(1, resource, all_contents, replace=True)
                else:
                    db.save_into_db(batch_pages[0][0], resource, all_contents, replace=False)
                    
        except Exception as e:
            logger.error(f"Error processing batch starting with page {batch_pages[0][0]}: {e}")

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
        
        db = Database()
        current_batch = []
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all pages for processing
            future_to_page = {
                executor.submit(
                    self.fetch_page, 
                    page, 
                    resource, 
                    action, 
                    params.copy(),  # Create a copy of params to avoid race conditions
                    page_label,
                    data_source,
                    records_label
                ): page for page in range(1, total_of_pages + 1)
            }
            
            for future in as_completed(future_to_page):
                page, contents = future.result()
                current_batch.append((page, contents))
                
                # Process batch when it reaches batch_size or is the last batch
                if len(current_batch) >= self.batch_size or page == total_of_pages:
                    self.process_batch(current_batch, resource, db)
                    current_batch = []

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