from src.endpoints import Endpoints
from src.controllers.paginations import PaginationController

endpoints = Endpoints()
endpoints = endpoints.get_all()

for endpoint in endpoints:
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