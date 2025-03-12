import json
from typing import Optional


def read_json(path: str) -> dict:
    with open(path, "r") as file:
        return json.load(file)


class Endpoints:
    def __init__(self) -> None:
        self.path = "src/endpoints/data/data.json"
        self.endpoints = read_json(self.path)

    def get_endpoint(
        self, resource: Optional[str] = None, action: Optional[str] = None
    ) -> dict:
        if action:
            for endpoint in self.endpoints:
                if endpoint.get("action") == action:
                    return [endpoint]
        elif resource:
            for endpoint in self.endpoints:
                if endpoint.get("resources") == resource:
                    return [endpoint]
        else:
            raise Exception("Resource or action not found")

    def get_all(self) -> list:
        return self.endpoints
