# sync/api_client.py
import httpx, time
from typing import Iterable, Dict, Any, Optional

class RemoteAPI:
    def __init__(self, base_url: str, token: str, timeout: float = 20.0):
        self.base_url = base_url.rstrip("/")
        self.client = httpx.Client(timeout=timeout, headers={
            "Authorization": f"Bearer {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        })

    # --- Workspaces ---
    def list_workspaces(self, updated_after: Optional[str] = None) -> list[dict]:
        params = {}
        if updated_after:
            params["updated_after"] = updated_after
        return self._get("/workspaces", params=params)

    def find_workspace(self, external_key: dict) -> Optional[dict]:
        # external_key could be {"slug": "..."} or {"name": "..."}
        return self._get_one("/workspaces/find", params=external_key)

    def create_workspace(self, payload: dict) -> dict:
        return self._post("/workspaces", json=payload)

    def update_workspace(self, sync_id: int, payload: dict) -> dict:
        return self._patch(f"/workspaces/{sync_id}", json=payload)

    # --- Pages ---
    def list_pages(self, workspace_sync_id: int, updated_after: Optional[str] = None) -> list[dict]:
        params = {"workspace_id": workspace_sync_id}
        if updated_after:
            params["updated_after"] = updated_after
        return self._get("/pages", params=params)

    def find_page(self, workspace_sync_id: int, page_number: int) -> Optional[dict]:
        return self._get_one("/pages/find", params={
            "workspace_id": workspace_sync_id, "page_number": page_number
        })

    def create_page(self, payload: dict) -> dict:
        return self._post("/pages", json=payload)

    def update_page(self, sync_id: int, payload: dict) -> dict:
        return self._patch(f"/pages/{sync_id}", json=payload)

    # --- Polygons ---
    def list_polygons(self, page_sync_id: int, updated_after: Optional[str] = None) -> list[dict]:
        params = {"page_id": page_sync_id}
        if updated_after:
            params["updated_after"] = updated_after
        return self._get("/polygons", params=params)

    def find_polygon(self, page_sync_id: int, polygon_id: int) -> Optional[dict]:
        return self._get_one("/polygons/find", params={
            "page_id": page_sync_id, "polygon_id": polygon_id
        })

    def create_polygon(self, payload: dict) -> dict:
        return self._post("/polygons", json=payload)

    def update_polygon(self, sync_id: int, payload: dict) -> dict:
        return self._patch(f"/polygons/{sync_id}", json=payload)

    # ---- helpers ----
    def _get(self, path: str, params: dict | None = None):
        return self._retry(lambda: self.client.get(self.base_url + path, params=params)).json()

    def _get_one(self, path: str, params: dict | None = None):
        r = self._retry(lambda: self.client.get(self.base_url + path, params=params))
        if r.status_code == 404:
            return None
        r.raise_for_status()
        return r.json()

    def _post(self, path: str, json: dict):
        return self._retry(lambda: self.client.post(self.base_url + path, json=json)).json()

    def _patch(self, path: str, json: dict):
        return self._retry(lambda: self.client.patch(self.base_url + path, json=json)).json()

    def _retry(self, fn, retries=3, backoff=0.5):
        for i in range(retries):
            try:
                r = fn()
                r.raise_for_status()
                return r
            except (httpx.HTTPError,) as e:
                if i == retries - 1:
                    raise
                time.sleep(backoff * (2 ** i))
