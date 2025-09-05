# sync/api_client_tto.py
from __future__ import annotations
import json
import time
from typing import Any, Dict, List, Optional, Sequence, Union

import httpx


# ---- Fixed Logic Apps endpoints (from your docs) ----
TTO_URLS = {
    "create_project": "https://prod-137.westeurope.logic.azure.com:443/workflows/c6d4e09bf0934be8baf9bcacfa635f1e/triggers/manual/paths/invoke?api-version=2016-06-01&sp=/triggers/manual/run&sv=1.0&sig=-9ASkHHVb14Kbm0q8hdbk-Oz0Fv2VtrAYzFtpdoENv0",
    "update_project": "https://prod-186.westeurope.logic.azure.com:443/workflows/d421ced654cd4367a944ab4ee4873d4a/triggers/manual/paths/invoke?api-version=2016-06-01&sp=/triggers/manual/run&sv=1.0&sig=V3_x3_jofjqfFbj_2gvU9pN-qIB5eRizUMFagPTMOvA",
    "delete_project": "https://prod-190.westeurope.logic.azure.com:443/workflows/4327d9f024734a2884205059dcd998b6/triggers/manual/paths/invoke?api-version=2016-06-01&sp=/triggers/manual/run&sv=1.0&sig=9zI3T8hImfV7zY96mv1-jnXHYbk0hF0J9o71SaIjaQI",
    "list_projects_by_user": "https://prod-08.westeurope.logic.azure.com:443/workflows/368d33d4128d4f6bb524817196d70d39/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=yEJc1GkYDc_KKQ93x8PJhptgngzHJ0IjnwOhnhj6Gho",
    "create_page": "https://prod-197.westeurope.logic.azure.com:443/workflows/747e6ef2acba4cdca74b8b2134cbf223/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=NtRHkTVud32b68XuemACUIGjdmk5hkPJbRI424CBhEs",
    "update_page": "https://prod-62.westeurope.logic.azure.com:443/workflows/a2a523ed5fc44b7bb0ed32a48f024b58/triggers/manual/paths/invoke?api-version=2016-06-01&sp=/triggers/manual/run&sv=1.0&sig=j2vLCFUaFOrY_j5z5iy4qlf__VRogW0iz_3Wj78Ffj8",
    "list_pages_for_project": "https://prod-87.westeurope.logic.azure.com:443/workflows/def03cf7c63a4f02884763fa482f725a/triggers/manual/paths/invoke?api-version=2016-06-01&sp=/triggers/manual/run&sv=1.0&sig=iC8Hn08LvacJGikPDyfwlTv9s4Fvyr9Out92d4-5u8I",
    "create_polygon": "https://prod-122.westeurope.logic.azure.com:443/workflows/47683654c87f4cb580296236bcd0b538/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=YSQRcK9d-6jnZyW756qgquDEn3-O8oYoLwG--D5hqE4",
    "update_polygon": "https://prod-107.westeurope.logic.azure.com:443/workflows/bcba09ce225148f39480b448113fc527/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=TP92kBPy3saR1IwVCelQ8EP61GXT3hrCxys70k1eMr8",
    "list_polygons_for_page": "https://prod-23.westeurope.logic.azure.com:443/workflows/e96909c0f88c485bb506cc0580545ae7/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=6fIe1lfGdPzufok-E6x6o3q334s8zDcBnNygfBGk8M4",
    "delete_polygon": "https://6a3cb7afb65948e28f25121cd9bace.bd.environment.api.powerplatform.com/powerautomate/automations/direct/workflows/799c43d162d947b692829904e3b8765b/triggers/manual/paths/invoke?api-version=1&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=Qr6n-4en16kYPhrxMzYmyXqiv82abTe8_a-nMPflqdA",
    # optional bulk test endpoint
    "bulk_update_polygons": "https://prod-252.westeurope.logic.azure.com:443/workflows/426b3fabf5b7411c80c9be7260921987/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=IKKLlx4O9Q27u8YXo9Ux35ngA3Z3-0grgCkrbtqeF5I",
    # auth endpoints (if you need them)
    "send_access_code": "https://prod-65.westeurope.logic.azure.com:443/workflows/2c17709dd3db43428d0959b89c753c07/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=wEvOtQReESJiEX5bFkFxQ_i-KTmuu5UoQPSlPPX-j44",
    "check_user_access": "https://prod-235.westeurope.logic.azure.com:443/workflows/889ab0d418624d68b384c9be6d4d0715/triggers/manual/paths/invoke?api-version=2016-06-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=_lPxc-sGDRdJj8fPJtFdkMaCNwCCuTqMe-4So_gGyR8",
}


class TTOApi:
    """
    Thin client for the TTO Logic Apps endpoints you provided.
    All calls include the required `auth_code`. For some endpoints we also send `user_email`
    and use `actor_email` as created_by/modified_by/deleted_by.
    """

    def __init__(
        self, auth_code: str, user_email: str, actor_email: str, timeout: float = 20.0
    ):
        self.auth_code = auth_code
        self.user_email = user_email
        self.actor_email = actor_email
        self.client = httpx.Client(
            timeout=timeout,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )

    # ------------ Projects ------------
    def list_projects_by_user(self) -> List[Dict[str, Any]]:
        payload = {"user_email": self.user_email, "auth_code": self.auth_code}
        data = self._post(TTO_URLS["list_projects_by_user"], payload)
        # Some Logic Apps return a single dict when there's one row; normalize to list
        if isinstance(data, dict) and "project_id" in data:
            return [data]
        return data or []

    def create_project(self, project_name: str, file_link: str = "") -> int:
        payload = {
            "project_name": project_name,
            "created_by": self.actor_email,
            "file_link": file_link,
            "auth_code": self.auth_code,
        }
        data = self._post(TTO_URLS["create_project"], payload)
        return int(data["new_id"])

    def update_project(
        self,
        project_id: int,
        project_name: Optional[str] = None,
        project_status: Optional[str] = None,
    ) -> None:
        payload = {
            "project_id": int(project_id),
            "project_name": project_name or "",
            "project_status": project_status or "",
            "modified_by": self.actor_email,
            "auth_code": self.auth_code,
        }
        self._post(TTO_URLS["update_project"], payload)

    def delete_project(self, project_id: int) -> None:
        payload = {
            "project_id": int(project_id),
            "deleted_by": self.actor_email,
            "auth_code": self.auth_code,
        }
        self._post(TTO_URLS["delete_project"], payload)

    # ------------ Pages ------------
    def list_pages_for_project(self, project_id: int) -> List[Dict[str, Any]]:
        payload = {
            "project_id": int(project_id),
            "user_email": self.user_email,
            "auth_code": self.auth_code,
        }
        data = self._post(TTO_URLS["list_pages_for_project"], payload)
        # If there are no records, service may return {"message": "No records found"}
        if isinstance(data, dict) and data.get("message"):
            return []
        return data or []

    def create_page(
        self,
        *,
        project_id: int,
        page_nb: int,
        picture_link: str = "",
        scale: str = "",
        unit: str = "",
        image_height: int = 0,
        image_width: int = 0,
        pdf_height: int = 0,
        pdf_width: int = 0,
        json_str: str = "",
    ) -> int:
        payload = {
            "project_id": int(project_id),
            "page_nb": int(page_nb),
            "picture_link": picture_link,
            "scale": scale,
            "unit": unit,
            "image_height": int(image_height),
            "image_width": int(image_width),
            "pdf_height": int(pdf_height),
            "pdf_width": int(pdf_width),
            "json": json_str,
            "created_by": self.actor_email,
            "auth_code": self.auth_code,
        }
        print(payload)
        data = self._post(TTO_URLS["create_page"], payload)
        return int(data["new_id"])

    def update_page(
        self,
        *,
        page_id: int,
        page_nb: Optional[int] = None,
        picture_link: Optional[str] = None,
        scale: Optional[float] = None,
        confirmed_scale: Optional[bool] = None,
        unit: Optional[str] = None,
        image_height: Optional[int] = None,
        image_width: Optional[int] = None,
        pdf_height: Optional[int] = None,
        pdf_width: Optional[int] = None,
        json_str: Optional[str] = None,
    ) -> None:
        payload = {
            "page_id": int(page_id),
            "page_nb": int(page_nb) if page_nb is not None else None,
            "picture_link": picture_link if picture_link is not None else "",
            "scale": f"{scale}",
            "confirmed_scale": "Yes" if bool(confirmed_scale) else "No",
            "unit": unit if unit is not None else "",
            "image_height": int(image_height) if image_height is not None else 0,
            "image_width": int(image_width) if image_width is not None else 0,
            "pdf_height": int(pdf_height) if pdf_height is not None else 0,
            "pdf_width": int(pdf_width) if pdf_width is not None else 0,
            "json": json_str if json_str is not None else "",
            "modified_by": self.actor_email,
            "auth_code": self.auth_code,
        }
        self._post(TTO_URLS["update_page"], payload)

    # ------------ Polygons ------------
    @staticmethod
    def _serialize_vertices(vertices: Sequence[Sequence[float]]) -> str:
        # API examples show a string; safest is proper JSON string
        return json.dumps(vertices, separators=(",", ":"))

    def list_polygons_for_page(self, page_id: int) -> Optional[List[Dict[str, Any]]]:
        payload = {
            "page_id": int(page_id),
            "user_email": self.user_email,
            "auth_code": self.auth_code,
        }
        data = self._post(TTO_URLS["list_polygons_for_page"], payload)

        # Normalize "no records" shape
        if isinstance(data, dict) and data.get("message") == "No records found":
            return None
        if data is None:
            return None

        # Accept only a JSON array; anything else -> None
        if isinstance(data, list):
            return data

        # If the service returned a JSON *string*, accept only if it's an array string
        if isinstance(data, str):
            s = data.strip()
            if s.startswith("[") and s.endswith("]"):
                try:
                    parsed = json.loads(s)
                    return parsed if isinstance(parsed, list) else None
                except ValueError:
                    return None
            return None

        # Single object or any other type -> None
        return None

    def create_polygon(
        self,
        *,
        project_id: int,
        page_id: int,
        poly_id: Union[str, int],
        vertices: Sequence[Sequence[float]],
        total_vertices: int,
    ) -> int:
        payload = {
            "project_id": int(project_id),
            "page_id": int(page_id),
            "polyID": str(poly_id),  # NOTE: create uses polyID (camelCase, capital D)
            "vertices": self._serialize_vertices(vertices),
            "totalVertices": int(
                total_vertices
            ),  # NOTE: create uses totalVertices (camelCase)
            "created_by": self.actor_email,
            "auth_code": self.auth_code,
        }
        data = self._post(TTO_URLS["create_polygon"], payload)
        return int(data["new_id"])

    def update_polygon(
        self,
        *,
        polygon_id: int,
        poly_id: Union[str, int],
        vertices: Sequence[Sequence[float]],
        total_vertices: int,
    ) -> None:
        payload = {
            "polygon_id": int(polygon_id),  # remote primary id
            "vertices": self._serialize_vertices(vertices),
            "total_vertices": int(
                total_vertices
            ),  # NOTE: update uses total_vertices (snake_case)
            "poly_id": str(poly_id),  # NOTE: update uses poly_id (snake_case)
            "modified_by": self.actor_email,
            "auth_code": self.auth_code,
        }
        self._post(TTO_URLS["update_polygon"], payload)

    def delete_polygon(self, polygon_id: int, project_id: int, page_id: int, poly_id: str) -> None:
        """
        Delete a polygon from TTO by its remote polygon_id.
        Uses the bulk delete API that accepts an array of polygons.
        """
        payload = {
            "auth_code": self.auth_code,
            "deleted_by": self.actor_email,
            "polygon_array": [
                {
                    "polygon_id": int(polygon_id),
                    "project_id": int(project_id),
                    "page_id": int(page_id),
                    "poly_id": str(poly_id)
                }
            ]
        }
        self._post(TTO_URLS["delete_polygon"], payload)

    def bulk_delete_polygons(self, polygon_array: List[Dict[str, Any]]) -> None:
        """
        Delete multiple polygons from TTO using the bulk delete API.
        """
        payload = {
            "auth_code": self.auth_code,
            "deleted_by": self.actor_email,
            "polygon_array": polygon_array
        }
        self._post(TTO_URLS["delete_polygon"], payload)

    def bulk_update_polygons(self, polygon_array: List[Dict[str, Any]]) -> None:
        # polygon_array items must match the test schema you pasted
        payload = {
            "auth_code": self.auth_code,
            "modified_by": self.actor_email,
            "polygon_array": polygon_array,
        }
        self._post(TTO_URLS["bulk_update_polygons"], payload)

    # ------------ Auth (optional) ------------
    def send_access_code(self, medium: str) -> None:
        payload = {
            "user_login": self.user_email,
            "medium": medium,
            "auth_code": self.auth_code,
        }
        self._post(TTO_URLS["send_access_code"], payload)

    def check_user_access(self, password: str) -> Dict[str, Any]:
        payload = {
            "user_login": self.user_email,
            "user_pwd": password,
            "auth_code": self.auth_code,
        }
        return self._post(TTO_URLS["check_user_access"], payload)

    # in TTOApi
    def _post(
        self,
        url: str,
        json_payload: Dict[str, Any],
        retries: int = 3,
        backoff: float = 0.5,
        ensure_list: bool = False,
    ):
        for i in range(retries):
            try:
                resp = self.client.post(url, json=json_payload)
                resp.raise_for_status()

                text = (resp.text or "").strip()
                if not text:
                    return [] if ensure_list else None

                # 1) try standard JSON path
                try:
                    data = resp.json()
                except ValueError:
                    data = None

                # 2) try parsing the raw text
                if data is None:
                    try:
                        data = json.loads(text)
                    except ValueError:
                        # 3) fallback: single object string -> wrap with [] and parse
                        if ensure_list and text.startswith("{") and text.endswith("}"):
                            try:
                                data = json.loads("[" + text + "]")
                            except ValueError:
                                return []
                        else:
                            return [] if ensure_list else None

                # Normalize "No records found"
                if isinstance(data, dict) and data.get("message") == "No records found":
                    return []

                # If caller expects a list, normalize shapes
                if ensure_list:
                    if isinstance(data, list):
                        return data
                    if isinstance(data, dict):
                        return [data]
                    if isinstance(data, str):  # sometimes APIs return a JSON string
                        s = data.strip()
                        try:
                            parsed = json.loads(s)
                        except ValueError:
                            if s.startswith("{") and s.endswith("}"):
                                try:
                                    return json.loads("[" + s + "]")
                                except ValueError:
                                    return []
                            return []
                        return parsed if isinstance(parsed, list) else [parsed]

                return data

            except httpx.HTTPError:
                if i == retries - 1:
                    raise
                time.sleep(backoff * (2**i))
