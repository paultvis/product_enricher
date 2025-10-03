import base64
import requests
import threading
from urllib.parse import urlencode


class AtroClient:
    def __init__(self, base_url: str, user: str, password: str, timeout: int = 15):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.timeout = timeout
        self._token = None
        self._lock = threading.Lock()

    def _auth(self):
        with self._lock:
            if self._token:
                return self._token
            url = f"{self.base_url}/api/v1/App/user-identity"
            r = requests.post(
                url,
                json={"userName": self.user, "password": self.password},
                timeout=self.timeout,
            )
            r.raise_for_status()
            data = r.json()
            self._token = data.get("token")
            if not self._token:
                raise RuntimeError("Failed to obtain auth token.")
            return self._token

    def _headers(self):
        return {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "X-Auth-Token": self._auth(),
        }

    def _request(self, method: str, path: str, **kwargs) -> requests.Response:
        url = f"{self.base_url}{path}"
        # First attempt
        r = requests.request(method, url, headers=self._headers(), timeout=self.timeout, **kwargs)
        if r.status_code == 401:
            # Token may have expired; refresh once
            with self._lock:
                self._token = None
            r = requests.request(method, url, headers=self._headers(), timeout=self.timeout, **kwargs)
        r.raise_for_status()
        return r

    @staticmethod
    def _json_or_empty(r: requests.Response):
        """
        Safely return JSON if present, otherwise {}.
        Handles 204 No Content and non-JSON responses gracefully.
        """
        if r.status_code == 204:
            return {}
        ct = (r.headers.get("Content-Type") or "").lower()
        if "application/json" not in ct:
            return {}
        if not (r.text or "").strip():
            return {}
        try:
            return r.json()
        except Exception:
            return {}

    # ---- Product
    def get_product_by_sku(self, sku: str, select_fields=None):
        params = {
            "maxSize": 50, "offset": 0, "sortBy": "sku", "asc": "false",
            "where[0][type]": "equals",
            "where[0][attribute]": "sku",
            "where[0][value]": sku
        }
        if select_fields:
            params["select"] = ",".join(select_fields)
        q = "?" + urlencode(params)
        r = self._request("GET", f"/api/v1/Product{q}")
        data = r.json()
        lst = data.get("list") or []
        return lst[0] if lst else None

    def get_product_by_brand_mpn(self, brand: str, mpn: str, select_fields=None):
        params = {
            "maxSize": 50, "offset": 0, "sortBy": "createdAt", "asc": "false",
            "where[0][type]": "equals",
            "where[0][attribute]": "brand",
            "where[0][value]": brand,
            "where[1][type]": "equals",
            "where[1][attribute]": "mpn",
            "where[1][value]": mpn,
        }
        if select_fields:
            params["select"] = ",".join(select_fields)
        q = "?" + urlencode(params)
        r = self._request("GET", f"/api/v1/Product{q}")
        data = r.json()
        lst = data.get("list") or []
        return lst[0] if lst else None

    def create_product(self, payload: dict):
        r = self._request("POST", "/api/v1/Product", json=payload)
        return r.json()

    def patch_product(self, product_id: str, payload: dict):
        r = self._request("PATCH", f"/api/v1/Product/{product_id}", json=payload)
        return self._json_or_empty(r)

    # ---- SEOProduct
    def find_seo_by_product(self, product_id: str, select_fields=None):
        params = {
            "maxSize": 50, "offset": 0, "sortBy": "createdAt", "asc": "false",
            "where[0][type]": "equals",
            "where[0][attribute]": "productId",
            "where[0][value]": product_id
        }
        if select_fields:
            params["select"] = ",".join(select_fields)
        q = "?" + urlencode(params)
        r = self._request("GET", f"/api/v1/SEOProduct{q}")
        data = r.json()
        lst = data.get("list") or []
        return lst[0] if lst else None

    def create_seo(self, payload: dict):
        r = self._request("POST", "/api/v1/SEOProduct", json=payload)
        return r.json()

    def patch_seo(self, seo_id: str, payload: dict):
        r = self._request("PATCH", f"/api/v1/SEOProduct/{seo_id}", json=payload)
        return self._json_or_empty(r)

    # ---- Specification keys
    def list_specs(self, page_size=200):
        # page through all specs, return {name_lower: {id, name, code}}
        out = {}
        offset = 0
        while True:
            params = {"select": "name,createdAt", "maxSize": page_size, "offset": offset, "sortBy": "name", "asc": "true"}
            q = "?" + urlencode(params)
            r = self._request("GET", f"/api/v1/Specification{q}")
            data = r.json()
            lst = data.get("list") or []
            for it in lst:
                nm = (it.get("name") or "").strip()
                if not nm:
                    continue
                out[nm.lower()] = {"id": it.get("id"), "name": nm, "code": it.get("code")}
            if len(lst) < page_size:
                break
            offset += page_size
        return out

    def create_spec(self, name: str, code: str):
        payload = {"name": name, "code": code}
        r = self._request("POST", "/api/v1/Specification", json=payload)
        return r.json()

    def create_spec_value(self, specification_id: str, seo_product_id: str, value: str):
        payload = {"specificationId": specification_id, "sEOProductId": seo_product_id, "value": value}
        r = self._request("POST", "/api/v1/SpecificationValue", json=payload)
        return r.json()

    # ---- Folders & Files
    def find_folder_by_name(self, name: str):
        params = {
            "maxSize": 50, "offset": 0, "sortBy": "createdAt", "asc": "false",
            "where[0][type]": "equals",
            "where[0][attribute]": "name",
            "where[0][value]": name
        }
        q = "?" + urlencode(params)
        r = self._request("GET", f"/api/v1/Folder{q}")
        data = r.json()
        lst = data.get("list") or []
        return lst[0] if lst else None

    def find_file_by_name_in_folder(self, name: str, folder_id: str):
        params = {
            "maxSize": 50, "offset": 0, "sortBy": "createdAt", "asc": "false",
            "where[0][type]": "equals",
            "where[0][attribute]": "name",
            "where[0][value]": name,
            "where[1][type]": "equals",
            "where[1][attribute]": "folderId",
            "where[1][value]": folder_id
        }
        q = "?" + urlencode(params)
        r = self._request("GET", f"/api/v1/File{q}")
        data = r.json()
        lst = data.get("list") or []
        return lst[0] if lst else None

    def upload_file(self, name: str, folder_id: str, data_url: str, file_size: int, mime_type: str, extension: str, hidden: bool = False):
        payload = {
            "name": name,
            "folderId": folder_id,
            "type": mime_type,
            "size": file_size,
            "extension": extension,
            "hidden": hidden,
            "source": "upload",
            "data": data_url,
        }
        r = self._request("POST", "/api/v1/File?silent=true", json=payload)
        return r.json()

    # ---- Link File → SEOProduct
    def link_file_to_seo(self, seo_product_id: str, file_id: str):
        payload = {"sEOProductId": seo_product_id, "fileId": file_id}
        r = self._request("POST", "/api/v1/SEOProductFile", json=payload)
        return r.json()

    # ---- Set primary image on SEOProduct
    def set_seo_main_image(self, seo_product_id: str, file_id: str):
        payload = {"mainImageId": file_id}
        r = self._request("PATCH", f"/api/v1/SEOProduct/{seo_product_id}", json=payload)
        return self._json_or_empty(r)
