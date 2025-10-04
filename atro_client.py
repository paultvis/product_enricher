import base64
import requests
import threading
import time
from urllib.parse import urlencode

#---- nudge
class AtroClient:
    def __init__(self, base_url: str, user: str, password: str, timeout: int = 15, upload_timeout: int | None = None, max_retries: int = 2):
        self.base_url = base_url.rstrip("/")
        self.user = user
        self.password = password
        self.timeout = timeout
        self.upload_timeout = upload_timeout or timeout
        self.max_retries = max(0, int(max_retries))
        self._token = None
        self._lock = threading.Lock()

    def _auth(self):
        creds = f"{self.user}:{self.password}".encode("iso-8859-1")
        headers = {
            "Accept": "application/json",
            "Authorization": "Basic " + base64.b64encode(creds).decode("ascii"),
            "Authorization-Token-Only": "true",
            "Authorization-Token-Lifetime": "0",
            "Authorization-Token-Idletime": "0",
        }
        r = requests.get(f"{self.base_url}/api/v1/App/user", headers=headers, timeout=self.timeout)
        r.raise_for_status()
        self._token = r.json()["authorizationToken"]

    def _headers(self):
        if not self._token:
            with self._lock:
                if not self._token:
                    self._auth()
        return {"Accept": "application/json", "Authorization": f"Bearer {self._token}"}

    def _request(self, method, path, **kwargs):
        """Resilient request with re-auth + limited retries for transient errors/timeouts."""
        url = f"{self.base_url}{path}"
        timeout = kwargs.pop("timeout", self.timeout)
        attempt = 0
        while True:
            try:
                r = requests.request(method, url, headers=self._headers(), timeout=timeout, **kwargs)
                if r.status_code == 401:
                    # refresh token once
                    with self._lock:
                        self._auth()
                    r = requests.request(method, url, headers=self._headers(), timeout=timeout, **kwargs)
                # retry on gateway/server hiccups
                if r.status_code in (502, 503, 504) and attempt < self.max_retries:
                    time.sleep(2 * (attempt + 1))
                    attempt += 1
                    continue
                r.raise_for_status()
                return r
            except requests.exceptions.RequestException:
                if attempt < self.max_retries:
                    time.sleep(2 * (attempt + 1))
                    attempt += 1
                    continue
                raise

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
        return (data.get("list") or [None])[0]

    def get_product_by_id(self, product_id: str, select_fields=None):
        sel = f"?select={','.join(select_fields)}" if select_fields else ""
        r = self._request("GET", f"/api/v1/Product/{product_id}{sel}")
        return r.json()

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
            for item in data.get("list", []):
                nm = (item.get("name") or "").strip()
                if nm:
                    out[nm.lower()] = item
            if len(data.get("list", [])) < page_size:
                break
            offset += page_size
        return out

    def create_spec(self, name: str, code: str):
        payload = {"name": name, "code": code}
        r = self._request("POST", "/api/v1/Specification", json=payload)
        return r.json()

    # ---- Specification value (link to SEOProduct)
    def create_spec_value(self, specification_id: str, seo_product_id: str, value: str):
        payload = {"specificationId": specification_id, "sEOProductId": seo_product_id, "value": value}
        r = self._request("POST", "/api/v1/SpecificationValue", json=payload)
        return r.json()
        
    # ---- Product by MPN
    def get_product_by_mpn(self, mpn: str, select_fields=None):
        params = {
            "maxSize": 50, "offset": 0, "sortBy": "name", "asc": "true",
            "where[0][type]": "equals",
            "where[0][attribute]": "mpn",
            "where[0][value]": mpn
        }
        if select_fields:
            params["select"] = ",".join(select_fields)
        q = "?" + urlencode(params)
        r = self._request("GET", f"/api/v1/Product{q}")
        data = r.json()
        return (data.get("list") or [None])[0]

    # ---- Folder lookup
    def find_folder_by_name(self, name: str):
        params = {
            "maxSize": 50, "offset": 0, "sortBy": "name", "asc": "true",
            "where[0][type]": "equals",
            "where[0][attribute]": "name",
            "where[0][value]": name
        }
        q = "?" + urlencode(params)
        r = self._request("GET", f"/api/v1/Folder{q}")
        data = r.json()
        lst = data.get("list") or []
        return lst[0] if lst else None

    # Optional: avoid dup uploads by name within a folder
    def find_file_by_name_in_folder(self, name: str, folder_id: str):
        params = {
            "maxSize": 50, "offset": 0, "sortBy": "createdAt", "asc": "false",
            "where[0][type]": "equals", "where[0][attribute]": "name", "where[0][value]": name,
            "where[1][type]": "equals", "where[1][attribute]": "folderId", "where[1][value]": folder_id,
        }
        q = "?" + urlencode(params)
        r = self._request("GET", f"/api/v1/File{q}")
        data = r.json()
        lst = data.get("list") or []
        return lst[0] if lst else None

    # ---- File upload
    def upload_file(
        self,
        *,
        name: str,
        folder_id: str,
        data_url: str,
        file_size: int | None = None,
        mime_type: str | None = None,
        extension: str | None = None,
        hidden: bool = False,
        tags: str | None = None,
    ):
        payload = {
            "name": name,
            "hidden": bool(hidden),
            "folderId": folder_id,
            "typeId": "file",                     # per your note: always 'file'
            "fileContents": data_url,            # data:<mime>;base64,<...>
        }
        if file_size is not None: payload["fileSize"] = int(file_size)
        if mime_type: payload["mimeType"] = mime_type
        if extension: payload["extension"] = extension.lstrip(".").lower()
        if tags: payload["tags"] = tags

        # Atro supports ?silent=true on create as you shared
        r = self._request("POST", "/api/v1/File?silent=true", json=payload, timeout=self.upload_timeout)
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
