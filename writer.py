# writer.py
from typing import Dict, Any
from atro_client import AtroClient
import re
import base64
import mimetypes
from pathlib import Path
from typing import List, Dict, Any, Optional

def _to_data_url(path: Path, force_ext: Optional[str] = None) -> tuple[str, int, str, str]:
    """
    Reads a local file and returns (data_url, size, mime, ext).
    If force_ext is provided (e.g., 'jpg'), we will adjust the extension and MIME accordingly,
    assuming the file bytes are already in that format. (Do conversion upstream.)
    """
    b = path.read_bytes()
    size = len(b)
    ext = (force_ext or path.suffix.lstrip(".") or "").lower()
    mime = mimetypes.types_map.get("." + ext, "application/octet-stream")
    data_url = f"data:{mime};base64,{base64.b64encode(b).decode('ascii')}"
    return data_url, size, mime, ext

def _ensure_folder_ids(client: AtroClient) -> dict:
    # Per your convention: images in 'Images', docs in 'Documents'
    img = client.find_folder_by_name("Images")
    doc = client.find_folder_by_name("Documents")
    if not img or not doc:
        raise RuntimeError("Required Atro folders not found: 'Images' and/or 'Documents'.")
    return {"images": img["id"], "documents": doc["id"]}

def sync_files_to_seo(
    client: AtroClient,
    seo_id: str,
    image_paths: List[Path],
    doc_paths: List[Path],
    prefer_first_as_primary: bool = True,
    force_image_ext: Optional[str] = None,  # e.g. "jpg" if you convert WEBP upstream
) -> Dict[str, Any]:
    """
    Uploads local images/docs -> /File, links -> /SEOProductFile, sets mainImageId.
    Returns a dict with ids: {"images": [..fileIds..], "documents": [..fileIds..], "primary_image_id": "..."}.
    """
    folders = _ensure_folder_ids(client)
    out = {"images": [], "documents": [], "primary_image_id": None}

    # Images
    for idx, p in enumerate(image_paths or []):
        name = p.name
        # Optional dedupe: same name in same folder
        existing = client.find_file_by_name_in_folder(name, folders["images"])
        if existing:
            file_id = existing["id"]
        else:
            data_url, size, mime, ext = _to_data_url(p, force_ext=force_image_ext)
            created = client.upload_file(
                name=name,
                folder_id=folders["images"],
                data_url=data_url,
                file_size=size,
                mime_type=mime,
                extension=ext,
                hidden=False,
            )
            file_id = created["id"]

        client.link_file_to_seo(seo_id, file_id)
        out["images"].append(file_id)

        if prefer_first_as_primary and idx == 0 and not out["primary_image_id"]:
            client.set_seo_main_image(seo_id, file_id)
            out["primary_image_id"] = file_id

    # Documents
    for p in doc_paths or []:
        name = p.name
        existing = client.find_file_by_name_in_folder(name, folders["documents"])
        if existing:
            file_id = existing["id"]
        else:
            data_url, size, mime, ext = _to_data_url(p, force_ext=None)
            created = client.upload_file(
                name=name,
                folder_id=folders["documents"],
                data_url=data_url,
                file_size=size,
                mime_type=mime,
                extension=ext,
                hidden=False,
            )
            file_id = created["id"]

        client.link_file_to_seo(seo_id, file_id)
        out["documents"].append(file_id)

    return out


def slug_code(name: str) -> str:
    s = re.sub(r'[^0-9A-Za-z]+', '', name or '')
    if not s:
        s = "Spec"
    if s[0].isdigit():
        s = "S" + s
    return s[:64]

def ensure_seo_for_product(
    client: AtroClient,
    product: Dict[str, Any],
    product_to_seo_map: Dict[str, str]
) -> (Dict[str, Any], bool):
    """
    Ensure an SEOProduct exists for the given Product.
    On first create:
      - set SEOProduct.name from Product.name (required by API)
      - copy base fields from Product to SEOProduct using product_to_seo_map
    Returns: (seo_product_dict, created_bool)
    """
    product_id = product["id"]
    existing = client.find_seo_by_product(product_id)
    if existing:
        return existing, False

    payload = {
        "productId": product_id,
        "name": product.get("name") or ""  # REQUIRED by /SEOProduct; Product.name is present in your payloads
    }

    # Copy mapped base fields (Product -> SEOProduct)
    for p_field, seo_field in (product_to_seo_map or {}).items():
        if p_field in product and product[p_field] is not None:
            payload[seo_field] = product[p_field]

    created = client.create_seo(payload)
    return created, True

def patch_ai_fields(
    client: AtroClient,
    seo_id: str,
    ai: Dict[str, Any],
    ai_field_map: Dict[str, str]
) -> None:
    """
    Patch only the mapped AI fields into the SEOProduct.
    ai_field_map: dict of input_key -> API field name on SEOProduct
    """
    payload = {}
    for k, api_field in ai_field_map.items():
        if k in ai and ai[k] is not None:
            payload[api_field] = ai[k]
    if payload:
        client.patch_seo(seo_id, payload)

def upsert_specs_and_values(
    client: AtroClient,
    seo_id: str,
    specs_dict: Dict[str, str],
    cached_specs: Dict[str, Any]
) -> None:
    """
    For each spec key/value:
      - ensure a Specification exists (create if missing)
      - create a SpecificationValue linked to the SEOProduct
    cached_specs: name_lower -> spec_obj (must contain 'id' and 'name')
    """
    for k, v in (specs_dict or {}).items():
        if not k or v is None:
            continue
        key_lower = k.strip().lower()
        spec = cached_specs.get(key_lower)
        if not spec:
            spec = client.create_spec(k.strip(), slug_code(k))
            cached_specs[key_lower] = spec
        client.create_spec_value(
            specification_id=spec["id"],
            seo_product_id=seo_id,
            value=str(v)
        )
