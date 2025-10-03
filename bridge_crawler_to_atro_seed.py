#!/usr/bin/env python3
"""
bridge_crawler_to_atro_seed.py

Bridges crawler outputs (raw_products/raw_assets) → AtroPIM (Product/SEOProduct),
uploads images and PDFs, patches brand content/specs, then seeds the enrichment queue with SKUs.

This version:
- Seeds SEOProduct base fields from Product on first create:
  Product.shortdescription2  -> SEOProduct.vMShortdescription
  Product.longDescription    -> SEOProduct.longDescription
- Writes crawler HTML into SEOProduct.brandcontent (aligns with enricher input).
- Defaults to --latest-run when no mode flag is provided.
- Enqueues to enrichment_queue with brand_name when that column exists, and backfills brand_name on existing rows.

Updates in this revision:
- Use pretty sibling names for uploads (images + PDFs).
- Robust asset lookup by URL for PDFs:
  * exact URL
  * URL with query/hash stripped
  * alternate scheme (https↔http)
  * path-based LIKE (suffix)
  * asset_type widened to include '', NULL, 'pdf', 'application/pdf'
  * filesystem fallback under ASSET_ROOT/doc for *__<basename> (then <basename>)
- Upload images to **Product Images**; PDFs to **Product Documents**.

Multithreading update:
- --workers (default 4) to process products in parallel via ThreadPoolExecutor
- --upload-parallel (default 3) global semaphore to throttle concurrent file uploads
- Per-thread AtroClient and DB connections (crawler + enrich)
- One-time Atro specs cache shared across threads (read-only)
"""
#----nudge
import os
import sys
import json
import logging
import argparse
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import mysql.connector

from atro_client import AtroClient
import writer  # ensure_seo_for_product, upsert_specs_and_values, _to_data_url

LOG = logging.getLogger("bridge")
LOG.setLevel(logging.INFO)
_ch = logging.StreamHandler(sys.stdout)
_ch.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
LOG.addHandler(_ch)

# ---------- ENV ----------
ATRO_BASE_URL = os.getenv("ATRO_BASE_URL", "http://192.168.0.29").strip().rstrip("/")
ATRO_USER     = os.getenv("ATRO_USER", "pault")
ATRO_PASS     = os.getenv("ATRO_PASS", "Albatr0ss22!")

CR_HOST = os.getenv("CRAWLER_DB_HOST", "127.0.0.1")
CR_USER = os.getenv("CRAWLER_DB_USER", "crawler")
CR_PASS = os.getenv("CRAWLER_DB_PASS", "Albatr0ss22!")
CR_NAME = os.getenv("CRAWLER_DB_NAME", "vwr")

EN_HOST = os.getenv("ENRICH_DB_HOST", "192.168.0.28")
EN_USER = os.getenv("ENRICH_DB_USER", "root")
EN_PASS = os.getenv("ENRICH_DB_PASS", "Albatr0ss22!")
EN_NAME = os.getenv("ENRICH_DB_NAME", "vwr")
EN_TABLE = os.getenv("ENRICH_DB_TABLE", "enrichment_queue")

ASSET_ROOT = os.getenv("ASSET_ROOT", "/opt/brand_crawler/data/assets")
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "0"))  # 0 = unlimited

# Global throttles / caches
_UPLOAD_SEM: Optional[threading.Semaphore] = None
_SPECS_CACHE: Optional[Dict[str, Any]] = None  # read-only map from Atro (list_specs())


# --- Product → SEOProduct base field mapping (seed on SEO creation only) ---
PRODUCT_TO_SEO_FIELD_MAP = {
    "shortdescription2": "vMShortdescription",
    "longDescription":   "longDescription",
}


def _mysql_connect(host, user, password, database):
    return mysql.connector.connect(
        host=host, user=user, password=password, database=database,
        use_pure=True, connection_timeout=10
    )


# ---------- General helpers ----------
def get_table_columns(cnx, table_schema: str, table_name: str) -> List[str]:
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT COLUMN_NAME
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
        """,
        (table_schema, table_name)
    )
    cols = [r[0] for r in cur.fetchall()]
    cur.close()
    return cols


# ---------- Atro helpers ----------
def ensure_brand(client: AtroClient, brand_name: str) -> Dict[str, Any]:
    if not brand_name:
        raise ValueError("brand_name required")

    if hasattr(writer, "urlencode"):
        q = "?" + writer.urlencode({
            "maxSize": 50, "offset": 0, "sortBy": "name", "asc": "true",
            "where[0][type]": "equals",
            "where[0][attribute]": "name",
            "where[0][value]": brand_name.strip()
        })
    else:
        from urllib.parse import urlencode
        q = "?" + urlencode({
            "maxSize": 50, "offset": 0, "sortBy": "name", "asc": "true",
            "where[0][type]": "equals",
            "where[0][attribute]": "name",
            "where[0][value]": brand_name.strip()
        })

    r = client._request("GET", f"/api/v1/Brand{q}")
    data = r.json()
    lst = data.get("list") or []
    if lst:
        return lst[0]

    payload = {"name": brand_name.strip()}
    r = client._request("POST", "/api/v1/Brand", json=payload)
    return r.json()


def ensure_product_by_mpn(
    client: AtroClient,
    brand: Dict[str, Any],
    mpn: str,
    name: Optional[str],
    sku_preferred: Optional[str]
) -> Dict[str, Any]:
    """
    Find Product by MPN; if missing, create minimal Product with sku=sku_preferred (or mpn),
    mpn=mpn, brandId=brand.id. If found but SKU missing, set sku=sku_preferred (or mpn).
    """
    prod = client.get_product_by_mpn(mpn, select_fields=["id", "name", "sku", "mpn", "brandId"])
    if not prod:
        payload = {
            "name": (name or mpn or "").strip() or mpn,
            "sku": (sku_preferred or mpn),
            "mpn": mpn,
            "brandId": brand["id"],
        }
        LOG.info(f"[ATRO] Create Product (minimal) :: mpn={mpn} sku={payload['sku']} brand={brand.get('name')}")
        r = client._request("POST", "/api/v1/Product", json=payload)
        prod = r.json()
    else:
        desired_sku = (sku_preferred or prod.get("sku") or "").strip()
        if not (prod.get("sku") or "").strip() and desired_sku:
            LOG.info(f"[ATRO] Patch Product (set sku) :: id={prod['id']} sku={desired_sku}")
            client._request("PATCH", f"/api/v1/Product/{prod['id']}", json={"sku": desired_sku})
            prod["sku"] = desired_sku
        if not prod.get("brandId") and brand and brand.get("id"):
            client._request("PATCH", f"/api/v1/Product/{prod['id']}", json={"brandId": brand["id"]})
            prod["brandId"] = brand["id"]
    return prod


def ensure_seo_for_product_seeded(client: AtroClient, product: Dict[str, Any]) -> Tuple[Dict[str, Any], bool]:
    """
    Ensure SEOProduct exists. On creation, copy selected base fields from Product into SEOProduct
    using PRODUCT_TO_SEO_FIELD_MAP.
    """
    needed = set(PRODUCT_TO_SEO_FIELD_MAP.keys())
    if not needed.issubset(set(product.keys())):
        select = ["id", "name"] + list(needed)
        prod_full = client.get_product_by_id(product["id"], select_fields=select)
        product = {**product, **{k: prod_full.get(k) for k in needed}}

    seo, created = writer.ensure_seo_for_product(client, product, PRODUCT_TO_SEO_FIELD_MAP)  # copies only on create
    if created:
        LOG.info(f"[ATRO] SEOProduct created and seeded from Product base fields :: seo={seo.get('id')}")
    return seo, created


def ensure_folder(client: AtroClient, name: str = "Product Images") -> Dict[str, Any]:
    f = client.find_folder_by_name(name)
    if f:
        return f
    r = client._request("POST", "/api/v1/Folder", json={"name": name})
    return r.json()


# ---------- Crawler DB access ----------
def fetch_latest_crawler_rows(cnx, brand_filter: Optional[str], since_run_id: Optional[int], limit: int) -> List[Dict[str, Any]]:
    cur = cnx.cursor(dictionary=True)

    where = ["part_number IS NOT NULL", "part_number <> ''"]
    params: List[Any] = []
    if brand_filter:
        where.append("brand = %s")
        params.append(brand_filter)
    if since_run_id is not None and since_run_id > 0:
        where.append("run_id >= %s")
        params.append(int(since_run_id))

    where_sql = " AND ".join(where)
    lim_sql = f" LIMIT {int(limit)}" if limit > 0 else ""

    sql = f"""
        SELECT rp.*
        FROM raw_products rp
        JOIN (
            SELECT brand, part_number, MAX(id) AS max_id
            FROM raw_products
            WHERE {where_sql}
            GROUP BY brand, part_number
        ) t
        ON rp.id = t.max_id
        ORDER BY rp.id DESC
        {lim_sql}
    """
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows


def fetch_rows_for_exact_run(cnx, run_id: int, brand_filter: Optional[str], limit: int) -> List[Dict[str, Any]]:
    cur = cnx.cursor(dictionary=True)
    where = ["part_number IS NOT NULL", "part_number <> ''", "run_id = %s"]
    params: List[Any] = [int(run_id)]
    if brand_filter:
        where.append("brand = %s")
        params.append(brand_filter)
    where_sql = " AND ".join(where)
    lim_sql = f" LIMIT {int(limit)}" if limit > 0 else ""
    sql = f"""
        SELECT rp.*
        FROM raw_products rp
        JOIN (
            SELECT brand, part_number, MAX(id) AS max_id
            FROM raw_products
            WHERE {where_sql}
            GROUP BY brand, part_number
        ) t
        ON rp.id = t.max_id
        ORDER BY rp.id DESC
        {lim_sql}
    """
    cur.execute(sql, params)
    rows = cur.fetchall()
    cur.close()
    return rows


# ---------- URL helpers for asset lookups ----------
def _strip_query_hash(u: str) -> str:
    try:
        from urllib.parse import urlparse, urlunparse
        p = urlparse(u)
        p2 = p._replace(query="", fragment="")
        return urlunparse(p2)
    except Exception:
        return u


def _alternate_scheme(u: str) -> str:
    try:
        from urllib.parse import urlparse, urlunparse
        p = urlparse(u)
        if p.scheme == "https":
            p = p._replace(scheme="http")
        elif p.scheme == "http":
            p = p._replace(scheme="https")
        return urlunparse(p)
    except Exception:
        return u


def pick_local_jpeg_for_url(cnx, url: str) -> Optional[Path]:
    cur = cnx.cursor()
    # 1) exact match
    cur.execute(
        """
        SELECT local_path, bytes
        FROM raw_assets
        WHERE url = %s AND asset_type = 'image' AND local_path IS NOT NULL AND local_path <> ''
        ORDER BY (CASE WHEN LOWER(local_path) LIKE '%%.jpg' THEN 0 ELSE 1 END), bytes DESC
        LIMIT 1
        """,
        (url,)
    )
    row = cur.fetchone()
    if not row:
        # 2) fallback: stripped query/hash
        url2 = _strip_query_hash(url)
        cur.execute(
            """
            SELECT local_path, bytes
            FROM raw_assets
            WHERE url = %s AND asset_type = 'image' AND local_path IS NOT NULL AND local_path <> ''
            ORDER BY (CASE WHEN LOWER(local_path) LIKE '%%.jpg' THEN 0 ELSE 1 END), bytes DESC
            LIMIT 1
            """,
            (url2,)
        )
        row = cur.fetchone()

    cur.close()
    if not row:
        return None
    p = Path(row[0])
    return p if p.exists() else None


def _pdf_row_query(cur, where_url: str):
    """
    Helper to run the widened PDF query: tolerate blank/NULL/variant asset_type.
    """
    cur.execute(
        """
        SELECT local_path, bytes
        FROM raw_assets
        WHERE url = %s
          AND (asset_type = 'document'
               OR asset_type = 'pdf'
               OR asset_type = 'application/pdf'
               OR asset_type = ''
               OR asset_type IS NULL)
          AND local_path IS NOT NULL AND local_path <> ''
        ORDER BY bytes DESC
        LIMIT 1
        """,
        (where_url,)
    )
    return cur.fetchone()


def pick_local_pdf_for_url(cnx, url: str) -> Optional[Path]:
    cur = cnx.cursor()

    # 1) exact match
    row = _pdf_row_query(cur, url)

    # 2) stripped query/hash
    if not row:
        row = _pdf_row_query(cur, _strip_query_hash(url))

    # 3) alternate scheme (http <-> https)
    if not row:
        alt = _alternate_scheme(url)
        if alt != url:
            row = _pdf_row_query(cur, alt)

    # 4) last resort: path-based LIKE (helps with minor host/query variants, especially Widen)
    if not row:
        try:
            from urllib.parse import urlparse
            p = urlparse(url)
            suffix = (p.path or "").strip()
            if suffix:
                like = f"%{suffix}"
                cur.execute(
                    """
                    SELECT local_path, bytes
                    FROM raw_assets
                    WHERE url LIKE %s
                      AND (asset_type = 'document'
                           OR asset_type = 'pdf'
                           OR asset_type = 'application/pdf'
                           OR asset_type = ''
                           OR asset_type IS NULL)
                      AND local_path IS NOT NULL AND local_path <> ''
                    ORDER BY bytes DESC
                    LIMIT 1
                    """,
                    (like,)
                )
                row = cur.fetchone()
        except Exception:
            pass

    cur.close()

    # 5) filesystem fallback under ASSET_ROOT/doc
    if not row:
        try:
            from urllib.parse import urlparse, unquote
            basename = unquote((urlparse(url).path or "").split("/")[-1] or "").strip()
            if basename:
                doc_root = Path(ASSET_ROOT) / "doc"
                # prefer pretty sibling pattern *__basename; then plain basename
                candidates = list(doc_root.rglob(f"*__{basename}"))
                if not candidates:
                    candidates = list(doc_root.rglob(basename))
                # choose the largest by size, which tends to be the full doc
                best = None
                best_sz = -1
                for c in candidates:
                    try:
                        sz = c.stat().st_size
                        if sz > best_sz:
                            best, best_sz = c, sz
                    except Exception:
                        continue
                if best and best.exists():
                    return best
        except Exception:
            pass

    if not row:
        return None
    p = Path(row[0])
    return p if p.exists() else None


# ---------- Pretty upload name helpers ----------
def _safe_base_name_from_url(u: Optional[str], fallback: str = "file") -> str:
    try:
        if not u:
            return fallback
        from urllib.parse import urlparse, unquote
        p = urlparse(u)
        stem = unquote((p.path or "").split("/")[-1] or "") or fallback
        # keep alnum, dot, underscore, dash; collapse other chars to '-'
        stem = "".join(ch if (ch.isalnum() or ch in "._-") else "-" for ch in stem)
        while "--" in stem:
            stem = stem.replace("--", "-")
        stem = stem.strip("-.")
        return stem or fallback
    except Exception:
        return fallback


def _force_ext(name: str, forced_ext: Optional[str]) -> str:
    if not forced_ext:
        return name
    # strip existing trailing image/doc chain and force ext
    import re as _re
    stem = _re.sub(r"\.(webp|avif|jpe?g|png|gif|bmp|tiff|pdf)+$", "", name, flags=_re.I)
    return f"{stem}.{forced_ext.lower()}"


def _compute_upload_name(local_path: Path, forced_ext: Optional[str], original_url: Optional[str]) -> str:
    """
    Prefer the 'pretty' sibling created by the crawler: <sha>__original-name.ext
    If missing, derive from the original URL; finally fall back to the local basename.
    Always enforce forced_ext if provided (e.g., 'jpg' or 'pdf').
    """
    try:
        sha = local_path.name  # hashed filename used by crawler storage
        parent = local_path.parent
        for entry in parent.iterdir():
            n = entry.name
            if n.startswith(f"{sha}__"):
                pretty = n.split("__", 1)[1] or n
                return _force_ext(pretty, forced_ext)
    except Exception:
        pass

    derived = _safe_base_name_from_url(original_url, fallback=local_path.name)
    return _force_ext(derived, forced_ext)


# ---------- Enrichment DB helpers ----------
def resolve_or_create_sku(cnx, manufacturer: str, mpn: str, title_or_name: str) -> str:
    cur = cnx.cursor()

    lookup_sql = """
        SELECT b.sku
        FROM supplier_partno_prefix a
        JOIN sku b ON b.Manufacturer = IFNULL(a.brand_name, a.manufacturer)
        WHERE b.manufacturer = %s AND b.manpartno = %s
        GROUP BY b.sku, b.manpartno, b.Manufacturer
    """
    cur.execute(lookup_sql, (manufacturer, mpn))
    r = cur.fetchone()
    if r and r[0]:
        cur.close()
        return str(r[0])

    cols = set(get_table_columns(cnx, cnx.database, "sku"))
    fields = ["Manufacturer", "manpartno"]
    values = [manufacturer, mpn]

    used_opt_col = None
    if "productname" in cols and title_or_name:
        fields.append("productname"); values.append(title_or_name); used_opt_col = "productname"
    elif "title" in cols and title_or_name:
        fields.append("title"); values.append(title_or_name); used_opt_col = "title"
    elif "name" in cols and title_or_name:
        fields.append("name"); values.append(title_or_name); used_opt_col = "name"

    placeholders = ", ".join(["%s"] * len(values))
    insert_sql = f"INSERT INTO sku ({', '.join(fields)}) VALUES ({placeholders})"
    cur.execute(insert_sql, tuple(values))
    cnx.commit()
    LOG.info(f"[SKU] inserted minimal row for mpn={mpn}; optional_column_used={used_opt_col or 'none'}")

    cur.execute(lookup_sql, (manufacturer, mpn))
    r2 = cur.fetchone()
    cur.close()
    return str(r2[0]) if r2 and r2[0] else ""


def enqueue_sku(cnx, sku: str, brand_name: Optional[str], source: str = "crawler"):
    """
    Insert a pending queue row with brand_name if the column exists.
    If the row already exists (INSERT IGNORE), backfill brand_name via UPDATE.
    """
    cols = set(get_table_columns(cnx, cnx.database, EN_TABLE))
    cur = cnx.cursor()

    if "brand_name" in cols:
        cur.execute(
            f"INSERT IGNORE INTO {EN_TABLE} (sku, brand_name, status, content_source) "
            f"VALUES (%s, %s, 'pending', %s)",
            (sku, brand_name or "", source)
        )
        cnx.commit()
        cur.execute(
            f"UPDATE {EN_TABLE} SET brand_name = %s "
            f"WHERE sku = %s AND (brand_name IS NULL OR brand_name = '')",
            (brand_name or "", sku)
        )
        cnx.commit()
    else:
        cur.execute(
            f"INSERT IGNORE INTO {EN_TABLE} (sku, status, content_source) "
            f"VALUES (%s, 'pending', %s)",
            (sku, source)
        )
        cnx.commit()

    cur.close()


# ---------- Core processing ----------
def process_one(
    client: AtroClient,
    crawler_cnx,
    enrich_cnx,
    images_folder_id: str,
    docs_folder_id: str,
    row: Dict[str, Any],
    upload_sem: Optional[threading.Semaphore],
    cached_specs: Optional[Dict[str, Any]],
):
    brand = (row.get("brand") or "").strip()
    mpn = (row.get("part_number") or "").strip()
    name = (row.get("title") or "").strip()
    canonical_url = (row.get("product_url") or "").strip()

    # Ensure Brand
    brand_obj = ensure_brand(client, brand)

    # Resolve SKU
    sku_for_queue = resolve_or_create_sku(enrich_cnx, brand, mpn, name or mpn)

    # Ensure Product
    product = ensure_product_by_mpn(client, brand_obj, mpn, name, sku_for_queue)

    # Ensure SEOProduct (seed with Product short/long on first create)
    seo, created = ensure_seo_for_product_seeded(client, product)

    # --- Patch brand content from crawler into SEOProduct.brandcontent ---
    brand_html = (row.get("description") or "").strip()
    if brand_html:
        client.patch_seo(seo["id"], {"brandcontent": brand_html})
        LOG.info(f"[ATRO] SEO patch brandcontent :: seo={seo['id']} bytes={len(brand_html)}")

    # --- Sync specs ---
    specs = {}
    try:
        specs = json.loads(row.get("specs_json") or "{}") or {}
    except Exception:
        specs = {}
    if specs:
        # use shared cached specs if available
        cached = cached_specs if cached_specs is not None else client.list_specs()
        writer.upsert_specs_and_values(client, seo_id=seo["id"], specs_dict=specs, cached_specs=cached)
        LOG.info(f"[ATRO] Specs upserted :: n={len(specs)}")

    # --- Sync images (upload local jpgs, link, set main) ---
    imgs = []
    try:
        imgs = json.loads(row.get("images_json") or "[]") or []
    except Exception:
        imgs = []

    uploaded_file_id_for_primary: Optional[str] = None
    if imgs:
        primary_url = None
        for it in imgs:
            if it and it.get("primary"):
                primary_url = it.get("url")
                break
        if not primary_url and imgs:
            primary_url = imgs[0].get("url")

        for it in imgs:
            url = (it.get("url") or "").strip()
            if not url:
                continue
            local_jpg = pick_local_jpeg_for_url(crawler_cnx, url)
            if not local_jpg:
                LOG.info(f"[IMG] No local jpg found for url={url}")
                continue

            upload_name = _compute_upload_name(local_jpg, forced_ext="jpg", original_url=url)

            data_url, size, mime, _ext = writer._to_data_url(local_jpg, force_ext="jpg")

            # throttle concurrent uploads if a semaphore is provided
            if upload_sem:
                upload_sem.acquire()
            try:
                file_meta = client.upload_file(
                    name=upload_name,
                    folder_id=images_folder_id,
                    data_url=data_url,
                    file_size=size,
                    mime_type=mime,
                    extension="jpg",
                    hidden=False,
                    tags=f"bridge,crawler,{brand}"
                )
            finally:
                if upload_sem:
                    upload_sem.release()

            client.link_file_to_seo(seo["id"], file_meta["id"])
            if url == primary_url and not uploaded_file_id_for_primary:
                uploaded_file_id_for_primary = file_meta["id"]

        if uploaded_file_id_for_primary:
            client.set_seo_main_image(seo["id"], uploaded_file_id_for_primary)
            LOG.info(f"[ATRO] main image set :: seo={seo['id']} file={uploaded_file_id_for_primary}")

    # --- Sync documents (upload local pdfs, link) ---
    docs = []
    try:
        docs = json.loads(row.get("docs_json") or "[]") or []
    except Exception:
        docs = []
    if docs:
        linked_docs = 0
        for it in docs:
            url = (it.get("url") or "").strip()
            if not url:
                continue
            local_pdf = pick_local_pdf_for_url(crawler_cnx, url)
            if not local_pdf:
                LOG.info(f"[DOC] No local pdf found for url={url}")
                continue

            upload_name = _compute_upload_name(local_pdf, forced_ext="pdf", original_url=url)

            data_url, size, mime, ext = writer._to_data_url(local_pdf, force_ext=None)

            # throttle concurrent uploads if a semaphore is provided
            if upload_sem:
                upload_sem.acquire()
            try:
                file_meta = client.upload_file(
                    name=upload_name,
                    folder_id=docs_folder_id,
                    data_url=data_url,
                    file_size=size,
                    mime_type=mime or "application/pdf",
                    extension=ext or "pdf",
                    hidden=False,
                    tags=f"bridge,crawler,{brand}"
                )
            finally:
                if upload_sem:
                    upload_sem.release()

            client.link_file_to_seo(seo["id"], file_meta["id"])
            linked_docs += 1

        LOG.info(f"[ATRO] documents linked :: seo={seo['id']} n={linked_docs}")

    # --- Enqueue SKU for the enricher (now includes brand_name when available) ---
    enqueue_sku(enrich_cnx, sku_for_queue, brand_name=brand, source="crawler")
    LOG.info(f"[QUEUE] seeded :: sku={sku_for_queue} brand={brand} source=crawler")


def _latest_run_id(cnx, brand_filter: Optional[str]) -> Optional[int]:
    cur = cnx.cursor()
    if brand_filter:
        cur.execute("SELECT MAX(run_id) FROM raw_products WHERE brand = %s", (brand_filter,))
    else:
        cur.execute("SELECT MAX(run_id) FROM raw_products")
    row = cur.fetchone()
    cur.close()
    return int(row[0]) if row and row[0] is not None else None


def _thread_worker(row: Dict[str, Any], images_folder_id: str, docs_folder_id: str, upload_sem: Optional[threading.Semaphore], cached_specs: Optional[Dict[str, Any]]):
    """
    Per-thread worker:
    - creates its own AtroClient
    - creates its own DB connections
    - processes a single row
    """
    client = AtroClient(ATRO_BASE_URL, ATRO_USER, ATRO_PASS)
    cr_cnx = _mysql_connect(CR_HOST, CR_USER, CR_PASS, CR_NAME)
    en_cnx = _mysql_connect(EN_HOST, EN_USER, EN_PASS, EN_NAME)
    try:
        process_one(client, cr_cnx, en_cnx, images_folder_id, docs_folder_id, row, upload_sem, cached_specs)
        return True
    finally:
        try:
            cr_cnx.close()
        except Exception:
            pass
        try:
            en_cnx.close()
        except Exception:
            pass


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--since-run-id", type=int, default=None, help="Process rows with run_id >= this value")
    ap.add_argument("--all", action="store_true", help="Process all latest per (brand, mpn)")
    ap.add_argument("--latest-run", action="store_true", help="Process only rows from the most recent run_id (optionally per --brand)")
    ap.add_argument("--brand", type=str, default=None, help="Restrict to brand name")
    ap.add_argument("--limit", type=int, default=0, help="Max rows to process (0 = no limit)")
    # Multithreading knobs
    ap.add_argument("--workers", type=int, default=4, help="Number of worker threads to process products")
    ap.add_argument("--upload-parallel", type=int, default=3, help="Max concurrent file uploads across all threads")
    args = ap.parse_args()

    # Default to --latest-run if no mode is set
    if args.since_run_id is None and not args.all and not args.latest_run:
        args.latest_run = True
        LOG.info("No mode flag provided; defaulting to --latest-run")

    if not ATRO_BASE_URL or not ATRO_USER or not ATRO_PASS:
        LOG.error("ATRO_BASE_URL / ATRO_USER / ATRO_PASS must be set in env.")
        return 2

    # Main-thread client to prepare shared resources (folders + specs cache)
    main_client = AtroClient(ATRO_BASE_URL, ATRO_USER, ATRO_PASS)

    cr_cnx = _mysql_connect(CR_HOST, CR_USER, CR_PASS, CR_NAME)

    # Use the correct folders for asset uploads
    images_folder = ensure_folder(main_client, "Product Images")
    docs_folder = ensure_folder(main_client, "Product Documents")
    images_folder_id = images_folder["id"]
    docs_folder_id = docs_folder["id"]

    limit = args.limit or BATCH_LIMIT or 0

    if args.latest_run:
        rid = _latest_run_id(cr_cnx, args.brand)
        if rid is None:
            LOG.info("No runs found to process.")
            cr_cnx.close()
            return 0
        LOG.info(f"Selected latest run_id={rid} (brand={args.brand or 'ALL'})")
        rows = fetch_rows_for_exact_run(cr_cnx, rid, brand_filter=args.brand, limit=limit)
    elif args.since_run_id is not None:
        LOG.info(f"Selected rows since run_id >= {args.since_run_id} (brand={args.brand or 'ALL'})")
        rows = fetch_latest_crawler_rows(cr_cnx, brand_filter=args.brand, since_run_id=args.since_run_id, limit=limit)
    else:
        LOG.info(f"Selected ALL latest per (brand, mpn) (brand filter={args.brand or 'ALL'})")
        rows = fetch_latest_crawler_rows(cr_cnx, brand_filter=args.brand, since_run_id=None, limit=limit)

    LOG.info(f"Selected {len(rows)} (brand,mpn) rows from crawler for bridging")

    # Build shared read-only specs cache once (optional optimization)
    global _SPECS_CACHE
    try:
        _SPECS_CACHE = main_client.list_specs()
    except Exception:
        _SPECS_CACHE = None

    # Global upload semaphore
    global _UPLOAD_SEM
    _UPLOAD_SEM = threading.Semaphore(max(1, int(args.upload_parallel)))

    processed = 0
    failed = 0

    # Thread pool over rows
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers))) as ex:
        futures = [ex.submit(_thread_worker, row, images_folder_id, docs_folder_id, _UPLOAD_SEM, _SPECS_CACHE) for row in rows]
        for fut in as_completed(futures):
            try:
                ok = fut.result()
                if ok:
                    processed += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                LOG.exception(f"[ERROR] worker failed: {e}")

    LOG.info(f"Done. processed={processed} failed={failed}")

    # close main-thread connection
    try:
        cr_cnx.close()
    except Exception:
        pass

    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
