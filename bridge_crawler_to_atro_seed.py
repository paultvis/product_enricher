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

Idempotency & hardening update:
- bridge_progress table (in enrichment DB) stores per-SKU content hashes (brand/specs/images/docs)
- Skip unchanged stages based on hashes (no Atro calls when not needed)
- Name-based dedupe in Atro folder before uploading (images + docs)
- Upload retries with exponential backoff

New in this version:
- **Early exit**: if all four hashes match, skip all Atro activity for the SKU
- **Duplicate-link protection**: prefetch existing SEO→File links, avoid re-link; treat 400 on link as benign “already linked”

Additional polish in this update:
- Thread-aware logs (include thread name)
- Best-effort queue seeding even when a worker errors
- Periodic throughput: rows/min and uploads/min
- Startup capability line: queue brand_name column present?, progress table ensured, parallelism
"""
#----nudge
import os
import sys
import json
import time
import hashlib
import logging
import argparse
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import mysql.connector
import requests

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

# Throughput counters
_UPLOADS_COUNT = 0
_UPLOADS_LOCK = threading.Lock()

def _inc_uploads(n: int = 1):
    global _UPLOADS_COUNT
    if n:
        with _UPLOADS_LOCK:
            _UPLOADS_COUNT += int(n)

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

# ---------- Idempotency helpers (hashes in enrichment DB) ----------
def ensure_bridge_progress_table(cnx):
    cur = cnx.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS bridge_progress (
            sku VARCHAR(32) NOT NULL,
            brand VARCHAR(255) NULL,
            mpn VARCHAR(255) NULL,
            brand_html_hash VARCHAR(64) NULL,
            specs_hash VARCHAR(64) NULL,
            images_hash VARCHAR(64) NULL,
            docs_hash VARCHAR(64) NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            PRIMARY KEY (sku)
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """
    )
    cnx.commit()
    cur.close()

def _stable_json_hash(value: Any) -> str:
    try:
        s = json.dumps(value, sort_keys=True, separators=(",", ":"))
    except Exception:
        s = str(value)
    return hashlib.sha256(s.encode("utf-8", "ignore")).hexdigest()

def _urls_hash(items: List[Dict[str, Any]], key: str = "url") -> str:
    urls = []
    for it in (items or []):
        u = (it.get(key) or "").strip()
        if u:
            urls.append(u)
    urls = sorted(set(urls))
    return _stable_json_hash(urls)

def get_progress(cnx, sku: str) -> Dict[str, Optional[str]]:
    cur = cnx.cursor(dictionary=True)
    cur.execute("SELECT brand_html_hash, specs_hash, images_hash, docs_hash FROM bridge_progress WHERE sku = %s", (sku,))
    row = cur.fetchone() or {}
    cur.close()
    return {
        "brand_html_hash": row.get("brand_html_hash"),
        "specs_hash": row.get("specs_hash"),
        "images_hash": row.get("images_hash"),
        "docs_hash": row.get("docs_hash"),
    }

def upsert_progress(cnx, sku: str, brand: str, mpn: str,
                    brand_html_hash: Optional[str],
                    specs_hash: Optional[str],
                    images_hash: Optional[str],
                    docs_hash: Optional[str]):
    cur = cnx.cursor()
    cur.execute(
        """
        INSERT INTO bridge_progress (sku, brand, mpn, brand_html_hash, specs_hash, images_hash, docs_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
          brand = VALUES(brand),
          mpn = VALUES(mpn),
          brand_html_hash = VALUES(brand_html_hash),
          specs_hash = VALUES(specs_hash),
          images_hash = VALUES(images_hash),
          docs_hash = VALUES(docs_hash),
          updated_at = CURRENT_TIMESTAMP
        """,
        (sku, brand, mpn, brand_html_hash, specs_hash, images_hash, docs_hash)
    )
    cnx.commit()
    cur.close()

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
        WHERE {where_sql}
        ORDER BY id DESC
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

    # 4) last resort: path-based LIKE (helps with minor host/query variants)
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
                candidates = list(doc_root.rglob(f"*__{basename}")) or list(doc_root.rglob(basename))
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
        sha = local_path.name
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

# ---------- Queue helpers (read/reset) ----------
def get_queue_row(cnx, sku: str):
    try:
        cur = cnx.cursor(dictionary=True)
        cur.execute(f"SELECT sku, status, content_source, brand_name, last_error FROM {EN_TABLE} WHERE sku=%s", (sku,))
        row = cur.fetchone()
        cur.close()
        return row
    except Exception:
        return None

def reset_queue_failed_to_pending(cnx, sku: str):
    try:
        cur = cnx.cursor()
        cur.execute(f"UPDATE {EN_TABLE} SET status='pending', last_error=NULL WHERE sku=%s AND status='failed'", (sku,))
        cnx.commit()
        cur.close()
    except Exception:
        pass

# ---------- Repair path (DB-only) ----------
def repair_one_row(en_cnx, row: dict, *, requeue_failed: bool=False):
    """
    DB-only repair:
      - ensure SKU
      - ensure queue row (content_source='crawler', brand_name if column exists)
      - upsert bridge_progress with current input hashes
      - optionally reset failed->pending
    """
    brand = (row.get("brand") or "").strip()
    mpn   = (row.get("part_number") or "").strip()
    name  = (row.get("title") or "").strip()

    # Resolve SKU
    sku = resolve_or_create_sku(en_cnx, brand, mpn, name or mpn)

    # Compute input hashes from crawler fields (same hashing as main path)
    current_brand_html = (row.get("description") or "").strip()
    try:
        current_specs = json.loads(row.get("specs_json") or "{}") or {}
    except Exception:
        current_specs = {}
    try:
        imgs_any = json.loads(row.get("images_json") or "[]") or []
    except Exception:
        imgs_any = []
    try:
        docs_any = json.loads(row.get("docs_json") or "[]") or []
    except Exception:
        docs_any = []

    brand_html_hash = _stable_json_hash(current_brand_html) if current_brand_html else None
    specs_hash      = _stable_json_hash(current_specs) if current_specs else None
    images_hash     = _urls_hash(imgs_any) if imgs_any else None
    docs_hash       = _urls_hash(docs_any) if docs_any else None

    # Ensure queue exists
    enqueue_sku(en_cnx, sku, brand_name=brand, source="crawler")

    # Optionally reset failed rows to pending
    qrow = get_queue_row(en_cnx, sku) or {}
    if requeue_failed and (qrow.get("status") == "failed"):
        reset_queue_failed_to_pending(en_cnx, sku)
        LOG.info(f"[REPAIR] requeued failed -> pending :: sku={sku}")

    # Ensure progress row
    upsert_progress(
        en_cnx, sku, brand, mpn,
        brand_html_hash=brand_html_hash,
        specs_hash=specs_hash,
        images_hash=images_hash,
        docs_hash=docs_hash,
    )

    LOG.info(f"[REPAIR] ensured queue+progress :: sku={sku} brand={brand} mpn={mpn}")
    return {"sku": sku, "requeued": (qrow.get("status") == "failed") and requeue_failed}

# ---------- Atro file helpers (dedupe + retries + link checks) ----------
def _find_file_in_folder_by_name(client: AtroClient, folder_id: str, name: str) -> Optional[Dict[str, Any]]:
    """
    Search Atro File by exact name within a folder. Returns the first match or None.
    """
    try:
        from urllib.parse import urlencode
        q = "?" + urlencode({
            "maxSize": 50, "offset": 0,
            "where[0][type]": "equals", "where[0][attribute]": "folderId", "where[0][value]": folder_id,
            "where[1][type]": "equals", "where[1][attribute]": "name", "where[1][value]": name,
        })
        r = client._request("GET", f"/api/v1/File{q}")
        data = r.json() or {}
        lst = data.get("list") or []
        return lst[0] if lst else None
    except Exception:
        return None

def _get_linked_file_ids_for_seo(client: AtroClient, seo_id: str) -> Set[str]:
    """
    Fetch the set of File IDs already linked to the given SEOProduct.
    """
    try:
        from urllib.parse import urlencode
        q = "?" + urlencode({
            "maxSize": 200, "offset": 0,
            "where[0][type]": "equals", "where[0][attribute]": "seoProductId", "where[0][value]": seo_id,
        })
        r = client._request("GET", f"/api/v1/SEOProductFile{q}")
        data = r.json() or {}
        lst = data.get("list") or []
        result = set()
        for it in lst:
            # records usually contain fileId
            fid = it.get("fileId")
            if fid:
                result.add(fid)
        return result
    except Exception:
        return set()

def _upload_with_retries(client: AtroClient, *, name: str, folder_id: str,
                         data_url: str, file_size: int, mime_type: str, extension: str,
                         tags: str, max_retries: int = 3) -> Dict[str, Any]:
    """
    Upload file with simple exponential backoff retries.
    """
    attempt = 0
    delay_seq = [2, 5, 10]
    while True:
        try:
            res = client.upload_file(
                name=name, folder_id=folder_id, data_url=data_url, file_size=file_size,
                mime_type=mime_type, extension=extension, hidden=False, tags=tags
            )
            _inc_uploads(1)
            return res
        except Exception as e:
            if attempt >= max_retries - 1:
                raise
            sleep_for = delay_seq[min(attempt, len(delay_seq) - 1)]
            LOG.warning(f"[ATRO] upload retry {attempt+1}/{max_retries} in {sleep_for}s :: name={name} err={e}")
            time.sleep(sleep_for)
            attempt += 1

def _safe_link_file_to_seo(client: AtroClient, seo_id: str, file_id: str, already_linked: Set[str]):
    """
    Link file to SEO unless already linked.
    Treat HTTP 400 as 'already linked' and continue.
    """
    if file_id in already_linked:
        return
    try:
        client.link_file_to_seo(seo_id, file_id)
        already_linked.add(file_id)
    except requests.HTTPError as e:
        # If server says bad request, assume duplicate relation and continue
        if e.response is not None and e.response.status_code == 400:
            LOG.info(f"[ATRO] link already exists (400), skipping :: seo={seo_id} file={file_id}")
            already_linked.add(file_id)
        else:
            raise

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

    # ---------- Idempotency: compute current hashes ----------
    current_brand_html = (row.get("description") or "").strip()
    try:
        current_specs = json.loads(row.get("specs_json") or "{}") or {}
    except Exception:
        current_specs = {}
    try:
        current_imgs = json.loads(row.get("images_json") or "[]") or []
    except Exception:
        current_imgs = []
    try:
        current_docs = json.loads(row.get("docs_json") or "[]") or []
    except Exception:
        current_docs = []

    brand_html_hash = _stable_json_hash(current_brand_html) if current_brand_html else None
    specs_hash = _stable_json_hash(current_specs) if current_specs else None
    images_hash = _urls_hash(current_imgs) if current_imgs else None
    docs_hash = _urls_hash(current_docs) if current_docs else None

    prog = get_progress(enrich_cnx, sku_for_queue)
    do_brand = (brand_html_hash is not None) and (brand_html_hash != prog.get("brand_html_hash"))
    do_specs = (specs_hash is not None) and (specs_hash != prog.get("specs_hash"))
    do_imgs  = (images_hash is not None) and (images_hash != prog.get("images_hash"))
    do_docs  = (docs_hash is not None) and (docs_hash != prog.get("docs_hash"))

    # ---------- Early exit: if all parts unchanged, skip all Atro work ----------
    if not (do_brand or do_specs or do_imgs or do_docs):
        LOG.info(f"[SKIP] all unchanged :: sku={sku_for_queue}")
        # still ensure the queue row exists
        enqueue_sku(enrich_cnx, sku_for_queue, brand_name=brand, source="crawler")
        # refresh progress row (keeps timestamps moving forward if desired)
        upsert_progress(
            enrich_cnx, sku_for_queue, brand, mpn,
            brand_html_hash=brand_html_hash,
            specs_hash=specs_hash,
            images_hash=images_hash,
            docs_hash=docs_hash
        )
        return

    # --- Patch brand content ---
    if do_brand:
        client.patch_seo(seo["id"], {"brandcontent": current_brand_html})
        LOG.info(f"[ATRO] brandcontent patched :: seo={seo['id']} bytes={len(current_brand_html)}")
    else:
        LOG.info(f"[SKIP] brandcontent unchanged :: sku={sku_for_queue}")

    # --- Upsert specs ---
    if do_specs and current_specs:
        try:
            writer.upsert_specs_and_values(client, seo["id"], current_specs, cached_specs or {})
            LOG.info(f"[ATRO] specs upserted :: seo={seo['id']} n={len(current_specs)}")
        except Exception as e:
            LOG.exception(f"[ERROR] specs upsert failed :: sku={sku_for_queue} err={e}")
            # carry on; images/docs may still succeed
    else:
        LOG.info(f"[SKIP] specs unchanged/empty :: sku={sku_for_queue}")

    # --- Upload/link images (name-dedupe + link-dedupe) ---
    if do_imgs and current_imgs:
        linked = 0
        linked_ids = _get_linked_file_ids_for_seo(client, seo["id"])
        for idx, u in enumerate(current_imgs):
            if not u:
                continue
            local = pick_local_jpeg_for_url(crawler_cnx, u.get("url") if isinstance(u, dict) else u)
            if not local:
                LOG.info(f"[SKIP] local image missing :: url={u}")
                continue
            upload_name = _compute_upload_name(local, forced_ext="jpg", original_url=(u.get("url") if isinstance(u, dict) else u))
            existing = _find_file_in_folder_by_name(client, images_folder_id, upload_name)
            if existing:
                file_meta = existing
            else:
                data_url, size, mime, _ext = writer._to_data_url(local, force_ext="jpg")
                if upload_sem:
                    upload_sem.acquire()
                try:
                    file_meta = _upload_with_retries(
                        client,
                        name=upload_name,
                        folder_id=images_folder_id,
                        data_url=data_url,
                        file_size=size,
                        mime_type=mime,
                        extension="jpg",
                        tags=f"bridge,crawler,{brand}",
                    )
                finally:
                    if upload_sem:
                        upload_sem.release()

            # link file if not already linked
            _safe_link_file_to_seo(client, seo["id"], file_meta["id"], linked_ids)
            linked += 1

        # set primary image to the largest area image already chosen by crawler (first element)
        if linked and not seo.get("mainImageId"):
            try:
                first = current_imgs[0]
                first_name = _compute_upload_name(
                    pick_local_jpeg_for_url(crawler_cnx, first.get("url") if isinstance(first, dict) else first),
                    forced_ext="jpg",
                    original_url=(first.get("url") if isinstance(first, dict) else first)
                )
                main_file = _find_file_in_folder_by_name(client, images_folder_id, first_name)
                if main_file:
                    client.set_seo_main_image(seo["id"], main_file["id"])
                    LOG.info(f"[ATRO] main image set :: seo={seo['id']} file={main_file['id']}")
            except Exception as e:
                LOG.warning(f"[WARN] set main image failed :: seo={seo['id']} err={e}")

        LOG.info(f"[ATRO] images linked :: seo={seo['id']} n={linked}")
    else:
        LOG.info(f"[SKIP] images unchanged/empty :: sku={sku_for_queue}")

    # --- Upload/link documents ---
    if do_docs and current_docs:
        linked_docs = 0
        linked_ids = _get_linked_file_ids_for_seo(client, seo["id"])
        for u in current_docs:
            if not u:
                continue
            local_pdf = pick_local_pdf_for_url(crawler_cnx, u.get("url") if isinstance(u, dict) else u)
            if not local_pdf:
                LOG.info(f"[SKIP] local PDF missing :: url={u}")
                continue
            upload_name = _compute_upload_name(local_pdf, forced_ext="pdf", original_url=(u.get("url") if isinstance(u, dict) else u))
            existing = _find_file_in_folder_by_name(client, docs_folder_id, upload_name)
            if existing:
                file_meta = existing
            else:
                data_url, size, mime, ext = writer._to_data_url(local_pdf, force_ext=None)
                if upload_sem:
                    upload_sem.acquire()
                try:
                    file_meta = _upload_with_retries(
                        client,
                        name=upload_name,
                        folder_id=docs_folder_id,
                        data_url=data_url,
                        file_size=size,
                        mime_type=mime or "application/pdf",
                        extension=ext or "pdf",
                        tags=f"bridge,crawler,{brand}",
                    )
                finally:
                    if upload_sem:
                        upload_sem.release()

            # link file if not already linked
            _safe_link_file_to_seo(client, seo["id"], file_meta["id"], linked_ids)
            linked_docs += 1

        LOG.info(f"[ATRO] documents linked :: seo={seo['id']} n={linked_docs}")
    else:
        LOG.info(f"[SKIP] documents unchanged :: sku={sku_for_queue}")

    # --- Enqueue SKU for the enricher (now includes brand_name when available) ---
    enqueue_sku(enrich_cnx, sku_for_queue, brand_name=brand, source="crawler")
    LOG.info(f"[QUEUE] seeded :: sku={sku_for_queue} brand={brand} source=crawler")

    # --- Persist progress hashes ---
    upsert_progress(
        enrich_cnx, sku_for_queue, brand, mpn,
        brand_html_hash=brand_html_hash,
        specs_hash=specs_hash,
        images_hash=images_hash,
        docs_hash=docs_hash
    )
    LOG.info(f"[PROGRESS] upserted :: sku={sku_for_queue} brand={brand} changed={{'brand': %s, 'specs': %s, 'images': %s, 'docs': %s}}" % (do_brand, do_specs, do_imgs, do_docs))

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
        # ensure progress table exists (once per-thread is safe & cheap)
        ensure_bridge_progress_table(en_cnx)
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
    ap.add_argument("--repair-missed", action="store_true",
                    help="DB-only pass: ensure queue+progress for selected crawler rows; no Atro calls")
    ap.add_argument("--requeue-failed", action="store_true",
                    help="With --repair-missed: reset status='failed' rows back to 'pending'")
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
    en_cnx = _mysql_connect(EN_HOST, EN_USER, EN_PASS, EN_NAME)

    # Ensure progress table once in main thread
    ensure_bridge_progress_table(en_cnx)
    try:
        cols = set(get_table_columns(en_cnx, EN_NAME, EN_TABLE))
    except Exception:
        cols = set()
    LOG.info(f"[CAPABILITY] queue_table={EN_TABLE} brand_name_col={'yes' if 'brand_name' in cols else 'no'} progress_table=bridge_progress workers={args.workers} upload_parallel={args.upload_parallel}")

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
            en_cnx.close()
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

    if args.repair_missed:
        LOG.info(f"[REPAIR] DB-only repair starting for {len(rows)} rows requeue_failed={args.requeue_failed}")
        repaired = 0
        requeued = 0
        for row in rows:
            try:
                res = repair_one_row(en_cnx, row, requeue_failed=args.requeue_failed)
                repaired += 1
                if res.get("requeued"):
                    requeued += 1
            except Exception as e:
                LOG.error(f"[REPAIR] failed for row brand={row.get('brand')} mpn={row.get('part_number')} err={e}")
        LOG.info(f"[REPAIR-SUMMARY] repaired={repaired} requeued_failed={requeued}")
        try:
            cr_cnx.close()
        except Exception:
            pass
        try:
            en_cnx.close()
        except Exception:
            pass
        return 0

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
    t0 = time.time()
    EVERY = max(5, int(args.workers))
    LOG.info(f"[START] workers={args.workers} upload_parallel={args.upload_parallel} rows={len(rows)} brand={args.brand or 'ALL'}")

    # Thread pool over rows
    with ThreadPoolExecutor(max_workers=max(1, int(args.workers)), thread_name_prefix="bridge") as ex:
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
            # periodic throughput
            done = processed + failed
            if done % EVERY == 0 or done == len(futures):
                elapsed = max(0.001, time.time() - t0)
                rpm = done / (elapsed / 60.0)
                up = _UPLOADS_COUNT
                upm = up / (elapsed / 60.0)
                LOG.info(f"[THROUGHPUT] done={done}/{len(futures)} rows/min={rpm:.1f} uploads_total={up} uploads/min={upm:.1f}")

    elapsed = time.time() - t0
    LOG.info(f"[DONE] processed={processed} failed={failed} elapsed={elapsed:.1f}s uploads_total={_UPLOADS_COUNT}")

    # close main-thread connections
    try:
        cr_cnx.close()
    except Exception:
        pass
    try:
        en_cnx.close()
    except Exception:
        pass

    return 1 if failed else 0

if __name__ == "__main__":
    sys.exit(main())
