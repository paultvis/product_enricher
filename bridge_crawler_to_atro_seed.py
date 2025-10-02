#!/usr/bin/env python3
"""
bridge_crawler_to_atro_seed.py

Bridges crawler outputs (raw_products/raw_assets) → AtroPIM (Product/SEOProduct),
uploads images and PDFs, patches brand_content/specs, then seeds the enrichment queue with SKUs.

Adds SKU resolution:
- First tries to fetch SKU from enrichment DB using the provided join query.
- If not found, inserts a row into `sku` (Manufacturer, manpartno, title) to generate a numeric SKU,
  and then re-queries to fetch it.

New in this revision:
- Upload & link locally-downloaded PDFs (from raw_assets, asset_type='document').
- Simple de-duplication by filename-in-folder for both images and documents.
- --latest-run flag to process only the most recent run (optionally scoped by --brand).

Environment variables:

# Atro
ATRO_BASE_URL       - e.g. https://atro.example.com
ATRO_USER           - AtroPIM user
ATRO_PASS           - AtroPIM password

# Crawler DB (MySQL 8+)
CRAWLER_DB_HOST
CRAWLER_DB_USER
CRAWLER_DB_PASS
CRAWLER_DB_NAME     - database name containing raw_products/raw_assets

# Enrichment DB (queue target + sku table)
ENRICH_DB_HOST
ENRICH_DB_USER
ENRICH_DB_PASS
ENRICH_DB_NAME
ENRICH_DB_TABLE     - (default: enrichment_queue)

Other:
BATCH_LIMIT         - optional int; process at most this many (brand,mpn) rows
ASSET_ROOT          - defaults to '/opt/brand_crawler/data/assets' if local paths are relative

Usage:
  python bridge_crawler_to_atro_seed.py --since-run-id 0
  python bridge_crawler_to_atro_seed.py --all
  python bridge_crawler_to_atro_seed.py --latest-run
  python bridge_crawler_to_atro_seed.py --latest-run --brand "Humminbird"
  python bridge_crawler_to_atro_seed.py --brand "Humminbird" --limit 200
"""

import os
import sys
import json
import time
import logging
import argparse
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

import mysql.connector

from atro_client import AtroClient
import writer  # uses your existing helpers: ensure_seo_for_product, upsert_specs_and_values, _to_data_url

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

EN_HOST = os.getenv("ENRICH_DB_HOST", "192.168.0.29")
EN_USER = os.getenv("ENRICH_DB_USER", "root")
EN_PASS = os.getenv("ENRICH_DB_PASS", "Albatr0ss22!")
EN_NAME = os.getenv("ENRICH_DB_NAME", "vwr")
EN_TABLE = os.getenv("ENRICH_DB_TABLE", "enrichment_queue")

ASSET_ROOT = os.getenv("ASSET_ROOT", "/opt/brand_crawler/data/assets")
BATCH_LIMIT = int(os.getenv("BATCH_LIMIT", "0"))  # 0 = unlimited


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


# ---------- Atro helpers using your AtroClient ----------

def ensure_brand(client: AtroClient, brand_name: str) -> Dict[str, Any]:
    """Find existing Brand by name or create it."""
    if not brand_name:
        raise ValueError("brand_name required")

    # Favor writer.urlencode if available (keeps consistency with your helpers)
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
        # ensure SKU if missing
        desired_sku = (sku_preferred or prod.get("sku") or "").strip()
        if not (prod.get("sku") or "").strip() and desired_sku:
            LOG.info(f"[ATRO] Patch Product (set sku) :: id={prod['id']} sku={desired_sku}")
            client._request("PATCH", f"/api/v1/Product/{prod['id']}", json={"sku": desired_sku})
            prod["sku"] = desired_sku
        # ensure brandId if missing
        if not prod.get("brandId") and brand and brand.get("id"):
            client._request("PATCH", f"/api/v1/Product/{prod['id']}", json={"brandId": brand["id"]})
            prod["brandId"] = brand["id"]
    return prod


def ensure_seo_for_product(client: AtroClient, product: Dict[str, Any]) -> Dict[str, Any]:
    """Ensure SEOProduct exists (uses your writer helper)."""
    cache: Dict[str, str] = {}
    seo, _created = writer.ensure_seo_for_product(client, product, cache)
    return seo


def ensure_folder(client: AtroClient, name: str = "SEO Uploads") -> Dict[str, Any]:
    """Find or create a folder to store uploaded files."""
    f = client.find_folder_by_name(name)
    if f:
        return f
    r = client._request("POST", "/api/v1/Folder", json={"name": name})
    return r.json()


# ---------- Crawler DB access ----------

def fetch_latest_crawler_rows(cnx, brand_filter: Optional[str], since_run_id: Optional[int], limit: int) -> List[Dict[str, Any]]:
    """
    Get latest row per (brand, mpn) from raw_products. Only rows with non-empty MPN.
    When since_run_id is provided, restrict to run_id >= since_run_id.
    """
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
    """
    Get latest row per (brand, mpn) but restricted to a specific run_id.
    """
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


def pick_local_jpeg_for_url(cnx, url: str) -> Optional[Path]:
    """
    Prefer a local .jpg sibling for the given URL; fall back to the largest image for that URL.
    """
    cur = cnx.cursor()
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
    cur.close()
    if not row:
        return None
    local_path = row[0]
    p = Path(local_path)
    return p if p.exists() else None


def pick_local_pdf_for_url(cnx, url: str) -> Optional[Path]:
    """
    Find a local PDF for the given URL from raw_assets (asset_type='document').
    Prefer the largest by bytes.
    """
    cur = cnx.cursor()
    cur.execute(
        """
        SELECT local_path, bytes
        FROM raw_assets
        WHERE url = %s AND asset_type = 'document' AND local_path IS NOT NULL AND local_path <> ''
        ORDER BY bytes DESC
        LIMIT 1
        """,
        (url,)
    )
    row = cur.fetchone()
    cur.close()
    if not row:
        return None
    p = Path(row[0])
    return p if p.exists() else None


# ---------- Enrichment DB helpers ----------

def resolve_or_create_sku(cnx, manufacturer: str, mpn: str, title: str) -> str:
    """
    Resolve SKU via join; if missing, INSERT minimal row and re-query.
    """
    cur = cnx.cursor()
    sql = """
        SELECT b.sku
        FROM supplier_partno_prefix a
        JOIN sku b ON b.Manufacturer = IFNULL(a.brand_name, a.manufacturer)
        WHERE b.manufacturer = %s AND b.manpartno = %s
        GROUP BY b.sku, b.manpartno, b.Manufacturer
    """
    cur.execute(sql, (manufacturer, mpn))
    r = cur.fetchone()
    if r and r[0]:
        cur.close()
        return str(r[0])

    # create minimal SKU
    cur.execute(
        "INSERT INTO sku (Manufacturer, manpartno, title) VALUES (%s, %s, %s)",
        (manufacturer, mpn, title or mpn)
    )
    cnx.commit()

    # re-query
    cur.execute(sql, (manufacturer, mpn))
    r2 = cur.fetchone()
    cur.close()
    return str(r2[0]) if r2 and r2[0] else ""


def enqueue_sku(cnx, sku: str, source: str = "crawler"):
    cur = cnx.cursor()
    cur.execute(
        f"INSERT IGNORE INTO {EN_TABLE} (sku, status, content_source) VALUES (%s, 'pending', %s)",
        (sku, source)
    )
    cnx.commit()
    cur.close()


# ---------- Core processing ----------

def process_one(client: AtroClient, crawler_cnx, enrich_cnx, folder_id: str, row: Dict[str, Any]):
    brand = (row.get("brand") or "").strip()
    mpn = (row.get("part_number") or "").strip()
    name = (row.get("title") or "").strip()
    canonical_url = (row.get("product_url") or "").strip()

    # Ensure Brand
    brand_obj = ensure_brand(client, brand)

    # Resolve SKU in enrichment DB
    sku_for_queue = resolve_or_create_sku(enrich_cnx, brand, mpn, name or mpn)

    # Ensure Product
    product = ensure_product_by_mpn(client, brand_obj, mpn, name, sku_for_queue)

    # Ensure SEOProduct
    seo = ensure_seo_for_product(client, product)

    # --- Patch SEO.brand_content (HTML description) ---
    brand_html = (row.get("description") or "").strip()
    patch_payload = {}
    if brand_html:
        # field name confirmed: brand_content on SEOProduct
        patch_payload["brand_content"] = brand_html

    if patch_payload:
        client.patch_seo(seo["id"], patch_payload)
        LOG.info(f"[ATRO] SEO patch brand_content :: seo={seo['id']} bytes={len(brand_html)}")

    # --- Sync specs ---
    specs = {}
    try:
        specs = json.loads(row.get("specs_json") or "{}") or {}
    except Exception:
        specs = {}
    if specs:
        cached_specs = client.list_specs()
        writer.upsert_specs_and_values(client, seo_id=seo["id"], specs_dict=specs, cached_specs=cached_specs)
        LOG.info(f"[ATRO] Specs upserted :: n={len(specs)}")

    # --- Sync images (upload local jpgs, link, set main) ---
    imgs = []
    try:
        imgs = json.loads(row.get("images_json") or "[]") or []
    except Exception:
        imgs = []

    uploaded_file_id_for_primary: Optional[str] = None
    if imgs:
        # choose primary from flag if present, else first
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

            # de-dupe by name in folder
            existing = client.find_file_by_name_in_folder(local_jpg.name, folder_id)
            if existing:
                file_id = existing["id"]
                LOG.info(f"[ATRO] reuse image :: name={local_jpg.name} id={file_id}")
            else:
                data_url, size, mime, _ext = writer._to_data_url(local_jpg, force_ext="jpg")
                file_meta = client.upload_file(
                    name=local_jpg.name,
                    folder_id=folder_id,
                    data_url=data_url,
                    file_size=size,
                    mime_type=mime,
                    extension="jpg",
                    hidden=False,
                    tags=f"bridge,crawler,{brand}"
                )
                file_id = file_meta["id"]
                LOG.info(f"[ATRO] image uploaded :: id={file_id} name={local_jpg.name}")

            client.link_file_to_seo(seo["id"], file_id)
            if url == primary_url and not uploaded_file_id_for_primary:
                uploaded_file_id_for_primary = file_id

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

            existing = client.find_file_by_name_in_folder(local_pdf.name, folder_id)
            if existing:
                file_id = existing["id"]
                LOG.info(f"[ATRO] reuse doc :: name={local_pdf.name} id={file_id}")
            else:
                data_url, size, mime, ext = writer._to_data_url(local_pdf, force_ext=None)
                # extension fallback if mime/type missing
                ext = ext or "pdf"
                file_meta = client.upload_file(
                    name=local_pdf.name,
                    folder_id=folder_id,
                    data_url=data_url,
                    file_size=size,
                    mime_type=mime or "application/pdf",
                    extension=ext,
                    hidden=False,
                    tags=f"bridge,crawler,{brand}"
                )
                file_id = file_meta["id"]
                LOG.info(f"[ATRO] doc uploaded :: id={file_id} name={local_pdf.name}")

            client.link_file_to_seo(seo["id"], file_id)
            linked_docs += 1

        LOG.info(f"[ATRO] documents linked :: seo={seo['id']} n={linked_docs}")

    # --- Enqueue SKU for the existing enricher ---
    enqueue_sku(enrich_cnx, sku_for_queue, source="crawler")
    LOG.info(f"[QUEUE] seeded :: sku={sku_for_queue} source=crawler")


def _latest_run_id(cnx, brand_filter: Optional[str]) -> Optional[int]:
    cur = cnx.cursor()
    if brand_filter:
        cur.execute("SELECT MAX(run_id) FROM raw_products WHERE brand = %s", (brand_filter,))
    else:
        cur.execute("SELECT MAX(run_id) FROM raw_products")
    row = cur.fetchone()
    cur.close()
    return int(row[0]) if row and row[0] is not None else None


def main():
    ap = argparse.ArgumentParser()
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--since-run-id", type=int, help="Process rows with run_id >= this value")
    group.add_argument("--all", action="store_true", help="Process all latest per (brand, mpn)")
    group.add_argument("--latest-run", action="store_true", help="Process only rows from the most recent run_id (optionally per --brand)")
    ap.add_argument("--brand", type=str, default=None, help="Restrict to brand name")
    ap.add_argument("--limit", type=int, default=0, help="Max rows to process (0 = no limit)")
    args = ap.parse_args()

    if not ATRO_BASE_URL or not ATRO_USER or not ATRO_PASS:
        LOG.error("ATRO_BASE_URL / ATRO_USER / ATRO_PASS must be set in env.")
        return 2

    client = AtroClient(ATRO_BASE_URL, ATRO_USER, ATRO_PASS)

    cr_cnx = _mysql_connect(CR_HOST, CR_USER, CR_PASS, CR_NAME)
    en_cnx = _mysql_connect(EN_HOST, EN_USER, EN_PASS, EN_NAME)

    # Ensure upload folder (kept as 'SEO Uploads' per your current convention)
    folder = ensure_folder(client, "SEO Uploads")
    folder_id = folder["id"]

    limit = args.limit or BATCH_LIMIT or 0

    # Row selection logic
    rows: List[Dict[str, Any]] = []
    if args.latest_run:
        rid = _latest_run_id(cr_cnx, args.brand)
        if rid is None:
            LOG.info("No runs found to process.")
            cr_cnx.close()
            en_cnx.close()
            return 0
        LOG.info(f"Selected latest run_id={rid} (brand={args.brand or 'ALL'})")
        rows = fetch_rows_for_exact_run(cr_cnx, rid, brand_filter=args.brand, limit=limit)
    else:
        since_run = args.since_run_id if args.since_run_id and args.since_run_id > 0 else None
        rows = fetch_latest_crawler_rows(cr_cnx, brand_filter=args.brand, since_run_id=since_run, limit=limit)

    LOG.info(f"Selected {len(rows)} (brand,mpn) rows from crawler for bridging")

    processed = 0
    failed = 0

    for row in rows:
        try:
            process_one(client, cr_cnx, en_cnx, folder_id, row)
            processed += 1
        except Exception as e:
            failed += 1
            LOG.exception(f"[ERROR] row id={row.get('id')} brand={row.get('brand')} mpn={row.get('part_number')}: {e}")

    LOG.info(f"Done. processed={processed} failed={failed}")
    cr_cnx.close()
    en_cnx.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
