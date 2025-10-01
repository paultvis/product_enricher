#!/usr/bin/env python3
"""
bridge_crawler_to_atro_seed.py

Bridges crawler outputs (raw_products/raw_assets) → AtroPIM (Product/SEOProduct),
uploads images, patches brand_content/specs, then seeds the enrichment queue with SKUs.

Adds SKU resolution:
- First tries to fetch SKU from enrichment DB using the provided join query.
- If not found, inserts a row into `sku` (Manufacturer, manpartno, title) to generate a numeric SKU,
  and then re-queries to fetch it.

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
ATRO_BASE_URL = os.getenv("ATRO_BASE_URL", "").strip().rstrip("/")
ATRO_USER     = os.getenv("ATRO_USER", "")
ATRO_PASS     = os.getenv("ATRO_PASS", "")

CR_HOST = os.getenv("CRAWLER_DB_HOST", "127.0.0.1")
CR_USER = os.getenv("CRAWLER_DB_USER", "")
CR_PASS = os.getenv("CRAWLER_DB_PASS", "")
CR_NAME = os.getenv("CRAWLER_DB_NAME", "brand_crawler")

EN_HOST = os.getenv("ENRICH_DB_HOST", "127.0.0.1")
EN_USER = os.getenv("ENRICH_DB_USER", "")
EN_PASS = os.getenv("ENRICH_DB_PASS", "")
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
    if not p.is_absolute():
        p = Path(ASSET_ROOT) / p
    return p if p.exists() else None


# ---------- SKU resolution / creation in enrichment DB ----------

def query_sku(en_cnx, manufacturer: str, manpartno: str) -> Optional[str]:
    """
    Your provided query: select SKU by joining supplier_partno_prefix + sku tables.
    Returns SKU as string if found, else None.
    """
    sql = """
    SELECT b.sku
    FROM supplier_partno_prefix a
    JOIN sku b ON b.Manufacturer = IFNULL(a.brand_name, a.manufacturer)
    WHERE b.manufacturer = %s
      AND b.manpartno = %s
    GROUP BY b.sku, b.manpartno, b.Manufacturer
    """
    cur = en_cnx.cursor()
    cur.execute(sql, (manufacturer, manpartno))
    row = cur.fetchone()
    cur.close()
    if row and row[0] is not None:
        return str(row[0])

    # Fallback (in case supplier_partno_prefix doesn't have a mapping row yet)
    cur = en_cnx.cursor()
    cur.execute(
        "SELECT sku FROM sku WHERE Manufacturer = %s AND manpartno = %s ORDER BY sku DESC LIMIT 1",
        (manufacturer, manpartno)
    )
    row = cur.fetchone()
    cur.close()
    if row and row[0] is not None:
        return str(row[0])

    return None


def insert_sku(en_cnx, manufacturer: str, manpartno: str, product_title: str) -> None:
    """
    Insert a row in `sku` with Manufacturer, manpartno, and the product title (best-effort column match).
    Assumes the table has an auto-generated numeric 'sku' that will be assigned.
    """
    cols = get_table_columns(en_cnx, EN_NAME, "sku")
    fields = ["Manufacturer", "manpartno"]
    values = [manufacturer, manpartno]

    # Try to place title into a sensible column if present
    title_col_candidates = ["title", "name", "product_name", "description", "productname"]
    title_col = next((c for c in title_col_candidates if c in cols), None)
    if title_col and product_title:
        fields.append(title_col)
        values.append(product_title)

    placeholders = ", ".join(["%s"] * len(values))
    sql = f"INSERT INTO sku ({', '.join(fields)}) VALUES ({placeholders})"
    cur = en_cnx.cursor()
    cur.execute(sql, tuple(values))
    en_cnx.commit()
    cur.close()


def ensure_sku(en_cnx, manufacturer: str, manpartno: str, product_title: str) -> str:
    """
    Returns existing SKU; if missing, inserts a row to generate one and re-queries.
    """
    sku = query_sku(en_cnx, manufacturer, manpartno)
    if sku:
        return sku

    LOG.info(f"[SKU] Not found; inserting :: manufacturer={manufacturer} mpn={manpartno}")
    insert_sku(en_cnx, manufacturer, manpartno, product_title)

    # re-query
    sku = query_sku(en_cnx, manufacturer, manpartno)
    if not sku:
        # Extremely unlikely unless constraints fail; fallback to using mpn as SKU to proceed.
        LOG.warning(f"[SKU] Still not found after insert; falling back to MPN as SKU :: {manpartno}")
        sku = manpartno
    return sku


# ---------- Enrichment queue insert ----------

def enqueue_sku(en_cnx, sku: str, source: str = "crawler") -> None:
    cur = en_cnx.cursor()
    try:
        cur.execute(
            f"INSERT IGNORE INTO {EN_TABLE} (sku, status, content_source, created_at) VALUES (%s, 'pending', %s, NOW())",
            (sku, source)
        )
        en_cnx.commit()
    finally:
        cur.close()


# ---------- Main workflow ----------

def process_one(
    client: AtroClient,
    crawler_cnx,
    enrich_cnx,
    folder_id: str,
    row: Dict[str, Any],
):
    brand = (row.get("brand") or "").strip()
    mpn = (row.get("part_number") or "").strip()
    title = (row.get("title") or "").strip()
    canonical_url = (row.get("product_url") or "").strip()

    if not brand or not mpn:
        LOG.info(f"[SKIP] Missing brand or mpn :: brand={brand} mpn={mpn}")
        return

    LOG.info(f"[BRIDGE] {brand} | mpn={mpn} :: resolve SKU, ensure brand/product/seo")

    # --- Resolve (or create) SKU in enrichment DB ---
    sku_resolved = ensure_sku(enrich_cnx, manufacturer=brand, manpartno=mpn, product_title=title)

    # --- Ensure Brand & Product in Atro ---
    brand_rec = ensure_brand(client, brand)
    product = ensure_product_by_mpn(client, brand_rec, mpn, name=title, sku_preferred=sku_resolved)
    sku_for_queue = (product.get("sku") or "").strip() or sku_resolved or mpn

    # --- Ensure SEOProduct ---
    seo = ensure_seo_for_product(client, product)

    # --- Sync brand_content from crawler (clean HTML already in raw_products.description) ---
    brand_html = (row.get("description") or "").strip()
    patch_payload = {}
    if brand_html:
        # field name confirmed: brand_content on SEOProduct
        patch_payload["brand_content"] = brand_html
    # Optional: store PDP URL somewhere if you have a custom field (commented out)
    # if canonical_url:
    #     patch_payload["pdp_url"] = canonical_url

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

            data_url, size, mime, ext = writer._to_data_url(local_jpg, force_ext="jpg")
            name = local_jpg.name
            file_meta = client.upload_file(
                name=name,
                folder_id=folder_id,
                data_url=data_url,
                file_size=size,
                mime_type=mime,
                extension="jpg",
                hidden=False,
                tags=f"bridge,crawler,{brand}"
            )
            client.link_file_to_seo(seo["id"], file_meta["id"])
            if url == primary_url and not uploaded_file_id_for_primary:
                uploaded_file_id_for_primary = file_meta["id"]

        if uploaded_file_id_for_primary:
            client.set_seo_main_image(seo["id"], uploaded_file_id_for_primary)
            LOG.info(f"[ATRO] main image set :: seo={seo['id']} file={uploaded_file_id_for_primary}")

    # --- Enqueue SKU for the existing enricher ---
    enqueue_sku(enrich_cnx, sku_for_queue, source="crawler")
    LOG.info(f"[QUEUE] seeded :: sku={sku_for_queue} source=crawler")


def main():
    ap = argparse.ArgumentParser()
    group = ap.add_mutually_exclusive_group(required=True)
    group.add_argument("--since-run-id", type=int, help="Process rows with run_id >= this value")
    group.add_argument("--all", action="store_true", help="Process all latest per (brand, mpn)")
    ap.add_argument("--brand", type=str, default=None, help="Restrict to brand name")
    ap.add_argument("--limit", type=int, default=0, help="Max rows to process (0 = no limit)")
    args = ap.parse_args()

    if not ATRO_BASE_URL or not ATRO_USER or not ATRO_PASS:
        LOG.error("ATRO_BASE_URL / ATRO_USER / ATRO_PASS must be set in env.")
        return 2

    client = AtroClient(ATRO_BASE_URL, ATRO_USER, ATRO_PASS)

    cr_cnx = _mysql_connect(CR_HOST, CR_USER, CR_PASS, CR_NAME)
    en_cnx = _mysql_connect(EN_HOST, EN_USER, EN_PASS, EN_NAME)

    # Ensure upload folder
    folder = ensure_folder(client, "SEO Uploads")
    folder_id = folder["id"]

    since_run = args.since_run_id if args.since_run_id and args.since_run_id > 0 else None
    limit = args.limit or BATCH_LIMIT or 0

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
