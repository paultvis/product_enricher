# enrich.py
import os
import argparse
import sys
import csv
import math
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from threading import Lock, local as thread_local

from atro_client import AtroClient
from job_queue import (
    ensure_tables, fetch_batch, mark_processing, mark_done, mark_failed,
    configure_db, brand_backlog_sparse, brand_backlog_supplier_enriched, note_last_error
)
from enricher import enrich_product
from writer import ensure_seo_for_product, patch_ai_fields, upsert_specs_and_values

# --- Product -> SEOProduct base-field mapping (copy on first SEOProduct create)
PRODUCT_BASE_FIELDS = [
    "descriptionpromotion",
    "shortdescription2",
    "contentHeader",
    "longDescription",
    "brandDescription",
    "contentfooter",
    "supShortDescription"
]

PRODUCT_TO_SEO_FIELD_MAP = {
    "descriptionpromotion": "descriptionpromotion",
    "shortdescription2":     "vMShortdescription",
    "contentHeader":         "contentHeader",
    "longDescription":       "longDescription",
    "brandDescription":      "brandDescription",
    "contentfooter":         "contentfooter",
    "supShortDescription":   "supplierShortDescription"
}

# --- AI field mapping (patched onto SEOProduct after enrichment)
AI_FIELD_MAP = {
    "aititle": "aititle",
    "aiDesc": "aiDesc",
    # Accept either new or legacy key for brand paragraph, map to aBrandDesc
    "aboutBrandHtml": "aBrandDesc",
    "aBrandDesc": "aBrandDesc",
    "metaName": "metaName",
    "metadescription": "metadescription",
    "metakeywords": "metakeywords",
}

# thread-local storage for per-thread clients
_TLS = thread_local()

# global lock for writing to CSV atomically
_LOG_LOCK = Lock()

def parse_args():
    p = argparse.ArgumentParser(description="Enrich products into SEOProduct (with provenance/backlog), parallelised")
    p.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "20")))
    p.add_argument("--workers", type=int, default=int(os.getenv("WORKERS", "0")),
                   help="Number of parallel workers. 0 = auto (min(32, cpu*4)).")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--log-errors", action="store_true")
    p.add_argument("--error-log", default=os.getenv("ENRICH_ERROR_LOG", "errors.csv"),
                   help="Path to CSV file for capturing sku+error rows. If relative, it will be created next to this script.")
    # DB overrides
    p.add_argument("--db-host", default=os.getenv("ENRICH_DB_HOST", "192.168.0.28"))
    p.add_argument("--db-name", default=os.getenv("ENRICH_DB_NAME", "vwr"))
    p.add_argument("--db-user", default=os.getenv("ENRICH_DB_USER"))
    p.add_argument("--db-pass", default=os.getenv("ENRICH_DB_PASS"))
    # Atro overrides
    p.add_argument("--atro-url", default=os.getenv("ATRO_BASE_URL", "http://192.168.0.29"))
    p.add_argument("--atro-user", default=os.getenv("ATRO_USER", "pault"))
    p.add_argument("--atro-pass", default=os.getenv("ATRO_PASS", "Albatr0ss22!"))
    return p.parse_args()

def _detect_content_source(prod: dict) -> str:
    """Very simple: if longDescription/shortdescription2 exist → supplier; later we’ll set to 'brand' when we have a brand corpus."""
    if (prod.get("longDescription") or prod.get("shortdescription2")):
        return "supplier"
    return "none"

def _resolve_log_path(path: str) -> str:
    """
    Make error-log path absolute. If 'path' has no directory component,
    create it next to this script.
    """
    if os.path.isabs(path):
        return path
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, path)

def _init_error_logger(path: str):
    """
    Prepare a CSV logger in append mode. Creates the file and header if missing.
    Returns the absolute path and a callable: log_error_row(sku, error_type, error_message).
    """
    log_path = _resolve_log_path(path)
    log_dir = os.path.dirname(log_path)
    if log_dir:
        os.makedirs(log_dir, exist_ok=True)

    if not os.path.exists(log_path):
        with open(log_path, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["timestamp_utc", "sku", "error_type", "error_message"])

    def _log_error_row(sku: str, error_type: str, error_message: str):
        try:
            with _LOG_LOCK:
                with open(log_path, "a", newline="", encoding="utf-8") as f:
                    w = csv.writer(f)
                    w.writerow([
                        datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"),
                        sku,
                        error_type,
                        (error_message or "")[:1800]  # keep row reasonable
                    ])
        except Exception as _e:
            # Make noise if logging itself fails
            print(f"[WARN] Failed to write error log at {log_path}: {_e}", file=sys.stderr)

    return log_path, _log_error_row

def _get_thread_client(atro_url: str, atro_user: str, atro_pass: str) -> AtroClient:
    """
    One AtroClient per worker thread; re-used across SKUs handled by the same thread.
    """
    client = getattr(_TLS, "client", None)
    if client is None:
        client = AtroClient(atro_url, atro_user, atro_pass)
        setattr(_TLS, "client", client)
        # also cache specs per thread (list is read-only enough for our usage)
        setattr(_TLS, "cached_specs", client.list_specs())
    return client

def _process_one_sku(sku: str, args, log_error_row, select_fields):
    """
    Worker body. Returns tuple (sku, processed_ok: bool, fail_msg_or_None)
    """
    try:
        client = _get_thread_client(args.atro_url, args.atro_user, args.atro_pass)
        cached_specs = getattr(_TLS, "cached_specs")

        prod = client.get_product_by_sku(sku, select_fields=select_fields)
        if not prod:
            raise RuntimeError(f"Product not found for SKU={sku}")

        brand_name = prod.get("brandName")
        content_source = _detect_content_source(prod)

        # Ensure SEO exists (and seed on first create from Product using PRODUCT_TO_SEO_FIELD_MAP)
        seo, created = ensure_seo_for_product(client, prod, PRODUCT_TO_SEO_FIELD_MAP)

        if not args.dry_run:
            # Category for context
            category = None
            cat_names = prod.get("categoriesNames") or {}
            if isinstance(cat_names, dict) and cat_names:
                category = next(iter(cat_names.values()), None)

            # Pull authoritative SEO fields to feed the model (Route A)
            seo_fresh = client.find_seo_by_product(
                prod["id"],
                select_fields=[
                    "id",
                    "brandcontent",
                    "vMShortdescription",
                    "contentHeader",
                    "longDescription",
                    "brandDescription",
                    "contentfooter",
                ],
            ) or {}

            # Build AI input (SEO first; Product as fallback)
            p_in = {
                "brand": brand_name,
                "mpn": prod.get("mpn"),
                "name": prod.get("name"),
                "category": category,
                "base": {
                    # SEO (authoritative / higher precedence for the prompt)
                    "brandcontent":       seo_fresh.get("brandcontent"),
                    "vmshortdescription": seo_fresh.get("vMShortdescription"),
                    "content_header":     seo_fresh.get("contentHeader"),
                    "long_description":   seo_fresh.get("longDescription"),
                    "brand_description":  seo_fresh.get("brandDescription"),
                    "content_footer":     seo_fresh.get("contentfooter"),

                    # Product fallbacks (used when SEO fields are empty)
                    "shortdescription2":    prod.get("shortdescription2"),
                    "contentHeader":        prod.get("contentHeader"),
                    "longDescription":      prod.get("longDescription"),
                    "brandDescription":     prod.get("brandDescription"),
                    "contentfooter":        prod.get("contentfooter"),
                    "descriptionpromotion": prod.get("descriptionpromotion"),
                    "supShortDescription":  prod.get("supShortDescription"),
                }
            }

            ai = enrich_product(p_in)

            # If the enricher fell back due to API/format errors, log CSV + DB note (non-fatal)
            if isinstance(ai, dict) and ai.get("_source") == "fallback":
                err_msg = str(ai.get("_error") or "fallback without reason")
                log_error_row(sku, "EnrichmentFallback", err_msg)
                try:
                    note_last_error(sku, err_msg)
                except Exception:
                    pass

            # Patch AI-only fields
            patch_ai_fields(client, seo["id"], ai, AI_FIELD_MAP)

            # Upsert specs + values
            upsert_specs_and_values(client, seo["id"], ai.get("specs") or {}, cached_specs)

            # Determine quality/refresh + brand backlog signals
            quality = ai.get("quality", "unknown")
            needs_refresh = 1 if quality == "sparse" else 0

            # updated_via for now mirrors detected content_source (until brand corpus stage)
            updated_via = "supplier" if content_source == "supplier" else "none"

            # Brand backlog counters
            if brand_name:
                if quality == "sparse":
                    brand_backlog_sparse(brand_name)
                elif updated_via == "supplier":
                    brand_backlog_supplier_enriched(brand_name)
        else:
            quality = "unknown"
            needs_refresh = 0
            updated_via = "none"
            brand_name = prod.get("brandName")

        # Mark done
        mark_done(
            sku,
            seo_created=created,
            quality=quality,
            needs_refresh=needs_refresh,
            brand_name=brand_name,
            content_source=content_source,
            updated_via=updated_via
        )
        return (sku, True, None)

    except Exception as e:
        msg = f"{type(e).__name__}: {e}"
        # stderr signal for this SKU
        print(f"[ERROR] SKU {sku}: {msg}", file=sys.stderr)
        # CSV row
        log_error_row(sku, type(e).__name__, f"{e}\n{traceback.format_exc(limit=3)}")
        # persist failure message
        try:
            note_last_error(sku, msg)
        except Exception:
            pass
        mark_failed(sku, msg)
        return (sku, False, msg)

def _auto_workers(n_cli: int) -> int:
    if n_cli and n_cli > 0:
        return n_cli
    try:
        import multiprocessing as mp
        cpu = mp.cpu_count()
    except Exception:
        cpu = 4
    # IO-bound workload (HTTP + DB): scale above CPU count
    return max(2, min(32, cpu * 4))

def main():
    args = parse_args()

    # Allow raw dump dir to be set here and passed via env (used by enricher)
    raw_dump_dir = os.getenv("RAW_DUMP_DIR")
    if not raw_dump_dir:
        raw_dump_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw_dumps")
        os.environ["RAW_DUMP_DIR"] = raw_dump_dir

    # Configure DB (errors if user/pass missing)
    configure_db(host=args.db_host, user=args.db_user, password=args.db_pass, database=args.db_name)

    ensure_tables()
    skus = fetch_batch(args.batch_size)
    if not skus:
        print("No pending SKUs.")
        return 0

    # Mark these SKUs as processing before fanning out
    mark_processing(skus)

    # Prepare error logger
    log_path, log_error_row = _init_error_logger(args.error_log)
    if args.log_errors:
        print(f"[INFO] Error log path: {log_path}", file=sys.stderr)
        print(f"[INFO] Raw dump dir: {raw_dump_dir}", file=sys.stderr)

    # Select fields once (thread-safe constant)
    select_fields = ["sku", "name", "mpn", "brandName", "categoriesNames", "classificationsNames"] + PRODUCT_BASE_FIELDS

    workers = _auto_workers(args.workers)
    print(f"Processing {len(skus)} SKUs with {workers} workers...")

    processed = failed = 0
    # Fan out
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="enrich") as ex:
        futures = {ex.submit(_process_one_sku, sku, args, log_error_row, select_fields): sku for sku in skus}
        for i, fut in enumerate(as_completed(futures), start=1):
            sku = futures[fut]
            try:
                _sku, ok, msg = fut.result()
                if ok:
                    processed += 1
                else:
                    failed += 1
            except Exception as e:
                failed += 1
                err = f"WorkerCrash {type(e).__name__}: {e}"
                print(f"[ERROR] SKU {sku}: {err}", file=sys.stderr)
                log_error_row(sku, "WorkerCrash", f"{e}\n{traceback.format_exc(limit=3)}")
                try:
                    note_last_error(sku, err)
                except Exception:
                    pass
                mark_failed(sku, err)

            # light progress heartbeat every 10 items
            if i % 10 == 0 or i == len(skus):
                print(f"[PROGRESS] {i}/{len(skus)} processed...")

    print(f"Done. processed={processed}, failed={failed}")
    return 1 if failed else 0

if __name__ == "__main__":
    sys.exit(main())
