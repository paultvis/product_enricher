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
import re
import json

from atro_client import AtroClient
from job_queue import (
    ensure_tables, fetch_batch, mark_processing, mark_done, mark_failed,
    configure_db, brand_backlog_sparse, brand_backlog_supplier_enriched, note_last_error
)
from enricher import enrich_product, _build_messages  # import private helper for payload dump
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

# ---------------------
# Quality inference helpers
# ---------------------

_TAG_RE = re.compile(r"<[^>]+>")
_H3_RE = re.compile(r"<\s*h3\b", flags=re.I)

def _strip_html(s: str) -> str:
    if not isinstance(s, str):
        return ""
    return _TAG_RE.sub(" ", s).replace("\xa0", " ").strip()

def _infer_quality(ai: dict) -> str:
    """
    Heuristic quality enum based on the generated payload.
    Returns one of: 'rich', 'sparse', 'unknown'.
    """
    try:
        # Fallback source from enricher is a strong sparse signal
        if str(ai.get("_source", "")).lower() == "fallback":
            return "sparse"

        desc_html = ai.get("aiDesc") or ""
        about_html = ai.get("aboutBrandHtml") or ai.get("aBrandDesc") or ""
        why_html = ai.get("whyBuyFromUsHtml") or ""
        specs = ai.get("specs") or {}

        text = _strip_html(desc_html)
        word_count = len([w for w in text.split() if w])
        h3_count = len(_H3_RE.findall(desc_html or ""))

        # Count specs heuristically
        if isinstance(specs, dict):
            spec_count = len([k for k, v in specs.items() if str(v).strip()])
        elif isinstance(specs, list):
            spec_count = len([x for x in specs if str(x).strip()])
        else:
            spec_count = 0

        has_brand = bool(_strip_html(about_html))
        has_why = bool(_strip_html(why_html))

        # Rich if there is substantial body, structure, and supporting blocks
        if (word_count >= 250 and h3_count >= 2 and (has_brand or has_why)) or word_count >= 450:
            return "rich"

        # Sparse if too short or unstructured
        if word_count < 120 or h3_count == 0:
            return "sparse"

        # If specs are present and body is reasonable, lean rich
        if word_count >= 220 and spec_count >= 3:
            return "rich"

        # Otherwise unknown (middle ground)
        return "unknown"
    except Exception:
        return "unknown"

# ---------------------
# Atro payload normalization
# ---------------------

def _normalize_ai_for_atro(ai: dict) -> dict:
    """
    Normalize model output so Atro receives DB-friendly types.
    - metakeywords: list -> comma-separated string
    """
    out = dict(ai or {})
    mk = out.get("metakeywords")
    if isinstance(mk, list):
        items = [str(x).strip() for x in mk if str(x).strip()]
        out["metakeywords"] = ", ".join(items) if items else ""
    elif mk is None:
        out["metakeywords"] = ""
    else:
        out["metakeywords"] = str(mk).strip()
    return out

# ---------------------
# Payload dump utilities
# ---------------------

def _payload_dir_from_args(args) -> str:
    # CLI flag takes priority; else env; else default next to script
    cli_dir = getattr(args, "payload_dir", None)
    if cli_dir:
        return cli_dir
    env_dir = os.getenv("PAYLOAD_DUMP_DIR")
    if env_dir:
        return env_dir
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base_dir, "payload_dumps")

def _dump_payload(sku: str, p_in: dict, messages: list, args):
    """
    Write p_in and messages to disk; print brief size stats to stderr if --log-errors.
    """
    try:
        out_dir = _payload_dir_from_args(args)
        os.makedirs(out_dir, exist_ok=True)

        p_in_path = os.path.join(out_dir, f"sku_{sku}__p_in.json")
        with open(p_in_path, "w", encoding="utf-8") as f:
            json.dump(p_in, f, ensure_ascii=False, indent=2)

        messages_path = os.path.join(out_dir, f"sku_{sku}__messages.json")
        with open(messages_path, "w", encoding="utf-8") as f:
            json.dump(messages, f, ensure_ascii=False, indent=2)

        # quick sizes
        p_in_bytes = len(json.dumps(p_in, ensure_ascii=False))
        messages_bytes = len(json.dumps(messages, ensure_ascii=False))
        approx_tokens = int((p_in_bytes + messages_bytes) / 4)  # rough heuristic

        if getattr(args, "log_errors", False):
            print(
                f"[PAYLOAD] sku={sku} p_in={p_in_bytes}B messages={messages_bytes}B approx_tokens~{approx_tokens}",
                file=sys.stderr
            )

    except Exception as e:
        # non-fatal
        if getattr(args, "log_errors", False):
            print(f"[WARN] Payload dump failed for sku={sku}: {e}", file=sys.stderr)

def parse_args():
    p = argparse.ArgumentParser(description="Enrich products into SEOProduct (with provenance/backlog), parallelised")
    p.add_argument("--batch-size", type=int, default=int(os.getenv("BATCH_SIZE", "20")))
    p.add_argument("--workers", type=int, default=int(os.getenv("WORKERS", "0")),
                   help="Number of parallel workers. 0 = auto (min(32, cpu*4)).")
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--log-errors", action="store_true")
    p.add_argument("--error-log", default=os.getenv("ENRICH_ERROR_LOG", "errors.csv"),
                   help="Path to CSV file for capturing sku+error rows. If relative, it will be created next to this script.")
    p.add_argument("--dump-payloads", action="store_true",
                   help="Dump the OpenAI input (p_in + messages) to payload_dumps/ (or --payload-dir).")
    p.add_argument("--payload-dir", default=None,
                   help="Directory for payload dumps. Defaults to ./payload_dumps or PAYLOAD_DUMP_DIR.")
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
                        (error_message or "")[:1800]
                    ])
        except Exception as _e:
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

            # ----- optional payload dump (before calling the API) -----
            if getattr(args, "dump_payloads", False):
                try:
                    messages = _build_messages(p_in)  # exact messages sent inside enricher
                except Exception:
                    messages = []
                _dump_payload(sku, p_in, messages, args)

            ai = enrich_product(p_in)

            # If the enricher fell back due to API/format errors, log CSV + DB note (non-fatal)
            if isinstance(ai, dict) and ai.get("_source") == "fallback":
                err_msg = str(ai.get("_error") or "fallback without reason")
                log_error_row(sku, "EnrichmentFallback", err_msg)
                try:
                    note_last_error(sku, err_msg)
                except Exception:
                    pass

            # Normalize payload so Atro doesn't choke on arrays (metakeywords)
            ai = _normalize_ai_for_atro(ai)

            # Patch AI-only fields
            patch_ai_fields(client, seo["id"], ai, AI_FIELD_MAP)

            # Upsert specs + values
            upsert_specs_and_values(client, seo["id"], ai.get("specs") or {}, cached_specs)

            # Determine quality/refresh + brand backlog signals
            allowed_qualities = {"unknown", "rich", "sparse"}
            q_in = str(ai.get("quality", "") or "").strip().lower()
            if q_in in allowed_qualities:
                quality = q_in
            else:
                quality = _infer_quality(ai)

            # Treat 'unknown' as in-need-of-refresh for now (unknown OR sparse)
            needs_refresh = 1 if quality in ("sparse", "unknown") else 0

            # updated_via for now mirrors detected content_source
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
        print(f"[ERROR] SKU {sku}: {msg}", file=sys.stderr)
        log_error_row(sku, type(e).__name__, f"{e}\n{traceback.format_exc(limit=3)}")
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
    return max(2, min(32, cpu * 4))

def main():
    args = parse_args()

    raw_dump_dir = os.getenv("RAW_DUMP_DIR")
    if not raw_dump_dir:
        raw_dump_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "raw_dumps")
        os.environ["RAW_DUMP_DIR"] = raw_dump_dir

    configure_db(host=args.db_host, user=args.db_user, password=args.db_pass, database=args.db_name)

    ensure_tables()
    skus = fetch_batch(args.batch_size)
    if not skus:
        print("No pending SKUs.")
        return 0

    mark_processing(skus)

    log_path, log_error_row = _init_error_logger(args.error_log)
    if args.log_errors:
        print(f"[INFO] Error log path: {log_path}", file=sys.stderr)
        print(f"[INFO] Raw dump dir: {raw_dump_dir}", file=sys.stderr)

    select_fields = ["sku", "name", "mpn", "brandName", "categoriesNames", "classificationsNames"] + PRODUCT_BASE_FIELDS

    workers = _auto_workers(args.workers)
    print(f"Processing {len(skus)} SKUs with {workers} workers...")

    processed = failed = 0
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

            if i % 10 == 0 or i == len(skus):
                print(f"[PROGRESS] {i}/{len(skus)} processed...")

    print(f"Done. processed={processed}, failed={failed}")
    return 1 if failed else 0

if __name__ == "__main__":
    sys.exit(main())
