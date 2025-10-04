# job_queue.py
import os
import mysql.connector
from datetime import datetime, timezone

# Module-level DB config; can be overridden via configure_db()
DB_CONFIG = {
    'host': os.getenv('ENRICH_DB_HOST', '192.168.0.28'),
    'user': os.getenv('ENRICH_DB_USER') or 'root',
    'password': os.getenv('ENRICH_DB_PASS') or 'Albatr0ss22!',
    'database': os.getenv('ENRICH_DB_NAME', 'vwr'),
    'use_pure': True,
    'connection_timeout': 10
}

QUEUE_TABLE = os.getenv('ENRICH_DB_TABLE', 'enrichment_queue')
BRAND_TABLE = os.getenv('ENRICH_BRAND_TABLE', 'brand_backlog')

DDL_QUEUE = f"""
CREATE TABLE IF NOT EXISTS {QUEUE_TABLE} (
  id BIGINT PRIMARY KEY AUTO_INCREMENT,
  sku VARCHAR(128) NOT NULL UNIQUE,
  brand_name VARCHAR(128) NULL,
  status ENUM('pending','processing','done','failed') NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  seo_product_created TINYINT(1) NOT NULL DEFAULT 0,
  content_source ENUM('supplier','brand','none') NOT NULL DEFAULT 'none',
  updated_via ENUM('supplier','brand','mixed','none') NOT NULL DEFAULT 'none',
  content_quality ENUM('unknown','rich','sparse') NOT NULL DEFAULT 'unknown',
  needs_refresh TINYINT(1) NOT NULL DEFAULT 0,
  last_enriched_at DATETIME NULL,
  last_error TEXT NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  KEY (brand_name),
  KEY (status),
  KEY (content_quality),
  KEY (needs_refresh)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

DDL_BRAND = f"""
CREATE TABLE IF NOT EXISTS {BRAND_TABLE} (
  brand_name VARCHAR(128) PRIMARY KEY,
  sparse_attempts INT NOT NULL DEFAULT 0,
  supplier_enriched INT NOT NULL DEFAULT 0,
  last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  status ENUM('todo','in_progress','done') NOT NULL DEFAULT 'todo',
  priority INT NOT NULL DEFAULT 0
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
"""

def configure_db(host=None, user=None, password=None, database=None):
    if host:
        DB_CONFIG['host'] = host
    if user is not None:
        DB_CONFIG['user'] = user
    if password is not None:
        DB_CONFIG['password'] = password
    if database:
        DB_CONFIG['database'] = database

def _require_creds():
    if not DB_CONFIG.get('user') or DB_CONFIG.get('password') == '':
        raise RuntimeError(
            "MySQL credentials not set. Provide --db-user/--db-pass or set ENRICH_DB_USER/ENRICH_DB_PASS."
        )

def get_conn():
    _require_creds()
    return mysql.connector.connect(**DB_CONFIG)

def ensure_tables():
    with get_conn() as cnx:
        with cnx.cursor() as cur:
            cur.execute(DDL_QUEUE)
            cur.execute(DDL_BRAND)
        cnx.commit()

def fetch_batch(limit: int):
    q = f"SELECT sku FROM {QUEUE_TABLE} WHERE status='pending' ORDER BY created_at ASC LIMIT %s"
    with get_conn() as cnx, cnx.cursor() as cur:
        cur.execute(q, (limit,))
        return [r[0] for r in cur.fetchall()]

def mark_processing(skus):
    if not skus:
        return
    placeholders = ",".join(["%s"] * len(skus))
    q = f"UPDATE {QUEUE_TABLE} SET status='processing', attempts=attempts+1 WHERE sku IN ({placeholders})"
    with get_conn() as cnx, cnx.cursor() as cur:
        cur.execute(q, skus)
        cnx.commit()

def mark_done(sku, seo_created: bool, quality: str = "unknown", needs_refresh: int = 0,
              brand_name: str | None = None, content_source: str = "none", updated_via: str = "none"):
    q = f"""UPDATE {QUEUE_TABLE}
            SET status='done',
                seo_product_created=%s,
                content_quality=%s,
                needs_refresh=%s,
                brand_name=%s,
                content_source=%s,
                updated_via=%s,
                last_enriched_at=%s,
                last_error=NULL
            WHERE sku=%s"""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    with get_conn() as cnx, cnx.cursor() as cur:
        cur.execute(q, (1 if seo_created else 0, quality, int(needs_refresh),
                        brand_name, content_source, updated_via, now, sku))
        cnx.commit()

def mark_failed(sku, err: str):
    q = f"UPDATE {QUEUE_TABLE} SET status='failed', last_error=%s WHERE sku=%s"
    with get_conn() as cnx, cnx.cursor() as cur:
        cur.execute(q, (err[:2000], sku))
        cnx.commit()

def note_last_error(sku: str, note: str):
    """Record a non-fatal note in last_error without changing status."""
    q = f"UPDATE {QUEUE_TABLE} SET last_error=%s WHERE sku=%s"
    with get_conn() as cnx, cnx.cursor() as cur:
        cur.execute(q, (note[:2000], sku))
        cnx.commit()

# --- Brand backlog helpers

def _calc_priority(sparse_attempts: int, supplier_enriched: int) -> int:
    return sparse_attempts * 2 + supplier_enriched

def brand_backlog_sparse(brand_name: str):
    """Increment sparse_attempts for brand (empty/useless content)."""
    if not brand_name:
        return
    with get_conn() as cnx, cnx.cursor() as cur:
        cur.execute(f"SELECT sparse_attempts, supplier_enriched FROM {BRAND_TABLE} WHERE brand_name=%s", (brand_name,))
        row = cur.fetchone()
        if row:
            sparse, sup = row
            sparse += 1
            pr = _calc_priority(sparse, sup)
            cur.execute(
                f"UPDATE {BRAND_TABLE} SET sparse_attempts=%s, priority=%s, status='todo' WHERE brand_name=%s",
                (sparse, pr, brand_name)
            )
        else:
            pr = _calc_priority(1, 0)
            cur.execute(
                f"INSERT INTO {BRAND_TABLE} (brand_name, sparse_attempts, supplier_enriched, priority, status) VALUES (%s,%s,%s,%s,'todo')",
                (brand_name, 1, 0, pr)
            )
        cnx.commit()

def brand_backlog_supplier_enriched(brand_name: str):
    """Increment supplier_enriched for brand (enriched using supplier text)."""
    if not brand_name:
        return
    with get_conn() as cnx, cnx.cursor() as cur:
        cur.execute(f"SELECT sparse_attempts, supplier_enriched FROM {BRAND_TABLE} WHERE brand_name=%s", (brand_name,))
        row = cur.fetchone()
        if row:
            sparse, sup = row
            sup += 1
            pr = _calc_priority(sparse, sup)
            cur.execute(
                f"UPDATE {BRAND_TABLE} SET supplier_enriched=%s, priority=%s, status='todo' WHERE brand_name=%s",
                (sup, pr, brand_name)
            )
        else:
            pr = _calc_priority(0, 1)
            cur.execute(
                f"INSERT INTO {BRAND_TABLE} (brand_name, sparse_attempts, supplier_enriched, priority, status) VALUES (%s,%s,%s,%s,'todo')",
                (brand_name, 0, 1, pr)
            )
        cnx.commit()
