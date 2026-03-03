#!/usr/bin/env python3
"""
MktList — MongoDB (locations) -> PostgreSQL Sync + Archive Checker
Incremental: only syncs new docs, archives dead listings.

Usage:
    python sync.py          # Run once (sync + archive)
    python sync.py --loop   # Loop every CYCLE_SLEEP seconds
    python sync.py --sync-only
    python sync.py --archive-only
"""

import os
import re
import json
import time
import logging
import argparse
import requests
from datetime import datetime, timezone

from pymongo import MongoClient
import psycopg2
from psycopg2.extras import execute_values, RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

MONGO_URI = os.environ["MONGODB_URI"]
MONGO_DB = os.getenv("MONGO_MKTLIST_DB", "mktlist")
MONGO_COL = os.getenv("MONGO_MKTLIST_COL", "locations")

PG_DSN = os.environ["POSTGRES_DSN"]
PG_TABLE = os.getenv("PG_TABLE_MKTLIST", "mktlist_listings")
PG_ARCHIVE = os.getenv("PG_ARCHIVE_MKTLIST", "mktlist_archive")

BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))
CYCLE_SLEEP = int(os.getenv("CYCLE_SLEEP", "86400"))
ARCHIVE_DELAY = 2
ARCHIVE_TIMEOUT = 15

HOMEPAGE_REDIRECTS = {
    "https://www.mktlist.ca",
}

CHECK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("mktlist-sync")

# ============================================================
# HELPERS
# ============================================================

def to_pg_array(lst):
    if not lst or not isinstance(lst, list):
        return None
    cleaned = [str(x) for x in lst if x]
    return cleaned if cleaned else None


def to_pg_timestamp(v):
    if v is None:
        return None
    if isinstance(v, datetime):
        return v
    if isinstance(v, dict) and "$date" in v:
        try:
            return datetime.fromisoformat(v["$date"].replace("Z", "+00:00"))
        except (ValueError, TypeError):
            return None
    if isinstance(v, (int, float)):
        try:
            if v > 1e12:
                v = v / 1000
            return datetime.fromtimestamp(v, tz=timezone.utc)
        except (OSError, ValueError, OverflowError):
            return None
    return None


def parse_int_safe(v):
    if v is None:
        return None
    if isinstance(v, int):
        return v
    try:
        return int(str(v).strip())
    except (ValueError, TypeError):
        return None


def parse_price_string(s):
    if s is None:
        return None
    if isinstance(s, (int, float)):
        return float(s)
    cleaned = re.sub(r"[^\d.]", "", str(s).replace(",", "").replace("\xa0", ""))
    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None


def parse_sqft_range(raw):
    if not raw:
        return None, None, None
    m = re.findall(r"(\d[\d,]*)", str(raw))
    nums = []
    for x in m:
        try:
            nums.append(int(x.replace(",", "")))
        except ValueError:
            pass
    if not nums:
        return None, None, None
    lo = nums[0]
    hi = nums[-1] if len(nums) > 1 else lo
    avg_m2 = int(round((lo + hi) / 2 * 0.092903))
    return lo, hi, avg_m2


def extract_city_province(title, address):
    city, province = None, None
    if title:
        parts = [p.strip() for p in title.split(",")]
        if len(parts) >= 3:
            province = parts[-2].strip() if parts[-2].strip() != "CA" else None
            city_part = parts[-3] if len(parts) >= 4 else parts[-2]
            city = re.sub(r"\s*\(.*?\)", "", city_part).strip()
    if not city and address:
        parts = [p.strip() for p in address.split(",")]
        for p in parts:
            clean = re.sub(r"\s*\(.*?\)", "", p).strip()
            if clean and not clean.startswith("#") and not re.match(r"^\d", clean):
                city = clean
                break
    return city, province


# ============================================================
# ROW BUILDER
# ============================================================

def build_row(d):
    pd = d.get("property_details") or {}
    realtor = d.get("realtor") or {}
    brokerage = d.get("brokerage") or {}
    price_val = parse_price_string(d.get("price"))
    sqft_raw = pd.get("square_footage")
    sqft_min, sqft_max, m2_approx = parse_sqft_range(sqft_raw)
    beds = parse_int_safe(d.get("beds"))
    baths = parse_int_safe(d.get("baths"))
    city, province = extract_city_province(d.get("title"), d.get("address"))
    views = parse_int_safe(d.get("views"))
    total_parking = parse_int_safe(pd.get("total_parking_spaces"))
    images = d.get("images") or []
    images = [img for img in images if img and "notavailable" not in img]
    images = to_pg_array(images)
    rooms = d.get("rooms")
    rooms_json = json.dumps(rooms) if rooms else None
    return (
        d.get("mkt_id") if d.get("mkt_id") != "No Data" else None,
        d.get("url"), d.get("address"), d.get("title"),
        city, province, pd.get("community_name"),
        pd.get("property_type"), pd.get("building_type"),
        beds, baths, sqft_raw, sqft_min, sqft_max, m2_approx,
        d.get("status"), d.get("price"), price_val, "CAD",
        d.get("description"), to_pg_array(d.get("features")),
        pd.get("parking_type"), total_parking, pd.get("heating_type"),
        pd.get("cooling"), pd.get("flooring"), pd.get("exterior_finish"),
        pd.get("basement_type"), pd.get("building_amenities"),
        pd.get("appliances_included"), images,
        len(images) if images else 0, rooms_json,
        realtor.get("name"), realtor.get("phone"), realtor.get("website"),
        brokerage.get("name"), brokerage.get("url"),
        views, d.get("added_on"), to_pg_timestamp(d.get("scraped_at")),
    )


INSERT_SQL = """
    INSERT INTO mktlist_listings (
        mkt_id, url, address, title, city,
        province, community_name, property_type, building_type,
        beds, baths, square_footage_raw, surface_sqft_min,
        surface_sqft_max, surface_m2_approx, status,
        price_raw, price_value, currency,
        description, features, parking_type, total_parking,
        heating_type, cooling, flooring, exterior_finish,
        basement_type, building_amenities, appliances,
        images, images_count, rooms,
        realtor_name, realtor_phone, realtor_website,
        brokerage_name, brokerage_url,
        views, added_on, scraped_at
    ) VALUES %s
    ON CONFLICT (url) DO NOTHING
"""

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS mktlist_listings (
    id SERIAL PRIMARY KEY,
    mkt_id VARCHAR(50),
    url TEXT UNIQUE NOT NULL,
    address VARCHAR(500),
    title VARCHAR(500),
    city VARCHAR(200),
    province VARCHAR(100),
    community_name VARCHAR(200),
    property_type VARCHAR(100),
    building_type VARCHAR(100),
    beds INTEGER,
    baths INTEGER,
    square_footage_raw VARCHAR(100),
    surface_sqft_min INTEGER,
    surface_sqft_max INTEGER,
    surface_m2_approx INTEGER,
    status VARCHAR(50),
    price_raw VARCHAR(100),
    price_value DOUBLE PRECISION,
    currency VARCHAR(5) DEFAULT 'CAD',
    description TEXT,
    features TEXT[],
    parking_type VARCHAR(100),
    total_parking INTEGER,
    heating_type VARCHAR(100),
    cooling VARCHAR(100),
    flooring VARCHAR(200),
    exterior_finish VARCHAR(200),
    basement_type VARCHAR(100),
    building_amenities VARCHAR(500),
    appliances VARCHAR(500),
    images TEXT[],
    images_count INTEGER DEFAULT 0,
    rooms JSONB,
    realtor_name VARCHAR(300),
    realtor_phone VARCHAR(50),
    realtor_website TEXT,
    brokerage_name VARCHAR(300),
    brokerage_url TEXT,
    views INTEGER,
    added_on VARCHAR(50),
    scraped_at TIMESTAMPTZ,
    synced_at TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_mktlist_city ON mktlist_listings(city);
CREATE INDEX IF NOT EXISTS idx_mktlist_price ON mktlist_listings(price_value);
"""

# ============================================================
# SCHEMA + ARCHIVE
# ============================================================

def ensure_schema(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(SCHEMA_SQL)
    pg_conn.commit()
    log.info("✅ Table mktlist_listings ensured")


def ensure_archive_table(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(
        "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)",
        (PG_ARCHIVE,)
    )
    if cur.fetchone()[0]:
        return
    cur.execute(f"""
        CREATE TABLE {PG_ARCHIVE} (
            LIKE {PG_TABLE} INCLUDING DEFAULTS INCLUDING GENERATED
        )
    """)
    cur.execute(f"""
        ALTER TABLE {PG_ARCHIVE}
            ADD COLUMN IF NOT EXISTS archived_at TIMESTAMPTZ DEFAULT NOW(),
            ADD COLUMN IF NOT EXISTS archive_reason VARCHAR(50) DEFAULT 'listing_removed'
    """)
    cur.execute(f"""
        SELECT conname FROM pg_constraint
        WHERE conrelid = '{PG_ARCHIVE}'::regclass
        AND contype IN ('u', 'p')
        AND conname != '{PG_ARCHIVE}_pkey'
    """)
    for row in cur.fetchall():
        cur.execute(f"ALTER TABLE {PG_ARCHIVE} DROP CONSTRAINT IF EXISTS {row[0]}")
    pg_conn.commit()
    log.info(f"✅ Archive table {PG_ARCHIVE} created")


# ============================================================
# SYNC
# ============================================================

def get_existing_ids(pg_conn):
    cur = pg_conn.cursor()
    cur.execute(f"SELECT url FROM {PG_TABLE} WHERE url IS NOT NULL")
    return {str(row[0]) for row in cur.fetchall()}


def _flush_batch(pg_conn, batch, stats):
    try:
        cur = pg_conn.cursor()
        execute_values(cur, INSERT_SQL, batch)
        pg_conn.commit()
        stats["new_synced"] += len(batch)
    except Exception as e:
        pg_conn.rollback()
        log.error(f"Batch insert error: {e}")
        recovered = 0
        for row in batch:
            try:
                cur = pg_conn.cursor()
                execute_values(cur, INSERT_SQL, [row])
                pg_conn.commit()
                recovered += 1
            except Exception:
                pg_conn.rollback()
                stats["errors"] += 1
        stats["new_synced"] += recovered


def sync(mongo_col, pg_conn):
    stats = {"total_mongo": 0, "already_in_pg": 0, "new_synced": 0, "errors": 0}
    stats["total_mongo"] = mongo_col.count_documents({})

    existing_ids = get_existing_ids(pg_conn)
    stats["already_in_pg"] = len(existing_ids)

    log.info(f"  MongoDB: {stats['total_mongo']} | PostgreSQL: {stats['already_in_pg']}")

    if stats["total_mongo"] == 0:
        log.info("  No documents in MongoDB — skipping")
        return stats

    batch = []
    for doc in mongo_col.find({}, batch_size=BATCH_SIZE):
        doc_id = doc.get("url")
        if not doc_id or str(doc_id) in existing_ids:
            continue
        try:
            batch.append(build_row(doc))
        except Exception as e:
            stats["errors"] += 1
            if stats["errors"] <= 5:
                log.warning(f"  Row build error (url={doc_id}): {e}")
        if len(batch) >= BATCH_SIZE:
            _flush_batch(pg_conn, batch, stats)
            batch = []
            log.info(f"  Synced {stats['new_synced']} so far...")

    if batch:
        _flush_batch(pg_conn, batch, stats)

    return stats


# ============================================================
# ARCHIVE
# ============================================================

def check_url_alive(url):
    try:
        resp = requests.head(
            url, headers=CHECK_HEADERS,
            timeout=ARCHIVE_TIMEOUT, allow_redirects=True
        )
        if resp.status_code in (404, 410):
            return False
        if resp.url.rstrip("/") in HOMEPAGE_REDIRECTS:
            return False
        if resp.status_code == 403:
            return True
        if resp.status_code < 400:
            return True
        if resp.status_code >= 500:
            return True
        return False
    except requests.RequestException:
        return True


def archive_listing(pg_conn, unique_val, reason="listing_removed"):
    cur = pg_conn.cursor()
    try:
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = %s AND column_name NOT IN ('archived_at', 'archive_reason')
            ORDER BY ordinal_position
        """, (PG_TABLE,))
        columns = [r[0] for r in cur.fetchall()]
        cols_str = ", ".join(columns)
        cur.execute(f"""
            INSERT INTO {PG_ARCHIVE} ({cols_str}, archived_at, archive_reason)
            SELECT {cols_str}, NOW(), %s FROM {PG_TABLE} WHERE url = %s
        """, (reason, unique_val))
        cur.execute(f"DELETE FROM {PG_TABLE} WHERE url = %s", (unique_val,))
        pg_conn.commit()
        return True
    except Exception as e:
        pg_conn.rollback()
        log.error(f"Archive error for {unique_val}: {e}")
        return False


def archive_check(pg_conn):
    stats = {"checked": 0, "alive": 0, "archived": 0, "errors": 0}
    cur = pg_conn.cursor(cursor_factory=RealDictCursor)
    cur.execute(f"SELECT url FROM {PG_TABLE} WHERE url IS NOT NULL")
    rows = cur.fetchall()
    log.info(f"  {len(rows)} listings to check")

    for i, row in enumerate(rows):
        url = row.get("url")
        if not url:
            continue
        alive = check_url_alive(url)
        stats["checked"] += 1
        if alive:
            stats["alive"] += 1
        else:
            if archive_listing(pg_conn, url):
                stats["archived"] += 1
                log.info(f"  📦 Archived: {url[:60]}")
            else:
                stats["errors"] += 1
        if (i + 1) % 50 == 0:
            log.info(f"  Progress: {i+1}/{len(rows)} | alive={stats['alive']} archived={stats['archived']}")
        time.sleep(ARCHIVE_DELAY)

    return stats


# ============================================================
# MAIN CYCLE
# ============================================================

def run_cycle(mongo_col, pg_conn, do_sync=True, do_archive=True):
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    log.info(f"\n{'='*60}")
    log.info(f"[🇨🇦 MKTLIST] CYCLE START: {now}")
    log.info(f"{'='*60}")

    ensure_schema(pg_conn)
    ensure_archive_table(pg_conn)

    if do_sync:
        log.info("\n--- PHASE 1: SYNC MongoDB -> PostgreSQL ---")
        stats = sync(mongo_col, pg_conn)
        log.info(f"  ✅ +{stats['new_synced']} new | {stats['errors']} errors")

    if do_archive:
        log.info("\n--- PHASE 2: ARCHIVE CHECK ---")
        stats = archive_check(pg_conn)
        log.info(f"  ✅ {stats['checked']} checked | {stats['alive']} alive | "
                 f"{stats['archived']} archived | {stats['errors']} errors")

    cur = pg_conn.cursor()
    cur.execute(f"SELECT COUNT(*) FROM {PG_TABLE}")
    active = cur.fetchone()[0]
    try:
        cur.execute(f"SELECT COUNT(*) FROM {PG_ARCHIVE}")
        archived = cur.fetchone()[0]
    except Exception:
        pg_conn.rollback()
        archived = 0

    log.info(f"\n📊 {PG_TABLE}: {active} active | {PG_ARCHIVE}: {archived} archived")
    log.info("✅ CYCLE COMPLETE\n")


def main():
    parser = argparse.ArgumentParser(description="MktList Sync + Archive")
    parser.add_argument("--sync-only", action="store_true")
    parser.add_argument("--archive-only", action="store_true")
    parser.add_argument("--loop", action="store_true")
    args = parser.parse_args()

    do_sync = not args.archive_only
    do_archive = not args.sync_only

    log.info("Connecting to MongoDB...")
    mongo = MongoClient(MONGO_URI)
    mongo.admin.command("ping")
    log.info("✅ MongoDB connected")

    log.info("Connecting to PostgreSQL...")
    pg = psycopg2.connect(PG_DSN)
    log.info("✅ PostgreSQL connected")

    try:
        if args.loop:
            while True:
                try:
                    col = mongo[MONGO_DB][MONGO_COL]
                    run_cycle(col, pg, do_sync, do_archive)
                except Exception as e:
                    log.error(f"Cycle error: {e}")
                    try:
                        pg.close()
                    except Exception:
                        pass
                    pg = psycopg2.connect(PG_DSN)
                log.info(f"💤 Sleeping {CYCLE_SLEEP}s until next cycle...")
                time.sleep(CYCLE_SLEEP)
        else:
            col = mongo[MONGO_DB][MONGO_COL]
            run_cycle(col, pg, do_sync, do_archive)

    except KeyboardInterrupt:
        log.info("\n⚠️ Stopped by user")
    finally:
        pg.close()
        mongo.close()
        log.info("🔌 Connections closed")


if __name__ == "__main__":
    main()
