#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MktList Data Cleaner — Locations (Incremental)
Normalise raw MongoDB → locations_clean collection.

Usage:
    python cleaner.py              # Incremental (only new docs)
    python cleaner.py --full       # Drop + recreate
    python cleaner.py --dry-run    # Preview, no writes
    python cleaner.py --sample 5   # Show N docs after cleaning
"""

import os
import re
import argparse
from datetime import datetime, timezone
from pymongo import MongoClient, UpdateOne, ASCENDING
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# CONFIG
# ============================================================

MONGODB_URI = os.getenv("MONGODB_URI", "")
if not MONGODB_URI:
    raise RuntimeError("MONGODB_URI is not set or empty")

MONGODB_DATABASE  = os.getenv("MONGO_MKTLIST_DB", "mktlist")
SOURCE_COLLECTION = os.getenv("MONGO_MKTLIST_COL", "locations")
CLEAN_COLLECTION  = os.getenv("MONGO_MKTLIST_COL_CLEAN", "locations_clean")
BATCH_SIZE        = int(os.getenv("BATCH_SIZE", "500"))

# CAD monthly rent thresholds
MIN_PRICE   = 200
MAX_PRICE   = 50_000
MIN_SURFACE = 10
MAX_SURFACE = 2_000
MAX_ROOMS   = 30

# ============================================================
# CLEANING HELPERS
# ============================================================

def parse_price(raw: str | None) -> float | None:
    """Parse price string like '$1,500/month' → 1500.0"""
    if raw is None:
        return None
    cleaned = re.sub(r"[^\d.]", "", raw.replace(",", ""))
    try:
        return float(cleaned) if cleaned else None
    except ValueError:
        return None


def parse_int(raw) -> int | None:
    if raw is None:
        return None
    try:
        return int(str(raw).strip())
    except (TypeError, ValueError):
        return None


def parse_surface(details: dict) -> float | None:
    """Extract surface from property_details dict."""
    for key in ("living_area", "total_area", "floor_space", "area"):
        val = details.get(key)
        if val:
            m = re.search(r"[\d.]+", str(val))
            if m:
                try:
                    return float(m.group(0))
                except ValueError:
                    pass
    return None


def parse_surface_sqft(details: dict) -> float | None:
    """Check if surface value is in sqft by looking for sqft keyword."""
    for key in ("living_area", "total_area", "floor_space", "area"):
        val = details.get(key)
        if val and "sqft" in str(val).lower():
            m = re.search(r"[\d.]+", str(val))
            if m:
                try:
                    return float(m.group(0))
                except ValueError:
                    pass
    return None


def clean_description(text: str | None) -> str | None:
    if not text:
        return None
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text).strip()
    return text if len(text) > 20 else None


def extract_city(address: str | None) -> str | None:
    """Best-effort: last comma-separated part is often city/province."""
    if not address:
        return None
    parts = [p.strip() for p in address.split(",")]
    # Typically: "123 Main St, City, Province  PostalCode"
    # City is usually the 2nd-to-last or last component before postal
    for part in reversed(parts):
        # Skip pure postal codes (e.g. "K1A 0B1")
        if re.match(r"^[A-Z]\d[A-Z]\s?\d[A-Z]\d$", part.strip()):
            continue
        if len(part.strip()) > 2:
            return part.strip()
    return parts[-1] if parts else None


def parse_bedrooms(raw) -> int | None:
    if raw is None:
        return None
    m = re.search(r"\d+", str(raw))
    return int(m.group(0)) if m else None


def normalize_property_type(details: dict, features: list) -> str | None:
    """Derive property type from property_details or features."""
    prop_type = details.get("property_type") or details.get("type_of_dwelling", "")
    prop_lower = prop_type.lower()
    if any(w in prop_lower for w in ("condo", "apartment", "flat", "studio")):
        return "apartment"
    if any(w in prop_lower for w in ("house", "detached", "semi-detached", "townhouse", "bungalow")):
        return "house"
    # Fallback: check features list
    for f in features:
        f_lower = f.lower()
        if "condo" in f_lower or "apartment" in f_lower:
            return "apartment"
        if "house" in f_lower or "detached" in f_lower:
            return "house"
    return prop_type or None


def clean_document(doc: dict) -> dict:
    c = {}

    mkt_id = doc.get("mkt_id")
    c["source_id"]        = str(mkt_id) if mkt_id else None
    c["source"]           = "mktlist"
    c["country"]          = "CA"
    c["transaction_type"] = "rent"

    c["url"] = doc.get("url")

    details  = doc.get("property_details") or {}
    features = doc.get("features") or []

    c["property_type"] = normalize_property_type(details, features)

    address = doc.get("address")
    c["address"] = address
    c["city"]    = extract_city(address)

    price = parse_price(doc.get("price"))
    c["price"]    = price
    c["currency"] = "CAD"

    # Surface: prefer sqft since mktlist is Canadian
    sqft = parse_surface_sqft(details)
    m2   = parse_surface(details)
    if sqft:
        c["surface_sqft"] = sqft
        c["surface_m2"]   = round(sqft * 0.0929, 2)
    elif m2:
        c["surface_m2"]   = m2
        c["surface_sqft"] = round(m2 * 10.7639, 2)

    c["bedrooms"]  = parse_bedrooms(doc.get("beds"))
    c["bathrooms"] = parse_int(doc.get("baths"))

    c["description"] = clean_description(doc.get("description"))
    c["title"]       = doc.get("title") or None

    images = doc.get("images")
    if isinstance(images, list):
        c["photos"]       = images
        c["photos_count"] = len(images)
    else:
        c["photos"]       = []
        c["photos_count"] = 0

    realtor = doc.get("realtor") or {}
    c["agency_name"] = (doc.get("brokerage") or {}).get("name") or realtor.get("name") or None

    if isinstance(features, list) and features:
        c["features"] = features

    price_val = c.get("price")
    if price_val and c.get("surface_m2") and c["surface_m2"] > 0:
        c["price_per_m2"] = round(price_val / c["surface_m2"], 2)
    if price_val and c.get("surface_sqft") and c["surface_sqft"] > 0:
        c["price_per_sqft"] = round(price_val / c["surface_sqft"], 2)
    if price_val and c.get("bedrooms") and c["bedrooms"] > 0:
        c["price_per_bedroom"] = round(price_val / c["bedrooms"], 2)

    c["scraped_at"] = doc.get("scraped_at")
    c["cleaned_at"] = datetime.now(timezone.utc)

    return {k: v for k, v in c.items() if v is not None and v != [] and v != ""}


# ============================================================
# VALIDATION
# ============================================================

def validate(doc: dict) -> tuple[bool, str | None]:
    price = doc.get("price")
    if not price or price < MIN_PRICE or price > MAX_PRICE:
        return False, "invalid_price"

    if not doc.get("source_id"):
        return False, "missing_source_id"

    if not doc.get("city"):
        return False, "missing_city"

    surface = doc.get("surface_m2")
    if surface and (surface < MIN_SURFACE or surface > MAX_SURFACE):
        return False, "invalid_surface"

    rooms = doc.get("bedrooms")
    if rooms and rooms > MAX_ROOMS:
        return False, "aberrant_rooms"

    return True, None


# ============================================================
# DB HELPERS
# ============================================================

def connect_db():
    client = MongoClient(MONGODB_URI)
    db = client[MONGODB_DATABASE]
    return client, db


def ensure_indexes(col):
    col.create_index([("source_id", ASCENDING)], unique=True, name="source_id_unique")
    col.create_index([("city", ASCENDING)])
    col.create_index([("price", ASCENDING)])
    col.create_index([("surface_m2", ASCENDING)])
    col.create_index([("property_type", ASCENDING)])
    col.create_index([("country", ASCENDING)])
    return col


def insert_batch(col, batch: list) -> tuple[int, int]:
    if not batch:
        return 0, 0
    ops = [
        UpdateOne(
            {"source_id": doc["source_id"]},
            {"$set": doc},
            upsert=True,
        )
        for doc in batch if doc.get("source_id")
    ]
    if not ops:
        return 0, 0
    r = col.bulk_write(ops, ordered=False)
    return r.upserted_count, r.modified_count


# ============================================================
# PIPELINE
# ============================================================

def run(source_col, clean_col, dry_run=False):
    total = source_col.count_documents({})
    print(f"   Source total: {total} docs")

    if not dry_run and clean_col is not None:
        existing_ids = {
            d.get("source_id") for d in clean_col.find({}, {"source_id": 1, "_id": 0})
            if d.get("source_id")
        }
        print(f"   Already cleaned: {len(existing_ids)}")
    else:
        existing_ids = set()

    if existing_ids:
        query = {"mkt_id": {"$nin": list(existing_ids)}}
    else:
        query = {}

    pending = source_col.count_documents(query)
    print(f"   Pending: {pending}\n")

    if pending == 0:
        print("   Nothing new to clean.")
        return

    stats = {
        "cleaned": 0, "inserted": 0, "updated": 0,
        "invalid_price": 0, "missing_source_id": 0, "missing_city": 0,
        "invalid_surface": 0, "aberrant_rooms": 0, "errors": 0,
    }

    batch = []
    cursor = source_col.find(query, batch_size=BATCH_SIZE, no_cursor_timeout=True)
    try:
        for i, doc in enumerate(cursor):
            try:
                cleaned = clean_document(doc)
                stats["cleaned"] += 1

                valid, reason = validate(cleaned)
                if not valid:
                    stats[reason] = stats.get(reason, 0) + 1
                    continue

                cleaned.pop("_id", None)

                if dry_run:
                    stats["inserted"] += 1
                    continue

                batch.append(cleaned)
                if len(batch) >= BATCH_SIZE:
                    ins, upd = insert_batch(clean_col, batch)
                    stats["inserted"] += ins
                    stats["updated"]  += upd
                    batch = []
                    print(f"   {i+1}/{pending} cleaned …", end="\r", flush=True)

            except Exception as e:
                stats["errors"] += 1
                if stats["errors"] <= 5:
                    print(f"\n   Error on {doc.get('mkt_id')}: {str(e)[:100]}")

        if batch and not dry_run:
            ins, upd = insert_batch(clean_col, batch)
            stats["inserted"] += ins
            stats["updated"]  += upd
    finally:
        cursor.close()

    print_stats(stats, dry_run)


def print_stats(s, dry_run=False):
    print(f"\n{'='*60}")
    print(f"CLEANING RESULTS {'(DRY RUN)' if dry_run else ''}")
    print(f"{'='*60}")
    print(f"   Processed:  {s['cleaned']}")
    print(f"   Inserted:   {s['inserted']}")
    print(f"   Updated:    {s['updated']}")
    rejected = s["cleaned"] - s["inserted"] - s["updated"]
    if rejected > 0:
        print(f"   Rejected:   {rejected}")
        for k in ("invalid_price", "missing_source_id", "missing_city", "invalid_surface", "aberrant_rooms"):
            if s.get(k):
                print(f"      {k}: {s[k]}")
    if s["errors"]:
        print(f"   Errors:     {s['errors']}")
    print(f"{'='*60}")


def show_sample(col, n=3):
    print(f"\nSAMPLE DOCS ({n}):")
    for doc in col.find({}, {"_id": 0}).limit(n):
        print("─" * 60)
        for k, v in doc.items():
            if k == "photos":
                print(f"   {k}: [{len(v)} urls]")
            elif k == "description":
                print(f"   {k}: {str(v)[:80]}...")
            else:
                print(f"   {k}: {v}")


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser(description="MktList Cleaner")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--full",    action="store_true")
    parser.add_argument("--sample",  type=int, default=0)
    args = parser.parse_args()

    print("\n" + "=" * 60)
    print("MKTLIST CLEANER — LOCATIONS")
    print(f"   {SOURCE_COLLECTION} → {CLEAN_COLLECTION}")
    mode = "DRY RUN" if args.dry_run else ("FULL RE-CLEAN" if args.full else "INCREMENTAL")
    print(f"   Mode: {mode}")
    print("=" * 60 + "\n")

    client, db = connect_db()
    source_col = db[SOURCE_COLLECTION]

    if args.dry_run:
        run(source_col, None, dry_run=True)
    elif args.full:
        clean_col = db[CLEAN_COLLECTION]
        clean_col.drop()
        print(f"   '{CLEAN_COLLECTION}' reset (full mode)")
        clean_col = ensure_indexes(db[CLEAN_COLLECTION])
        run(source_col, clean_col)
    else:
        clean_col = ensure_indexes(db[CLEAN_COLLECTION])
        run(source_col, clean_col)
        if args.sample > 0:
            show_sample(clean_col, args.sample)
        print(f"\n   Done! '{CLEAN_COLLECTION}': {clean_col.count_documents({})} total docs")

    client.close()


if __name__ == "__main__":
    main()
