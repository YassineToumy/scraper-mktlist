"""
MktList.ca Scraper — Rental listings → MongoDB
Config via .env
"""

import os
import requests
from bs4 import BeautifulSoup
import time
import random
from pymongo import MongoClient
from urllib.parse import urljoin
from dotenv import load_dotenv

load_dotenv()

# =============================
# CONFIG — from .env
# =============================

BASE_URL = "https://www.mktlist.ca"
SEARCH_URL = "https://www.mktlist.ca/Property/Search/rent?page={}"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9"
}

MONGO_URI = os.environ["MONGODB_URI"]
MONGO_DB = os.getenv("MONGO_MKTLIST_DB", "mktlist")
MONGO_COLLECTION = os.getenv("MONGO_MKTLIST_COL", "locations")

# =============================
# MONGO SETUP
# =============================

client = MongoClient(MONGO_URI)
db = client[MONGO_DB]
collection = db[MONGO_COLLECTION]
collection.create_index("url", unique=True)

# =============================
# HELPERS
# =============================

def get_html(url):
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        if r.status_code == 200:
            return r.text
        else:
            print(f"Failed: {url} | Status: {r.status_code}")
            return None
    except Exception as e:
        print("Request error:", e)
        return None


def safe_text(element):
    return element.get_text(strip=True) if element else None


def extract_summary_fields(soup):
    fields = {}
    label_divs = soup.select("div.panel-body div[style*='color: #cc171b']")
    for label_div in label_divs:
        key = label_div.get_text(strip=True)
        value_div = label_div.find_next_sibling("div")
        value = value_div.get_text(strip=True) if value_div else None
        if key and value:
            clean_key = key.lower().replace(" ", "_").replace("&", "and")
            fields[clean_key] = value
    return fields


def extract_rooms(soup):
    rooms = []
    room_table = soup.select_one("#rooms .table-responsive table")
    if not room_table:
        return rooms

    current_level = None
    for row in room_table.select("tr"):
        cells = row.select("td")
        if len(cells) < 3:
            continue

        level_text = cells[0].get_text(strip=True)
        room_name = cells[1].get_text(strip=True)
        dimensions = cells[2].get_text(strip=True)

        if level_text:
            current_level = level_text

        if room_name:
            rooms.append({
                "level": current_level,
                "room": room_name,
                "dimensions": dimensions
            })
    return rooms


def extract_features(soup):
    features = []
    feature_panel = soup.select_one("div#features")
    if not feature_panel:
        for panel in soup.select("div.panel"):
            header = panel.select_one("th")
            if header and "Features" in header.get_text():
                feature_panel = panel
                break

    if feature_panel:
        for td in feature_panel.select("table tr td"):
            if td.find("th"):
                continue
            check_icon = td.select_one("span.fa-check-circle")
            if check_icon:
                text = td.get_text(strip=True)
                text = " ".join(text.split())
                if text:
                    features.append(text)
    return features


def extract_images(soup):
    images = []
    seen = set()

    main_slider = soup.select_one("div#slider ul.slides")
    if not main_slider:
        main_slider = soup.select_one("ul#lightgallery")

    if main_slider:
        for li in main_slider.select("li"):
            src = li.get("data-src", "")
            if not src:
                img = li.select_one("img")
                if img:
                    src = img.get("src", "") or img.get("data-src", "")

            if src and src not in seen:
                src = src.replace("&amp;", "&")
                seen.add(src)
                images.append(src)

    if not images:
        for img in soup.select("img.img-responsive"):
            src = img.get("src", "")
            if src and ("realtor.ca" in src or "weserv.nl" in src):
                src = src.replace("&amp;", "&")
                if src not in seen:
                    seen.add(src)
                    images.append(src)

    return images


# =============================
# STEP 1: GET LISTING LINKS
# =============================

def get_listing_links(page_number):
    url = SEARCH_URL.format(page_number)
    print(f"\nScraping search page {page_number}...")

    html = get_html(url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    links = []

    for tag in soup.select("div.item-detail h4 a"):
        href = tag.get("href")
        full_url = urljoin(BASE_URL, href)
        links.append(full_url)

    print(f"Found {len(links)} listings on page {page_number}")
    return links


# =============================
# STEP 2: SCRAPE DETAIL PAGE
# =============================

def parse_listing_detail(url):
    html = get_html(url)
    if not html:
        return None

    soup = BeautifulSoup(html, "html.parser")

    title = safe_text(soup.select_one("div.head-info h1"))

    address = safe_text(soup.select_one("div.head-info h4"))
    if address and address.startswith("Address:"):
        address = address.replace("Address:", "").strip()

    price = safe_text(soup.select_one("span.price-show"))

    beds = None
    baths = None
    status = None
    header_div = soup.select_one("div.head-info")
    if header_div:
        spans = header_div.select("span.dash")
        for span in spans:
            text = span.get_text(strip=True)
            if "Bed" in text:
                beds = text.replace("Beds", "").strip()
            elif "Bath" in text:
                baths = text.replace("Baths", "").strip()
        status_span = header_div.select_one("span.label-red")
        if status_span:
            status = status_span.get_text(strip=True).replace("Status:", "").strip()

    views = None
    views_el = header_div.select_one("i.fa-eye") if header_div else None
    if views_el:
        views_text = views_el.get_text(strip=True)
        views = views_text.replace("Views", "").replace(":", "").strip()

    mkt_id = None
    summary_items = soup.select("div.summary-list li")
    for li in summary_items:
        div = li.select_one("div")
        span = li.select_one("span")
        if div and span:
            label = div.get_text(strip=True)
            value = span.get_text(strip=True)
            if label == "MKT ID":
                mkt_id = value

    description = None
    desc_section = soup.select_one("div.property-overvie-in p")
    if desc_section:
        description = desc_section.get_text(strip=True)

    realtor_name = safe_text(soup.select_one("div.property-realtor-info-detail div.info h2"))
    brokerage_el = soup.select_one("div.real-ad a")
    brokerage = safe_text(brokerage_el)
    brokerage_url = urljoin(BASE_URL, brokerage_el["href"]) if brokerage_el and brokerage_el.get("href") else None

    realtor_phone = None
    realtor_modal = soup.select_one("#Realtor")
    if realtor_modal:
        for h4 in realtor_modal.select("h4"):
            if "Phone" in h4.get_text():
                phone_p = h4.find_next_sibling("p")
                if phone_p:
                    realtor_phone = phone_p.get_text(strip=True)
                break

    realtor_website = None
    if realtor_modal:
        for h4 in realtor_modal.select("h4"):
            if "Website" in h4.get_text():
                website_p = h4.find_next_sibling("p")
                if website_p:
                    a_tag = website_p.select_one("a")
                    realtor_website = a_tag["href"] if a_tag else None
                break

    property_details = extract_summary_fields(soup)
    rooms = extract_rooms(soup)
    features = extract_features(soup)
    images = extract_images(soup)

    added_on = None
    for li in summary_items:
        div = li.select_one("div")
        span = li.select_one("span")
        if div and span and "Added On" in div.get_text(strip=True):
            added_on = span.get_text(strip=True)
            break

    data = {
        "url": url,
        "mkt_id": mkt_id,
        "title": title,
        "address": address,
        "price": price,
        "beds": beds,
        "baths": baths,
        "status": status,
        "views": views,
        "description": description,
        "added_on": added_on,
        "realtor": {
            "name": realtor_name,
            "phone": realtor_phone,
            "website": realtor_website,
        },
        "brokerage": {
            "name": brokerage,
            "url": brokerage_url,
        },
        "property_details": property_details,
        "rooms": rooms,
        "features": features,
        "images": images,
        "scraped_at": time.time()
    }

    return data


# =============================
# MAIN CRAWLER
# =============================

def run_scraper(max_pages=500):
    total_inserted = 0

    for page in range(1, max_pages + 1):
        links = get_listing_links(page)

        if not links:
            print("No more listings found. Stopping pagination.")
            break

        for link in links:
            if collection.find_one({"url": link}):
                print("Already scraped:", link)
                continue

            print("Scraping detail:", link)
            data = parse_listing_detail(link)

            if data:
                try:
                    collection.update_one(
                        {"url": data["url"]},
                        {"$set": data},
                        upsert=True
                    )
                    total_inserted += 1
                    print(f"Inserted ✔ — {data.get('mkt_id', 'N/A')} | {data.get('price', 'N/A')}")
                except Exception as e:
                    print("Mongo error:", e)

            time.sleep(random.uniform(1, 2))

        time.sleep(random.uniform(2, 4))

    print(f"\nDone. Total new inserted: {total_inserted}")


if __name__ == "__main__":
    run_scraper()