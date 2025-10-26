

# Base URL - change if your account docs specify a different host

# cuelinks_v2_kbeauty.py
# pip install requests
# Usage: set API_TOKEN (and optionally PUBLISHER_ID), then run: python cuelinks_v2_kbeauty.py

import requests
import time
import json
import csv
import os
from datetime import datetime, timedelta
from urllib.parse import urlencode

# ---------------- CONFIG ----------------
PUBLISHER_ID = "218104"
API_TOKEN = "HejnDRldgoMOt8iqdKl10r5EUvyIBOSRpZ3_2Ao6izo"
BASE_HOST = "https://www.cuelinks.com"
OFFERS_PATH = "/api/v2/offers.json"
GET_LINK_PATH = "/api/v2/get_link.json"  # POST expected (payload below)
OUTPUT_DIR = "output"

PER_PAGE = 50
MAX_PAGES = 5
SLEEP = 0.25

# Example filters (you can change these)
CATEGORIES = "4,10"         # same format as your example
CAMPAIGNS = "1,817"         # same format as your example
OFFER_TYPES = "1,2"         # same format as your example

# K-beauty keywords (lowercase)
KBEAUTY_KEYWORDS = [
    "cosrx","laneige","innisfree","beauty of joseon","klairs","mizon","sulwhasoo",
    "missha","tonymoly","etude","banila co","heimish","dr.jart","round lab",
    "isntree","some by mi","goodal","hanyul","belif","skincare","korean"
]

# ---------------- HELPERS ----------------
def ensure_out():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

def headers():
    """
    Cuelinks v2 header style from your example:
      Authorization: Token token=Enter_Your_API_Token
      Content-Type: application/json
    """
    return {
        "Authorization": f"Token token={API_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

def iso_dt(dt: datetime):
    # produce ISO with offset Z or local; Cuelinks example used +05:30 encoded.
    # We'll produce UTC Z by default; you can pass a custom string if needed.
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def last_n_days_iso(n=30):
    end = datetime.utcnow()
    start = end - timedelta(days=n)
    return iso_dt(start), iso_dt(end)

def looks_like_kbeauty(text):
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in KBEAUTY_KEYWORDS)

# ---------------- Cuelinks API calls ----------------
def build_offers_url(start_date, end_date, page=1, per_page=PER_PAGE,
                     categories=CATEGORIES, campaigns=CAMPAIGNS, offer_types=OFFER_TYPES):
    """
    Build the full offers URL with query params as in your example.
    Example:
    https://www.cuelinks.com/api/v2/offers.json?start_date=...&end_date=...&categories=4,10&campaigns=1,817&offer_types=1,2&page=1&per_page=50
    """
    q = {
        "start_date": start_date,
        "end_date": end_date,
        "categories": categories,
        "campaigns": campaigns,
        "offer_types": offer_types,
        "page": page,
        "per_page": per_page
    }
    return f"{BASE_HOST}{OFFERS_PATH}?{urlencode(q)}"

def get_offers_page(start_date, end_date, page=1):
    url = build_offers_url(start_date, end_date, page=page)
    print("GET", url)
    resp = requests.get(url, headers=headers(), timeout=20)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print("Offers GET error:", e, resp.status_code, resp.text[:400])
        return None
    return resp.json()

def get_affiliate_link_for_url(destination_url):
    """
    POST to /api/v2/get_link.json with payload like:
      {"publisher_id":"...","url":"https://product.url"}
    Response commonly contains affiliate_url / tracking_url / short_url keys.
    """
    url = f"{BASE_HOST}{GET_LINK_PATH}"
    payload = {"url": destination_url}
    # optionally include publisher_id if required by your account
    if PUBLISHER_ID and PUBLISHER_ID != "YOUR_PUBLISHER_ID":
        payload["publisher_id"] = PUBLISHER_ID
    print("POST get_link for:", destination_url)
    resp = requests.post(url, headers=headers(), json=payload, timeout=20)
    try:
        resp.raise_for_status()
    except requests.HTTPError as e:
        print("Get link POST error:", e, resp.status_code, resp.text[:400])
        return None
    data = resp.json()
    # try common keys
    return data.get("affiliate_url") or data.get("tracking_url") or data.get("short_url") or data.get("url") or None

# ---------------- Processing helpers ----------------
def extract_product_from_offer(offer):
    """
    Map Cuelinks offer object to a product dict.
    Field names vary; inspect raw offer if mapping needs adjustment.
    """
    title = offer.get("title") or offer.get("name") or ""
    desc = offer.get("description") or offer.get("short_description") or ""
    image = offer.get("image_url") or offer.get("image") or offer.get("image_url_https") or ""
    merchant = offer.get("merchant_name") or offer.get("merchant") or ""
    # product URL might be landing_page, product_url or url
    product_url = offer.get("product_url") or offer.get("landing_page") or offer.get("url") or ""
    offer_id = offer.get("id") or offer.get("offer_id")
    return {
        "title": title,
        "description": desc,
        "image": image,
        "merchant": merchant,
        "product_url": product_url,
        "offer_id": offer_id,
        "raw": offer
    }

def filter_kbeauty(offers):
    out = []
    for o in offers:
        p = extract_product_from_offer(o)
        if looks_like_kbeauty(p["title"]) or looks_like_kbeauty(p["description"]) or looks_like_kbeauty(p["merchant"]):
            out.append(p)
    return out

# ---------------- OUTPUT ----------------
def save_json(products, path=os.path.join(OUTPUT_DIR, "kbeauty_products.json")):
    ensure_out()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(products, f, indent=2, ensure_ascii=False)
    print("Saved JSON:", path)

def save_csv(products, path=os.path.join(OUTPUT_DIR, "kbeauty_products.csv")):
    ensure_out()
    keys = ["title","merchant","product_url","affiliate_url","image","offer_id"]
    with open(path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=keys)
        writer.writeheader()
        for p in products:
            writer.writerow({k: p.get(k,"") for k in keys})
    print("Saved CSV:", path)

def render_html(products, path=os.path.join(OUTPUT_DIR, "kbeauty_shop.html")):
    ensure_out()
    head = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Mokshiri Shop</title><link href="https://cdn.jsdelivr.net/npm/tailwindcss@2.2.19/dist/tailwind.min.css" rel="stylesheet"></head><body class="bg-pink-50 text-gray-800"><main class="max-w-6xl mx-auto p-6"><h1 class="text-3xl font-bold text-pink-700 mb-4">🛍️ Mokshiri — K-Beauty Picks</h1><div class="grid grid-cols-1 sm:grid-cols-2 md:grid-cols-3 gap-6">"""
    cards = ""
    for p in products:
        t = (p.get("title") or "No title").replace('"','&quot;')
        img = p.get("image") or "https://via.placeholder.com/400x400?text=No+Image"
        merchant = p.get("merchant") or ""
        aff = p.get("affiliate_url") or p.get("product_url") or "#"
        card = f'''
<div class="bg-white rounded-2xl shadow p-4">
  <img src="{img}" alt="{t}" class="w-full h-48 object-cover rounded-lg mb-3">
  <h3 class="font-semibold">{t}</h3>
  <p class="text-sm text-gray-500">{merchant}</p>
  <div class="mt-3">
    <a href="{aff}" target="_blank" rel="nofollow noopener noreferrer" class="inline-block px-4 py-2 bg-pink-500 text-white rounded-full hover:bg-pink-600 transition">Buy</a>
  </div>
</div>
'''
        cards += card
    tail = "</div><p class='mt-6 text-sm text-gray-500'>Affiliate links — Mokshiri may earn commission on purchases.</p></main></body></html>"
    with open(path, "w", encoding="utf-8") as f:
        f.write(head + cards + tail)
    print("Saved HTML:", path)

# ---------------- MAIN ----------------
def fetch_and_build(start_date_iso, end_date_iso, max_pages=MAX_PAGES):
    all_candidates = []
    for page in range(1, max_pages + 1):
        data = get_offers_page(start_date_iso, end_date_iso, page=page)
        if not data:
            print("No data (or error) on page", page)
            break
        # common fields container: "offers" or "data" etc.
        offers = data.get("offers") or data.get("data") or data.get("results") or []
        if not offers:
            print("No offers in response; raw keys:", list(data.keys()))
            break
        print(f"Total offers in page {page}:", len(offers))
        kbeauty = filter_kbeauty(offers)
        print(f"Found {len(kbeauty)} K-beauty candidates on page {page}")
        all_candidates.extend(kbeauty)
        if len(offers) < PER_PAGE:
            break
        time.sleep(SLEEP)
    # dedupe by product_url or title
    seen = set()
    unique = []
    for p in all_candidates:
        key = (p.get("product_url") or p.get("title")).strip()
        if key in seen:
            continue
        seen.add(key)
        unique.append(p)
    print("Unique K-beauty candidates:", len(unique))
    # generate affiliate links
    enriched = []
    for p in unique:
        dest = p.get("product_url")
        if dest:
            aff = get_affiliate_link_for_url(dest)
            if aff:
                p["affiliate_url"] = aff
            else:
                p["affiliate_url"] = dest
        else:
            p["affiliate_url"] = None
        enriched.append(p)
        time.sleep(SLEEP)
    # outputs
    save_json(enriched)
    save_csv(enriched)
    render_html(enriched)
    return enriched

if __name__ == "__main__":
    # default: last 30 days
    sd, ed = last_n_days_iso(30)
    print("Using date range:", sd, "to", ed)
    # if you want custom dates, replace sd and ed with ISO strings like "2016-09-01T02:30:17+05:30"
    products = fetch_and_build(sd, ed, max_pages=MAX_PAGES)
    print("Completed. Found total products:", len(products))
