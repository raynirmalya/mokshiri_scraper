#!/usr/bin/env python3
"""
feed_ingest_to_mysql.py

- Parse Admitad Atom (g:) and YML/shop feeds (local file or URL).
- Filter beauty/skincare products.
- Create Admitad deeplink (if ADMITAD credentials provided) for items missing affiliate_url.
- Upsert into MySQL using mysql.connector.pooling.MySQLConnectionPool.

Configuration via .env:
  DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME, DB_SSL_MODE (optional), DB_SSL_CA (optional)
  FEED_PATH_OR_URL (local path or http(s) url)
  FEED_SOURCE_ID (int)
  ADMITAD_CLIENT_ID (optional)
  ADMITAD_CLIENT_SECRET (optional)
  ADVCAMPAIGN (optional, required for deeplink generation)
  ONLY_BEAUTY (optional, "1" or "0")
  MIN_PRICE (optional, numeric)

Requirements:
  pip install lxml requests python-dotenv mysql-connector-python
"""

import os
import time
import json
import re
import logging
from urllib.parse import urlparse, parse_qs, unquote, quote
from datetime import datetime
from lxml import etree
import requests
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import pooling

# ---- load env ----
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME")
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "DISABLED").upper()
DB_SSL_CA = os.getenv("DB_SSL_CA")

FEED_PATH_OR_URL = os.getenv("FEED_PATH_OR_URL", "alibaba_feed.xml")
FEED_SOURCE_ID = int(os.getenv("FEED_SOURCE_ID", "1"))

ADMITAD_CLIENT_ID = os.getenv("ADMITAD_CLIENT_ID")
ADMITAD_CLIENT_SECRET = os.getenv("ADMITAD_CLIENT_SECRET")
ADVCAMPAIGN = os.getenv("ADVCAMPAIGN")  # e.g., ali_express_com

ONLY_BEAUTY = os.getenv("ONLY_BEAUTY", "1") == "1"
MIN_PRICE = float(os.getenv("MIN_PRICE", "0.0"))

POOL_NAME = os.getenv("DB_POOL_NAME", "feed_pool")
POOL_SIZE = int(os.getenv("DB_POOL_SIZE", "6"))

# ---- logging ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("feed_ingest")

# ---- sanity checks ----
if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    raise SystemExit("DB config missing in .env (DB_USER, DB_PASS, DB_HOST, DB_NAME required).")

# ---- DB pool setup ----
pool_args = {
    "user": DB_USER,
    "password": DB_PASS,
    "host": DB_HOST,
    "port": DB_PORT,
    "database": DB_NAME,
    "pool_name": POOL_NAME,
    "pool_size": POOL_SIZE,
    "autocommit": False,
}

# ssl if required
if DB_SSL_MODE in ("REQUIRED", "PREFERRED") and DB_SSL_CA:
    pool_args["ssl_ca"] = DB_SSL_CA
elif DB_SSL_MODE == "REQUIRED" and not DB_SSL_CA:
    logger.warning("DB_SSL_MODE=REQUIRED but DB_SSL_CA not provided; connection may fail if server enforces certs.")

try:
    db_pool = pooling.MySQLConnectionPool(**pool_args)
    logger.info("DB pool created (name=%s size=%d)", POOL_NAME, POOL_SIZE)
except Exception as e:
    logger.exception("Failed to create DB pool: %s", e)
    raise

# ---- utility helpers ----
BEAUTY_KEYWORDS = [
    "beauty", "skincare", "skin care", "makeup", "cosmetic",
    "face", "mask", "serum", "toner", "cream", "cleanser",
    "sunscreen", "personal care", "hair", "body lotion", "lipstick"
]

def detect_beauty(text):
    if not text:
        return False
    t = text.lower()
    return any(k in t for k in BEAUTY_KEYWORDS)

def parse_price(text):
    if not text:
        return (None, None)
    m = re.search(r"([\d\.,]+)", text)
    num = None
    if m:
        try:
            num = float(m.group(1).replace(",", ""))
        except:
            num = None
    cur = None
    for c in ("USD", "INR", "KRW", "EUR", "JPY", "$", "₹"):
        if c in (text or ""):
            cur = c
            break
    return num, cur

def decode_ulp_from_url(maybe_redirect_url):
    if not maybe_redirect_url:
        return (None, None)
    try:
        parsed = urlparse(maybe_redirect_url)
        qs = parse_qs(parsed.query)
        for param in ("ulp", "url", "redirect"):
            if param in qs and qs[param]:
                orig = unquote(qs[param][0])
                return (maybe_redirect_url, orig)
        return (maybe_redirect_url, None)
    except Exception:
        return (maybe_redirect_url, None)

# ---- Admitad token & deeplink ----
ADMITAD_TOKEN_URL = "https://api.admitad.com/token/"
def get_admitad_token(client_id, client_secret):
    if not client_id or not client_secret:
        return None
    payload = {
        "client_id": client_id,
        "client_secret": client_secret,
        "grant_type": "client_credentials"
    }
    headers = {"Content-Type": "application/x-www-form-urlencoded"}
    try:
        r = requests.post(ADMITAD_TOKEN_URL, data=payload, headers=headers, timeout=15)
        if r.status_code != 200:
            logger.warning("Admitad token request failed: %s %s", r.status_code, r.text[:400])
            return None
        j = r.json()
        token = j.get("access_token")
        return token
    except Exception as e:
        logger.exception("Admitad token request exception: %s", e)
        return None

def create_admitad_deeplink(token, advcampaign, url):
    if not token or not advcampaign or not url:
        return None
    api = "https://api.admitad.com/deeplink/"
    headers = {"Authorization": f"Bearer {token}"}
    data = {"advcampaign": advcampaign, "ulp": url}
    try:
        r = requests.post(api, headers=headers, data=data, timeout=15)
        if r.status_code == 401:
            logger.warning("Admitad deeplink: 401 Unauthorized (token may be invalid)")
            return None
        r.raise_for_status()
        j = r.json()
        deeplink = j.get("deeplink") or j.get("url") or j.get("data")
        if isinstance(deeplink, dict):
            deeplink = json.dumps(deeplink, ensure_ascii=False)
        return deeplink
    except Exception as e:
        logger.exception("Admitad deeplink error: %s", e)
        return None

# ---- feed loader (local file or URL) ----
def load_feed(path_or_url):
    # if URL -> download
    if str(path_or_url).startswith(("http://", "https://")):
        logger.info("Downloading feed from URL: %s", path_or_url)
        r = requests.get(path_or_url, timeout=30)
        r.raise_for_status()
        content = r.content
        parser = etree.XMLParser(recover=True, huge_tree=True)
        root = etree.fromstring(content, parser=parser)
        return root
    # else local path
    abspath = os.path.abspath(path_or_url)
    if not os.path.exists(abspath):
        raise FileNotFoundError(f"Feed file not found: {abspath}")
    logger.info("Loading local feed file: %s", abspath)
    parser = etree.XMLParser(recover=True, huge_tree=True)
    tree = etree.parse(abspath, parser=parser)
    return tree.getroot()

# ---- feed parsers ----
def detect_feed_type(root):
    raw = (etree.tostring(root, encoding="utf-8", method="xml") or b"").lower()
    if b"<yml_catalog" in raw or b"<offers" in raw:
        return "yml"
    if b"http://base.google.com/ns/1.0" in raw or b"<entry" in raw:
        return "atom"
    return "atom"

def parse_atom_feed(root):
    ns = {'g': 'http://base.google.com/ns/1.0', 'atom': 'http://www.w3.org/2005/Atom'}
    results = []
    # try namespaced entry first
    entries = root.findall('.//{http://www.w3.org/2005/Atom}entry') or root.findall('.//entry')
    if not entries:
        try:
            entries = root.xpath('//atom:entry', namespaces=ns)
        except Exception:
            entries = []
    logger.info("Atom entries found: %d", len(entries))
    for entry in entries:
        title = (entry.findtext('{http://base.google.com/ns/1.0}title') or
                 entry.findtext('{http://www.w3.org/2005/Atom}title') or entry.findtext('title') or "")
        description = (entry.findtext('{http://base.google.com/ns/1.0}description') or
                       entry.findtext('{http://www.w3.org/2005/Atom}summary') or "")
        price_raw = entry.findtext('{http://base.google.com/ns/1.0}price') or ""
        price, currency = parse_price(price_raw)
        image = entry.findtext('{http://base.google.com/ns/1.0}image_link') or entry.findtext('{http://base.google.com/ns/1.0}additional_image_link') or ""
        product_type = entry.findtext('{http://base.google.com/ns/1.0}product_type') or ""
        vendor = entry.findtext('{http://base.google.com/ns/1.0}brand') or ""
        offer_id = entry.findtext('{http://base.google.com/ns/1.0}id') or entry.findtext('id') or ""
        link = (entry.findtext('{http://base.google.com/ns/1.0}link') or entry.findtext('{http://www.w3.org/2005/Atom}link') or entry.findtext('link'))
        if not link:
            link_elem = entry.find('{http://www.w3.org/2005/Atom}link')
            if link_elem is not None:
                link = link_elem.get("href")
        affiliate_url, original_url = decode_ulp_from_url(link)
        is_beauty = detect_beauty(" ".join([title or "", description or "", product_type or ""]))
        results.append({
            "feed_offer_id": str(offer_id or ""),
            "title": (title or "").strip(),
            "description": (description or "").strip(),
            "price": price,
            "currency": currency,
            "price_raw": price_raw or "",
            "picture_primary": image,
            "affiliate_url": affiliate_url,
            "original_url": original_url,
            "url": link,
            "product_type": product_type,
            "vendor": vendor,
            "is_beauty": is_beauty,
            "params": {}
        })
    return results

def parse_yml_feed(root):
    results = []
    offers = root.findall('.//offer')
    logger.info("YML offers found: %d", len(offers))
    for o in offers:
        offer_id = o.get("id") or ""
        title = (o.findtext("name") or o.findtext("title") or "").strip()
        description = (o.findtext("description") or "").strip()
        vendor = o.findtext("vendor") or ""
        price_raw = o.findtext("price") or ""
        price, currency = parse_price(price_raw)
        url = o.findtext("url") or ""
        image = o.findtext("picture") or o.findtext("image") or ""
        product_type = o.findtext("model") or o.findtext("categoryId") or ""
        affiliate_url, original_url = decode_ulp_from_url(url)
        # params
        params = {}
        for p in o.findall("param"):
            key = p.get("name")
            val = p.text or ""
            if key:
                params[key.lower()] = val
        is_beauty = detect_beauty(" ".join([title or "", description or "", product_type or ""]))
        results.append({
            "feed_offer_id": str(offer_id),
            "title": title,
            "description": description,
            "price": price,
            "currency": currency,
            "price_raw": price_raw or "",
            "picture_primary": image,
            "affiliate_url": affiliate_url,
            "original_url": original_url,
            "url": url,
            "product_type": product_type,
            "vendor": vendor,
            "is_beauty": is_beauty,
            "params": params
        })
    return results

# ---- DB upsert helpers ----
def upsert_offer_and_images(offer_record):
    """
    Insert/Update into `offer` and insert images into offer_image table.
    Assumes `offer` has UNIQUE(feed_source_id, feed_offer_id)
    """
    conn = None
    try:
        conn = db_pool.get_connection()
        cur = conn.cursor()
        # Upsert offer
        sql_offer = """
        INSERT INTO offer (
            feed_source_id, shop_id, feed_offer_id, sku, title, description, vendor,
            product_type, availability, currency, price, price_raw, picture_primary,
            affiliate_url, original_url, url, category_id, country, is_beauty, params, raw, last_seen_at
        ) VALUES (
            %(feed_source_id)s, %(shop_id)s, %(feed_offer_id)s, %(sku)s, %(title)s, %(description)s, %(vendor)s,
            %(product_type)s, %(availability)s, %(currency)s, %(price)s, %(price_raw)s, %(picture_primary)s,
            %(affiliate_url)s, %(original_url)s, %(url)s, %(category_id)s, %(country)s, %(is_beauty)s, %(params)s, %(raw)s, NOW()
        )
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            description = VALUES(description),
            price = VALUES(price),
            currency = VALUES(currency),
            picture_primary = VALUES(picture_primary),
            affiliate_url = IFNULL(offer.affiliate_url, VALUES(affiliate_url)),
            original_url = IFNULL(offer.original_url, VALUES(original_url)),
            url = VALUES(url),
            product_type = VALUES(product_type),
            vendor = VALUES(vendor),
            params = VALUES(params),
            raw = VALUES(raw),
            last_seen_at = NOW(),
            updated_at = CURRENT_TIMESTAMP;
        """
        offer_data = {
            "feed_source_id": FEED_SOURCE_ID,
            "shop_id": None,
            "feed_offer_id": offer_record.get("feed_offer_id", ""),
            "sku": offer_record.get("sku", ""),
            "title": (offer_record.get("title") or "")[:500],
            "description": offer_record.get("description") or "",
            "vendor": (offer_record.get("vendor") or "")[:255],
            "product_type": offer_record.get("product_type") or "",
            "availability": offer_record.get("availability") or "in stock",
            "currency": offer_record.get("currency") or "",
            "price": offer_record.get("price"),
            "price_raw": offer_record.get("price_raw") or "",
            "picture_primary": (offer_record.get("picture_primary") or "")[:1000],
            "affiliate_url": offer_record.get("affiliate_url") or None,
            "original_url": offer_record.get("original_url") or None,
            "url": (offer_record.get("url") or "")[:2000],
            "category_id": None,
            "country": offer_record.get("country") or None,
            "is_beauty": 1 if offer_record.get("is_beauty") else 0,
            "params": json.dumps(offer_record.get("params") or {}, ensure_ascii=False),
            "raw": json.dumps(offer_record, ensure_ascii=False)
        }
        cur.execute(sql_offer, offer_data)
        # retrieve last inserted / existing offer id
        # MySQL: if duplicate key, cur.lastrowid returns the AUTO_INCREMENT value only when inserted.
        # To get the offer id regardless, query using unique key
        conn.commit()
        # fetch offer id
        sel = "SELECT id FROM offer WHERE feed_source_id=%s AND feed_offer_id=%s LIMIT 1"
        cur.execute(sel, (FEED_SOURCE_ID, offer_data["feed_offer_id"]))
        row = cur.fetchone()
        offer_id = row[0] if row else None

        # insert image(s) if present (here we only insert picture_primary as single image)
        image_url = offer_record.get("picture_primary")
        if image_url and offer_id:
            # delete old images? for simplicity, insert but avoid duplicates by checking url exists
            cur.execute("SELECT COUNT(1) FROM offer_image WHERE offer_id=%s AND url=%s", (offer_id, image_url))
            exists = cur.fetchone()[0]
            if not exists:
                cur.execute(
                    "INSERT INTO offer_image (offer_id, url, position, inserted_at) VALUES (%s, %s, %s, NOW())",
                    (offer_id, image_url, 0)
                )

        conn.commit()
        cur.close()
        return True
    except Exception as e:
        logger.exception("DB upsert failed for %s: %s", offer_record.get("feed_offer_id"), e)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False
    finally:
        if conn:
            conn.close()

# ---- main orchestrator ----
def run():
    logger.info("Starting feed ingestion (feed=%s)", FEED_PATH_OR_URL)

    # get admitad token if credentials provided
    token = None
    if ADMITAD_CLIENT_ID and ADMITAD_CLIENT_SECRET:
        token = get_admitad_token(ADMITAD_CLIENT_ID, ADMITAD_CLIENT_SECRET)
        if token:
            logger.info("Got Admitad token.")
        else:
            logger.warning("Admitad token not available; deeplink generation will be skipped.")

    # load feed
    try:
        root = load_feed(FEED_PATH_OR_URL)
    except Exception as e:
        logger.exception("Failed to load feed: %s", e)
        return

    feed_type = detect_feed_type(root)
    logger.info("Detected feed type: %s", feed_type)

    # parse products
    if feed_type == "atom":
        products = parse_atom_feed(root)
    else:
        products = parse_yml_feed(root)

    logger.info("Parsed %d products from feed", len(products))

    # filter for beauty and price
    filtered = []
    for p in products:
        if ONLY_BEAUTY and not p.get("is_beauty"):
            continue
        if MIN_PRICE and p.get("price") is not None:
            try:
                if float(p.get("price")) < float(MIN_PRICE):
                    continue
            except:
                pass
        filtered.append(p)
    logger.info("After filters: %d products", len(filtered))

    inserted = 0
    for i, p in enumerate(filtered, start=1):
        # if affiliate_url missing, create deeplink via Admitad if possible
        if (not p.get("affiliate_url")) and token and ADVCAMPAIGN:
            target = p.get("original_url") or p.get("url")
            if target:
                dl = create_admitad_deeplink(token, ADVCAMPAIGN, target)
                if dl:
                    p["affiliate_url"] = dl
                else:
                    # If deeplink failed due to token expiry, try to refresh token once
                    token = get_admitad_token(ADMITAD_CLIENT_ID, ADMITAD_CLIENT_SECRET)
                    if token:
                        dl = create_admitad_deeplink(token, ADVCAMPAIGN, target)
                        if dl:
                            p["affiliate_url"] = dl
            time.sleep(0.35)  # throttle

        ok = upsert_offer_and_images(p)
        if ok:
            inserted += 1
            if inserted % 50 == 0:
                logger.info("Inserted/upserted %d offers...", inserted)
        else:
            logger.warning("Failed to upsert offer: %s", p.get("feed_offer_id"))

    logger.info("Completed ingestion. Upserted %d offers.", inserted)

if __name__ == "__main__":
    run()
