#!/usr/bin/env python3
"""
kbizoom_to_sql.py

Scrape today's articles (k-pop, k-drama, celebrity) from kbizoom, rewrite title+summary via GPT rewriter,
and store in 'articles' table using DB credentials from .env.

Table columns (assumed existing):
id int AUTO_INCREMENT PRIMARY KEY,
category varchar(50),
title varchar(500),
link varchar(1000) UNIQUE,
summary text,
image_url varchar(1000),
author varchar(255),
published varchar(100),
created_at datetime,
views bigint,
is_featured tinyint(1),
featured_rank int,
last_metrics_update datetime,
trend_score double,
uuid binary(16)
"""

import os
import time
import random
import json
import logging
import uuid as uuidlib
from datetime import datetime
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
import pytz
from dotenv import load_dotenv
import mysql.connector
from mysql.connector import pooling

# ---- rewriter import: adjust if name differs ----
# This function must return dict {"header":..., "summary":...}
# e.g. from gpt_rewriter_balanced import rewrite_with_gpt_balanced
from gpt_rewriter_expanded import rewrite_with_gpt_expanded

# ---- Load env ----
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME")
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "DISABLED").upper()  # DISABLED / PREFERRED / REQUIRED
DB_SSL_CA = os.getenv("DB_SSL_CA")  # optional path to CA

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
# if your rewrite module needs OPENAI env, ensure it's present; else rewrite will handle errors.

if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    raise SystemExit("Missing DB config in .env. Please set DB_USER, DB_PASS, DB_HOST, DB_NAME, DB_PORT (if custom).")

# ---- Logging ----
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("kbizoom_to_sql")
DOMAIN = "https://kbizoom.com"
# ---- Config ----
START_PAGES = [
    ("kpop", "https://kbizoom.com/k-pop/"),
    ("kdrama", "https://kbizoom.com/k-drama/"),
    ("kpop_celeb", "https://kbizoom.com/celebrity/"),
]
MAX_PAGES_PER_CATEGORY = 2
TIMEZONE = pytz.timezone("Asia/Kolkata")
USER_AGENT = "Mozilla/5.0 (compatible; MokshiriScraper/1.0; +https://example.com/bot)"
HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 12
SLEEP_MIN = 1.0
SLEEP_MAX = 2.0

# ---- DB pool ----
pool_args = {
    "user": DB_USER,
    "password": DB_PASS,
    "host": DB_HOST,
    "port": DB_PORT,
    "database": DB_NAME,
    "pool_name": "kbizoom_pool",
    "pool_size": 5,
    "autocommit": False,
}
# Add SSL options if required
if DB_SSL_MODE in ("REQUIRED", "PREFERRED") and DB_SSL_CA:
    pool_args["ssl_ca"] = DB_SSL_CA
elif DB_SSL_MODE == "REQUIRED" and not DB_SSL_CA:
    logger.warning("DB_SSL_MODE=REQUIRED but DB_SSL_CA not provided; connection may fail if server enforces certs.")

try:
    db_pool = pooling.MySQLConnectionPool(**pool_args)
except Exception as e:
    logger.exception("Failed creating DB pool: %s", e)
    raise

# ---- Helper funcs: fetch, scraping, parsing ----
def fetch(url, session=None, retries=3):
    session = session or requests.Session()
    for attempt in range(1, retries + 1):
        try:
            r = session.get(url, headers=HEADERS, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            return r
        except Exception as exc:
            logger.warning("Fetch failed (%s) attempt %d/%d: %s", url, attempt, retries, exc)
            if attempt == retries:
                raise
            time.sleep(0.5 * attempt)
    raise RuntimeError("unreachable")

def looks_like_article_anchor(a, base_domain):
    txt = (a.get_text(" ", strip=True) or "")
    if len(txt) < 20:
        return False
    href = a.get("href")
    if not href:
        return False
    parsed = urlparse(href)
    if parsed.netloc and base_domain not in parsed.netloc:
        return False
    path = parsed.path or ""
    if "/20" in path or path.count("/") >= 2:
        return True
    return True

def find_article_links(html, base_url):
    soup = BeautifulSoup(html, "lxml")
    anchors = soup.find_all("a", href=True)
    domain = urlparse(base_url).netloc
    seen = set()
    results = []
    for a in anchors:
        try:
            if not looks_like_article_anchor(a, domain):
                continue
            href = urljoin(base_url, a["href"])
            if href in seen:
                continue
            text = a.get_text(" ", strip=True)
            low = text.lower()
            if any(skip in low for skip in ("read more", "home", "category", "contact", "search")):
                continue
            if len(text) < 20:
                continue
            seen.add(href)
            results.append({"title": text, "url": href})
        except Exception:
            continue
    return results

def extract_article_content(html):
    soup = BeautifulSoup(html, "lxml")
    selectors = [
        "div.entry-content",
        "div.post-content",
        "article .entry-content",
        "article",
        "div.single-content",
        "div.content",
        "div.post"
    ]
    for sel in selectors:
        node = soup.select_one(sel)
        if node:
            for junk in node.select("script, style, .share, .ads, .wp-block-embed"):
                junk.decompose()
            paragraphs = [p.get_text(" ", strip=True) for p in node.find_all("p")]
            text = " ".join([p for p in paragraphs if p])
            if text and len(text) > 40:
                return text
    paragraphs = soup.find_all("p")
    if paragraphs:
        text = " ".join(p.get_text(" ", strip=True) for p in paragraphs[:10])
        return text.strip()
    return ""

def extract_published_date(html):
    soup = BeautifulSoup(html, "lxml")
    time_tag = soup.find("time")
    if time_tag:
        dt = time_tag.get("datetime") or time_tag.get_text(" ", strip=True)
        if dt:
            try:
                parsed = dateparser.parse(dt, fuzzy=True)
                return parsed
            except Exception:
                pass
    meta_keys = [
        ('meta', {'property': 'article:published_time'}),
        ('meta', {'name': 'pubdate'}),
        ('meta', {'name': 'publishdate'}),
        ('meta', {'name': 'timestamp'}),
        ('meta', {'name': 'date'}),
    ]
    for tag, attrs in meta_keys:
        m = soup.find(tag, attrs=attrs)
        if m:
            val = m.get('content') or m.get('value') or ''
            if val:
                try:
                    parsed = dateparser.parse(val, fuzzy=True)
                    return parsed
                except Exception:
                    pass
    # fallback: try to find a date-like string near top of article (less reliable)
    header = soup.find(["h1","h2"])
    if header:
        text = header.get_text(" ", strip=True)
        try:
            parsed = dateparser.parse(text, fuzzy=False)
            return parsed
        except Exception:
            pass
    return None

def is_published_today(dt):
    if dt is None:
        return False
    if dt.tzinfo is None:
        # assume UTC when naive, then convert; better than skipping
        dt = pytz.UTC.localize(dt)
    local = dt.astimezone(TIMEZONE)
    return local.date() == datetime.now(TIMEZONE).date()

# ---- DB insert function ----
def upsert_article(record):
    """
    record keys: category, title, link, summary, image_url, author, published (iso), uuid_bytes
    Uses ON DUPLICATE KEY UPDATE by link (assuming link unique)
    """
    conn = None
    try:
        conn = db_pool.get_connection()
        cur = conn.cursor()
        sql = """
        INSERT INTO articles
            (category, title, link, summary, image_url, author, published, created_at, views, is_featured, featured_rank, last_metrics_update, trend_score, uuid)
        VALUES
            (%s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            title = VALUES(title),
            summary = VALUES(summary),
            image_url = VALUES(image_url),
            author = VALUES(author),
            published = VALUES(published),
            last_metrics_update = NOW()
        """
        params = (
            record.get("category"),
            record.get("title")[:500],
            record.get("link")[:1000],
            record.get("summary"),
            record.get("image_url")[:1000],
            record.get("author")[:255] if record.get("author") else None,
            record.get("published"),
            record.get("views", 0),
            record.get("is_featured", 0),
            record.get("featured_rank", None),
            record.get("last_metrics_update", None),
            record.get("trend_score", 0.0),
            record.get("uuid_bytes")
        )
        cur.execute(sql, params)
        conn.commit()
        cur.close()
        return True
    except Exception as e:
        logger.exception("DB insert failed for %s: %s", record.get("link"), e)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        return False
    finally:
        if conn:
            conn.close()

# ---- Main flow: scrape categories ----
def scrape_category_today(category_name, start_url, max_pages=2, max_articles=None):
    session = requests.Session()
    items = []
    page = 0
    url = start_url
    visited = set()

    while url and page < max_pages:
        logger.info("Fetching listing %s page %d: %s", category_name, page+1, url)
        resp = fetch(url, session)
        links = find_article_links(resp.text, DOMAIN)
        logger.info("Found %d candidate links", len(links))
        # dedupe
        unique = []
        seen = set()
        for l in links:
            if l["url"] not in seen:
                unique.append(l)
                seen.add(l["url"])
        for l in unique:
            if max_articles and len(items) >= max_articles:
                break
            if l["url"] in visited:
                continue
            try:
                logger.info("Fetching article page: %s", l["url"])
                art = fetch(l["url"], session)
                visited.add(l["url"])
                dt = extract_published_date(art.text)
                if not dt:
                    logger.info("No date found, skipping %s", l["url"])
                    continue
                if not is_published_today(dt):
                    logger.info("Article not from today (%s), skipping %s", dt, l["url"])
                    break
                summary = extract_article_content(art.text)
                if not summary:
                    logger.info("No summary extracted, skipping %s", l["url"])
                    continue
                # image
                soup = BeautifulSoup(art.text, "lxml")
                img_tag = soup.select_one("div.entry-content img, article img, .post-content img, .single-content img")
                image = ""
                if img_tag:
                    src = img_tag.get("data-src") or img_tag.get("src") or img_tag.get("data-original")
                    if src:
                        image = urljoin(DOMAIN, src)
                # author best-effort
                author = None
                a_tag = soup.find(lambda t: t.name in ("span","a","div") and t.get("class") and any("author" in c or "byline" in c for c in t.get("class")))
                if a_tag:
                    author = a_tag.get_text(" ", strip=True)
                # published iso
                pub_iso = dt.astimezone(TIMEZONE).isoformat() if dt.tzinfo else TIMEZONE.localize(dt).isoformat()

                # rewrite with GPT rewriter (ensures not shorter than original)
                try:
                    rew = rewrite_with_gpt_expanded(l["title"], summary)
                    new_title = rew.get("header") or l["title"]
                    new_summary = rew.get("summary") or summary
                except Exception as e:
                    logger.exception("Rewriter failed, using original: %s", e)
                    new_title = l["title"]
                    new_summary = summary

                rec = {
                    "category": category_name,
                    "title": new_title,
                    "link": l["url"],
                    "summary": new_summary,
                    "image_url": image,
                    "author": author,
                    "published": pub_iso,
                    "views": 0,
                    "is_featured": 0,
                    "featured_rank": None,
                    "last_metrics_update": None,
                    "trend_score": 0.0,
                    "uuid_bytes": uuidlib.uuid4().bytes
                }

                items.append(rec)
                logger.info("Collected article: %s (len summary %d)", new_title[:80], len(new_summary))
                # small polite delay per article
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            except Exception as e:
                logger.exception("Failed processing link %s: %s", l["url"], e)
                continue

        # find next link (listing)
        soup = BeautifulSoup(resp.text, "lxml")
        next_link = None
        a_next = soup.find("a", rel="next")
        if a_next and a_next.get("href"):
            next_link = urljoin(DOMAIN, a_next["href"])
        else:
            for a in soup.find_all("a", href=True):
                txt = a.get_text(" ", strip=True).lower()
                if "older posts" in txt or txt == "older posts" or txt == "older":
                    next_link = urljoin(DOMAIN, a["href"])
                    break

        url = next_link
        page += 1
        time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))

    return items

def main():
    all_collected = []
    for cat, url in START_PAGES:
        try:
            collected = scrape_category_today(cat, url, max_pages=MAX_PAGES_PER_CATEGORY)
            logger.info("Category %s: collected %d items", cat, len(collected))
            for rec in collected:
                ok = upsert_article(rec)
                if ok:
                    logger.info("Saved to DB: %s", rec["link"])
                else:
                    logger.warning("Failed to save: %s", rec["link"])
            all_collected.extend(collected)
        except Exception as e:
            logger.exception("Failed category %s: %s", cat, e)

    # At end, print JSON array of inserted records to stdout
    print(json.dumps(all_collected, ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
