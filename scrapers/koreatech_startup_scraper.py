#!/usr/bin/env python3
"""
koreatech_startup_to_sql.py

Scrape today's articles from KoreaTechDesk Startup category, rewrite title+summary via GPT rewriter,
and store in 'articles' table using DB credentials from .env.

Assumes an existing `articles` table with at least the columns used in the INSERT/UPSERT below.
(See your example schema — adjust column lengths/types if needed.)

Sample .env:
DB_USER=youruser
DB_PASS=yourpass
DB_HOST=127.0.0.1
DB_PORT=3306
DB_NAME=yourdb
DB_SSL_MODE=DISABLED
DB_SSL_CA=
OPENAI_API_KEY=sk-...

Usage:
  pip install requests beautifulsoup4 python-dateutil pytz python-dotenv mysql-connector-python
  python koreatech_startup_to_sql.py
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

# ---- GPT rewriter import (pluggable) ----
# The rewriter must expose a function that takes (title, body) and returns {"header": new_title, "summary": new_summary}
# Replace `gpt_rewriter_expanded` with your module (same as your kbizoom example).
from gpt_rewriter_expanded import rewrite_with_gpt_expanded  # adjust import name if different

# ---- Load env ----
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME")
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "DISABLED").upper()
DB_SSL_CA = os.getenv("DB_SSL_CA")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")  # optional for rewriter

if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    raise SystemExit("Missing DB config in .env. Please set DB_USER, DB_PASS, DB_HOST, DB_NAME.")

# ---- Logging ----
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger("koreatech_startup_to_sql")

# ---- Config ----
DOMAIN = "https://koreatechdesk.com"
START_PAGES = [
    ("tech_startups", "https://koreatechdesk.com/category/startup/"),
]
MAX_PAGES_PER_CATEGORY = 3
TIMEZONE = pytz.timezone("Asia/Kolkata")
USER_AGENT = "Mozilla/5.0 (compatible; MokshiriScraper/1.0; +https://example.com/bot)"
HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 12
SLEEP_MIN = 0.8
SLEEP_MAX = 1.8

# ---- DB pool ----
pool_args = {
    "user": DB_USER,
    "password": DB_PASS,
    "host": DB_HOST,
    "port": DB_PORT,
    "database": DB_NAME,
    "pool_name": "koreatech_pool",
    "pool_size": 5,
    "autocommit": False,
}
if DB_SSL_MODE in ("REQUIRED", "PREFERRED") and DB_SSL_CA:
    pool_args["ssl_ca"] = DB_SSL_CA
elif DB_SSL_MODE == "REQUIRED" and not DB_SSL_CA:
    logger.warning("DB_SSL_MODE=REQUIRED but DB_SSL_CA not provided; connection may fail if server enforces certs.")

try:
    db_pool = pooling.MySQLConnectionPool(**pool_args)
except Exception as e:
    logger.exception("Failed creating DB pool: %s", e)
    raise

# ---- Helpers ----
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
    if len(txt) < 18:
        return False
    href = a.get("href")
    if not href:
        return False
    parsed = urlparse(href)
    if parsed.netloc and base_domain not in parsed.netloc:
        return False
    # typical article links contain year or multiple path segments, but be permissive:
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
            # skip common non-article anchors
            if any(skip in low for skip in ("read more", "more", "home", "category", "contact", "search", "tag:")):
                continue
            # avoid short labels
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
            for junk in node.select("script, style, .share, .ads, .wp-block-embed, .related"):
                junk.decompose()
            paragraphs = [p.get_text(" ", strip=True) for p in node.find_all("p")]
            text = " ".join([p for p in paragraphs if p])
            if text and len(text) > 40:
                return text
    # fallback: first paragraphs on page
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
    # try meta tags
    meta_candidates = [
        ('meta', {'property': 'article:published_time'}),
        ('meta', {'name': 'pubdate'}),
        ('meta', {'name': 'publishdate'}),
        ('meta', {'name': 'timestamp'}),
        ('meta', {'name': 'date'}),
        ('meta', {'name': 'DC.date.issued'}),
    ]
    for tag, attrs in meta_candidates:
        m = soup.find(tag, attrs=attrs)
        if m:
            val = m.get('content') or m.get('value') or ''
            if val:
                try:
                    parsed = dateparser.parse(val, fuzzy=True)
                    return parsed
                except Exception:
                    pass
    # last resort: look for date-like strings near header
    header = soup.find(["h1","h2"])
    if header:
        try:
            parsed = dateparser.parse(header.get_text(" ", strip=True), fuzzy=True)
            return parsed
        except Exception:
            pass
    return None

def is_published_today(dt):
    if dt is None:
        return False
    if dt.tzinfo is None:
        dt = pytz.UTC.localize(dt)
    local = dt.astimezone(TIMEZONE)
    return local.date() == datetime.now(TIMEZONE).date()

# ---- DB upsert ----
def upsert_article(record):
    """
    record keys: category, title, link, summary, image_url, author, published, uuid_bytes, views, is_featured, featured_rank, trend_score
    Uses ON DUPLICATE KEY UPDATE by link (assuming unique key on link).
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
            (record.get("title") or "")[:500],
            (record.get("link") or "")[:1000],
            record.get("summary"),
            (record.get("image_url") or "")[:1000],
            (record.get("author") or "")[:255],
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

# ---- Scrape flow ----
def scrape_category_today(category_name, start_url, max_pages=2, max_articles=None):
    session = requests.Session()
    items = []
    page = 0
    url = start_url
    visited = set()
    while url and page < max_pages:
        logger.info("Fetching listing %s page %d: %s", category_name, page + 1, url)
        resp = fetch(url, session)
        links = find_article_links(resp.text, DOMAIN)
        logger.info("Found %d candidate links on listing", len(links))
        # dedupe preserving order
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
                logger.info("Fetching article: %s", l["url"])
                art = fetch(l["url"], session)
                visited.add(l["url"])
                dt = extract_published_date(art.text)
                if not dt:
                    logger.info("No date found; skipping: %s", l["url"])
                    continue
                if not is_published_today(dt):
                    logger.info("Article not from today (%s); skipping: %s", dt, l["url"])
                    # continue looking other links (do not break: other links may be today's)
                    continue
                summary = extract_article_content(art.text)
                if not summary or len(summary) < 80:
                    logger.info("No usable summary extracted; skipping: %s", l["url"])
                    continue
                soup = BeautifulSoup(art.text, "lxml")
                # pick main image if present
                img_tag = soup.select_one("div.entry-content img, article img, .post-content img, .single-content img")
                image = ""
                if img_tag:
                    src = img_tag.get("data-src") or img_tag.get("src") or img_tag.get("data-original")
                    if src:
                        image = urljoin(DOMAIN, src)
                # author best-effort
                author = None
                author_selectors = [
                    ".byline a", ".author", ".entry-author", ".posted-by", ".byline", ".meta-author"
                ]
                for sel in author_selectors:
                    node = soup.select_one(sel)
                    if node:
                        author = node.get_text(" ", strip=True)
                        break
                # published iso
                pub_iso = dt.astimezone(TIMEZONE).isoformat() if dt.tzinfo else TIMEZONE.localize(dt).isoformat()
                # rewrite via GPT rewriter (safe fallback to original)
                try:
                    rew = rewrite_with_gpt_expanded(l["title"], summary)
                    new_title = rew.get("header") or l["title"]
                    new_summary = rew.get("summary") or summary
                except Exception as e:
                    logger.exception("Rewriter failed, using original content: %s", e)
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
                logger.info("Collected article: %s (summary len %d)", new_title[:80], len(new_summary))
                time.sleep(random.uniform(SLEEP_MIN, SLEEP_MAX))
            except Exception as e:
                logger.exception("Failed processing link %s: %s", l["url"], e)
                continue
        # pagination: try rel="next" or "older posts" anchors on the listing
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
            logger.info("Category %s collected %d items", cat, len(collected))
            for rec in collected:
                ok = upsert_article(rec)
                if ok:
                    logger.info("Saved to DB: %s", rec["link"])
                else:
                    logger.warning("Failed to save: %s", rec["link"])
            all_collected.extend(collected)
        except Exception as e:
            logger.exception("Category %s failed: %s", cat, e)
    # print summary JSON to stdout for logging/cron capture
    print(json.dumps([{"title": r["title"], "link": r["link"], "published": r["published"]} for r in all_collected], ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
