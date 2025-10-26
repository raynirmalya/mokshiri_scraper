#!/usr/bin/env python3
"""
thepickool_multi_tags_to_sql.py

Scrape today's articles from ThePickool tags:
 - https://www.thepickool.com/tag/startups/  -> category "tech_startups"
 - https://www.thepickool.com/tag/life-culture/ -> category "culture"

Rewrites title+summary via a pluggable GPT rewriter and upserts into MySQL `articles` table using .env DB credentials.

Sample .env:
DB_USER=youruser
DB_PASS=yourpass
DB_HOST=127.0.0.1
DB_PORT=3306
DB_NAME=yourdb
DB_SSL_MODE=DISABLED
DB_SSL_CA=
OPENAI_API_KEY=sk-...

Install:
  pip install requests beautifulsoup4 python-dateutil pytz python-dotenv mysql-connector-python

Usage:
  python thepickool_multi_tags_to_sql.py
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
from mysql.connector import pooling

# ---- GPT rewriter import (pluggable) ----
# Must expose rewrite_with_gpt_expanded(title, body) -> {"header": ..., "summary": ...}
from gpt_rewriter_expanded import rewrite_with_gpt_expanded  # adjust to your module name

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
logger = logging.getLogger("thepickool_multi_tags_to_sql")

# ---- Config ----
DOMAIN = "https://www.thepickool.com"
START_PAGES = [
    ("tech_startups", "https://www.thepickool.com/tag/startups/"),
    ("culture", "https://www.thepickool.com/tag/life-culture/"),
]
MAX_PAGES_PER_CATEGORY = 4
TIMEZONE = pytz.timezone("Asia/Kolkata")
USER_AGENT = "Mozilla/5.0 (compatible; MokshiriScraper/1.0; +https://example.com/bot)"
HEADERS = {"User-Agent": USER_AGENT}
REQUEST_TIMEOUT = 15
SLEEP_MIN = 0.8
SLEEP_MAX = 1.6

# ---- DB pool ----
pool_args = {
    "user": DB_USER,
    "password": DB_PASS,
    "host": DB_HOST,
    "port": DB_PORT,
    "database": DB_NAME,
    "pool_name": "thepickool_pool",
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

def find_article_links(html, base_url):
    """
    Robust link-finder specialised for ThePickool (Ghost) pages:
    - Prefer anchors inside h1/h2/h3/article/.post-card areas
    - Fall back to scanning anchors with lighter filters
    Returns list of dicts: {"title":..., "url":...}
    """
    soup = BeautifulSoup(html, "lxml")
    domain = urlparse(base_url).netloc
    results = []
    seen = set()

    # Prefer title anchors
    for sel in ("h1 a", "h2 a", "h3 a", "article a", ".post-card a", ".post-card-title a", ".entry-footer a"):
        for a in soup.select(sel):
            try:
                href = a.get("href")
                if not href:
                    continue
                full = urljoin(base_url, href)
                if full in seen:
                    continue
                parsed = urlparse(full)
                if parsed.netloc and domain not in parsed.netloc:
                    continue
                text = a.get_text(" ", strip=True)
                if not text:
                    continue
                seen.add(full)
                results.append({"title": text, "url": full})
            except Exception:
                continue

    # Fallback: scan anchors lightly
    if not results:
        for a in soup.find_all("a", href=True):
            try:
                href = a["href"]
                full = urljoin(base_url, href)
                if full in seen:
                    continue
                parsed = urlparse(full)
                if parsed.netloc and domain not in parsed.netloc:
                    continue
                text = a.get_text(" ", strip=True)
                low = text.lower()
                if any(skip in low for skip in ("read more", "subscribe", "share", "tag", "category", "comments", "next", "previous")):
                    continue
                if len(text) < 8:
                    continue
                seen.add(full)
                results.append({"title": text, "url": full})
            except Exception:
                continue

    logger.info("find_article_links: discovered %d links (preview up to 8)", len(results))
    for i, r in enumerate(results[:8]):
        logger.debug("link[%d] = %s -> %s", i, r["title"], r["url"])
    return results

def extract_article_content(html):
    soup = BeautifulSoup(html, "lxml")
    selectors = [
        "article .post-content",
        "article .entry-content",
        "div.post-content",
        "div.entry-content",
        "article",
        "div.single-post",
        "div.content"
    ]
    for sel in selectors:
        node = soup.select_one(sel)
        if node:
            for junk in node.select("script, style, .share, .ads, .related, .wp-block-embed"):
                junk.decompose()
            paragraphs = [p.get_text(" ", strip=True) for p in node.find_all("p")]
            text = " ".join([p for p in paragraphs if p])
            if text and len(text) > 40:
                return text
    paragraphs = soup.find_all("p")
    if paragraphs:
        text = " ".join(p.get_text(" ", strip=True) for p in paragraphs[:12])
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
                    continue
                summary = extract_article_content(art.text)
                if not summary or len(summary) < 80:
                    logger.info("No usable summary extracted; skipping: %s", l["url"])
                    continue
                soup = BeautifulSoup(art.text, "lxml")
                img_tag = soup.select_one("article img, .post-content img, .entry-content img, .single-post img")
                image = ""
                if img_tag:
                    src = img_tag.get("data-src") or img_tag.get("src") or img_tag.get("data-original")
                    if src:
                        image = urljoin(DOMAIN, src)
                author = None
                for sel in (".byline a", ".author", ".entry-author", ".posted-by", ".byline", ".meta-author"):
                    node = soup.select_one(sel)
                    if node:
                        author = node.get_text(" ", strip=True)
                        break
                pub_iso = dt.astimezone(TIMEZONE).isoformat() if dt.tzinfo else TIMEZONE.localize(dt).isoformat()
                try:
                    rew = rewrite_with_gpt_expanded(l["title"], summary)
                    new_title = rew.get("header") or l["title"]
                    new_summary = rew.get("summary") or summary
                except Exception as e:
                    logger.exception("Rewriter failed; using original: %s", e)
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
        # pagination: ThePickool may use JS "load more" or numbered pages; try rel="next" or page patterns
        soup = BeautifulSoup(resp.text, "lxml")
        next_link = None
        a_next = soup.find("a", rel="next")
        if a_next and a_next.get("href"):
            next_link = urljoin(DOMAIN, a_next["href"])
        else:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if "/page/" in href or "page=" in href:
                    next_link = urljoin(DOMAIN, href)
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
    # print a compact JSON summary for logs
    print(json.dumps([{"category": r["category"], "title": r["title"], "link": r["link"], "published": r["published"]} for r in all_collected], ensure_ascii=False, indent=2))

if __name__ == "__main__":
    main()
