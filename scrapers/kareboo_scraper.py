#!/usr/bin/env python3
"""
koreaboo_scraper_page_style.py

Visits Koreaboo listing pages using the /news/page/{n}/ pattern:
 - page 1 -> https://www.koreaboo.com/news/
 - page 2 -> https://www.koreaboo.com/news/page/2/
 - page N -> https://www.koreaboo.com/news/page/N/

Preserves previous behavior:
 - Playwright rendering + BeautifulSoup extraction
 - Keyword classifier: kpop, kdrama, kpop_celeb, fallback news
 - MySQL upsert into `articles` (uuid stored as bytes)
 - JSON-safe merged output; corrupted JSON backed up
"""

import os
import asyncio
import json
import logging
import random
import re
import uuid as uuidlib
from urllib.parse import urljoin
from datetime import datetime

import pytz
from dateutil import parser as dateparser
from dotenv import load_dotenv
from bs4 import BeautifulSoup

import mysql.connector
from mysql.connector import pooling

from playwright.async_api import async_playwright

# Load env
load_dotenv()

# --- Configuration ---
LISTING_URL = os.getenv("LISTING_URL", "https://www.koreaboo.com/news/")
BASE_CATEGORY = os.getenv("BASE_CATEGORY", "news")
MAX_PAGES = int(os.getenv("MAX_PAGES", "3"))  # visits pages 1..MAX_PAGES
TIMEZONE = pytz.timezone(os.getenv("TIMEZONE", "Asia/Kolkata"))
OUTPUT_JSON = os.getenv("OUTPUT_JSON", "koreaboo_news_today_db.json")

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME")
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "DISABLED").upper()
DB_SSL_CA = os.getenv("DB_SSL_CA") or None

USER_AGENT = os.getenv("USER_AGENT",
                       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")
NAV_TIMEOUT = int(os.getenv("NAV_TIMEOUT", "30000"))  # ms

# Heuristics
TITLE_BLACKLIST_WORDS = {"about", "policy", "privacy", "terms", "contact", "advertise"}
DOMAIN_HOSTS = ("www.koreaboo.com", "koreaboo.com")

# Keywords for classifier
KPOP_KEYWORDS = {
    "bts", "blackpink", "twice", "exo", "aespa", "stray kids", "seventeen", "nct",
    "comeback", "mv", "music", "single", "album", "stage", "idol", "solo", "debut",
    "kpop", "chart", "billboard", "spotify", "song", "concert", "tour"
}
KDRAMA_KEYWORDS = {
    "drama", "kdrama", "episode", "series", "tvn", "jtbc", "mbc", "sbs", "kbs",
    "netflix", "season", "casting", "premiere", "aired", "ratings", "role",
    "filming", "script", "screenplay", "lead", "supporting"
}
CELEB_KEYWORDS = {
    "actor", "actress", "celebrity", "rumor", "relationship", "dating", "marriage",
    "girlfriend", "boyfriend", "scandal", "arrest", "lawsuit", "photos", "spotted",
    "netizen", "agency", "agency statement", "profile", "birthday", "statement"
}

# Logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger("koreaboo_news")

# Validate DB config
if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    raise SystemExit("Missing DB configuration in .env (DB_USER, DB_PASS, DB_HOST, DB_NAME required)")

# Create DB pool
pool_args = {
    "user": DB_USER,
    "password": DB_PASS,
    "host": DB_HOST,
    "port": DB_PORT,
    "database": DB_NAME,
    "pool_name": "koreaboo_pool",
    "pool_size": 5,
    "autocommit": False
}
if DB_SSL_MODE in ("REQUIRED", "PREFERRED") and DB_SSL_CA:
    pool_args["ssl_ca"] = DB_SSL_CA

try:
    db_pool = pooling.MySQLConnectionPool(**pool_args)
except Exception as e:
    logger.exception("Failed to create DB pool: %s", e)
    raise

# UPSERT SQL
UPSERT_SQL = """
INSERT INTO articles
  (category, title, link, summary, image_url, author, published, created_at, views, is_featured, featured_rank, last_metrics_update, trend_score, uuid)
VALUES
  (%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  title = VALUES(title),
  summary = VALUES(summary),
  image_url = VALUES(image_url),
  author = VALUES(author),
  published = VALUES(published),
  last_metrics_update = NOW()
"""

def db_upsert(record: dict) -> bool:
    conn = None
    try:
        conn = db_pool.get_connection()
        cur = conn.cursor()
        params = (
            record.get("category"),
            (record.get("title")[:500]) if record.get("title") else None,
            (record.get("link")[:1000]) if record.get("link") else None,
            record.get("summary"),
            (record.get("image_url")[:1000]) if record.get("image_url") else None,
            (record.get("author")[:255]) if record.get("author") else None,
            record.get("published"),
            record.get("views", 0),
            record.get("is_featured", 0),
            record.get("featured_rank", None),
            record.get("last_metrics_update", None),
            record.get("trend_score", 0.0),
            record.get("uuid_bytes")
        )
        cur.execute(UPSERT_SQL, params)
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

# --- HTML helpers ---
def extract_main_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        ("div", "post-content"),
        ("div", "entry-content"),
        ("div", "article-content"),
        ("article", None),
        ("div", "article"),
        ("div", "main"),
        ("div", "content")
    ]
    for tag, cls in selectors:
        node = soup.find(tag, class_=cls) if cls else soup.find(tag)
        if node:
            for junk in node.find_all(["script", "style", "iframe"]):
                junk.decompose()
            for j in node.find_all(class_=re.compile(r"(share|ads|related|wp-block-embed|social)")):
                j.decompose()
            paragraphs = [p.get_text(" ", strip=True) for p in node.find_all("p")]
            text = " ".join([p for p in paragraphs if p])
            if text and len(text) > 40:
                return text
    paras = soup.find_all("p")
    if paras:
        text = " ".join(p.get_text(" ", strip=True) for p in paras[:15])
        return text.strip()
    return ""

def extract_main_image(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    meta_og = soup.find("meta", property="og:image")
    if meta_og and meta_og.get("content"):
        return meta_og.get("content")
    for sel_cls in ("post-content", "entry-content", "featured", "article"):
        container = soup.find("div", class_=sel_cls)
        if container:
            img = container.find("img")
            if img:
                return img.get("data-src") or img.get("src") or img.get("data-original") or ""
    article = soup.find("article")
    if article:
        i = article.find("img")
        if i:
            return i.get("data-src") or i.get("src") or ""
    meta_img = soup.find("meta", attrs={"name": "image"})
    if meta_img and meta_img.get("content"):
        return meta_img.get("content")
    return ""

def extract_published_date(html: str):
    soup = BeautifulSoup(html, "html.parser")
    time_tag = soup.find("time")
    if time_tag:
        dt = time_tag.get("datetime") or time_tag.get_text(" ", strip=True)
        if dt:
            try:
                return dateparser.parse(dt, fuzzy=True)
            except Exception:
                pass
    for prop in ("article:published_time", "og:updated_time", "og:published_time", "date", "pubdate"):
        m = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if m and m.get("content"):
            try:
                return dateparser.parse(m.get("content"), fuzzy=True)
            except Exception:
                pass
    possible = soup.find(lambda t: t.name in ("div", "span") and t.get_text() and re.search(r"\b(20\d{2}|\d{1,2}\s+\w+\s+20\d{2})\b", t.get_text()))
    if possible:
        try:
            return dateparser.parse(possible.get_text(" ", strip=True), fuzzy=True)
        except Exception:
            pass
    return None

def extract_author(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", attrs={"name": "author"})
    if meta and meta.get("content"):
        return meta.get("content").strip()
    sel = soup.find(lambda t: t.name in ("span", "div", "a") and t.get("class") and any("author" in c or "byline" in c for c in t.get("class")))
    if sel:
        return sel.get_text(" ", strip=True)
    byline = soup.find(string=re.compile(r"^\s*By\s+", re.I))
    if byline:
        txt = byline.strip()
        return re.sub(r"^\s*By\s+", "", txt, flags=re.I).strip()
    return ""

# classifier with kpop_celeb
def classify_article(title: str, summary: str) -> str:
    text = " ".join(filter(None, [title, summary])).lower()
    score = {"kpop": 0, "kdrama": 0, "kpop_celeb": 0}
    for kw in KPOP_KEYWORDS:
        if kw in text:
            score["kpop"] += 1
    for kw in KDRAMA_KEYWORDS:
        if kw in text:
            score["kdrama"] += 1
    for kw in CELEB_KEYWORDS:
        if kw in text:
            score["kpop_celeb"] += 1
    if all(v == 0 for v in score.values()):
        return BASE_CATEGORY
    order = {"kpop": 3, "kdrama": 2, "kpop_celeb": 1}
    best = max(score.keys(), key=lambda k: (score[k], order.get(k, 0)))
    return best

# helper to build page URL using /news/page/{n}/ pattern
def page_url(page_num: int) -> str:
    """
    page_num == 1 -> LISTING_URL (normalized)
    page_num > 1  -> LISTING_URL + 'page/{page_num}/'
    """
    base = LISTING_URL.rstrip("/") + "/"
    if page_num == 1:
        return base
    return urljoin(base, f"page/{page_num}/")

# --- Main scraping loop: direct pages 1..MAX_PAGES using /news/page/{n}/ ---
async def scrape_pages_page_style():
    results_for_json = []
    results_db_records = []

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(user_agent=USER_AGENT)

        for page_num in range(1, MAX_PAGES + 1):
            current_listing = page_url(page_num)
            logger.info("Loading listing (direct page-style): %s", current_listing)
            try:
                await page.goto(current_listing, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                await page.wait_for_timeout(800 + random.randint(0, 1400))
            except Exception as e:
                logger.warning("Failed to load listing %s: %s", current_listing, e)
                continue

            # gather anchors containing '/news/' (safe eval)
            pairs = await page.evaluate("""() => {
                const arr = [];
                try {
                    const anchors = Array.from(document.querySelectorAll('a[href*="/news/"]'));
                    const seen = new Set();
                    for (const a of anchors) {
                        try {
                            const href = a.href || '';
                            let text = (a.innerText || '').trim();
                            if (!text && a.getAttribute) text = a.getAttribute('aria-label') || '';
                            if (!text) {
                                const img = a.querySelector && a.querySelector('img');
                                if (img) text = img.alt || '';
                            }
                            if (!text) {
                                const h = a.closest && a.closest('h1,h2,h3,h4,h5');
                                if (h) text = (h.innerText || '').trim();
                            }
                            if (!text) {
                                const pcont = a.closest && (a.closest('div') || a.parentElement);
                                if (pcont) text = (pcont.innerText || '').trim().slice(0,200);
                            }
                            if (!href) continue;
                            if (seen.has(href)) continue;
                            seen.add(href);
                            arr.push({href: href.split('?')[0], text: text});
                        } catch (e) {}
                    }
                } catch (err) {
                    return [];
                }
                return arr;
            }""")

            # filter anchors for actual article slugs
            seen = set()
            candidates = []
            for pitem in pairs:
                href = pitem.get("href") or ""
                text = (pitem.get("text") or "").strip()
                if not href or not text or len(text) < 6:
                    continue
                href = href.split("?")[0]
                # ensure we exclude pure numeric pagination links like /news/page/2/ or /news/2/
                # keep links that have a letter or hyphen after /news/
                if not re.search(r"/news/(?:page/)?[a-z0-9\-]", href, re.I):
                    continue
                if href in seen:
                    continue
                seen.add(href)
                lt = text.lower()
                if any(b in lt for b in TITLE_BLACKLIST_WORDS):
                    continue
                candidates.append({"href": href, "text": text})

            logger.info("Found %d candidate article links on page %d", len(candidates), page_num)
            logger.debug("CANDIDATES: %s", [c['href'] for c in candidates][:40])

            # Visit each article candidate
            for cand in candidates:
                href = cand["href"]
                logger.info("Visiting article: %s", href)
                try:
                    await page.goto(href, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                    await page.wait_for_timeout(700 + random.randint(0, 1200))
                    art_html = await page.content()
                except Exception as e:
                    logger.warning("Failed to load article %s: %s", href, e)
                    continue

                title = cand["text"] or ""
                body = extract_main_text(art_html)
                if not body or len(body) < 50:
                    logger.info("Article body missing/too short; skipping %s", href)
                    continue
                image_url = extract_main_image(art_html) or ""
                author = extract_author(art_html) or ""
                dt = extract_published_date(art_html)
                if not dt:
                    logger.info("No published date; skipping %s", href)
                    continue
                if dt.tzinfo is None:
                    dt = pytz.UTC.localize(dt)
                local_dt = dt.astimezone(TIMEZONE)
                published_iso = local_dt.isoformat()

                category = classify_article(title, body)

                rec_db = {
                    "category": category,
                    "title": title,
                    "link": href,
                    "summary": body,
                    "image_url": image_url,
                    "author": author,
                    "published": published_iso,
                    "views": 0,
                    "is_featured": 0,
                    "featured_rank": None,
                    "last_metrics_update": None,
                    "trend_score": 0.0,
                    "uuid_bytes": uuidlib.uuid4().bytes
                }

                rec_json = dict(rec_db)
                try:
                    rec_json["uuid"] = uuidlib.UUID(bytes=rec_json["uuid_bytes"]).hex
                except Exception:
                    rec_json["uuid"] = uuidlib.uuid4().hex
                if "uuid_bytes" in rec_json:
                    del rec_json["uuid_bytes"]

                ok = db_upsert(rec_db)
                if ok:
                    logger.info("Upserted to DB (%s): %s", category, href)
                else:
                    logger.warning("DB upsert failed for: %s", href)

                results_db_records.append(rec_db)
                results_for_json.append(rec_json)

                await asyncio.sleep(0.5 + random.random() * 1.5)

            # short pause between listing pages
            await asyncio.sleep(1 + random.random() * 1.5)

        await browser.close()

    # Merge/Write JSON (backup corrupted if needed)
    merged = {}
    existing = []
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except json.JSONDecodeError as e:
            logger.exception("Existing JSON malformed: %s. Backing up corrupted file.", e)
            try:
                ts = datetime.now().strftime("%Y%m%d%H%M%S")
                bak = f"{OUTPUT_JSON}.broken.{ts}"
                os.replace(OUTPUT_JSON, bak)
                logger.info("Backed up corrupted JSON to: %s", bak)
            except Exception as ex:
                logger.exception("Failed to backup corrupted JSON file: %s", ex)
            existing = []
        except Exception as e:
            logger.exception("Failed to load existing JSON; proceeding fresh: %s", e)
            existing = []

    if isinstance(existing, list):
        for e in existing:
            if isinstance(e, dict) and e.get("link"):
                merged[e["link"]] = e

    for rec in results_for_json:
        merged[rec["link"]] = rec

    merged_list = list(merged.values())
    try:
        with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
            json.dump(merged_list, f, ensure_ascii=False, indent=2)
        logger.info("Wrote %d total article records to %s", len(merged_list), OUTPUT_JSON)
    except Exception as e:
        logger.exception("Failed to write JSON: %s", e)

    return results_db_records

def main():
    results_db = asyncio.run(scrape_pages_page_style())
    print(f"DB upsert attempts in this run: {len(results_db)}")

if __name__ == "__main__":
    main()
