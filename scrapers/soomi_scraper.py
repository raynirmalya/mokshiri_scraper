#!/usr/bin/env python3
"""
soompi_playwright_to_sql_multi.py

Scrapes multiple Soompi categories (Playwright-rendered), rewrites (optional), and upserts into MySQL `articles` table.

Category mapping:
 - https://www.soompi.com/category/celeb         -> kpop_celeb
 - https://www.soompi.com/category/tvfilm       -> kdrama
 - https://www.soompi.com/category/music        -> kpop
 - https://www.soompi.com/category/fashionbeauty -> kfashion
"""

import os
import asyncio
import json
import logging
import random
import re
import uuid as uuidlib
from urllib.parse import urlparse, urljoin
from datetime import datetime

import pytz
from dateutil import parser as dateparser
from dotenv import load_dotenv
from bs4 import BeautifulSoup

import mysql.connector
from mysql.connector import pooling

from playwright.async_api import async_playwright

# Optional rewriter
try:
    from gpt_rewriter_expanded import gpt_rewriter_expanded
    HAVE_REWRITER = True
except Exception:
    HAVE_REWRITER = False

# Load env
load_dotenv()

# --- Configuration ---
CATEGORY_MAP = {
    "https://www.soompi.com/category/celeb": "kpop_celeb",
    "https://www.soompi.com/category/tvfilm": "kdrama",
    "https://www.soompi.com/category/music": "kpop",
    "https://www.soompi.com/category/fashionbeauty": "kfashion",
}

MAX_PAGES = int(os.getenv("MAX_PAGES", "2"))
TIMEZONE = pytz.timezone(os.getenv("TIMEZONE", "Asia/Kolkata"))
OUTPUT_JSON = os.getenv("OUTPUT_JSON", "soompi_multi_today_db.json")

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
HREF_BLACKLIST_WORDS = {
    "guidelines", "about", "contact", "support", "policy", "privacy",
    "terms", "advertise", "subscribe", "community", "submit", "help",
    "user", "login", "tag", "author", "wp-admin", "cookies"
}
TITLE_BLACKLIST_WORDS = {
    "guidelines", "policy", "privacy", "terms", "contact", "about",
    "site guidelines", "community guidelines", "advertise"
}
DOMAIN_HOSTS = ("www.soompi.com", "soompi.com")

# Logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger("soompi_multi")

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
    "pool_name": "soompi_multi_pool",
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

# UPSERT SQL: assumes `link` has UNIQUE constraint
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

# Helpers for parsing article html (BeautifulSoup)
def extract_main_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        ("div", "entry-content"), ("div", "post-content"), ("div", "article-content"),
        ("article", None), ("div", "article"), ("div", "main"), ("div", "content")
    ]
    for tag, cls in selectors:
        node = soup.find(tag, class_=cls) if cls else soup.find(tag)
        if node:
            for junk in node.find_all(["script", "style", "iframe"]):
                junk.decompose()
            for j in node.find_all(class_=re.compile(r"(share|ads|related|wp-block-embed)")):
                j.decompose()
            paragraphs = [p.get_text(" ", strip=True) for p in node.find_all("p")]
            text = " ".join([p for p in paragraphs if p])
            if text and len(text) > 40:
                return text
    # fallback
    paras = soup.find_all("p")
    if paras:
        text = " ".join(p.get_text(" ", strip=True) for p in paras[:12])
        return text.strip()
    return ""

def extract_main_image(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    meta_og = soup.find("meta", property="og:image")
    if meta_og and meta_og.get("content"):
        return meta_og.get("content")
    for sel_cls in ("entry-content", "post-content", "featured", "article"):
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
    for prop in ("article:published_time", "og:updated_time", "date", "pubdate"):
        m = soup.find("meta", property=prop) or soup.find("meta", attrs={"name": prop})
        if m and m.get("content"):
            try:
                return dateparser.parse(m.get("content"), fuzzy=True)
            except Exception:
                pass
    header = soup.find(["h1", "h2"])
    if header:
        try:
            return dateparser.parse(header.get_text(" ", strip=True), fuzzy=True)
        except Exception:
            pass
    return None

def extract_author(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    meta = soup.find("meta", attrs={"name": "author"})
    if meta and meta.get("content"):
        return meta.get("content")
    sel = soup.find(lambda t: t.name in ("span","div","a") and t.get("class") and any("author" in c or "byline" in c for c in t.get("class")))
    if sel:
        return sel.get_text(" ", strip=True)
    return ""

def is_post_like(href: str) -> bool:
    if not href:
        return False
    parsed = urlparse(href)
    netloc = parsed.netloc.lower()
    path = parsed.path.lower()
    query = parsed.query.lower()
    if not any(h in netloc for h in DOMAIN_HOSTS):
        return False
    if "support.soompi.com" in netloc or "/hc/" in path or "ticket_form_id" in query:
        return False
    if any(x in path for x in ("/user/", "/login", "/subscribe", "/wp-admin", "/tag/", "/category/", "/author/")):
        return False
    for bw in HREF_BLACKLIST_WORDS:
        if f"/{bw}" in path or path.endswith(f"-{bw}") or path.startswith(f"/{bw}"):
            return False
    if re.search(r"/20\d{2}/", path):
        return True
    last = path.rstrip("/").split("/")[-1]
    if "-" in last and len(last) > 4:
        if any(bw in last for bw in HREF_BLACKLIST_WORDS):
            return False
        return True
    return False

# Main Playwright scraping that iterates category pages
async def scrape_all_categories():
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(user_agent=USER_AGENT)

        for listing_url, mapped_category in CATEGORY_MAP.items():
            logger.info("Starting category: %s => %s", listing_url, mapped_category)
            current_listing = listing_url
            pages_scraped = 0

            while current_listing and pages_scraped < MAX_PAGES:
                logger.info("Loading listing: %s", current_listing)
                try:
                    await page.goto(current_listing, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                    await page.wait_for_timeout(1000 + random.randint(0, 2000))
                except Exception as e:
                    logger.warning("Failed to load listing %s: %s", current_listing, e)
                    break

                # use page.evaluate to get absolute hrefs + text
                pairs = await page.evaluate("""() => {
                    const arr = [];
                    document.querySelectorAll("a").forEach(a => {
                        const href = a.href || "";
                        const text = (a.innerText || a.textContent || "").trim();
                        arr.push({href: href, text: text});
                    });
                    return arr;
                }""")

                # filter post-like anchors
                seen = set()
                candidates = []
                for pitem in pairs:
                    href = pitem.get("href") or ""
                    text = (pitem.get("text") or "").strip()
                    if not href or not text or len(text) < 10:
                        continue
                    if not is_post_like(href):
                        continue
                    if href in seen:
                        continue
                    seen.add(href)
                    lt = text.lower()
                    if any(b in lt for b in TITLE_BLACKLIST_WORDS):
                        continue
                    candidates.append({"href": href, "text": text})

                logger.info("Found %d candidate article links on listing %s", len(candidates), listing_url)

                # Visit each article
                for cand in candidates:
                    href = cand["href"]
                    logger.info("Visiting article: %s", href)
                    try:
                        await page.goto(href, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                        await page.wait_for_timeout(800 + random.randint(0, 1200))
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
                    if local_dt.date() != datetime.now(TIMEZONE).date():
                        logger.info("Not today's article (%s); skipping %s", local_dt.date(), href)
                        continue

                    published_iso = local_dt.isoformat()

                    # rewrite if available
                    title_original = title
                    summary_original = body
                    if HAVE_REWRITER:
                        try:
                            rew = gpt_rewriter_expanded(title_original, summary_original)
                            title_final = rew.get("header") or title_original
                            summary_final = rew.get("summary") or summary_original
                        except Exception as e:
                            logger.warning("Rewriter fail: %s — using original", e)
                            title_final = title_original
                            summary_final = summary_original
                    else:
                        title_final = title_original
                        summary_final = summary_original

                    rec = {
                        "category": mapped_category,
                        "title": title_final,
                        "link": href,
                        "summary": summary_final,
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

                    ok = db_upsert(rec)
                    if ok:
                        logger.info("Upserted to DB: %s", href)
                    else:
                        logger.warning("DB upsert failed for: %s", href)
                    results.append(rec)

                    await asyncio.sleep(0.5 + random.random() * 1.5)

                # find next page link in the listing
                content = await page.content()
                soup = BeautifulSoup(content, "html.parser")
                next_link = None
                a_next = soup.find("a", rel="next")
                if a_next and a_next.get("href"):
                    next_link = a_next.get("href")
                    if not next_link.startswith("http"):
                        next_link = urljoin(listing_url, next_link)
                else:
                    for a in soup.find_all("a", href=True):
                        txt = (a.get_text(" ", strip=True) or "").lower()
                        if "older posts" in txt or txt == "older posts" or txt == "older":
                            next_link = a.get("href")
                            if next_link and not next_link.startswith("http"):
                                next_link = urljoin(listing_url, next_link)
                            break

                if next_link == current_listing:
                    # avoid infinite loop
                    next_link = None

                current_listing = next_link
                pages_scraped += 1
                await asyncio.sleep(1 + random.random() * 1.5)

        await browser.close()
    return results

def main():
    results = asyncio.run(scrape_all_categories())
    # save JSON
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    # print JSON to stdout
    print(json.dumps(results, ensure_ascii=False))

if __name__ == "__main__":
    main()
