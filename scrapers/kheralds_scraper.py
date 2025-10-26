#!/usr/bin/env python3
"""
korea_herald_playwright_debug_scrape.py

Playwright + BeautifulSoup scraper for Korea Herald listings with debugging:
 - Saves listing HTML to listing_debug_<slug>.html
 - Prints first 200 anchors captured on each listing page (href + text)
 - Filters anchors using multiple KoreaHerald heuristics
 - Visits article pages, extracts title/summary/image/author/published
 - Supports --dry-run to avoid DB writes (prints JSON only)
 - Upserts into `articles` MySQL table if not dry-run (uses .env DB creds)

Usage:
    python korea_herald_playwright_debug_scrape.py            # real run (writes DB)
    python korea_herald_playwright_debug_scrape.py --dry-run  # only JSON output, no DB

Requirements:
    pip install playwright mysql-connector-python python-dotenv python-dateutil pytz beautifulsoup4
    python -m playwright install
"""

import os
import sys
import argparse
import asyncio
import json
import logging
import random
import re
import uuid as uuidlib
from urllib.parse import urljoin, urlparse
from datetime import datetime

import pytz
from dateutil import parser as dateparser
from dotenv import load_dotenv
from bs4 import BeautifulSoup

# Playwright + MySQL
from playwright.async_api import async_playwright
import mysql.connector
from mysql.connector import pooling

# optional rewriter
try:
    from gpt_rewriter_expanded import rewrite_with_gpt_expanded
    HAVE_REWRITER = True
except Exception:
    HAVE_REWRITER = False

# -------- CONFIG ----------
load_dotenv()
TIMEZONE = pytz.timezone(os.getenv("TIMEZONE", "Asia/Kolkata"))
NAV_TIMEOUT = int(os.getenv("NAV_TIMEOUT", "30000"))
USER_AGENT = os.getenv("USER_AGENT", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
MAX_PAGES = int(os.getenv("MAX_PAGES", "2"))
OUTPUT_JSON = os.getenv("OUTPUT_JSON", "koreaherald_debug_output.json")

CATEGORY_MAP = {
    "https://www.koreaherald.com/LifenCulture/Culture": "culture",
    "https://www.koreaherald.com/LifenCulture/Travel": "travel",
    "https://www.koreaherald.com/LifenCulture/Food": "food",
    "https://www.koreaherald.com/LifenCulture/People": "kpop_celeb",
    "https://www.koreaherald.com/Kpop": "kpop",
}

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME")

# DB pool (created only if not dry-run)
DB_POOL = None

# Logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger("koreaherald_debug")

# UPSERT SQL (expects UNIQUE on `link`)
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

# --------- Helpers: heuristics & parsers ----------
def looks_like_kh_article(href: str) -> bool:
    """Heuristics for KoreaHerald article URLs."""
    if not href:
        return False
    parsed = urlparse(href)
    path = parsed.path.lower()
    query = parsed.query.lower()
    # canonical KH patterns:
    # - view.php?ud=YYYYMMDD...
    # - /news/ followed by numbers or text
    # - contains ud= or view.php or /news/ or hyphenated slugs or year segments
    if "view.php" in path or "view.php" in href or "ud=" in query:
        return True
    if "/news/" in path or path.startswith("/news"):
        return True
    if re.search(r"/20\d{2}/", path):
        return True
    last = path.rstrip("/").split("/")[-1]
    if "-" in last and len(last) > 4:
        return True
    if re.search(r"/\d{5,}/", path) or re.search(r"\d{6,}", href):
        return True
    return False

def extract_text_from_article(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # try main KH containers
    for sel in (".article-view .view_con", ".article-body", ".view_con", ".article", "#articleText", ".news_txt"):
        node = soup.select_one(sel)
        if node:
            for junk in node.find_all(["script", "style", "iframe", "ins"]):
                junk.decompose()
            ps = [p.get_text(" ", strip=True) for p in node.find_all("p")]
            text = " ".join([p for p in ps if p])
            if text and len(text) > 40:
                return text
    # fallback: first paragraphs
    paras = soup.find_all("p")
    if paras:
        return " ".join(p.get_text(" ", strip=True) for p in paras[:12]).strip()
    return ""

def extract_image_from_article(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    m = soup.find("meta", property="og:image")
    if m and m.get("content"):
        return m["content"]
    imgs = soup.select("article img, .article img, .view_img img, img")
    for img in imgs:
        src = img.get("data-src") or img.get("src") or img.get("data-original")
        if src and src.strip():
            return src
    return ""

def extract_date_from_article(html: str):
    soup = BeautifulSoup(html, "html.parser")
    # check meta
    m = soup.find("meta", property="article:published_time") or soup.find("meta", attrs={"name":"date"})
    if m and m.get("content"):
        try:
            return dateparser.parse(m.get("content"), fuzzy=True)
        except Exception:
            pass
    # check time or small date classes
    t = soup.find("time")
    if t:
        txt = t.get("datetime") or t.get_text(" ", strip=True)
        try:
            return dateparser.parse(txt, fuzzy=True)
        except Exception:
            pass
    # small selectors
    candidate = soup.select_one(".date, .time, .byline, .byline span")
    if candidate:
        try:
            return dateparser.parse(candidate.get_text(" ", strip=True), fuzzy=True)
        except Exception:
            pass
    # try to find 'ud=' in canonical tag
    return None

def extract_author_from_article(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    ma = soup.find("meta", attrs={"name":"author"})
    if ma and ma.get("content"):
        return ma.get("content")
    sel = soup.select_one(".byline, .writer, .author")
    if sel:
        return sel.get_text(" ", strip=True)
    return ""

# ---- DB upsert (defensive) ----
def db_upsert(record: dict) -> bool:
    global DB_POOL
    if DB_POOL is None:
        logger.error("DB_POOL is not initialized")
        return False
    conn = None
    cur = None
    try:
        conn = DB_POOL.get_connection()
        cur = conn.cursor()
        params = (
            (record.get("category")[:50]) if record.get("category") else None,
            (record.get("title")[:500]) if record.get("title") else None,
            (record.get("link")[:1000]) if record.get("link") else None,
            record.get("summary"),
            (record.get("image_url")[:1000]) if record.get("image_url") else None,
            (record.get("author")[:255]) if record.get("author") else None,
            record.get("published"),
            int(record.get("views") or 0),
            int(record.get("is_featured") or 0),
            (int(record.get("featured_rank")) if record.get("featured_rank") is not None else None),
            record.get("last_metrics_update"),
            float(record.get("trend_score") or 0.0),
            record.get("uuid_bytes")
        )
        cur.execute(UPSERT_SQL, params)
        conn.commit()
        logger.info("DB upsert OK for %s", record.get("link"))
        return True
    except Exception as e:
        logger.exception("DB upsert failed for %s: %s — params: %s", record.get("link"), e, repr(params))
        if conn:
            try: conn.rollback()
            except Exception: pass
        return False
    finally:
        try:
            if cur: cur.close()
        except Exception: pass
        try:
            if conn: conn.close()
        except Exception: pass

# -------- Main scraping routine (Playwright) ----------
async def scrape_listing(listing_url: str, mapped_category: str, dry_run: bool):
    results = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(user_agent=USER_AGENT)
        current = listing_url
        pages_scraped = 0

        while current and pages_scraped < MAX_PAGES:
            logger.info("Loading listing: %s (category=%s)", current, mapped_category)
            try:
                await page.goto(current, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                await page.wait_for_timeout(1000 + random.randint(0, 1500))
            except Exception as e:
                logger.warning("Failed to load listing %s: %s", current, e)
                break

            content = await page.content()
            # save debug listing HTML
            safe_name = re.sub(r"[^0-9a-zA-Z_\-\.]", "_", urlparse(listing_url).path.strip("/") or "listing")
            debug_file = f"listing_debug_{safe_name}.html"
            try:
                with open(debug_file, "w", encoding="utf-8") as fh:
                    fh.write(content)
                logger.info("Saved listing HTML to %s", debug_file)
            except Exception:
                logger.exception("Failed saving listing debug HTML")

            # capture anchors using page.evaluate (absolute hrefs)
            pairs = await page.evaluate("""() => {
                const arr = [];
                document.querySelectorAll("a").forEach(a => {
                    const href = a.href || "";
                    const text = (a.innerText || a.textContent || "").trim();
                    if (href) arr.push({href: href, text: text});
                });
                return arr;
            }""")

            logger.info("Captured %d anchors on listing", len(pairs))
            # print first 200 anchors for debugging
            for i, it in enumerate(pairs[:200], 1):
                logger.info("Anchor %03d: href=%s ; text=%s", i, (it.get("href") or "")[:180], (it.get("text") or "")[:140])

            # filter anchors with stronger heuristics
            seen = set()
            candidates = []
            for it in pairs:
                href = it.get("href") or ""
                text = (it.get("text") or "").strip()
                if not href or not text or len(text) < 5:
                    continue
                if looks_like_kh_article(href):
                    if href not in seen:
                        seen.add(href)
                        candidates.append({"href": href, "title": text})

            logger.info("Filtered %d post-like candidates", len(candidates))

            # fallback: regex scan on raw HTML if no candidates
            if not candidates:
                logger.info("No candidates from evaluate; trying regex fallback")
                hrefs = re.findall(r'href=["\\\']([^"\\\']+)["\\\']', content, re.I)
                for h in hrefs:
                    full = h if h.startswith("http") else urljoin(current, h)
                    if looks_like_kh_article(full) and full not in seen:
                        seen.add(full)
                        candidates.append({"href": full, "title": ""})
                logger.info("Regex fallback found %d candidates", len(candidates))

            # visit each candidate
            for cand in candidates:
                href = cand["href"]
                title_hint = cand.get("title") or ""
                logger.info("Visiting: %s", href)
                try:
                    await page.goto(href, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                    await page.wait_for_timeout(800 + random.randint(0, 800))
                    art_html = await page.content()
                except Exception as e:
                    logger.warning("Failed load article %s: %s", href, e)
                    continue

                # parse article page
                title_on_page = ""
                try:
                    soup = BeautifulSoup(art_html, "html.parser")
                    h1 = soup.find("h1")
                    if h1:
                        title_on_page = h1.get_text(" ", strip=True)
                except Exception:
                    pass
                title = title_on_page or title_hint or href
                body = extract_text_from_article(art_html)
                if not body or len(body) < 50:
                    logger.info("Body too short; skipping %s", href)
                    continue

                dt = extract_date_from_article(art_html)
                if not dt:
                    logger.info("No published date found; skipping %s", href)
                    continue
                if dt.tzinfo is None:
                    dt = pytz.UTC.localize(dt)
                local_dt = dt.astimezone(TIMEZONE)
                if local_dt.date() != datetime.now(TIMEZONE).date():
                    logger.info("Article not today's (%s); skipping %s", local_dt.date(), href)
                    break

                published_iso = local_dt.isoformat()
                author = extract_author_from_article(art_html) or ""
                image = extract_image_from_article(art_html) or ""

                # optional rewrite
                if HAVE_REWRITER:
                    try:
                        rew = rewrite_with_gpt_expanded(title, body)
                        title_final = rew.get("header") or title
                        summary_final = rew.get("summary") or body
                    except Exception as e:
                        logger.warning("Rewriter failed: %s", e)
                        title_final = title
                        summary_final = body
                else:
                    title_final = title
                    summary_final = body

                rec = {
                    "category": mapped_category,
                    "title": title_final,
                    "link": href,
                    "summary": summary_final,
                    "image_url": image,
                    "author": author,
                    "published": published_iso,
                    "views": 0,
                    "is_featured": 0,
                    "featured_rank": None,
                    "last_metrics_update": None,
                    "trend_score": 0.0,
                    "uuid_bytes": uuidlib.uuid4().bytes
                }

                if not dry_run:
                    ok = db_upsert(rec)
                    if not ok:
                        logger.warning("DB insert failed for %s", href)
                else:
                    logger.info("[dry-run] would save: %s", title_final[:120])

                results.append(rec)
                await asyncio.sleep(0.5 + random.random() * 1.0)

            # pagination: try rel=next or common patterns
            soup = BeautifulSoup(content, "html.parser")
            next_link = None
            a_next = soup.find("a", rel="next")
            if a_next and a_next.get("href"):
                next_link = a_next.get("href")
                if not next_link.startswith("http"):
                    next_link = urljoin(current, next_link)
            else:
                # search for "Older" / page numbers
                for a in soup.find_all("a", href=True):
                    txt = (a.get_text(" ", strip=True) or "").lower()
                    if "older" in txt or txt.startswith("page"):
                        next_link = a.get("href")
                        if next_link and not next_link.startswith("http"):
                            next_link = urljoin(current, next_link)
                        break

            if not next_link or next_link == current:
                current = None
            else:
                current = next_link
            pages_scraped += 1
            await asyncio.sleep(0.8 + random.random() * 1.2)

        await browser.close()
    return results

# ------------- Runner -------------
def init_db_pool():
    global DB_POOL
    pool_args = {
        "user": DB_USER,
        "password": DB_PASS,
        "host": DB_HOST,
        "port": DB_PORT,
        "database": DB_NAME,
        "pool_name": "koreaherald_pool",
        "pool_size": 4,
        "autocommit": False
    }
    try:
        DB_POOL = pooling.MySQLConnectionPool(**pool_args)
        logger.info("DB pool created")
    except Exception as e:
        logger.exception("Failed to create DB pool: %s", e)
        raise

async def run_all(dry_run: bool):
    if not dry_run:
        # require DB envs
        if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
            raise SystemExit("Missing DB config in .env - cannot write to DB")
        init_db_pool()
    all_results = []
    for listing_url, cat in CATEGORY_MAP.items():
        logger.info("=== Scraping listing %s -> category=%s ===", listing_url, cat)
        res = await scrape_listing(listing_url, cat, dry_run)
        all_results.extend(res)
    return all_results

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="Do not write to DB, only output JSON.")
    return p.parse_args()

def main():
    args = parse_args()
    results = asyncio.run(run_all(dry_run=args.dry_run))
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    logger.info("Saved %d records to %s (dry-run=%s)", len(results), OUTPUT_JSON, args.dry_run)
    # also print compact JSON to stdout
    print(json.dumps(results, ensure_ascii=False))

if __name__ == "__main__":
    main()
