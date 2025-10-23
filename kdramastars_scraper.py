#!/usr/bin/env python3
"""
kdramastars_playwright_to_sql_v2.py

Playwright-based KDramaStars scraper (improved heuristics + debug).
Scrapes these mapping URLs:
 - https://www.kdramastars.com/drama-stories => kdrama
 - https://www.kdramastars.com/celebs => kpop_celeb
 - https://www.kdramastars.com/fashion-style => kfashion

Features:
 - saves listing HTML to kdramastars_list_debug.html (for debugging)
 - prints top anchors found on listing (first 50)
 - filters anchors by `/articles/` or /20xx/ or hyphenated slug
 - visits article pages and extracts main text, image, author, published date
 - optional rewriter (gpt_rewriter_balanced) if present
 - upserts to MySQL `articles` table (uses .env DB config)
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

# optional rewriter
try:
    from gpt_rewriter_expanded import rewrite_with_gpt_expanded
    HAVE_REWRITER = True
except Exception:
    HAVE_REWRITER = False

# ---- Load env ----
load_dotenv()

CATEGORY_MAP = {
    "https://www.kdramastars.com/drama-stories": "kdrama",
    "https://www.kdramastars.com/celebs": "kpop_celeb",
    "https://www.kdramastars.com/fashion-style": "kfashion",
}

MAX_PAGES = int(os.getenv("MAX_PAGES", "2"))
TIMEZONE = pytz.timezone(os.getenv("TIMEZONE", "Asia/Kolkata"))
OUTPUT_JSON = os.getenv("OUTPUT_JSON", "kdramastars_today_v2.json")

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

# DB pool
if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    raise SystemExit("Missing DB config in .env: DB_USER, DB_PASS, DB_HOST, DB_NAME required")

pool_args = {
    "user": DB_USER,
    "password": DB_PASS,
    "host": DB_HOST,
    "port": DB_PORT,
    "database": DB_NAME,
    "pool_name": "kds_pool_v2",
    "pool_size": 5,
    "autocommit": False
}
if DB_SSL_MODE in ("REQUIRED", "PREFERRED") and DB_SSL_CA:
    pool_args["ssl_ca"] = DB_SSL_CA

try:
    db_pool = pooling.MySQLConnectionPool(**pool_args)
except Exception as e:
    raise SystemExit(f"Failed to create DB pool: {e}")

# Logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger("kdramastars_v2")

# UPSERT SQL (assumes `link` UNIQUE)
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

# ---- Heuristics tuned for KDramaStars ----
def looks_like_kds_article(href: str) -> bool:
    """Return True if href looks like a KDramaStars article."""
    if not href:
        return False
    parsed = urlparse(href)
    path = parsed.path.lower()
    # KDramaStars commonly uses /articles/... (observed pattern) OR year segments OR hyphen slugs
    if "/articles/" in path:
        return True
    if re.search(r"/20\d{2}/", path):
        return True
    last = path.rstrip("/").split("/")[-1]
    if "-" in last and len(last) > 4:
        return True
    return False

# ---- Article parsing helpers (BeautifulSoup) ----
def extract_main_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    # KDramaStars article body often in .article-body, .entry-content, or div with id article-body
    node = soup.find("div", class_="article-body") or soup.find("div", id="article-body") or soup.find("div", class_="entry-content") or soup.find("article")
    if node:
        for junk in node.find_all(["script","style","iframe","ins"]):
            junk.decompose()
        for j in node.find_all(class_=re.compile(r"(share|ads|related|wp-block-embed)")):
            j.decompose()
        paragraphs = [p.get_text(" ", strip=True) for p in node.find_all("p")]
        text = " ".join([p for p in paragraphs if p])
        if text and len(text) > 40:
            return text
    # fallback: first paragraphs from page
    paras = soup.find_all("p")
    if paras:
        return " ".join(p.get_text(" ", strip=True) for p in paras[:12]).strip()
    return ""

def extract_image(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    meta_og = soup.find("meta", property="og:image")
    if meta_og and meta_og.get("content"):
        return meta_og.get("content")
    # first useful img
    img = soup.find("img")
    if img and img.get("src"):
        return img.get("src")
    return ""

def extract_published_date(html: str):
    soup = BeautifulSoup(html, "html.parser")
    # prefer <time datetime="">
    t = soup.find("time")
    if t and t.get("datetime"):
        try:
            return dateparser.parse(t.get("datetime"), fuzzy=True)
        except Exception:
            pass
    # meta article:published_time
    m = soup.find("meta", property="article:published_time")
    if m and m.get("content"):
        try:
            return dateparser.parse(m.get("content"), fuzzy=True)
        except Exception:
            pass
    # parse any date-like string near top
    header = soup.find(["h1","h2"])
    if header:
        try:
            return dateparser.parse(header.get_text(" ", strip=True), fuzzy=True)
        except Exception:
            pass
    return None

def extract_author(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    ma = soup.find("meta", attrs={"name":"author"})
    if ma and ma.get("content"):
        return ma.get("content")
    a = soup.find("a", class_=re.compile(r"author", re.I))
    if a:
        return a.get_text(" ", strip=True)
    span = soup.find("span", class_=re.compile(r"(author|byline)", re.I))
    if span:
        return span.get_text(" ", strip=True)
    return ""

# ---- Main Playwright flow ----
async def scrape_category_with_debug(listing_url: str, mapped_category: str):
    results = []
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page(user_agent=USER_AGENT)
        current = listing_url
        pages_scraped = 0

        while current and pages_scraped < MAX_PAGES:
            logger.info("Loading listing: %s", current)
            try:
                await page.goto(current, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                # wait a bit for any lazy content
                await page.wait_for_timeout(1200 + random.randint(0,1800))
            except Exception as e:
                logger.warning("Failed to load listing %s: %s", current, e)
                break

            # save raw listing html for debugging
            content = await page.content()
            with open("kdramastars_list_debug.html", "wb") as fh:
                fh.write(content.encode("utf-8"))
            logger.info("Saved listing HTML to kdramastars_list_debug.html")

            # gather anchors via JS snapshot (absolute hrefs)
            pairs = await page.evaluate("""() => {
                const arr = [];
                document.querySelectorAll("a").forEach(a => {
                    const href = a.href || "";
                    const txt = (a.innerText || a.textContent || "").trim();
                    if (href) arr.push({href: href, text: txt});
                });
                return arr;
            }""")

            logger.info("Total anchors captured on page: %d", len(pairs))
            # print first 50 anchors for debugging
            for i, item in enumerate(pairs[:50], 1):
                logger.info("Anchor %d: href=%s ; text=%s", i, item.get("href")[:180], (item.get("text") or "")[:140])

            # filter anchors tuned for KDramaStars
            candidates = []
            seen = set()
            for item in pairs:
                href = item.get("href") or ""
                text = (item.get("text") or "").strip()
                if not href or not text or len(text) < 8:
                    continue
                if looks_like_kds_article(href):
                    # normalize absolute url
                    href_abs = href
                    if href_abs not in seen:
                        seen.add(href_abs)
                        candidates.append({"href": href_abs, "title": text})

            logger.info("Filtered %d post-like candidates on listing %s", len(candidates), current)

            # If we found zero candidates, also try regex scanning of anchors (fallback)
            if not candidates:
                logger.info("No candidates found by heuristics — trying regex fallback")
                raw_html = content
                hrefs = re.findall(r'href=["\']([^"\']+)["\']', raw_html, re.I)
                for href in hrefs:
                    full = href if href.startswith("http") else urljoin(current, href)
                    if looks_like_kds_article(full) and full not in seen:
                        seen.add(full)
                        candidates.append({"href": full, "title": ""})
                logger.info("Regex fallback found %d candidates", len(candidates))

            # Visit each candidate
            for cand in candidates:
                href = cand["href"]
                logger.info("Visiting article: %s", href)
                try:
                    await page.goto(href, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                    await page.wait_for_timeout(800 + random.randint(0,1200))
                    art_html = await page.content()
                except Exception as e:
                    logger.warning("Failed to load article %s: %s", href, e)
                    continue

                # parse article
                title_guess = cand.get("title") or ""
                title_from_page = ""
                try:
                    soup = BeautifulSoup(art_html, "html.parser")
                    h1 = soup.find("h1")
                    if h1:
                        title_from_page = h1.get_text(" ", strip=True)
                except Exception:
                    pass
                title = title_from_page or title_guess or href

                body = extract_main_text(art_html)
                if not body or len(body) < 60:
                    logger.info("No significant body extracted; skipping %s", href)
                    continue

                image_url = extract_image(art_html) or ""
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
                if HAVE_REWRITER:
                    try:
                        rew = rewrite_with_gpt_expanded(title, body)
                        title_final = rew.get("header") or title
                        summary_final = rew.get("summary") or body
                    except Exception as e:
                        logger.warning("Rewriter failed for %s: %s", href, e)
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
                    logger.info("Upserted: %s", href)
                else:
                    logger.warning("DB upsert failed for: %s", href)

                # polite delay
                await asyncio.sleep(0.6 + random.random() * 0.9)

            # pagination: try rel=next or "Older Posts"
            soup = BeautifulSoup(content, "html.parser")
            next_link = None
            a_next = soup.find("a", rel="next")
            if a_next and a_next.get("href"):
                next_link = a_next.get("href")
                if not next_link.startswith("http"):
                    next_link = urljoin(current, next_link)
            else:
                # search for "Older Posts" text
                for a in soup.find_all("a", href=True):
                    txt = (a.get_text(" ", strip=True) or "").lower()
                    if "older posts" in txt or txt == "older posts" or txt == "older":
                        next_link = a.get("href")
                        if next_link and not next_link.startswith("http"):
                            next_link = urljoin(current, next_link)
                        break

            current = next_link
            pages_scraped += 1
            await asyncio.sleep(1 + random.random() * 1.5)

        await browser.close()
    return results

# ---- Wrapper to process all categories ----
async def run_all():
    all_results = []
    for listing_url, mapped_category in CATEGORY_MAP.items():
        logger.info(f"=== Starting scrape for category: {mapped_category} ===")
        res = await scrape_category_with_debug(listing_url, mapped_category)
        all_results.extend(res)
    return all_results


def main():
    results = asyncio.run(run_all())
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)
    print(json.dumps(results, ensure_ascii=False))


if __name__ == "__main__":
    main()

