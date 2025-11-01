#!/usr/bin/env python3
"""
kennews_scraper.py

Scrapes k-ennews listing pages and upserts into MySQL `articles`.
Robust anchor extraction for k-ennews (href, onclick, data-idxno, parent nodes).
"""

import os
import asyncio
import json
import logging
import random
import re
import uuid as uuidlib
from urllib.parse import urlparse, urljoin, parse_qs, urlencode, urlunparse
from datetime import datetime

import pytz
from dateutil import parser as dateparser
from dotenv import load_dotenv
from bs4 import BeautifulSoup

import mysql.connector
from mysql.connector import pooling

from playwright.async_api import async_playwright

# ----------------- Config -----------------
load_dotenv()

LISTINGS = {
    "https://www.k-ennews.com/news/articleList.html?sc_section_code=S1N1&view_type=tm": "kpop",
    "https://www.k-ennews.com/news/articleList.html?sc_section_code=S1N2&view_type=tm": "kdrama",
    "https://www.k-ennews.com/news/articleList.html?sc_section_code=S1N3&view_type=tm": "kpop_celeb",
    "https://www.k-ennews.com/news/articleList.html?sc_section_code=S1N4&view_type=tm": "news"
}

MAX_PAGES = int(os.getenv("MAX_PAGES", "2"))
TIMEZONE = pytz.timezone(os.getenv("TIMEZONE", "Asia/Kolkata"))
OUTPUT_JSON = os.getenv("OUTPUT_JSON", "kennews_articles.json")

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
NAV_TIMEOUT = int(os.getenv("NAV_TIMEOUT", "30000"))

TITLE_BLACKLIST_WORDS = {"about", "privacy", "terms", "contact", "advertise", "policy"}
DOMAIN_HOSTS = ("www.k-ennews.com", "k-ennews.com")

# Logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s:%(name)s: %(message)s")
logger = logging.getLogger("kennews")

# ---------- Validate DB config ----------
if not all([DB_USER, DB_PASS, DB_HOST, DB_NAME]):
    raise SystemExit("Missing DB configuration in .env (DB_USER, DB_PASS, DB_HOST, DB_NAME required)")

pool_args = {
    "user": DB_USER,
    "password": DB_PASS,
    "host": DB_HOST,
    "port": DB_PORT,
    "database": DB_NAME,
    "pool_name": "kennews_pool",
    "pool_size": int(os.getenv("DB_POOL_SIZE", "5")),
    "autocommit": False
}
if DB_SSL_MODE in ("REQUIRED", "PREFERRED") and DB_SSL_CA:
    pool_args["ssl_ca"] = DB_SSL_CA

try:
    db_pool = pooling.MySQLConnectionPool(**pool_args)
except Exception as e:
    logger.exception("Failed to create DB pool: %s", e)
    raise

# ---------- UPSERT SQL (LAST_INSERT_ID trick) ----------
UPSERT_SQL = """
INSERT INTO articles
  (category, title, link, summary, image_url, author, published, created_at, views, is_featured, featured_rank, last_metrics_update, trend_score, uuid)
VALUES
  (%s,%s,%s,%s,%s,%s,%s,NOW(),%s,%s,%s,%s,%s,%s)
ON DUPLICATE KEY UPDATE
  id = LAST_INSERT_ID(id),
  title = VALUES(title),
  summary = VALUES(summary),
  image_url = VALUES(image_url),
  author = VALUES(author),
  published = VALUES(published),
  last_metrics_update = NOW()
"""

# ---------- HTML extraction helpers ----------
def extract_main_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    selectors = [
        ("div", "article_txt"),
        ("div", "article_body"),
        ("div", "content"),
        ("article", None),
        ("div", "news_view")
    ]
    for tag, cls in selectors:
        node = soup.find(tag, class_=cls) if cls else soup.find(tag)
        if node:
            for junk in node.find_all(["script", "style", "iframe"]):
                junk.decompose()
            for j in node.find_all(class_=re.compile(r"(share|ads|related|social|wp-block-embed)")):
                j.decompose()
            paragraphs = [p.get_text(" ", strip=True) for p in node.find_all("p")]
            text = " ".join([p for p in paragraphs if p])
            if text and len(text) > 40:
                return text
    paras = soup.find_all("p")
    if paras:
        text = " ".join(p.get_text(" ", strip=True) for p in paras[:20])
        return text.strip()
    return ""

def extract_main_image(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    meta_og = soup.find("meta", property="og:image")
    if meta_og and meta_og.get("content"):
        return meta_og.get("content")
    for sel in ("article", "div", "figure"):
        node = soup.find(sel)
        if node:
            img = node.find("img")
            if img and img.get("src"):
                return img.get("src")
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
    possible = soup.find(lambda t: t.name in ("div","span") and t.get_text() and re.search(r"\b(20\d{2}|\d{1,2}\s+\w+\s+20\d{2})\b", t.get_text()))
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
    sel = soup.find(lambda t: t.name in ("span","div","a") and t.get("class") and any("author" in c or "byline" in c for c in t.get("class")))
    if sel:
        return sel.get_text(" ", strip=True)
    byline = soup.find(string=re.compile(r"^\s*By\s+", re.I))
    if byline:
        return re.sub(r"^\s*By\s+", "", byline.strip(), flags=re.I)
    return ""

# ---------- Helper: build page URL by adding page query param ----------
def build_page_url(listing_url: str, page_num: int) -> str:
    if page_num <= 1:
        return listing_url
    parsed = urlparse(listing_url)
    qs = parse_qs(parsed.query, keep_blank_values=True)
    qs["page"] = [str(page_num)]
    new_query = urlencode(qs, doseq=True)
    new = parsed._replace(query=new_query)
    return urlunparse(new)

# ---------- DB upsert (single-connection, last_insert_id, counts) ----------
def db_upsert(record: dict) -> bool:
    conn = None
    link = record.get("link")
    category = record.get("category")
    logger.info("🟦 Entering db_upsert for link: %s", link)

    try:
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
            record.get("uuid_bytes"),
        )

        logger.debug("Prepared SQL params for link=%s | title_len=%d summary_len=%d",
                     link, len(record.get("title") or ""), len(record.get("summary") or ""))

        conn = db_pool.get_connection()

        # Optional pre-check to know if link existed before
        existed_before = None
        try:
            pre_cur = conn.cursor(buffered=True)
            pre_cur.execute("SELECT id FROM articles WHERE link = %s LIMIT 1", (link,))
            existed_before = bool(pre_cur.fetchone())
            pre_cur.close()
            logger.debug("Pre-upsert existed_before=%s for %s", existed_before, link)
        except Exception:
            existed_before = None
            logger.debug("Pre-upsert existence check failed/skipped for %s", link)

        cur = conn.cursor()
        cur.execute(UPSERT_SQL, params)

        try:
            rc = cur.rowcount
        except Exception:
            rc = None
        try:
            inserted_id = cur.lastrowid
        except Exception:
            inserted_id = None

        conn.commit()
        logger.info("✅ DB COMMIT successful for link: %s", link)
        logger.info("SQL executed for %s | cursor.rowcount=%s lastrowid=%s", link, rc, inserted_id)

        if rc == 1:
            logger.info("🎉 INSERTED new record id=%s for %s", inserted_id, link)
        elif rc == 2:
            logger.info("🔁 UPDATED existing record id=%s for %s", inserted_id, link)
        else:
            logger.info("ℹ️ Upsert outcome ambiguous (rowcount=%s) for %s (existed_before=%s)", rc, link, existed_before)

        try:
            cur.close()
        except Exception:
            pass

        # Query authoritative counts on same connection (buffered cursor)
        try:
            cur_counts = conn.cursor(buffered=True)
            cur_counts.execute("SELECT COUNT(*) FROM articles")
            total_rows = cur_counts.fetchone()[0] if cur_counts.rowcount != 0 else 0

            cur_counts.execute("SELECT COUNT(*) FROM articles WHERE category = %s", (category,))
            category_rows = cur_counts.fetchone()[0] if cur_counts.rowcount != 0 else 0

            logger.info("📊 DB counts after upsert: total=%d, category(%s)=%d", total_rows, category, category_rows)
            cur_counts.close()
        except Exception as cnt_err:
            logger.exception("Failed to fetch counts after upsert for %s: %s", link, cnt_err)

        return True

    except Exception as e:
        logger.error("❌ Exception during DB upsert for %s: %s", link, e, exc_info=True)

        try:
            rec_copy = dict(record)
            if rec_copy.get("uuid_bytes"):
                try:
                    rec_copy["uuid"] = uuidlib.UUID(bytes=rec_copy["uuid_bytes"]).hex
                except Exception:
                    rec_copy["uuid"] = None
                rec_copy.pop("uuid_bytes", None)
            for k in ("summary", "title"):
                if rec_copy.get(k) and len(rec_copy[k]) > 3000:
                    rec_copy[k] = rec_copy[k][:3000] + "...(truncated)"
            failed_file = "failed_knenews_records.json"
            arr = []
            if os.path.exists(failed_file):
                try:
                    with open(failed_file, "r", encoding="utf-8") as fh:
                        arr = json.load(fh)
                except Exception:
                    arr = []
            arr.append({"link": link, "error": str(e), "record": rec_copy, "ts": datetime.now().isoformat()})
            with open(failed_file, "w", encoding="utf-8") as fh:
                json.dump(arr, fh, ensure_ascii=False, indent=2)
            logger.warning("💾 Saved failing record to %s", failed_file)
        except Exception as ex2:
            logger.exception("Failed to save failing record for %s: %s", link, ex2)

        if conn:
            try:
                conn.rollback()
                logger.warning("🔄 DB ROLLBACK executed for %s", link)
            except Exception:
                logger.warning("⚠️ Rollback failed for %s", link)
        return False

    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                logger.warning("⚠️ Failed to close DB connection for %s", link)
        logger.info("⬜ Exiting db_upsert for link: %s", link)

# ---------- Main scraping loop ----------
async def scrape_all_listings():
    results_db = []
    results_for_json = []

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True)
        page = await browser.new_page(user_agent=USER_AGENT)

        for listing_url, mapped_cat in LISTINGS.items():
            logger.info("Starting listing: %s -> %s", listing_url, mapped_cat)
            for page_num in range(1, MAX_PAGES + 1):
                url = build_page_url(listing_url, page_num)
                logger.info("Loading listing page: %s", url)
                try:
                    await page.goto(url, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                    await page.wait_for_timeout(800 + random.randint(0,1200))
                except Exception as e:
                    logger.warning("Failed to load listing %s: %s", url, e)
                    continue

                # --- Robust anchor extraction (JS side) for k-ennews ---
                pairs = await page.evaluate("""() => {
    const makeAbs = (href) => {
        try { return new URL(href, window.location.href).href; } catch (e) { return href; }
    };
    const arr = [];
    const seen = new Set();

    // anchors with articleView/idxno in href
    document.querySelectorAll('a[href]').forEach(a => {
        try {
            const href = (a.getAttribute('href') || '').trim();
            const text = (a.innerText || a.getAttribute('aria-label') || '').trim();
            if (!href) return;
            const low = href.toLowerCase();
            if (low.includes('articleview') || low.includes('idxno') || /idxno=\\d+/.test(href)) {
                const full = makeAbs(href);
                if (!seen.has(full)) { seen.add(full); arr.push({href: full, text: text}); }
            }
        } catch (e) {}
    });

    // anchors with onclick containing an idxno
    document.querySelectorAll('a[onclick]').forEach(a => {
        try {
            const onclick = a.getAttribute('onclick') || '';
            const m = onclick.match(/idxno\\s*=?\\s*(\\d{3,})/i) || onclick.match(/(\\d{3,})/);
            if (m) {
                const idx = m[1];
                const url = new URL('/news/articleView.html?idxno=' + idx, window.location.origin).href;
                const text = (a.innerText || a.getAttribute('aria-label') || '').trim();
                if (!seen.has(url)) { seen.add(url); arr.push({href: url, text: text}); }
            }
        } catch (e) {}
    });

    // elements with data-idxno/data-id
    document.querySelectorAll('[data-idxno],[data-id]').forEach(el => {
        try {
            const idx = el.getAttribute('data-idxno') || el.getAttribute('data-id');
            if (idx && /\\d{3,}/.test(idx)) {
                const url = new URL('/news/articleView.html?idxno=' + idx, window.location.origin).href;
                let text = '';
                const a = el.querySelector && el.querySelector('a[href]');
                if (a) text = (a.innerText || a.getAttribute('aria-label') || '').trim();
                if (!text) {
                    const h = el.querySelector && el.querySelector('h1,h2,h3,h4,h5');
                    if (h) text = (h.innerText || '').trim();
                }
                if (!text) text = (el.innerText || '').trim().slice(0, 120);
                if (!seen.has(url)) { seen.add(url); arr.push({href: url, text: text}); }
            }
        } catch (e) {}
    });

    // fallback: list items / article blocks
    document.querySelectorAll('li, .list_item, .news_list, .article').forEach(node => {
        try {
            if (!node) return;
            const a = node.querySelector('a[href*="articleView"], a[href*="idxno"], a[href*="/news/"]');
            if (a) {
                const href = a.href || a.getAttribute('href') || '';
                const full = makeAbs(href);
                const text = (a.innerText || a.getAttribute('aria-label') || '').trim();
                if (full && !seen.has(full)) { seen.add(full); arr.push({href: full, text: text}); }
            } else {
                const html = node.innerHTML || '';
                const m = html.match(/idxno\\s*[:=]\\s*(\\d{3,})/) || html.match(/idxno=(\\d{3,})/);
                if (m) {
                    const idx = m[1];
                    const url = new URL('/news/articleView.html?idxno=' + idx, window.location.origin).href;
                    const titleNode = node.querySelector && (node.querySelector('h2') || node.querySelector('h3') || node.querySelector('a'));
                    const text = titleNode ? (titleNode.innerText || '').trim() : (node.innerText || '').trim().slice(0,120);
                    if (!seen.has(url)) { seen.add(url); arr.push({href: url, text: text}); }
                }
            }
        } catch (e) {}
    });

    return arr;
}""")

                # --- Python-side filtering/dedupe ---
                seen_links = set()
                candidates = []
                for pitem in pairs:
                    href = (pitem.get("href") or "").strip()
                    text = (pitem.get("text") or "").strip()
                    if not href or len(href) < 10:
                        continue
                    href = href.split('#')[0].strip()
                    if 'articleview' not in href.lower() and 'idxno=' not in href.lower():
                        continue
                    if href.lower().endswith('articlelist.html') or href.lower().endswith('articleview.html'):
                        if 'idxno=' not in href.lower():
                            continue
                    if href in seen_links:
                        continue
                    seen_links.add(href)
                    if not text:
                        text = href.split('/')[-1]
                    candidates.append({"href": href, "text": text})

                logger.info("Found %d candidate article links on listing page %s", len(candidates), url)
                # logger.debug("RAW CANDIDATES: %s", candidates[:40])

                # Visit each article candidate
                for cand in candidates:
                    href = cand["href"]
                    title = cand["text"] or ""
                    logger.info("Visiting article: %s", href)
                    try:
                        await page.goto(href, timeout=NAV_TIMEOUT, wait_until="domcontentloaded")
                        await page.wait_for_timeout(700 + random.randint(0,1100))
                        art_html = await page.content()
                    except Exception as e:
                        logger.warning("Failed to load article %s: %s", href, e)
                        continue

                    body = extract_main_text(art_html)
                    if not body or len(body) < 40:
                        logger.info("Article body missing/too short; skipping %s", href)
                        continue
                    image = extract_main_image(art_html) or ""
                    author = extract_author(art_html) or ""
                    dt = extract_published_date(art_html)
                    if not dt:
                        logger.info("No published date; skipping %s", href)
                        continue
                    if dt.tzinfo is None:
                        dt = pytz.UTC.localize(dt)
                    local_dt = dt.astimezone(TIMEZONE)
                    published_iso = local_dt.isoformat()

                    category = mapped_cat or "news"

                    rec_db = {
                        "category": category,
                        "title": title,
                        "link": href,
                        "summary": body,
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

                    rec_json = dict(rec_db)
                    try:
                        rec_json["uuid"] = uuidlib.UUID(bytes=rec_json["uuid_bytes"]).hex
                    except Exception:
                        rec_json["uuid"] = uuidlib.uuid4().hex
                    rec_json.pop("uuid_bytes", None)

                    ok = db_upsert(rec_db)
                    if ok:
                        logger.info("Upserted to DB (%s): %s", category, href)
                    else:
                        logger.warning("DB upsert failed for: %s", href)

                    results_db.append(rec_db)
                    results_for_json.append(rec_json)

                    await asyncio.sleep(0.5 + random.random() * 1.2)

                await asyncio.sleep(0.8 + random.random() * 1.2)

        await browser.close()

    # Merge JSON with existing file (backup broken files)
    merged = {}
    existing = []
    if os.path.exists(OUTPUT_JSON):
        try:
            with open(OUTPUT_JSON, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except json.JSONDecodeError as e:
            logger.exception("Existing JSON malformed: %s — backing up", e)
            try:
                ts = datetime.now().strftime("%Y%m%d%H%M%S")
                bak = f"{OUTPUT_JSON}.broken.{ts}"
                os.replace(OUTPUT_JSON, bak)
                logger.info("Backed up corrupted JSON to %s", bak)
            except Exception as ex:
                logger.exception("Failed to backup corrupted JSON: %s", ex)
            existing = []
        except Exception as e:
            logger.exception("Failed to load existing JSON: %s", e)
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

    return results_db

def main():
    results = asyncio.run(scrape_all_listings())
    print(f"DB upsert attempts in this run: {len(results)}")

if __name__ == "__main__":
    main()
