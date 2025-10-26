#!/usr/bin/env python3
"""
update_articles_image_and_translate.py
-------------------------------------
Native MySQL Connector version (no SQLAlchemy).

- Reads .env for DB connection (DB_USER, DB_PASS, DB_HOST, DB_PORT, DB_NAME, DB_SSL_MODE, DB_SSL_CA)
- Sanitizes 'title' into image_name
- Downloads images from image_url
- Updates image_name column
- Uses Lecto API to translate title & summary into multiple languages (2 at a time)
- Inserts translated records into the same 'articles' table
"""

import os
import re
import uuid
import time
import shutil
import logging
import requests
import mysql.connector
from urllib.parse import urlparse
from dotenv import load_dotenv
from tqdm import tqdm

# -------------------------
# Load .env config
# -------------------------
load_dotenv()

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_NAME = os.getenv("DB_NAME")
DB_SSL_MODE = os.getenv("DB_SSL_MODE", "DISABLED").upper()
DB_SSL_CA = os.getenv("DB_SSL_CA")

IMAGE_OUTPUT_DIR = os.getenv("IMAGE_OUTPUT_DIR", r"D:\projects\mok_final\mokshiri\ui\src\assets\images\articles")
LECTO_API_KEY = os.getenv("LECTO_API_KEY")
LECTO_ENDPOINT = os.getenv("LECTO_API_URL", "https://api.lecto.ai/v1/translate/text")
LANGUAGES = [l.strip() for l in os.getenv("LANGUAGES", "en,ko,hi,id,es,ja,th,vi,fr,ar").split(",") if l.strip()]
DOWNLOAD_RETRIES = int(os.getenv("DOWNLOAD_RETRIES", "3"))
LECTO_RETRIES = int(os.getenv("LECTO_RETRIES", "3"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))

# Ensure image dir exists
os.makedirs(IMAGE_OUTPUT_DIR, exist_ok=True)

# Logging setup
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("mokshiri")

# -------------------------
# DB Connection
# -------------------------
def get_db_connection():
    ssl_config = None
    if DB_SSL_MODE in ("REQUIRED", "PREFERRED") and DB_SSL_CA and os.path.isfile(DB_SSL_CA):
        ssl_config = {"ca": DB_SSL_CA}
        logger.info(f"✅ Using SSL with CA: {DB_SSL_CA}")
    else:
        logger.info("⚙️ SSL not enforced or CA not found, connecting without SSL")

    try:
        conn = mysql.connector.connect(
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            ssl_ca=ssl_config["ca"] if ssl_config else None,
            autocommit=False,
        )
        logger.info("✅ Connected to MySQL successfully.")
        return conn
    except Exception as e:
        logger.error(f"❌ MySQL connection failed: {e}")
        raise

# -------------------------
# Utility Functions
# -------------------------
def sanitize_title_for_filename(title: str) -> str:
    """Lowercase, remove special chars, replace spaces with hyphens."""
    if not title:
        return uuid.uuid4().hex[:8]
    s = title.strip().lower()
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"[^\w\-]", "", s, flags=re.UNICODE)
    s = re.sub(r"-{2,}", "-", s).strip("-")
    return s or uuid.uuid4().hex[:8]

def get_extension_from_url(url: str) -> str:
    parsed = urlparse(url)
    _, ext = os.path.splitext(parsed.path)
    if ext:
        return ext
    # fallback by checking content-type
    try:
        r = requests.head(url, timeout=REQUEST_TIMEOUT)
        ctype = r.headers.get("Content-Type", "")
        if "jpeg" in ctype:
            return ".jpg"
        if "png" in ctype:
            return ".png"
        if "webp" in ctype:
            return ".webp"
    except Exception:
        pass
    return ".jpg"

def download_file(url: str, dest_path: str) -> bool:
    headers = {"User-Agent": "MokshiriBot/1.0"}
    for attempt in range(1, DOWNLOAD_RETRIES + 1):
        try:
            with requests.get(url, headers=headers, stream=True, timeout=REQUEST_TIMEOUT) as r:
                r.raise_for_status()
                with open(dest_path, "wb") as f:
                    shutil.copyfileobj(r.raw, f)
            return True
        except Exception as e:
            logger.warning(f"Download attempt {attempt} failed for {url}: {e}")
            time.sleep(1 + attempt)
    if os.path.exists(dest_path):
        os.remove(dest_path)
    return False

# -------------------------
# Lecto API Translation
# -------------------------
def lecto_translate_batch(texts, target_langs, source_lang="en"):
    if not LECTO_API_KEY:
        raise RuntimeError("LECTO_API_KEY not set")
    headers = {
        "X-API-Key": LECTO_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {"texts": texts, "to": target_langs, "from": source_lang}
    for attempt in range(1, LECTO_RETRIES + 1):
        try:
            r = requests.post(LECTO_ENDPOINT, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            return data.get("translations", {})
        except Exception as e:
            logger.warning(f"Lecto attempt {attempt} failed for {target_langs}: {e}")
            time.sleep(1 + attempt)
    logger.error(f"Lecto failed for languages {target_langs}")
    return {}

# -------------------------
# Core Logic
# -------------------------
def process_articles(limit=None):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    query = "SELECT * FROM articles WHERE image_url IS NOT NULL"
    if limit:
        query += f" LIMIT {limit}"
    cursor.execute(query)
    articles = cursor.fetchall()
    logger.info(f"Found {len(articles)} articles to process.")

    for article in tqdm(articles, desc="Processing articles"):
        article_id = article["id"]
        title = article.get("title", "")
        summary = article.get("summary", "")
        image_url = article.get("image_url", "")
        source_lang = article.get("lang", "en")

        # Generate new image name
        sanitized = sanitize_title_for_filename(title)
        ext = get_extension_from_url(image_url)
        new_filename = f"{sanitized}{ext}"
        dest_path = os.path.join(IMAGE_OUTPUT_DIR, new_filename)

        # Download image if missing
        if not os.path.exists(dest_path):
            ok = download_file(image_url, dest_path)
            if not ok:
                logger.warning(f"Skipping article {article_id} - image download failed.")
                continue

        # Update DB
        try:
            cursor.execute("UPDATE articles SET image_name=%s WHERE id=%s", (new_filename, article_id))
            conn.commit()
            logger.info(f"✅ Updated image_name for article {article_id}: {new_filename}")
        except Exception as e:
            logger.error(f"DB update failed for article {article_id}: {e}")
            conn.rollback()
            continue

        # Create translations (2 at a time)
        target_langs = [l for l in LANGUAGES if l != source_lang]
        for i in range(0, len(target_langs), 2):
            pair = target_langs[i:i+2]
            logger.info(f"Translating article {article_id} -> {pair}")
            translations = lecto_translate_batch([title, summary], pair, source_lang)
            for lang in pair:
                if lang not in translations:
                    continue
                t_list = translations[lang]
                title_t = t_list[0] if len(t_list) > 0 else title
                summary_t = t_list[1] if len(t_list) > 1 else summary
                try:
                    insert_sql = """
                    INSERT INTO articles (title, summary, image_name, image_url, lang, is_published)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    """
                    cursor.execute(insert_sql, (title_t, summary_t, new_filename, image_url, lang, 0))
                    conn.commit()
                    logger.info(f"Inserted translated record (lang={lang}) for article {article_id}")
                except Exception as e:
                    logger.error(f"Failed to insert translation for lang {lang}: {e}")
                    conn.rollback()

    cursor.close()
    conn.close()
    logger.info("✅ Processing completed.")

# -------------------------
# Entry Point
# -------------------------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Update image_name, download images, and translate articles using Lecto.")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of articles to process")
    args = parser.parse_args()
    process_articles(limit=args.limit)
