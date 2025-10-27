import os
import json
import time
import requests
import logging
import mysql.connector
from dotenv import load_dotenv

# ============= Setup Logging =============
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("translate_articles.log", encoding="utf-8"),
        logging.StreamHandler()
    ]
)

# ============= Load Env =============
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "mokshiridb")
DB_TABLE = os.getenv("DB_TABLE", "articles")

LECTO_API_KEY = os.getenv("LECTO_API_KEY")
LECTO_URL = "https://api.lecto.ai/v1/translate/text"

# ============= Translation Targets =============
TARGET_LANGUAGES = ["ko", "ja", "es", "id", "vi"]
BATCH_SIZE = 2  # translate 2 languages at a time

# ============= Database Helpers =============
def db_connect():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME
    )

def fetch_articles():
    """Fetch all unpublished English articles."""
    conn = db_connect()
    cur = conn.cursor(dictionary=True)
    query = f"SELECT * FROM {DB_TABLE} WHERE lang = 'en' AND is_published = 0"
    cur.execute(query)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows

def insert_translated_article(original, lang, translated_title, translated_summary):
    """Insert translated article into DB as unpublished."""
    conn = db_connect()
    cur = conn.cursor()
    sql = f"""
        INSERT INTO {DB_TABLE}
        (category, title, link, summary, image_url, lang, is_published, image_name, author, published,
         created_at, views, is_featured, featured_rank, last_metrics_update, trend_score, uuid)
        VALUES (%s, %s, %s, %s, %s, %s, 0, %s, %s, %s, %s, %s, %s, %s, %s, %s, UUID_TO_BIN(UUID()))
    """
    values = (
        original.get("category"),
        translated_title,
        original.get("link"),
        translated_summary,
        original.get("image_url"),
        lang,
        original.get("image_name"),
        original.get("author"),
        original.get("published"),
        original.get("created_at"),
        original.get("views", 0),
        original.get("is_featured", 0),
        original.get("featured_rank"),
        original.get("last_metrics_update"),
        original.get("trend_score", 0.0),
    )
    try:
        cur.execute(sql, values)
        conn.commit()
        inserted = cur.rowcount
    except mysql.connector.Error as err:
        logging.error(f"❌ MySQL insert failed for lang={lang}: {err}")
        inserted = 0
    finally:
        cur.close()
        conn.close()
    return inserted

def mark_articles_as_published():
    """After translation, mark all English articles as published."""
    try:
        conn = db_connect()
        cur = conn.cursor()
        update_query = f"UPDATE {DB_TABLE} SET is_published = 1 WHERE lang = 'en' AND is_published = 0"
        cur.execute(update_query)
        conn.commit()
        updated_rows = cur.rowcount
        logging.info(f"✅ Marked {updated_rows} English articles as published.")
        cur.close()
        conn.close()
    except Exception as e:
        logging.error(f"❌ Failed to update publication status: {e}")

# ============= Translation API Call =============
def translate_texts(texts, target_langs, from_lang="en"):
    """Translate multiple target languages in one call."""
    headers = {
        "X-API-Key": LECTO_API_KEY,
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    payload = {"texts": texts, "to": target_langs, "from": from_lang}
    try:
        response = requests.post(LECTO_URL, headers=headers, data=json.dumps(payload))
        response.raise_for_status()
        result = response.json()
        translations = result.get("translations") or result.get("data") or []
        if not translations:
            logging.warning(f"⚠️ Empty translation response: {result}")
        return translations
    except Exception as e:
        logging.error(f"Translation API error: {e}")
        return []

# ============= Main Logic =============
def process_articles():
    articles = fetch_articles()
    if not articles:
        logging.info("No unpublished English articles found.")
        return

    logging.info(f"Found {len(articles)} unpublished English articles to translate.")

    for article in articles:
        article_id = article["id"]
        title = article.get("title") or ""
        summary = article.get("summary") or ""

        if not title.strip():
            logging.warning(f"Skipping article ID {article_id}: empty title.")
            continue

        logging.info(f"\n🌐 Translating Article ID {article_id}: {title[:60]}...")

        for i in range(0, len(TARGET_LANGUAGES), BATCH_SIZE):
            lang_batch = TARGET_LANGUAGES[i:i + BATCH_SIZE]
            logging.info(f"Requesting translation for {lang_batch}")

            translations = translate_texts([title, summary], lang_batch)
            if not translations:
                logging.warning(f"❌ No translations returned for {lang_batch}. Skipping.")
                continue

            for tr in translations:
                lang = tr.get("to")
                translated_texts = tr.get("translated") or tr.get("texts") or []

                if not translated_texts:
                    logging.warning(f"⚠️ No translated texts for lang={lang}. Response keys: {list(tr.keys())}")
                    continue

                translated_title = translated_texts[0] if len(translated_texts) > 0 else title
                translated_summary = translated_texts[1] if len(translated_texts) > 1 else summary

                logging.info(f"✅ Received {lang} → title='{translated_title[:40]}...'")

                inserted = insert_translated_article(article, lang, translated_title, translated_summary)
                if inserted:
                    logging.info(f"✅ Inserted translation for {lang} (Article ID {article_id})")
                else:
                    logging.warning(f"⚠️ No row inserted for {lang} (Article ID {article_id})")

            # polite delay per batch
            time.sleep(2)

    logging.info("🎯 Translation batch completed.")

    # Mark all English articles as published
    mark_articles_as_published()

# ============= Entry Point =============
if __name__ == "__main__":
    process_articles()
