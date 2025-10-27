"""
batch_watermark_r2.py

Batch-process article images:
 - read rows from `articles` table
 - for each row with image_url and empty image_name, stream image, watermark, upload to Cloudflare R2
 - update image_name column with the full R2 URL

Dependencies:
  pip install boto3 pillow requests mysql-connector-python python-dotenv
"""

import os
import io
import uuid
import time
import logging
from urllib.parse import urljoin

import boto3
import requests
from PIL import Image, ImageDraw, ImageFont
import mysql.connector
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# -------------------------
# Configuration (from .env)
# -------------------------
R2_ENDPOINT = os.getenv("R2_ENDPOINT")  # e.g. https://<accountid>.r2.cloudflarestorage.com
R2_ACCESS_KEY_ID = os.getenv("R2_ACCESS_KEY_ID")
R2_SECRET_ACCESS_KEY = os.getenv("R2_SECRET_ACCESS_KEY")
R2_BUCKET = os.getenv("R2_BUCKET", "mokshiri-images")
R2_PUBLIC_BASE = os.getenv("R2_PUBLIC_BASE", "").strip()  # optional

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = int(os.getenv("DB_PORT", "3306"))
DB_USER = os.getenv("DB_USER", "root")
DB_PASS = os.getenv("DB_PASS", "")
DB_NAME = os.getenv("DB_NAME", "mokshiridb")
DB_TABLE = os.getenv("DB_TABLE", "articles")

# Processing filters - change these to suit your needs
# We will process rows where image_url IS NOT NULL/empty and (image_name IS NULL or empty)
SQL_SELECT_PENDING = f"""
SELECT id, image_url
FROM {DB_TABLE}
WHERE image_url IS NOT NULL
  AND image_url != ''
  AND (image_name IS NULL OR image_name = '')
ORDER BY id
LIMIT 500
"""  # limit to batch size to avoid huge single-run loads

# -------------------------
# Setup S3 (R2) client
# -------------------------
s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=R2_ACCESS_KEY_ID,
    aws_secret_access_key=R2_SECRET_ACCESS_KEY,
    region_name="auto",
)

# -------------------------
# Helper functions
# -------------------------
def db_connect():
    return mysql.connector.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME
    )

def fetch_pending_rows(limit_sql=SQL_SELECT_PENDING):
    conn = db_connect()
    cur = conn.cursor()
    cur.execute(limit_sql)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return rows  # list of tuples (id, image_url)

def add_watermark(image_bytes: bytes, text="mokshiri.com") -> bytes:
    """Return JPEG bytes of the watermarked image. Compatible with multiple Pillow versions."""
    image = Image.open(io.BytesIO(image_bytes)).convert("RGBA")
    width, height = image.size

    # Create transparent layer for watermark
    txt_layer = Image.new("RGBA", image.size, (255, 255, 255, 0))
    draw = ImageDraw.Draw(txt_layer)

    # Choose a font size relative to image height; fallback to default if truetype not found
    font_size = max(12, int(height / 25))
    try:
        font = ImageFont.truetype("arial.ttf", font_size)
    except Exception:
        try:
            # sometimes arial not available on linux servers; try DejaVu
            font = ImageFont.truetype("DejaVuSans.ttf", font_size)
        except Exception:
            font = ImageFont.load_default()

    # Robust text size measurement: prefer textbbox, then font.getsize, then fallback
    def measure_text(draw_obj, txt, fnt):
        if hasattr(draw_obj, "textbbox"):
            bbox = draw_obj.textbbox((0, 0), txt, font=fnt)
            w = bbox[2] - bbox[0]
            h = bbox[3] - bbox[1]
            return w, h
        if hasattr(fnt, "getsize"):
            try:
                return fnt.getsize(txt)
            except Exception:
                pass
        # Fallback approximation
        approx_w = int(len(txt) * (getattr(fnt, "size", font_size) * 0.6))
        approx_h = int(getattr(fnt, "size", font_size))
        return approx_w, approx_h

    text_width, text_height = measure_text(draw, text, font)
    margin = int(max(8, height * 0.01))
    x = width - text_width - margin
    y = height - text_height - margin

    # draw a soft shadow then the main text for readability
    shadow_color = (0, 0, 0, 160)
    text_color = (255, 255, 255, 200)
    # Draw multiple tiny offsets for a subtle border
    offsets = [(-1, -1), (1, 1), (-1, 1), (1, -1)]
    for ox, oy in offsets:
        draw.text((x + ox, y + oy), text, font=font, fill=shadow_color)
    draw.text((x, y), text, font=font, fill=text_color)

    watermarked = Image.alpha_composite(image, txt_layer)
    out = io.BytesIO()
    watermarked.convert("RGB").save(out, format="JPEG", quality=90)
    return out.getvalue()

def upload_to_r2(image_bytes: bytes, folder="uploads", filename=None) -> str:
    """Upload bytes to R2 and return public/full path stored in image_name."""
    if filename is None:
        filename = f"{uuid.uuid4().hex}.jpg"
    key = f"{folder.rstrip('/')}/{filename}"
    s3.put_object(Bucket=R2_BUCKET, Key=key, Body=image_bytes, ContentType="image/jpeg")
    if R2_PUBLIC_BASE:
        return urljoin(R2_PUBLIC_BASE.rstrip("/") + "/", key)
    # default constructed URL (works if bucket/object is public or you use CDN)
    return f"{R2_ENDPOINT.rstrip('/')}/{R2_BUCKET}/{key}"

def update_image_name_in_db(record_id: int, image_name_value: str):
    conn = db_connect()
    cur = conn.cursor()
    try:
        cur.execute(f"UPDATE {DB_TABLE} SET image_name = %s WHERE id = %s", (image_name_value, record_id))
        conn.commit()
        logging.info("DB updated id=%s image_name=%s", record_id, image_name_value)
    finally:
        cur.close()
        conn.close()

# -------------------------
# Main processor
# -------------------------
def process_row(record_id: int, image_url: str, retries=2, delay=2):
    logging.info("Processing id=%s url=%s", record_id, image_url)
    # Try to download with retries
    last_err = None
    for attempt in range(1, retries + 2):
        try:
            resp = requests.get(image_url, timeout=15, stream=True)
            resp.raise_for_status()
            content = resp.content
            # small size checks (optional)
            if len(content) < 1000:
                raise ValueError("Downloaded content too small to be a valid image")
            break
        except Exception as exc:
            last_err = exc
            logging.warning("Download attempt %s failed for id=%s: %s", attempt, record_id, exc)
            if attempt <= retries:
                time.sleep(delay)
            else:
                logging.error("Giving up download for id=%s after %s attempts", record_id, attempt)
                return False, f"download_failed: {exc}"

    # Watermark
    try:
        watermarked_bytes = add_watermark(content, text="mokshiri.com")
    except Exception as exc:
        logging.exception("Watermarking failed for id=%s: %s", record_id, exc)
        return False, f"watermark_failed: {exc}"

    # Upload
    try:
        filename = f"{record_id}_{uuid.uuid4().hex}.jpg"
        r2_url = upload_to_r2(watermarked_bytes, folder="uploads", filename=filename)
    except Exception as exc:
        logging.exception("Upload failed for id=%s: %s", record_id, exc)
        return False, f"upload_failed: {exc}"

    # Update DB
    try:
        update_image_name_in_db(record_id, r2_url)
    except Exception as exc:
        logging.exception("DB update failed for id=%s: %s", record_id, exc)
        return False, f"db_update_failed: {exc}"

    logging.info("Completed id=%s -> %s", record_id, r2_url)
    return True, r2_url

def run_batch():
    pending = fetch_pending_rows()
    if not pending:
        logging.info("No pending rows found.")
        return

    logging.info("Found %s pending rows (processing sequentially)", len(pending))
    stats = {"success": 0, "fail": 0}
    for row in pending:
        record_id, image_url = row
        try:
            ok, info = process_row(record_id, image_url)
            if ok:
                stats["success"] += 1
            else:
                stats["fail"] += 1
                logging.error("Record %s failed: %s", record_id, info)
        except Exception as exc:
            stats["fail"] += 1
            logging.exception("Unhandled error processing id=%s: %s", record_id, exc)

    logging.info("Batch finished. success=%s fail=%s", stats["success"], stats["fail"])

# -------------------------
# Entrypoint
# -------------------------
if __name__ == "__main__":
    run_batch()
