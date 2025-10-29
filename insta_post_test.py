#!/usr/bin/env python3
"""
insta_post_r2_mysql.py

Flow:
 - fetch one English article from MySQL (LIMIT 1)
 - download article image -> downloads/
 - overlay title -> processed/
 - process (crop/resize) -> processed/
 - optionally combine with AUDIO_PATH into mp4 via ffmpeg
 - upload processed image/video to Cloudflare R2 (boto3 S3)
 - obtain public or presigned URL, validate it
 - create IG media container and publish
 - cleanup uploaded R2 object(s)

Configure via environment variables or edit top-of-file defaults.
"""

import os
import sys
import time
import json
import shutil
import subprocess
from pathlib import Path
from urllib.parse import urlparse

import requests
from PIL import Image, ImageDraw, ImageFont
import boto3
from botocore.client import Config
import mysql.connector
from mysql.connector import Error
from dotenv import load_dotenv

load_dotenv()

# -------------------------
# CONFIG (via env or edit)
# -------------------------
# MySQL to fetch article
MYSQL_HOST = os.getenv("DB_HOST", "127.0.0.1")
MYSQL_PORT = int(os.getenv("DB_PORT", "3306"))
MYSQL_USER = os.getenv("DB_USER", "root")
MYSQL_PASS = os.getenv("DB_PASS", "")
MYSQL_DB = os.getenv("DB_NAME", "mokshiridb")
ARTICLES_TABLE = os.getenv("ARTICLES_TABLE", "articles")

# Instagram Graph API
ACCESS_TOKEN = os.getenv("PAGE_ACCESS_TOKEN") or os.getenv("IG_LONG_LIVED_TOKEN") or "YOUR_LONG_LIVED_TOKEN"
IG_USER_ID = os.getenv("IG_USER_ID") or "YOUR_IG_BUSINESS_ACCOUNT_ID"

# R2 / S3
R2_ENDPOINT = os.getenv("R2_ENDPOINT") or "https://<account_id>.r2.cloudflarestorage.com"
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY_ID") or os.getenv("R2_ACCESS_KEY") or "YOUR_R2_ACCESS_KEY"
R2_SECRET_KEY = os.getenv("R2_SECRET_ACCESS_KEY") or os.getenv("R2_SECRET_KEY") or "YOUR_R2_SECRET_KEY"
R2_BUCKET = os.getenv("R2_BUCKET") or "your-bucket-name"
R2_PUBLIC_BASE = os.getenv("R2_PUBLIC_BASE") or ""  # e.g. https://pub-xxxxx.r2.dev (optional)

PRESIGNED_EXPIRES_IN = int(os.getenv("PRESIGNED_EXPIRES_IN", "3600"))

# Optional fallback image URL if DB row has none
FALLBACK_SOURCE_IMAGE_URL = os.getenv("SOURCE_IMAGE_URL") or ""

# Optional local audio to combine (must be a file path). If empty, post image.
AUDIO_PATH = os.getenv("AUDIO_PATH", "").strip()

# OpenAI for captions (optional)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()

# Local paths
DOWNLOAD_DIR = Path("downloads"); DOWNLOAD_DIR.mkdir(exist_ok=True)
PROCESSED_DIR = Path("processed"); PROCESSED_DIR.mkdir(exist_ok=True)
TMP_DIR = Path("tmp"); TMP_DIR.mkdir(exist_ok=True)

# Visual / font
FONT_PATH = os.getenv("FONT_PATH", "")  # e.g., "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf"
FONT_SIZE = int(os.getenv("FONT_SIZE", "40"))

# Instagram sizing
MIN_RATIO = 4/5.0
MAX_RATIO = 1.91
MAX_DIM = 1080

API_VER = "v21.0"
GRAPH_BASE = "https://graph.facebook.com"

# -------------------------
# DATABASE: fetch one article
# -------------------------
def get_db_article():
    """
    Fetch one English article (latest) using mysql-connector and LIMIT 1.
    Expects columns: id, title, summary, image_url (optional), lang
    Returns dict or None.
    """
    conn = None
    try:
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASS,
            database=MYSQL_DB,
            connection_timeout=10
        )
        if not conn.is_connected():
            print("MySQL: connection failed")
            return None
        cursor = conn.cursor(dictionary=True)
        sql = f"SELECT id, title, summary, image_url FROM `{ARTICLES_TABLE}` WHERE lang = %s ORDER BY id DESC LIMIT 1"
        cursor.execute(sql, ("en",))
        row = cursor.fetchone()
        cursor.close()
        return row
    except Error as e:
        print("MySQL error:", e)
        return None
    finally:
        if conn is not None and conn.is_connected():
            conn.close()

# -------------------------
# Image helpers
# -------------------------
def download_image_to_file(url: str) -> Path:
    print("Downloading image:", url)
    parsed = urlparse(url)
    fname = Path(parsed.path).name or f"image_{int(time.time())}.jpg"
    local = DOWNLOAD_DIR / fname
    with requests.get(url, stream=True, timeout=30) as r:
        r.raise_for_status()
        with open(local, "wb") as f:
            for chunk in r.iter_content(8192):
                f.write(chunk)
    print("Saved original to:", local)
    return local

def ensure_rgb(img: Image.Image) -> Image.Image:
    if img.mode in ("RGBA","LA"):
        bg = Image.new("RGB", img.size, (255,255,255))
        bg.paste(img, mask=img.split()[-1])
        return bg
    if img.mode != "RGB":
        return img.convert("RGB")
    return img

def center_crop_to_ratio(img: Image.Image, target_ratio: float) -> Image.Image:
    w,h = img.size
    current = w / h
    if abs(current - target_ratio) < 1e-6:
        return img
    if current > target_ratio:
        new_w = int(target_ratio * h)
        left = (w - new_w) // 2
        return img.crop((left, 0, left+new_w, h))
    else:
        new_h = int(w / target_ratio)
        top = (h - new_h)//2
        return img.crop((0, top, w, top+new_h))

def clamp_to_instagram_ratio(img: Image.Image) -> Image.Image:
    w,h = img.size
    ratio = w / h
    if MIN_RATIO <= ratio <= MAX_RATIO:
        return img
    target = MIN_RATIO if ratio < MIN_RATIO else MAX_RATIO
    return center_crop_to_ratio(img, target)

def resize_for_instagram(img: Image.Image, max_dim=MAX_DIM) -> Image.Image:
    w,h = img.size
    if max(w,h) <= max_dim:
        return img
    if w >= h:
        new_w = max_dim
        new_h = int(h * (max_dim / w))
    else:
        new_h = max_dim
        new_w = int(w * (max_dim / h))
    return img.resize((new_w, new_h), Image.LANCZOS)

from PIL import Image, ImageDraw, ImageFont

def overlay_title_on_image(img_path: Path, title: str, palette=None) -> Path:
    """
    Bigger, high-contrast, colorful title overlay.
    - font size tries to be large (up to ~12% of width) and only shrinks if too many lines.
    - draws soft shadow + stroke outline to improve readability.
    - increases bottom band height to fit large text.
    """
    if palette is None:
        palette = [
            (255, 99, 71),    # tomato
            (255, 165, 0),    # orange
            (60, 179, 113),   # mediumseagreen
            (30, 144, 255),   # dodgerblue
            (148, 0, 211),    # darkviolet
        ]

    img = Image.open(img_path)
    img = ensure_rgb(img)
    w, h = img.size

    # target large font: 12% of width, clamped
    target_font_size = int(w * 0.12)
    target_font_size = max(32, min(220, target_font_size))

    # try to load font
    def load_font(size):
        try:
            if FONT_PATH:
                return ImageFont.truetype(FONT_PATH, size)
            return ImageFont.truetype("DejaVuSans-Bold.ttf", size)
        except Exception:
            return ImageFont.load_default()

    # initial font
    font = load_font(target_font_size)
    draw = ImageDraw.Draw(img)

    # wrap text to fit ~90% width
    max_text_w = int(w * 0.9)
    words = title.strip().split()
    lines = []
    cur = ""
    for word in words:
        test = (cur + " " + word).strip()
        tw, th = draw.textsize(test, font=font)
        if tw <= max_text_w:
            cur = test
        else:
            if cur:
                lines.append(cur)
            cur = word
    if cur:
        lines.append(cur)

    # If too many lines (>=4), reduce font gradually until <=3 lines or font gets small
    attempts = 0
    while len(lines) > 3 and target_font_size > 28 and attempts < 6:
        attempts += 1
        target_font_size = int(target_font_size * 0.75)
        font = load_font(target_font_size)
        # recompute lines
        draw = ImageDraw.Draw(img)
        lines = []
        cur = ""
        for word in title.strip().split():
            test = (cur + " " + word).strip()
            tw, th = draw.textsize(test, font=font)
            if tw <= max_text_w:
                cur = test
            else:
                if cur:
                    lines.append(cur)
                cur = word
        if cur:
            lines.append(cur)

    # compute band height based on lines and font metrics
    line_heights = [draw.textsize(line, font=font)[1] for line in lines]
    total_text_h = sum(line_heights) + (len(lines)-1)*12
    band_h = max(int(h * 0.20), total_text_h + 60)  # bigger band to fit large text comfortably
    band_y = h - band_h

    # assemble RGBA base
    base = img.convert("RGBA")
    band = Image.new("RGBA", (w, band_h), (0,0,0,160))
    base.paste(band, (0, band_y), band)
    draw = ImageDraw.Draw(base)

    # start y
    y = band_y + (band_h - total_text_h)//2

    # paint each line with shadow + outline + colorful fill
    for idx, line in enumerate(lines):
        tw, th = draw.textsize(line, font=font)
        x = (w - tw)//2

        fill_color = palette[idx % len(palette)]
        stroke_color = (0,0,0)
        stroke_w = max(2, int(target_font_size * 0.04))
        shadow_offset = max(3, int(target_font_size * 0.06))

        # shadow (soft)
        draw.text((x + shadow_offset, y + shadow_offset), line, font=font, fill=(0,0,0,160))

        # stroke + fill (prefer stroke_width if available)
        try:
            draw.text((x, y), line, font=font, fill=fill_color, stroke_width=stroke_w, stroke_fill=stroke_color)
        except TypeError:
            # fallback manual stroke
            for ox in range(-stroke_w, stroke_w+1):
                for oy in range(-stroke_w, stroke_w+1):
                    if ox == 0 and oy == 0:
                        continue
                    draw.text((x+ox, y+oy), line, font=font, fill=stroke_color)
            draw.text((x, y), line, font=font, fill=fill_color)

        y += th + 12

    out_path = PROCESSED_DIR / f"{img_path.stem}_titled_big.jpg"
    base.convert("RGB").save(out_path, format="JPEG", quality=92, optimize=True)
    print("Saved titled (large/color) image to:", out_path)
    return out_path

def process_image_file(in_path: Path) -> Path:
    img = Image.open(in_path)
    img = ensure_rgb(img)
    img = clamp_to_instagram_ratio(img)
    img = resize_for_instagram(img)
    out = PROCESSED_DIR / f"{in_path.stem}_ig.jpg"
    img.save(out, format="JPEG", quality=92, optimize=True)
    print("Saved processed image to:", out)
    return out

# -------------------------
# R2 / S3 helpers
# -------------------------
def make_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_ENDPOINT,
        aws_access_key_id=R2_ACCESS_KEY,
        aws_secret_access_key=R2_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="auto"
    )

def upload_file_to_r2_and_get_presigned(local_path: Path, object_key: str=None, presign_expires=PRESIGNED_EXPIRES_IN):
    if object_key is None:
        object_key = f"{int(time.time())}_{local_path.name}"
    s3 = make_s3_client()
    print("Uploading", local_path, "->", R2_BUCKET, object_key)
    with open(local_path, "rb") as f:
        s3.upload_fileobj(f, R2_BUCKET, object_key, ExtraArgs={"ContentType": "image/jpeg"})
    if R2_PUBLIC_BASE and R2_PUBLIC_BASE.strip().lower().startswith("http"):
        public_url = f"{R2_PUBLIC_BASE.rstrip('/')}/{R2_BUCKET}/{object_key}"
    else:
        public_url = s3.generate_presigned_url("get_object", Params={"Bucket": R2_BUCKET, "Key": object_key}, ExpiresIn=presign_expires)
    # validate
    try:
        head = requests.head(public_url, allow_redirects=True, timeout=15)
        if head.status_code==200 and head.headers.get("Content-Type","").startswith("image/"):
            print("HEAD OK:", head.headers.get("Content-Type"))
            return public_url, object_key
        print("HEAD not OK; trying GET")
        get = requests.get(public_url, stream=True, timeout=20); get.raise_for_status()
        ctype = get.headers.get("Content-Type","")
        if not ctype.startswith("image/"):
            raise RuntimeError("Uploaded URL not image; Content-Type="+ctype)
        print("GET OK:", ctype)
        return public_url, object_key
    except Exception as e:
        print("Validation failed:", e)
        try:
            s3.delete_object(Bucket=R2_BUCKET, Key=object_key)
            print("Deleted uploaded object due to validation failure.")
        except Exception as ee:
            print("Cleanup delete failed:", ee)
        raise

# -------------------------
# optional: make video from image + audio (requires ffmpeg)
# -------------------------
def make_video_from_image_and_audio(image_path: Path, audio_path: Path, out_mp4: Path):
    if not shutil.which("ffmpeg"):
        raise RuntimeError("ffmpeg not found on PATH. Install ffmpeg to combine audio+image.")
    cmd = [
        "ffmpeg", "-y",
        "-loop", "1",
        "-i", str(image_path),
        "-i", str(audio_path),
        "-c:v", "libx264",
        "-tune", "stillimage",
        "-c:a", "aac",
        "-b:a", "192k",
        "-shortest",
        "-pix_fmt", "yuv420p",
        "-vf", "scale=trunc(iw/2)*2:trunc(ih/2)*2",
        str(out_mp4)
    ]
    print("Running ffmpeg:", " ".join(cmd))
    subprocess.run(cmd, check=True)
    print("Video created:", out_mp4)
    return out_mp4

# -------------------------
# OpenAI caption helper (optional)
# -------------------------
def gen_caption_and_tags(title, summary):
    """
    Return a plain caption string (caption + newline + hashtags).
    If OpenAI returns JSON, parse it. Otherwise ensure hashtags exist.
    """
    # helper to auto-generate hashtags from title
    def auto_hashtags(t, max_tags=8):
        tags = []
        for w in re.findall(r"[A-Za-z]{3,}", t):
            tag = "#" + w.lower()
            if tag not in tags:
                tags.append(tag)
            if len(tags) >= max_tags:
                break
        return tags

    if not OPENAI_API_KEY:
        tags = auto_hashtags(title, max_tags=8)
        caption = f"{title}\n\n{summary[:220].rstrip()}..." if summary else title
        if tags:
            caption = f"{caption}\n\n{' '.join(tags)}"
        return caption

    # call OpenAI
    url = "https://api.openai.com/v1/chat/completions"
    headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type": "application/json"}
    prompt = (
        "Given the article title and short summary, produce a concise Instagram caption (2-3 sentences) "
        "and 6-10 relevant hashtags. Return either a JSON object like "
        '{"caption":"...","hashtags":["#a","#b",...]} OR plain text. If JSON is returned, do not include any explanatory text.'
        f"\n\nTitle: {title}\n\nSummary: {summary}"
    )
    payload = {"model":"gpt-4o-mini","messages":[{"role":"user","content":prompt}], "temperature":0.6, "max_tokens":300}
    try:
        r = requests.post(url, headers=headers, json=payload, timeout=20)
        r.raise_for_status()
        resp = r.json()
        content = resp["choices"][0]["message"]["content"].strip()
    except Exception:
        # fallback
        tags = auto_hashtags(title, max_tags=8)
        caption = f"{title}\n\n{summary[:220].rstrip()}..." if summary else title
        if tags:
            caption = f"{caption}\n\n{' '.join(tags)}"
        return caption

    # remove triple backticks
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.IGNORECASE).strip()

    # try parse JSON
    try:
        parsed = json.loads(cleaned)
        cap = parsed.get("caption","").strip()
        hashtags = parsed.get("hashtags", [])
        tags_text = ""
        if isinstance(hashtags, list) and hashtags:
            tags_text = " ".join([h.strip() for h in hashtags])
        elif isinstance(hashtags, str) and hashtags.strip():
            tags_text = hashtags.strip()
        if cap:
            if tags_text:
                return f"{cap}\n\n{tags_text}"
            return cap
    except Exception:
        pass

    # fallback: if cleaned contains lines, ensure hashtags appended if not present
    if "#" not in cleaned:
        tags = auto_hashtags(title, max_tags=8)
        if tags:
            cleaned = f"{cleaned}\n\n{' '.join(tags)}"
    return cleaned

# -------------------------
# IG Graph helpers
# -------------------------
def create_media_container(ig_user_id, image_url, caption, token):
    url = f"{GRAPH_BASE}/{API_VER}/{ig_user_id}/media"
    payload = {"image_url": image_url, "caption": caption, "access_token": token}
    print("Creating media container (image_url):", image_url)
    r = requests.post(url, data=payload, timeout=30)
    print("Create response:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

def publish_media_container(ig_user_id, creation_id, token):
    url = f"{GRAPH_BASE}/{API_VER}/{ig_user_id}/media_publish"
    payload = {"creation_id": creation_id, "access_token": token}
    r = requests.post(url, data=payload, timeout=30)
    print("Publish response:", r.status_code, r.text)
    r.raise_for_status()
    return r.json()

# -------------------------
# Cleanup uploaded keys
# -------------------------
def cleanup_uploaded_keys(keys):
    if not keys:
        return
    s3 = make_s3_client()
    for k in keys:
        try:
            s3.delete_object(Bucket=R2_BUCKET, Key=k)
            print("Deleted R2 object:", k)
        except Exception as e:
            print("Failed to delete R2 object", k, ":", e)

# -------------------------
# MAIN
# -------------------------
def main():
    # sanity checks
    if ACCESS_TOKEN.startswith("YOUR_") or IG_USER_ID.startswith("YOUR_"):
        print("Set ACCESS_TOKEN and IG_USER_ID environment variables. Aborting.")
        return
    if R2_ACCESS_KEY.startswith("YOUR_") or R2_SECRET_KEY.startswith("YOUR_") or R2_BUCKET == "your-bucket-name":
        print("Set R2 credentials and bucket. Aborting.")
        return

    # 1) fetch article
    article = get_db_article()
    if not article:
        print("No article returned from DB; falling back to env/default.")
        article = {"id": 0, "title": "Sample Title", "summary": "Sample summary", "image_url": FALLBACK_SOURCE_IMAGE_URL}

    title = (article.get("title") or "Untitled").strip()
    summary = (article.get("summary") or "").strip()
    source_image_url = article.get("image_url") or FALLBACK_SOURCE_IMAGE_URL
    if not source_image_url:
        print("No source image URL available. Aborting.")
        return

    # 2) download original
    try:
        orig = download_image_to_file(source_image_url)
    except Exception as e:
        print("Download failed:", e)
        return

    # 3) overlay title
    try:
        titled = overlay_title_on_image(orig, title)
    except Exception as e:
        print("Overlay failed, proceeding with original image:", e)
        titled = orig

    # 4) process image
    try:
        processed = process_image_file(titled)
    except Exception as e:
        print("Processing failed:", e)
        return

    uploaded_keys = []
    media_url = None
    is_video = False

    # 5) optional: make video if AUDIO_PATH provided
    if AUDIO_PATH:
        audio_p = Path(AUDIO_PATH)
        if audio_p.exists():
            try:
                out_mp4 = TMP_DIR / f"{processed.stem}_video.mp4"
                make_video_from_image_and_audio(processed, audio_p, out_mp4)
                # upload video
                s3 = make_s3_client()
                object_key = f"insta/{time.strftime('%Y/%m/%d')}/{out_mp4.name}"
                with open(out_mp4, "rb") as f:
                    s3.upload_fileobj(f, R2_BUCKET, object_key, ExtraArgs={"ContentType": "video/mp4"})
                uploaded_keys.append(object_key)
                if R2_PUBLIC_BASE and R2_PUBLIC_BASE.strip().lower().startswith("http"):
                    media_url = f"{R2_PUBLIC_BASE.rstrip('/')}/{R2_BUCKET}/{object_key}"
                else:
                    media_url = s3.generate_presigned_url("get_object", Params={"Bucket": R2_BUCKET, "Key": object_key}, ExpiresIn=PRESIGNED_EXPIRES_IN)
                # validate video URL quickly
                resp = requests.head(media_url, allow_redirects=True, timeout=15)
                if not (resp.status_code == 200 and resp.headers.get("Content-Type","").startswith("video/")):
                    # fallback to GET
                    g = requests.get(media_url, stream=True, timeout=20); g.raise_for_status()
                    if not g.headers.get("Content-Type","").startswith("video/"):
                        raise RuntimeError("Uploaded video URL not returning video Content-Type")
                is_video = True
            except Exception as e:
                print("Video creation/upload failed:", e)
                is_video = False
        else:
            print("AUDIO_PATH provided but file not found:", AUDIO_PATH)

    # 6) if not video, upload processed image
    if not is_video:
        try:
            media_url, obj_key = upload_file_to_r2_and_get_presigned(processed, object_key=f"insta/{time.strftime('%Y/%m/%d')}/{processed.name}")
            uploaded_keys.append(obj_key)
        except Exception as e:
            print("Upload failed:", e)
            return

    print("Final media_url (for IG):", media_url)

    # 7) generate caption & hashtags
    try:
        caption = gen_caption_and_tags(title, summary)
    except Exception as e:
        print("Caption generation failed, using fallback:", e)
        caption = f"{title}\n\n{summary[:200]}"

    # 8) post to IG
    try:
        container = create_media_container(IG_USER_ID, media_url, caption, ACCESS_TOKEN)
    except requests.HTTPError as he:
        print("Create container failed:", he.response.text if he.response is not None else he)
        cleanup_uploaded_keys(uploaded_keys)
        return

    creation_id = container.get("id")
    if not creation_id:
        print("No creation id:", container)
        cleanup_uploaded_keys(uploaded_keys)
        return

    try:
        pub = publish_media_container(IG_USER_ID, creation_id, ACCESS_TOKEN)
        print("Publish response:", pub)
    except requests.HTTPError as he:
        print("Publish failed:", he.response.text if he.response is not None else he)
        cleanup_uploaded_keys(uploaded_keys)
        return

    # 9) cleanup uploaded R2 objects
    cleanup_uploaded_keys(uploaded_keys)
    print("Done. Post published successfully.")

if __name__ == "__main__":
    main()
