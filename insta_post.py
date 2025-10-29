"""
post_ig_reel_feed_status_mysqlconnector.py

Same flow as before, but uses mysql.connector (mysql-connector-python) for DB access.
"""
# put this at the very top of your script
import os
import moviepy.config as mp_cfg

IM_PATH = r"C:\Program Files\ImageMagick-7.1.2-Q16-HDRI\magick.exe"  # adjust if different
if not os.path.exists(IM_PATH):
    raise FileNotFoundError(f"ImageMagick not found at: {IM_PATH}")

# for MoviePy ≥2
mp_cfg.change_settings({"IMAGEMAGICK_BINARY": IM_PATH})

# (optional) for older MoviePy versions that read env var:
os.environ["IMAGEMAGICK_BINARY"] = IM_PATH

# now import the rest
from moviepy.editor import TextClip, ImageClip, CompositeVideoClip, VideoFileClip

import os
import io
import time
import json
import requests
import mysql.connector
import boto3
from moviepy.editor import ImageClip, AudioFileClip, CompositeVideoClip, TextClip
from PIL import Image, ImageDraw, ImageFont
from urllib.parse import urljoin
from dotenv import load_dotenv

load_dotenv()

# ---------- CONFIG ----------
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 3306)),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASS", ""),
    "database": os.getenv("DB_NAME", "mokshiridb"),
    "charset": "utf8mb4",
}
print("✅ DB Config:", DB_CONFIG)
R2_CONFIG = {
    "endpoint_url": os.getenv("R2_ENDPOINT", "https://<accountid>.r2.cloudflarestorage.com"),
    "access_key": os.getenv("R2_ACCESS_KEY_ID", ""),
    "secret_key": os.getenv("R2_SECRET_ACCESS_KEY", ""),
    "bucket": os.getenv("R2_BUCKET", "my-bucket"),
    "public_root": os.getenv("R2_PUBLIC_ROOT", "https://<accountid>.r2.cloudflare.com/my-bucket/"),
}

R2_IMAGE_ROOT = os.getenv("R2_IMAGE_ROOT", "https://<accountid>.r2.cloudflare.com/my-bucket/images/")
LOCAL_TMP_DIR = os.getenv("TMP_DIR", "D:\\projects\\mokshiri-scrapes\\mokshiri_media")

IG_CONFIG = {
    "ig_user_id": os.getenv("IG_USER_ID", ""),         # numeric IG Business user id
    "page_access_token": os.getenv("PAGE_ACCESS_TOKEN", ""),  # token with publish rights
    "graph_api_version": os.getenv("GRAPH_API_VERSION", "v17.0"),
    "page_id": os.getenv("FB_PAGE_ID", ""),  # needed if posting to FB page feed
}

BACKGROUND_MUSIC_PATH = os.getenv("BGM_PATH", "bgm_sample.mp3")  # licensed audio file path

# Image/video sizes
VIDEO_WIDTH = 1080
VIDEO_HEIGHT = 1920
VIDEO_DURATION = int(os.getenv("VIDEO_DURATION", 20))
TEXT_FONT_PATH = os.getenv("TEXT_FONT_PATH", "")  # optional TTF path
TITLE_FONT_SIZE = int(os.getenv("TITLE_FONT_SIZE", 72))
SUBTITLE_FONT_SIZE = int(os.getenv("SUBTITLE_FONT_SIZE", 36))
TEXT_PADDING = 40

# create tmp
os.makedirs(LOCAL_TMP_DIR, exist_ok=True)

# ---------- Helpers ----------
def fetch_articles(limit=5):
    """
    Fetch articles from MySQL using mysql.connector.
    Adjust the SELECT to match your schema.
    Returns list of dicts with keys: id, title, image_name, summary
    """
    conn = None
    try:
        conn = mysql.connector.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            database=DB_CONFIG["database"],
            charset=DB_CONFIG.get("charset", "utf8mb4"),
        )
        cur = conn.cursor(dictionary=True)
        cur.execute(
            "SELECT id, title, image_name, summary FROM articles WHERE image_name IS NOT NULL LIMIT %s",
            (limit,),
        )
        rows = cur.fetchall()
        return rows
    finally:
        if conn:
            conn.close()

def r2_client():
    return boto3.client(
        "s3",
        endpoint_url=R2_CONFIG["endpoint_url"],
        aws_access_key_id=R2_CONFIG["access_key"],
        aws_secret_access_key=R2_CONFIG["secret_key"],
    )

def upload_file_to_r2(local_path, key):
    client = r2_client()
    bucket = R2_CONFIG["bucket"]
    client.upload_file(local_path, bucket, key)
    public_url = urljoin(R2_CONFIG.get("public_root", R2_CONFIG["endpoint_url"] + "/" + bucket + "/"), key)
    return public_url

def download_image(image_url):
    resp = requests.get(image_url, timeout=30)
    resp.raise_for_status()
    return io.BytesIO(resp.content)

# ---------- Image editing (overlay text at top) ----------
def overlay_text_on_image(image_bytes_io, title, subtitle=None, output_image_path=None):
    image_bytes_io.seek(0)
    im = Image.open(image_bytes_io).convert("RGBA")
    w, h = im.size

    overlay = Image.new("RGBA", im.size, (255,255,255,0))
    draw = ImageDraw.Draw(overlay)

    if TEXT_FONT_PATH and os.path.exists(TEXT_FONT_PATH):
        title_font = ImageFont.truetype(TEXT_FONT_PATH, TITLE_FONT_SIZE)
        subtitle_font = ImageFont.truetype(TEXT_FONT_PATH, SUBTITLE_FONT_SIZE)
    else:
        # fallback - default font metrics may be small; adjust if needed
        title_font = ImageFont.load_default()
        subtitle_font = ImageFont.load_default()

    def wrap_text(text, font, max_width):
        lines = []
        words = text.split()
        cur = ""
        for w_word in words:
            test = cur + (" " if cur else "") + w_word
            width, _ = draw.textsize(test, font=font)
            if width <= max_width:
                cur = test
            else:
                if cur:
                    lines.append(cur)
                cur = w_word
        if cur:
            lines.append(cur)
        return lines

    max_text_width = int(w - TEXT_PADDING*2)
    title_lines = wrap_text(title, title_font, max_text_width)
    subtitle_lines = wrap_text(subtitle, subtitle_font, max_text_width) if subtitle else []

    # estimate heights
    try:
        line_height_title = title_font.getsize("Ay")[1]
    except Exception:
        line_height_title = 24
    try:
        line_height_sub = subtitle_font.getsize("Ay")[1]
    except Exception:
        line_height_sub = 16

    rect_h = TEXT_PADDING + len(title_lines) * (line_height_title+8) + (len(subtitle_lines) * (line_height_sub+6)) + TEXT_PADDING

    rect_color = (0,0,0,180)
    draw.rectangle(((0,0),(w, rect_h)), fill=rect_color)

    y = TEXT_PADDING//2
    x = TEXT_PADDING
    for line in title_lines:
        draw.text((x, y), line, font=title_font, fill=(255,255,255,255))
        y += line_height_title + 8

    if subtitle_lines:
        y += 6
        for line in subtitle_lines:
            draw.text((x, y), line, font=subtitle_font, fill=(230,230,230,255))
            y += line_height_sub + 6

    combined = Image.alpha_composite(im, overlay)
    if not output_image_path:
        output_image_path = os.path.join(LOCAL_TMP_DIR, f"edited_{int(time.time()*1000)}.jpg")
    combined.convert("RGB").save(output_image_path, format="JPEG", quality=90)
    return output_image_path

# ---------- Create vertical video (reel) from image ----------
def create_reel_from_image(image_path, title_text, output_path, music_path=None, duration=VIDEO_DURATION):
    clip = ImageClip(image_path).set_duration(duration)
    clip = clip.resize(width=VIDEO_WIDTH)
    if clip.h < VIDEO_HEIGHT:
        clip = clip.resize(height=VIDEO_HEIGHT)
    clip = clip.crop(x_center=clip.w/2, y_center=clip.h/2, width=VIDEO_WIDTH, height=VIDEO_HEIGHT)
    title_clip = TextClip(title_text, fontsize=40, color='white', method='caption', size=(VIDEO_WIDTH-120, None))
    title_clip = title_clip.set_position(("center", 60)).set_duration(duration)
    final = CompositeVideoClip([clip, title_clip], size=(VIDEO_WIDTH, VIDEO_HEIGHT))
    if music_path and os.path.exists(music_path):
        audio = AudioFileClip(music_path)
        audio = audio.subclip(0, min(duration, audio.duration))
        final = final.set_audio(audio)
    final.write_videofile(output_path, fps=24, codec="libx264", audio_codec="aac", bitrate="3000k", threads=4, verbose=False, logger=None)
    return output_path

# ---------- Instagram Graph API interactions ----------
def create_media_container_image(ig_user_id, image_url, caption):
    print("Creating media container for image...")
    url = f"https://graph.facebook.com/{IG_CONFIG['graph_api_version']}/{ig_user_id}/media"
    params = {
        "image_url": image_url,
        "caption": caption,
        "access_token": IG_CONFIG["page_access_token"],
    }
    print("Media container params:", params)
    resp = requests.post(url, data=params, timeout=30)
    resp.raise_for_status()
    return resp.json().get("id")

def create_media_container_video(ig_user_id, video_url, caption, is_reel=True):
    url = f"https://graph.facebook.com/{IG_CONFIG['graph_api_version']}/{ig_user_id}/media"
    params = {
        "video_url": video_url,
        "caption": caption,
        "access_token": IG_CONFIG["page_access_token"],
    }
    if is_reel:
        params["is_reel"] = "true"
    resp = requests.post(url, data=params, timeout=30)
    resp.raise_for_status()
    try:
        print(resp.json())  # often contains error.message and error.code
    except:
        print(resp.text)
    return resp.json().get("id")

def publish_media(ig_user_id, creation_id):
    print("Publishing media id:", creation_id)
    url = f"https://graph.facebook.com/{IG_CONFIG['graph_api_version']}/{ig_user_id}/media_publish"
    params = {
        "creation_id": creation_id,
        "access_token": IG_CONFIG["page_access_token"],
    }
    print("Publishing media with params:", params)
    resp = requests.post(url, data=params, timeout=30)
    resp.raise_for_status()
    try:
        print(resp.json())  # often contains error.message and error.code
    except:
        print(resp.text)
    return resp.json()

# ---------- Facebook Page post (status) ----------
def post_to_facebook_page(page_id, message):
    url = f"https://graph.facebook.com/{IG_CONFIG['graph_api_version']}/{page_id}/feed"
    params = {
        "message": message,
        "access_token": IG_CONFIG["page_access_token"],
    }
    resp = requests.post(url, data=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

# ---------- Caption / hashtags ----------
def build_caption(article, max_summary_chars=200):
    title = article.get("title") or ""
    summary = (article.get("summary") or "")[:max_summary_chars]
    words = [w.strip("#.,!?()[]{}").lower() for w in title.split() if len(w) > 3]
    hashtags = []
    for w in words[:6]:
        hashtag = "#" + "".join(ch for ch in w if ch.isalnum())
        hashtags.append(hashtag)
    tags = " ".join(hashtags + ["#mokshiri", "#koreanstyle"])
    caption = f"{title}\n\n{summary}...\n\n{tags}\n\nRead more: https://your-site.example/articles/{article['id']}"
    return caption

# ---------- Main pipeline ----------
def process_and_publish(limit=5):
    articles = fetch_articles(limit=limit)
    for art in articles:
        print(f"[+] Article {art['id']} - {art.get('title')}")
        try:
            image_url = urljoin(R2_IMAGE_ROOT, art["image_name"])
            img_bytes = download_image(image_url)

            edited_image_path = overlay_text_on_image(img_bytes, art.get("title") or "", subtitle=None)
            print("Edited image saved:", edited_image_path)

            video_local_path = os.path.join(LOCAL_TMP_DIR, f"reel_{art['id']}_{int(time.time())}.mp4")
            create_reel_from_image(edited_image_path, art.get("title") or "", video_local_path, music_path=BACKGROUND_MUSIC_PATH, duration=VIDEO_DURATION)
            print("Reel video created:", video_local_path)

            image_key = f"generated/feeds/edited_{art['id']}_{int(time.time())}.jpg"
            video_key = f"generated/reels/reel_{art['id']}_{int(time.time())}.mp4"
            image_public_url = upload_file_to_r2(edited_image_path, image_key)
            video_public_url = upload_file_to_r2(video_local_path, video_key)
            print("Uploaded image:", image_public_url)
            print("Uploaded video:", video_public_url)

            caption = build_caption(art)

            try:
                creation_id_img = create_media_container_image(IG_CONFIG["ig_user_id"], image_public_url, caption)
                publish_resp_img = publish_media(IG_CONFIG["ig_user_id"], creation_id_img)
                print("Published feed image:", publish_resp_img)
            except Exception as e:
                print("Feed image publish failed:", e, getattr(e, "response", None))

            try:
                creation_id_vid = create_media_container_video(IG_CONFIG["ig_user_id"], video_public_url, caption, is_reel=True)
                publish_resp_vid = publish_media(IG_CONFIG["ig_user_id"], creation_id_vid)
                print("Published reel:", publish_resp_vid)
            except Exception as e:
                print("Reel publish failed:", e, getattr(e, "response", None))

            if IG_CONFIG.get("page_id"):
                try:
                    fb_resp = post_to_facebook_page(IG_CONFIG["page_id"], caption)
                    print("Posted to FB Page feed:", fb_resp)
                except Exception as e:
                    print("FB page post failed:", e, getattr(e, "response", None))

        except Exception as e:
            print("Error processing article:", e)

if __name__ == "__main__":
    process_and_publish(limit=1)
