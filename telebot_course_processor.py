# language: Python, file: telebot_course_processor.py, runtime: Python 3.10+, target: Render / Docker / Windows / Linux

import os
import re
import sys
import time
import json
import shutil
import hashlib
import asyncio
import logging
import subprocess
from pathlib import Path
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler

# Python 3.10+ MainThread event loop initialization for MTProto clients
try:
    asyncio.get_event_loop()
except RuntimeError:
    asyncio.set_event_loop(asyncio.new_event_loop())

import aiohttp
import requests
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton
)

try:
    import imageio_ffmpeg
    DEFAULT_FFMPEG = imageio_ffmpeg.get_ffmpeg_exe()
except Exception:
    DEFAULT_FFMPEG = "ffmpeg"


def get_ffmpeg_binary() -> str:
    if shutil.which("ffmpeg"):
        return "ffmpeg"
    if os.path.exists(DEFAULT_FFMPEG):
        return DEFAULT_FFMPEG
    return "ffmpeg"


FFMPEG_BIN = get_ffmpeg_binary()
WORKDIR = Path("telebot_downloads")
WORKDIR.mkdir(parents=True, exist_ok=True)
DB_PATH = Path("bot_database.json")

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
    "Accept": "*/*",
    "Referer": "https://appx.co.in/",
    "Origin": "https://appx.co.in"
}

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


# ==========================================
# Persistent Storage & Deduplication Engine
# ==========================================
class Database:
    def __init__(self, file_path: Path):
        self.file_path = file_path
        self.data = {
            "uploaded_items": {},  # key: "target_chat:item_hash", value: {title, type, timestamp}
            "user_settings": {}   # key: "user_id", value: {last_target: str, default_quality: str}
        }
        self.load()

    def load(self):
        if self.file_path.exists():
            try:
                with open(self.file_path, "r", encoding="utf-8") as f:
                    self.data = json.load(f)
            except Exception:
                pass

    def save(self):
        try:
            with open(self.file_path, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2, ensure_ascii=False)
        except Exception as e:
            logger.error(f"DB save error: {e}")

    def get_item_hash(self, title: str, url: str) -> str:
        base_url = url.split("?")[0]
        return hashlib.sha256(f"{title}_{base_url}".encode("utf-8")).hexdigest()[:16]

    def is_already_uploaded(self, target_chat: str, title: str, url: str) -> bool:
        item_hash = self.get_item_hash(title, url)
        key = f"{target_chat}:{item_hash}"
        return key in self.data["uploaded_items"]

    def record_upload(self, target_chat: str, title: str, url: str, item_type: str = "video"):
        item_hash = self.get_item_hash(title, url)
        key = f"{target_chat}:{item_hash}"
        self.data["uploaded_items"][key] = {
            "title": title,
            "type": item_type,
            "target": str(target_chat),
            "timestamp": int(time.time())
        }
        self.save()

    def get_user_settings(self, user_id: int) -> dict:
        uid = str(user_id)
        if uid not in self.data["user_settings"]:
            self.data["user_settings"][uid] = {
                "last_target": "",
                "default_quality": "480p"
            }
            self.save()
        return self.data["user_settings"][uid]

    def update_user_setting(self, user_id: int, key: str, value: str):
        uid = str(user_id)
        settings = self.get_user_settings(user_id)
        settings[key] = value
        self.data["user_settings"][uid] = settings
        self.save()

    def clear_history(self) -> int:
        count = len(self.data["uploaded_items"])
        self.data["uploaded_items"] = {}
        self.save()
        return count


db = Database(DB_PATH)
PENDING_JOBS = {}


# ==========================================
# Render Health Check Server
# ==========================================
class HealthCheckHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "text/plain")
        self.end_headers()
        self.wfile.write(b"Course Telegram Bot (2GB MTProto Engine) is ACTIVE.")

    def log_message(self, format, *args):
        return


def run_health_server(port: int = 8080):
    try:
        server = HTTPServer(("0.0.0.0", port), HealthCheckHandler)
        logger.info(f"Health server active on port {port}")
        server.serve_forever()
    except Exception as e:
        logger.warning(f"Health server notice: {e}")


# ==========================================
# Configuration Bootstrap
# ==========================================
def load_config():
    env_file = Path(".env")
    token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("BOT_TOKEN", "")
    api_id = os.getenv("TELEGRAM_API_ID") or os.getenv("API_ID", "")
    api_hash = os.getenv("TELEGRAM_API_HASH") or os.getenv("API_HASH", "")

    if env_file.exists():
        try:
            with open(env_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not token and (line.startswith("TELEGRAM_BOT_TOKEN=") or line.startswith("BOT_TOKEN=")):
                        token = line.split("=", 1)[1].strip().strip('"').strip("'")
                    elif not api_id and (line.startswith("TELEGRAM_API_ID=") or line.startswith("API_ID=")):
                        api_id = line.split("=", 1)[1].strip().strip('"').strip("'")
                    elif not api_hash and (line.startswith("TELEGRAM_API_HASH=") or line.startswith("API_HASH=")):
                        api_hash = line.split("=", 1)[1].strip().strip('"').strip("'")
        except Exception:
            pass

    api_id_int = int(api_id) if str(api_id).isdigit() else 33466201
    return token.strip(), api_id_int, api_hash.strip()


BOT_TOKEN, API_ID, API_HASH = load_config()

if not BOT_TOKEN:
    logger.error("FATAL: TELEGRAM_BOT_TOKEN is missing in environment variables and .env!")


# ==========================================
# Media Processing Pipeline
# ==========================================
class MediaPipeline:
    QUALITY_PRESETS = {
        "1080p": {"vf": "scale=-2:1080", "crf": "22"},
        "720p": {"vf": "scale=-2:720", "crf": "24"},
        "480p": {"vf": "scale=-2:480", "crf": "26"},
        "360p": {"vf": "scale=-2:360", "crf": "28"}
    }

    @staticmethod
    def sanitize(title: str) -> str:
        clean = re.sub(r'[\\/*?:"<>|]', "", title)
        return re.sub(r"\s+", " ", clean).strip()[:100]

    @staticmethod
    def normalize_url(url: str) -> str:
        if "static-db-v2.appx.co.in" in url:
            url = url.replace("static-db-v2.appx.co.in", "appx-content-v2.classx.co.in")
        elif "static-db.appx.co.in" in url:
            url = url.replace("static-db.appx.co.in", "appx-content-v2.classx.co.in")
        return url

    @staticmethod
    def classify_entry(url: str) -> str:
        clean_url = url.lower()
        if ".pdf" in clean_url or "appx-pdf" in clean_url:
            return "pdf"
        return "video"

    @staticmethod
    def parse_manifest(text: str) -> list:
        entries = []
        for line in text.splitlines():
            line = line.strip()
            if not line or ":" not in line:
                continue
            match = re.search(r"(https?://\S+)", line)
            if match:
                url = match.group(1).strip()
                title = line[:match.start()].rstrip(": ").strip()
                norm_url = MediaPipeline.normalize_url(url)
                media_type = MediaPipeline.classify_entry(norm_url)
                entries.append({
                    "title": title,
                    "url": norm_url,
                    "type": media_type
                })
        return entries

    @staticmethod
    async def download_pdf(url: str, output_path: Path) -> tuple[bool, str]:
        try:
            async with aiohttp.ClientSession(headers=HEADERS) as session:
                async with session.get(url, timeout=180, ssl=False) as resp:
                    if resp.status == 404:
                        return False, "404 Not Found (Expired signature or missing on CDN)"
                    if resp.status != 200:
                        return False, f"HTTP Error {resp.status}"
                    with open(output_path, "wb") as f:
                        while True:
                            chunk = await resp.content.read(1024 * 1024)
                            if not chunk:
                                break
                            f.write(chunk)
            if output_path.exists() and output_path.stat().st_size > 0:
                return True, "OK"
            return False, "Zero byte PDF downloaded"
        except Exception as e:
            return False, f"PDF Download Error: {e}"

    @staticmethod
    async def capture_full_video(url: str, output_path: Path, quality: str = "480p") -> tuple[bool, str]:
        # Direct stream copy with TLS bypass (100% full duration, lossless audio)
        cmd_copy = [
            FFMPEG_BIN, "-y",
            "-tls_verify", "0",
            "-headers", f"User-Agent: {HEADERS['User-Agent']}\r\nReferer: {HEADERS['Referer']}\r\n",
            "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
            "-i", url,
            "-c", "copy",
            "-bsf:a", "aac_adtstoasc",
            str(output_path)
        ]
        proc = await asyncio.create_subprocess_exec(*cmd_copy, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await proc.communicate()

        if proc.returncode == 0 and output_path.exists() and output_path.stat().st_size > 1024 * 100:
            return True, "OK (Full Stream Copy)"

        # Transcode fallback
        preset = MediaPipeline.QUALITY_PRESETS.get(quality, MediaPipeline.QUALITY_PRESETS["480p"])
        cmd_transcode = [
            FFMPEG_BIN, "-y",
            "-tls_verify", "0",
            "-headers", f"User-Agent: {HEADERS['User-Agent']}\r\nReferer: {HEADERS['Referer']}\r\n",
            "-protocol_whitelist", "file,http,https,tcp,tls,crypto",
            "-i", url,
            "-c:v", "libx264",
            "-vf", preset["vf"],
            "-crf", preset["crf"],
            "-preset", "veryfast",
            "-c:a", "aac",
            "-b:a", "128k",
            str(output_path)
        ]
        proc2 = await asyncio.create_subprocess_exec(*cmd_transcode, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        await proc2.communicate()

        if proc2.returncode == 0 and output_path.exists() and output_path.stat().st_size > 0:
            return True, "OK (Transcoded Video)"

        return False, "Failed to capture HLS video stream"

    @staticmethod
    async def fetch_mp4(url: str, output_path: Path) -> tuple[bool, str]:
        try:
            async with aiohttp.ClientSession(headers=HEADERS) as session:
                async with session.get(url, timeout=300, ssl=False) as resp:
                    if resp.status == 404:
                        return False, "404 Not Found"
                    if resp.status != 200:
                        return False, f"HTTP Error {resp.status}"
                    with open(output_path, "wb") as f:
                        while True:
                            chunk = await resp.content.read(1024 * 1024 * 2)
                            if not chunk:
                                break
                            f.write(chunk)
            return True, "OK"
        except Exception as e:
            return False, str(e)


# ==========================================
# Pyrogram MTProto Bot Client
# ==========================================
app = Client(
    "mtproto_course_bot",
    api_id=API_ID,
    api_hash=API_HASH,
    bot_token=BOT_TOKEN,
    ipv6=False,
    in_memory=True
)


def create_progress_callback(status_msg: Message, title: str, last_time: list):
    async def progress(current: int, total: int):
        now = time.time()
        if now - last_time[0] > 4 or current == total:
            last_time[0] = now
            pct = (current / total) * 100
            cur_mb = current / (1024 * 1024)
            tot_mb = total / (1024 * 1024)
            try:
                await status_msg.edit_text(
                    f"📤 **Uploading to Telegram (MTProto 2GB):**\n`{title}`\n"
                    f"📊 **Progress:** `{pct:.1f}%` ({cur_mb:.1f} MB / {tot_mb:.1f} MB)"
                )
            except Exception:
                pass
    return progress


async def execute_batch_task(client: Client, user_id: int, source_chat_id: int, entries: list, quality: str, target_chat: str, status_msg: Message):
    total = len(entries)
    skipped_duplicates = 0
    failed_count = 0
    uploaded_videos = 0
    uploaded_pdfs = 0

    for i, item in enumerate(entries, start=1):
        title = MediaPipeline.sanitize(item["title"])
        url = item["url"]
        media_type = item["type"]

        # 1. Deduplication Memory Check
        if db.is_already_uploaded(target_chat, title, url):
            skipped_duplicates += 1
            try:
                await client.send_message(
                    chat_id=source_chat_id,
                    text=f"⏭️ **Skipped (Already in Destination):**\n`{title}` ({media_type.upper()})"
                )
            except Exception:
                pass
            continue

        # 2. PDF Document Pipeline (Supports up to 2GB)
        if media_type == "pdf":
            out_path = WORKDIR / f"{title}.pdf"
            try:
                await status_msg.edit_text(
                    f"📄 **[{i}/{total}] Downloading PDF Notes:**\n`{title}`\n"
                    f"🎯 **Target:** `{target_chat}`"
                )
            except Exception:
                pass

            ok, msg_reason = await MediaPipeline.download_pdf(url, out_path)
            if ok and out_path.exists():
                last_time = [0]
                try:
                    await client.send_document(
                        chat_id=target_chat,
                        document=str(out_path),
                        caption=f"📄 **{title}**\n📁 *Class Notes PDF*",
                        progress=create_progress_callback(status_msg, f"{title}.pdf", last_time)
                    )
                    uploaded_pdfs += 1
                    db.record_upload(target_chat, title, url, "pdf")
                except Exception as e:
                    failed_count += 1
                    await client.send_message(
                        chat_id=source_chat_id,
                        text=f"❌ **PDF Upload Error to {target_chat}:** `{e}`\n*(Ensure bot is Admin in target channel/group)*"
                    )
                out_path.unlink(missing_ok=True)
            else:
                failed_count += 1
                await client.send_message(chat_id=source_chat_id, text=f"❌ **PDF Download Failed:** `{title}`\nReason: `{msg_reason}`")

        # 3. Full Video Lecture Pipeline (Supports up to 2GB)
        else:
            out_path = WORKDIR / f"{title}.mp4"
            try:
                await status_msg.edit_text(
                    f"🎬 **[{i}/{total}] Downloading Full Video ({quality}):**\n`{title}`\n"
                    f"🎯 **Target:** `{target_chat}`"
                )
            except Exception:
                pass

            ok = False
            msg_reason = ""
            if ".m3u8" in url or "vodclasses" in url or "liveclasses" in url:
                ok, msg_reason = await MediaPipeline.capture_full_video(url, out_path, quality)
            elif ".mp4" in url:
                ok, msg_reason = await MediaPipeline.fetch_mp4(url, out_path)
            else:
                ok, msg_reason = await MediaPipeline.capture_full_video(url, out_path, quality)

            if ok and out_path.exists():
                file_size_mb = out_path.stat().st_size / (1024 * 1024)
                last_time = [0]
                try:
                    await client.send_video(
                        chat_id=target_chat,
                        video=str(out_path),
                        caption=f"🎬 **{title}**\n⚡ *Full Video Lecture* ({file_size_mb:.1f} MB)",
                        supports_streaming=True,
                        progress=create_progress_callback(status_msg, f"{title}.mp4", last_time)
                    )
                    uploaded_videos += 1
                    db.record_upload(target_chat, title, url, "video")
                except Exception as e:
                    failed_count += 1
                    await client.send_message(
                        chat_id=source_chat_id,
                        text=f"❌ **Video Upload Error to {target_chat}:** `{e}`\n*(Ensure bot is Admin with Post permissions)*"
                    )
                out_path.unlink(missing_ok=True)
            else:
                failed_count += 1
                await client.send_message(chat_id=source_chat_id, text=f"❌ **Video Failed:** `{title}`\nReason: `{msg_reason}`")

    try:
        await status_msg.edit_text(
            f"🏁 **Batch Task Completed!**\n\n"
            f"📊 **Summary:**\n"
            f"• Total Loaded: `{total}`\n"
            f"• 🎬 Full Videos Sent: `{uploaded_videos}`\n"
            f"• 📄 PDF Notes Sent: `{uploaded_pdfs}`\n"
            f"• ⏭️ Skipped (Already Present): `{skipped_duplicates}`\n"
            f"• ❌ Failed / 404: `{failed_count}`\n\n"
            f"🎯 **Delivered Directly To:** `{target_chat}`"
        )
    except Exception:
        pass


# ==========================
# Interactive Wizard UI
# ==========================
async def prompt_quality_step(message: Message, entries: list):
    user_id = message.from_user.id
    video_count = sum(1 for e in entries if e["type"] == "video")
    pdf_count = sum(1 for e in entries if e["type"] == "pdf")

    PENDING_JOBS[user_id] = {
        "entries": entries,
        "source_chat_id": message.chat.id,
        "step": "AWAITING_QUALITY"
    }

    markup = InlineKeyboardMarkup([
        [
            InlineKeyboardButton("1080p (Full HD)", callback_data="wiz_qual_1080p"),
            InlineKeyboardButton("720p (HD)", callback_data="wiz_qual_720p")
        ],
        [
            InlineKeyboardButton("480p (Direct / Fast)", callback_data="wiz_qual_480p"),
            InlineKeyboardButton("360p (Data Saver)", callback_data="wiz_qual_360p")
        ],
        [InlineKeyboardButton("❌ Cancel Batch", callback_data="wiz_cancel")]
    ])

    await message.reply_text(
        f"📁 **Found {len(entries)} items in manifest!**\n"
        f"• 🎬 Video Lectures: `{video_count}`\n"
        f"• 📄 PDF Notes Documents: `{pdf_count}`\n\n"
        "🎬 **Step 1/2: Select Video Quality for video streams:**",
        reply_markup=markup
    )


async def prompt_destination_step(call: CallbackQuery, user_id: int):
    job = PENDING_JOBS.get(user_id)
    if not job:
        await call.answer("Session expired. Upload .txt file again.", show_alert=True)
        return

    job["step"] = "AWAITING_DESTINATION"
    saved_target = db.get_user_settings(user_id).get("last_target", "")

    btns = [[InlineKeyboardButton("💬 Upload to Current Chat", callback_data="wiz_dest_current")]]
    if saved_target and saved_target != str(call.message.chat.id):
        btns.append([InlineKeyboardButton(f"📢 Upload to Saved Target: {saved_target}", callback_data="wiz_dest_saved")])
    btns.append([InlineKeyboardButton("✏️ Type Custom Channel/Group (@channel or -100xxx)", callback_data="wiz_dest_custom")])
    btns.append([InlineKeyboardButton("❌ Cancel Batch", callback_data="wiz_cancel")])

    await call.message.edit_text(
        f"✅ Video Quality: **{job['quality']}**\n\n"
        "📢 **Step 2/2: Where do you want to upload all Videos & PDF Documents?**",
        reply_markup=InlineKeyboardMarkup(btns)
    )


# ==========================
# Handlers
# ==========================
@app.on_message(filters.command(["start", "help"]))
async def start_handler(_, message: Message):
    await message.reply_text(
        "🐺 **AIwolfie 2GB MTProto Course & Notes Engine**\n\n"
        "• ⚡ **No 50MB Bot Limit:** Full support up to **2,000 MB (2 GB)** files.\n"
        "• 📄 **PDFs** are delivered as authentic full PDF documents.\n"
        "• 🎬 **Videos** (.m3u8/.mp4) are captured with 100% duration & lossless audio.\n"
        "• 🧠 **Memory Engine** automatically prevents re-uploading duplicate classes.\n\n"
        "📥 Upload your `.txt` file or paste course links to get started!"
    )


@app.on_message(filters.command("clear_history"))
async def clear_history_handler(_, message: Message):
    cleared = db.clear_history()
    await message.reply_text(f"🧹 Cleared **{cleared}** records from deduplication memory.")


@app.on_callback_query(filters.regex(r"^wiz_"))
async def callback_handler(client: Client, call: CallbackQuery):
    user_id = call.from_user.id
    job = PENDING_JOBS.get(user_id)

    if call.data == "wiz_cancel":
        PENDING_JOBS.pop(user_id, None)
        await call.message.edit_text("❌ **Batch operation cancelled.**")
        return

    if not job:
        await call.answer("No active session. Please upload a .txt file.", show_alert=True)
        return

    if call.data.startswith("wiz_qual_"):
        job["quality"] = call.data.replace("wiz_qual_", "")
        await prompt_destination_step(call, user_id)

    elif call.data == "wiz_dest_current":
        target = str(job["source_chat_id"])
        db.update_user_setting(user_id, "last_target", target)
        entries = job["entries"]
        quality = job["quality"]
        source_chat = job["source_chat_id"]
        PENDING_JOBS.pop(user_id, None)

        status_msg = await call.message.edit_text(
            f"🚀 **Queue active:** {len(entries)} items\n"
            f"⚡ Quality: `{quality}` | 🎯 Destination: `Current Chat`\n"
            "Starting MTProto direct stream worker..."
        )
        asyncio.create_task(execute_batch_task(client, user_id, source_chat, entries, quality, target, status_msg))

    elif call.data == "wiz_dest_saved":
        target = db.get_user_settings(user_id).get("last_target", str(job["source_chat_id"]))
        entries = job["entries"]
        quality = job["quality"]
        source_chat = job["source_chat_id"]
        PENDING_JOBS.pop(user_id, None)

        status_msg = await call.message.edit_text(
            f"🚀 **Queue active:** {len(entries)} items\n"
            f"⚡ Quality: `{quality}` | 🎯 Destination: `{target}`\n"
            "Starting MTProto direct stream worker..."
        )
        asyncio.create_task(execute_batch_task(client, user_id, source_chat, entries, quality, target, status_msg))

    elif call.data == "wiz_dest_custom":
        job["step"] = "AWAITING_CUSTOM_INPUT"
        await call.message.edit_text(
            "✏️ **Send your Channel / Group Username or ID now:**\n\n"
            "Examples:\n"
            "• `@sachintxtt`\n"
            "• `-1001928374650`\n\n"
            "*(Ensure the bot is added as an Admin with 'Post Messages' permission)*"
        )


@app.on_message(filters.document)
async def doc_handler(_, message: Message):
    if not message.document.file_name.endswith(".txt"):
        await message.reply_text("❌ Please send a valid `.txt` file.")
        return

    tmp_file = await message.download()
    with open(tmp_file, "r", encoding="utf-8", errors="ignore") as f:
        text_content = f.read()
    if os.path.exists(tmp_file):
        os.remove(tmp_file)

    entries = MediaPipeline.parse_manifest(text_content)
    if not entries:
        await message.reply_text("❌ No valid `Title:URL` pairs found in file.")
        return

    await prompt_quality_step(message, entries)


@app.on_message(filters.text & ~filters.command(["start", "help", "clear_history"]))
async def text_handler(client: Client, message: Message):
    user_id = message.from_user.id
    job = PENDING_JOBS.get(user_id)

    if job and job.get("step") == "AWAITING_CUSTOM_INPUT":
        target = message.text.strip()
        db.update_user_setting(user_id, "last_target", target)
        entries = job["entries"]
        quality = job["quality"]
        source_chat = job["source_chat_id"]
        PENDING_JOBS.pop(user_id, None)

        status_msg = await message.reply_text(
            f"🚀 **Destination locked:** `{target}`\n"
            f"⚡ Quality: `{quality}` | Total: {len(entries)} items\n"
            "Starting MTProto (2GB) background worker..."
        )
        asyncio.create_task(execute_batch_task(client, user_id, source_chat, entries, quality, target, status_msg))
        return

    entries = MediaPipeline.parse_manifest(message.text)
    if entries:
        await prompt_quality_step(message, entries)
    else:
        await message.reply_text("📥 Send a `.txt` file or paste `Title:URL` course links to begin.")


if __name__ == "__main__":
    port = int(os.getenv("PORT", "8080"))
    Thread(target=run_health_server, args=(port,), daemon=True).start()

    print("=" * 60)
    print("[+] AIwolfie 2GB MTProto Bot Engine starting...")
    print(f"[+] Bound FFmpeg binary: {FFMPEG_BIN}")
    print("=" * 60)
    app.run()
