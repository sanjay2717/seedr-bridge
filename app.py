import os
import threading
import asyncio
import requests
import queue
import uuid
import time
from io import IOBase
from flask import Flask, request, jsonify
from pyrogram import Client
from pyrogram.errors import FloodWait, ChannelPrivate, ChatAdminRequired

app = Flask(__name__)

# --- CONFIGURATION ---
API_ID = os.environ.get("TG_API_ID")
API_HASH = os.environ.get("TG_API_HASH")
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")

if API_ID:
    try:
        API_ID = int(API_ID)
    except:
        pass

HEADERS_STREAM = {
    "User-Agent": "Seedr Android/1.0",
    "Content-Type": "application/x-www-form-urlencoded"
}

# --- 1. SMART STREAMER ---
class SmartStream(IOBase):
    def __init__(self, url, name):
        super().__init__()
        self.url = url
        self.name = name
        self.mode = 'rb'
        print(f"STREAM: Connecting to {url[:40]}...", flush=True)
        try:
            head = requests.head(url, allow_redirects=True, timeout=10, headers=HEADERS_STREAM)
            self.total_size = int(head.headers.get('content-length', 0))
            print(f"STREAM: Size {self.total_size} bytes ({self.total_size/1024/1024:.1f}MB)", flush=True)
        except Exception as e:
            print(f"STREAM WARNING: {e}", flush=True)
            self.total_size = 0
        
        self.response = requests.get(url, stream=True, timeout=30, headers=HEADERS_STREAM)
        self.raw = self.response.raw
        self.raw.decode_content = True
        self.current_pos = 0
        self._closed = False
    
    def read(self, size=-1):
        if self._closed: 
            raise ValueError("I/O closed")
        data = self.raw.read(size)
        if data: 
            self.current_pos += len(data)
        return data
    
    def seek(self, offset, whence=0):
        if whence == 0: 
            self.current_pos = offset
        elif whence == 1: 
            self.current_pos += offset
        elif whence == 2: 
            self.current_pos = self.total_size + offset
        return self.current_pos
    
    def tell(self): 
        return self.current_pos
    
    def close(self):
        if not self._closed:
            self._closed = True
            if hasattr(self, 'response'): 
                self.response.close()
    
    def fileno(self): 
        return None

# --- 2. ASYNC UPLOAD LOGIC (PUBLIC CHANNEL OPTIMIZED) ---
async def perform_upload(file_url, chat_target, caption, filename):
    """
    Optimized for PUBLIC channels with username
    - Accepts @username (recommended for public channels)
    - Accepts numeric ID as fallback
    - NO invite link support (causes BOT_METHOD_INVALID)
    """
    async with Client(
        "bot_session", 
        api_id=API_ID, 
        api_hash=API_HASH, 
        bot_token=BOT_TOKEN, 
        workdir="/tmp"
    ) as app:
        print("WORKER: Bot connected!", flush=True)
        
        final_chat_id = None
        chat_str = str(chat_target).strip()
        
        # --- STRATEGY 1: PUBLIC USERNAME (RECOMMENDED) ---
        if chat_str.startswith("@"):
            print(f"WORKER: Public username: {chat_str}", flush=True)
            try:
                chat = await app.get_chat(chat_str)
                final_chat_id = chat.id
                print(f"WORKER: ✅ Resolved to ID: {final_chat_id}", flush=True)
            except Exception as e:
                raise Exception(f"Username resolution failed: {e}")
        
        # --- STRATEGY 2: NUMERIC ID (FALLBACK) ---
        elif chat_str.lstrip("-").isdigit():
            final_chat_id = int(chat_str)
            print(f"WORKER: Using numeric ID: {final_chat_id}", flush=True)
            
            # Try to verify access
            try:
                print("WORKER: Verifying access...", flush=True)
                chat_info = await app.get_chat(final_chat_id)
                print(f"WORKER: ✅ Verified: {chat_info.title}", flush=True)
            except ChannelPrivate:
                raise Exception("Channel is private and bot has no access. Add bot as admin first.")
            except Exception as e:
                print(f"WORKER: ⚠️ Verification failed: {e}", flush=True)
                raise Exception(f"Cannot access channel: {e}")
        
        # --- REJECT INVITE LINKS ---
        elif "t.me/+" in chat_str or "joinchat" in chat_str:
            raise Exception(
                "Bots cannot join channels via invite links. "
                "Please use @username for public channels or add bot as admin and use numeric ID for private channels."
            )
        
        else:
            raise Exception(f"Invalid chat format: {chat_str}. Use @username or numeric ID.")
        
        if not final_chat_id:
            raise Exception("Could not resolve chat ID")
        
        # --- UPLOAD VIDEO ---
        with SmartStream(file_url, filename) as stream:
            if stream.total_size == 0:
                raise Exception("File size is 0. Seedr link expired.")
            
            print(f"WORKER: Uploading {filename} ({stream.total_size/1024/1024:.1f}MB)...", flush=True)
            
            msg = await app.send_video(
                chat_id=final_chat_id,
                video=stream,
                caption=caption,
                file_name=filename,
                supports_streaming=True,
                progress=lambda c, t: print(
                    f"📊 {c/1024/1024:.1f}/{t/1024/1024:.1f}MB ({c*100//t}%)", 
                    flush=True
                ) if c % (50*1024*1024) < 1024*1024 else None
            )
            
            # Generate private link
            clean_id = str(msg.chat.id).replace('-100', '')
            private_link = f"https://t.me/c/{clean_id}/{msg.id}"
            
            print(f"WORKER: ✅ Upload complete! {private_link}", flush=True)
            
            return {
                "success": True,
                "message_id": msg.id,
                "chat_id": msg.chat.id,
                "file_id": msg.video.file_id,
                "private_link": private_link,
                "file_size": msg.video.file_size,
                "duration": msg.video.duration
            }

# --- 3. QUEUE WORKER ---
JOB_QUEUE = queue.Queue()
JOBS = {} 
WORKER_THREAD = None
WORKER_LOCK = threading.Lock()

def worker_loop():
    """Background worker"""
    print("SYSTEM: Queue Worker Started", flush=True)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    while True:
        job_id = None
        try:
            job_id, data = JOB_QUEUE.get()
            print(f"WORKER: Job {job_id}", flush=True)
            JOBS[job_id]['status'] = 'processing'
            JOBS[job_id]['started'] = time.time()
            
            result = loop.run_until_complete(perform_upload(
                file_url=data['url'],
                chat_target=data['chat_id'],
                caption=data.get('caption', ''),
                filename=data.get('filename', 'video.mp4')
            ))
            
            JOBS[job_id]['status'] = 'done'
            JOBS[job_id]['result'] = result
            JOBS[job_id]['completed'] = time.time()
            print(f"WORKER: ✅ Job {job_id} done!", flush=True)
            
        except Exception as e:
            print(f"WORKER ERROR: {e}", flush=True)
            if job_id:
                JOBS[job_id]['status'] = 'failed'
                JOBS[job_id]['error'] = str(e)
                JOBS[job_id]['failed'] = time.time()
        finally:
            if job_id:
                JOB_QUEUE.task_done()
            
            # Cleanup old jobs
            if len(JOBS) > 100:
                old_jobs = sorted(JOBS.items(), key=lambda x: x[1].get('created', 0))[:50]
                for old_id, _ in old_jobs:
                    del JOBS[old_id]

def ensure_worker_alive():
    """Start worker thread"""
    global WORKER_THREAD
    with WORKER_LOCK:
        if WORKER_THREAD is None or not WORKER_THREAD.is_alive():
            print("SYSTEM: Starting worker thread...", flush=True)
            WORKER_THREAD = threading.Thread(target=worker_loop, daemon=True)
            WORKER_THREAD.start()

# --- FLASK ROUTES ---
@app.route('/')
def home(): 
    """Health check"""
    ensure_worker_alive()
    return jsonify({
        "status": "online",
        "queue": JOB_QUEUE.qsize(),
        "jobs": len(JOBS),
        "worker_alive": WORKER_THREAD.is_alive() if WORKER_THREAD else False
    })

@app.route('/upload-telegram', methods=['POST'])
def upload_telegram():
    """
    Upload video to Telegram
    Body: {
        "url": "https://...",
        "chat_id": "@username OR -100...",
        "caption": "Title (1080p)",
        "filename": "movie.mp4"
    }
    """
    data = request.json
    
    if not data or not data.get('url'):
        return jsonify({"error": "Missing 'url'"}), 400
    if not data.get('chat_id'):
        return jsonify({"error": "Missing 'chat_id'"}), 400
    
    ensure_worker_alive()
    
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {
        'status': 'queued',
        'created': time.time()
    }
    JOB_QUEUE.put((job_id, data))
    
    print(f"API: Job {job_id} queued", flush=True)
    
    return jsonify({
        "job_id": job_id,
        "status": "queued"
    })

@app.route('/job-status/<job_id>', methods=['GET'])
def job_status(job_id):
    """Check job status"""
    ensure_worker_alive()
    
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404
    
    return jsonify(job)

# --- SEEDR ROUTES ---
@app.route('/add-magnet', methods=['POST'])
def add_magnet():
    """Add magnet to Seedr"""
    try:
        resp = requests.post(
            "https://www.seedr.cc/oauth_test/resource.php?json=1",
            data={
                "access_token": request.json.get('token'),
                "func": "add_torrent",
                "torrent_magnet": request.json.get('magnet')
            },
            timeout=30
        )
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/list-files', methods=['POST'])
def list_files():
    """List Seedr files"""
    try:
        data = request.json
        token = data.get('token')
        folder_id = str(data.get('folder_id', "0"))
        
        if folder_id == "0":
            url = "https://www.seedr.cc/api/folder"
        else:
            url = f"https://www.seedr.cc/api/folder/{folder_id}"
        
        resp = requests.get(
            url, 
            params={"access_token": token}, 
            headers=HEADERS_STREAM,
            timeout=30
        )
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get-link', methods=['POST'])
def get_link():
    """Get Seedr download link"""
    try:
        resp = requests.post(
            "https://www.seedr.cc/oauth_test/resource.php?json=1",
            data={
                "access_token": request.json.get('token'),
                "func": "fetch_file",
                "folder_file_id": str(request.json.get('file_id'))
            },
            timeout=30
        )
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    print("=" * 50, flush=True)
    print("🚀 Seedr-Telegram Bridge Starting", flush=True)
    print("=" * 50, flush=True)
    ensure_worker_alive()
    app.run(host='0.0.0.0', port=10000)
