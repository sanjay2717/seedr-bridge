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

# --- QUEUE SYSTEM ---
JOB_QUEUE = queue.Queue()
JOBS = {} 
WORKER_THREAD = None # Keep track of the thread

# --- 1. SMART STREAMER ---
class SmartStream(IOBase):
    def __init__(self, url, name):
        super().__init__()
        self.url = url
        self.name = name
        self.mode = 'rb'
        
        print(f"STREAM: Connecting to {url[:40]}...")
        try:
            head = requests.head(url, allow_redirects=True, timeout=10, headers=HEADERS_STREAM)
            self.total_size = int(head.headers.get('content-length', 0))
        except:
            self.total_size = 0

        self.response = requests.get(url, stream=True, timeout=30, headers=HEADERS_STREAM)
        self.raw = self.response.raw
        self.raw.decode_content = True
        self.current_pos = 0
        self._closed = False

    def read(self, size=-1):
        if self._closed: raise ValueError("I/O on closed file")
        data = self.raw.read(size)
        if data: self.current_pos += len(data)
        return data

    def seek(self, offset, whence=0):
        if whence == 0: self.current_pos = offset
        elif whence == 1: self.current_pos += offset
        elif whence == 2: self.current_pos = self.total_size + offset
        return self.current_pos
    
    def tell(self): return self.current_pos
    def close(self):
        if not self._closed:
            self._closed = True
            if hasattr(self, 'response'): self.response.close()
    def fileno(self): return None

# --- 2. BACKGROUND WORKER ---
def worker_loop():
    print("SYSTEM: Queue Worker Started")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    while True:
        try:
            # Wait for a job (blocking)
            job_id, data = JOB_QUEUE.get()
            print(f"WORKER: Processing Job {job_id}")
            JOBS[job_id]['status'] = 'processing'
            
            # Run upload
            result = loop.run_until_complete(perform_upload(data))
            JOBS[job_id]['status'] = 'done'
            JOBS[job_id]['result'] = result
            
        except Exception as e:
            print(f"WORKER ERROR: {e}")
            if 'job_id' in locals():
                JOBS[job_id]['status'] = 'failed'
                JOBS[job_id]['error'] = str(e)
        finally:
            if 'job_id' in locals():
                JOB_QUEUE.task_done()

async def perform_upload(data):
    file_url = data['url']
    chat_id = data['chat_id']
    caption = data['caption']
    filename = data.get('filename', 'video.mp4')

    # Handle Username vs ID
    target = chat_id
    try:
        target = int(chat_id)
    except:
        pass

    async with Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workdir="/tmp") as app:
        # Resolve Peer
        try:
            chat = await app.get_chat(target)
            target = chat.id
        except:
            print("WORKER: Could not resolve peer, trying blind upload...")

        # Upload
        with SmartStream(file_url, filename) as stream:
            if stream.total_size == 0:
                raise Exception("File size 0. Link expired.")
            
            print(f"WORKER: Streaming {filename}...")
            msg = await app.send_video(
                chat_id=target,
                video=stream,
                caption=caption,
                supports_streaming=True,
                file_name=filename,
                progress=lambda c, t: print(f"Up: {c/1024/1024:.1f}MB") if c % (20*1024*1024) == 0 else None
            )
            
            return {
                "message_id": msg.id,
                "chat_id": msg.chat.id,
                "link": f"https://t.me/c/{str(msg.chat.id).replace('-100', '')}/{msg.id}"
            }

# --- HELPER: WAKE UP WORKER ---
def ensure_worker_alive():
    global WORKER_THREAD
    if WORKER_THREAD is None or not WORKER_THREAD.is_alive():
        print("SYSTEM: Restarting Zombie Thread...")
        WORKER_THREAD = threading.Thread(target=worker_loop, daemon=True)
        WORKER_THREAD.start()

# --- ROUTES ---

@app.route('/')
def home(): 
    ensure_worker_alive()
    return f"Queue Active. Jobs: {JOB_QUEUE.qsize()}"

@app.route('/upload-telegram', methods=['POST'])
def upload_telegram():
    data = request.json
    if not data.get('url') or not data.get('chat_id'):
        return jsonify({"error": "Missing params"}), 400
    
    # 1. Wake up the worker if it died
    ensure_worker_alive()
    
    # 2. Add job
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {'status': 'queued', 'submitted_at': time.time()}
    JOB_QUEUE.put((job_id, data))
    
    print(f"API: Added Job {job_id} to queue")
    return jsonify({"job_id": job_id, "status": "queued"})

@app.route('/job-status/<job_id>', methods=['GET'])
def job_status(job_id):
    # Wake up worker just in case
    ensure_worker_alive()
    
    job = JOBS.get(job_id)
    if not job: return jsonify({"status": "not_found"}), 404
    return jsonify(job)

# --- SEEDR ROUTES (UNCHANGED) ---
@app.route('/auth/code', methods=['GET'])
def get_code():
    try:
        resp = requests.get("https://www.seedr.cc/oauth_device/create", params={"client_id": "seedr_xbmc"})
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/auth/token', methods=['GET'])
def get_token():
    try:
        resp = requests.get("https://www.seedr.cc/oauth_device/token", params={"client_id": "seedr_xbmc", "grant_type": "device_token", "device_code": request.args.get('device_code')})
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/add-magnet', methods=['POST'])
def add_magnet():
    try:
        resp = requests.post("https://www.seedr.cc/oauth_test/resource.php?json=1", data={"access_token": request.json.get('token'), "func": "add_torrent", "torrent_magnet": request.json.get('magnet')})
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)})

@app.route('/list-files', methods=['POST'])
def list_files():
    data = request.json
    token = data.get('token')
    folder_id = str(data.get('folder_id', "0"))
    if folder_id == "0": url = "https://www.seedr.cc/api/folder"
    else: url = f"https://www.seedr.cc/api/folder/{folder_id}"
    try:
        resp = requests.get(url, params={"access_token": token}, headers=HEADERS_STREAM)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/get-link', methods=['POST'])
def get_link():
    try:
        resp = requests.post("https://www.seedr.cc/oauth_test/resource.php?json=1", data={"access_token": request.json.get('token'), "func": "fetch_file", "folder_file_id": str(request.json.get('file_id'))})
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    # Start initial worker
    ensure_worker_alive()
    app.run(host='0.0.0.0', port=10000)
