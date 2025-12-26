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
from pyrogram.errors import UserAlreadyParticipant

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
            print(f"STREAM: Size {self.total_size}", flush=True)
        except:
            self.total_size = 0
        self.response = requests.get(url, stream=True, timeout=30, headers=HEADERS_STREAM)
        self.raw = self.response.raw
        self.raw.decode_content = True
        self.current_pos = 0
        self._closed = False
    def read(self, size=-1):
        if self._closed: raise ValueError("I/O closed")
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

# --- 2. ASYNC UPLOAD LOGIC ---
async def perform_upload(data):
    file_url = data['url']
    chat_target = data['chat_id']
    caption = data['caption']
    filename = data.get('filename', 'video.mp4')

    # Start Pyrogram Client
    async with Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workdir="/tmp") as app:
        print("WORKER: Bot connected!", flush=True)
        
        final_chat_id = None
        
        # --- A. RESOLVE TARGET (Link vs ID vs Username) ---
        
        # Case 1: It is an Invite Link (https://t.me/+...)
        if "t.me/" in str(chat_target) or "joinchat" in str(chat_target):
            print("WORKER: Detected Invite Link. Joining...", flush=True)
            try:
                # join_chat returns the Chat object with the ID
                chat = await app.join_chat(chat_target)
                final_chat_id = chat.id
                print(f"WORKER: Joined! Resolved to ID: {final_chat_id}", flush=True)
            except UserAlreadyParticipant:
                print("WORKER: Already joined. resolving via get_chat...", flush=True)
                # If already joined, we can try getting the chat info using the link or blindly proceed
                try:
                    # Sometimes get_chat works with invite links if bot is member
                    chat = await app.get_chat(chat_target)
                    final_chat_id = chat.id
                except:
                    print("WORKER: Could not resolve link directly. Are you sure the Bot is Admin?", flush=True)
                    raise Exception("Bot is in channel but cannot resolve ID. Ensure Bot is Admin.")

        # Case 2: It is a Username (@movie) or ID (-100...)
        else:
            try:
                peer = int(chat_target) # Try ID
            except:
                peer = chat_target # Keep Username
            
            try:
                print(f"WORKER: Resolving {peer}...", flush=True)
                chat = await app.get_chat(peer)
                final_chat_id = chat.id
            except Exception as e:
                print(f"WORKER ERROR: Resolve failed: {e}", flush=True)
                # If ID fails resolution, we might be hitting Cold Start. 
                # Only Invite Links solve Cold Start for Private Channels.
                if isinstance(peer, int):
                    final_chat_id = peer # Try blind upload

        if not final_chat_id:
            raise Exception("Could not resolve Chat ID. Please check permissions or use Invite Link.")

        # --- B. STREAM UPLOAD ---
        with SmartStream(file_url, filename) as stream:
            if stream.total_size == 0:
                raise Exception("File size 0. Link expired.")
            
            print(f"WORKER: Streaming to {final_chat_id}...", flush=True)
            msg = await app.send_video(
                chat_id=final_chat_id,
                video=stream,
                caption=caption,
                file_name=filename,
                supports_streaming=True,
                progress=lambda c, t: print(f"Up: {c/1024/1024:.1f}MB") if c % (20*1024*1024) == 0 else None
            )
            
            # Generate Deep Link
            clean_id = str(msg.chat.id).replace('-100', '')
            return {
                "message_id": msg.id,
                "chat_id": msg.chat.id,
                "link": f"https://t.me/c/{clean_id}/{msg.id}"
            }

# --- 3. QUEUE WORKER (SYNC WRAPPER) ---
JOB_QUEUE = queue.Queue()
JOBS = {} 
WORKER_THREAD = None

def worker_loop():
    print("SYSTEM: Queue Worker Started", flush=True)
    # Create the Event Loop for this thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    while True:
        try:
            job_id, data = JOB_QUEUE.get()
            print(f"WORKER: Job {job_id}", flush=True)
            JOBS[job_id]['status'] = 'processing'
            
            # Run the Async Upload Logic inside this Sync Loop
            result = loop.run_until_complete(perform_upload(data))
            
            JOBS[job_id]['status'] = 'done'
            JOBS[job_id]['result'] = result
            print(f"WORKER: Job Done! Link: {result['link']}", flush=True)
            
        except Exception as e:
            print(f"WORKER ERROR: {e}", flush=True)
            if 'job_id' in locals():
                JOBS[job_id]['status'] = 'failed'
                JOBS[job_id]['error'] = str(e)
        finally:
            if 'job_id' in locals(): JOB_QUEUE.task_done()

def ensure_worker_alive():
    global WORKER_THREAD
    if WORKER_THREAD is None or not WORKER_THREAD.is_alive():
        print("SYSTEM: Restarting Zombie Thread...", flush=True)
        WORKER_THREAD = threading.Thread(target=worker_loop, daemon=True)
        WORKER_THREAD.start()

# --- ROUTES ---
@app.route('/')
def home(): 
    ensure_worker_alive()
    return "Ready"

@app.route('/upload-telegram', methods=['POST'])
def upload_telegram():
    data = request.json
    if not data.get('url') or not data.get('chat_id'): return jsonify({"error": "Missing params"}), 400
    ensure_worker_alive()
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {'status': 'queued'}
    JOB_QUEUE.put((job_id, data))
    return jsonify({"job_id": job_id, "status": "queued"})

@app.route('/job-status/<job_id>', methods=['GET'])
def job_status(job_id):
    ensure_worker_alive()
    job = JOBS.get(job_id)
    if not job: return jsonify({"status": "not_found"}), 404
    return jsonify(job)

# SEEDR ROUTES
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
    except: return jsonify({})

@app.route('/list-files', methods=['POST'])
def list_files():
    data = request.json
    token = data.get('token')
    folder_id = str(data.get('folder_id', "0"))
    if folder_id == "0": url = "https://www.seedr.cc/api/folder"
    else: url = f"https://www.seedr.cc/api/folder/{folder_id}"
    params = {"access_token": token}
    try:
        resp = requests.get(url, params=params, headers=HEADERS_STREAM)
        return jsonify(resp.json())
    except: return jsonify({})

@app.route('/get-link', methods=['POST'])
def get_link():
    try:
        resp = requests.post("https://www.seedr.cc/oauth_test/resource.php?json=1", data={"access_token": request.json.get('token'), "func": "fetch_file", "folder_file_id": str(request.json.get('file_id'))})
        return jsonify(resp.json())
    except: return jsonify({})

if __name__ == '__main__':
    ensure_worker_alive()
    app.run(host='0.0.0.0', port=10000)
