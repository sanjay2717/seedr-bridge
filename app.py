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
from pyrogram.errors import UserAlreadyParticipant, FloodWait

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
    
    def tell(self): 
        return self.current_pos
    
    def close(self):
        if not self._closed:
            self._closed = True
            if hasattr(self, 'response'): 
                self.response.close()
    
    def fileno(self): 
        return None

# --- 2. ASYNC UPLOAD LOGIC (FIXED!) ---
async def perform_upload(file_url, chat_target, caption, filename):
    """Main upload worker - now properly async"""
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
        
        # --- STRATEGY 1: INVITE LINK ---
        if "t.me/+" in chat_str or "joinchat" in chat_str:
            print("WORKER: Invite link detected", flush=True)
            try:
                chat = await app.join_chat(chat_str)
                final_chat_id = chat.id
                print(f"WORKER: ✅ Resolved Link → ID: {final_chat_id}", flush=True)
                
            except UserAlreadyParticipant:
                # FIX: Re-fetch the chat to get the ID
                print("WORKER: Already in channel, re-fetching...", flush=True)
                try:
                    # Extract hash and try again
                    chat = await app.get_chat(chat_str)
                    final_chat_id = chat.id
                    print(f"WORKER: ✅ Fallback resolved → ID: {final_chat_id}", flush=True)
                except Exception as e:
                    raise Exception(f"UserAlreadyParticipant but cannot resolve: {e}")
                    
            except FloodWait as e:
                print(f"WORKER: FloodWait {e.value}s...", flush=True)
                await asyncio.sleep(e.value)
                chat = await app.join_chat(chat_str)
                final_chat_id = chat.id
                
            except Exception as e:
                raise Exception(f"Invite link failed: {e}")
        
        # --- STRATEGY 2: USERNAME ---
        elif chat_str.startswith("@") or (not chat_str.lstrip("-").isdigit()):
            print(f"WORKER: Username: {chat_str}", flush=True)
            try:
                chat = await app.get_chat(chat_str)
                final_chat_id = chat.id
                print(f"WORKER: ✅ Resolved → ID: {final_chat_id}", flush=True)
            except Exception as e:
                raise Exception(f"Username failed: {e}")
        
        # --- STRATEGY 3: NUMERIC ID ---
        else:
            try:
                final_chat_id = int(chat_str)
                print(f"WORKER: Numeric ID: {final_chat_id}", flush=True)
                await app.get_chat(final_chat_id)  # Validate
                print(f"WORKER: ✅ Valid", flush=True)
            except Exception as e:
                raise Exception(f"Numeric ID failed: {e}. Use invite link for private channels.")
        
        if not final_chat_id:
            raise Exception("Could not resolve chat")
        
        # --- UPLOAD ---
        with SmartStream(file_url, filename) as stream:
            if stream.total_size == 0:
                raise Exception("File size 0. Link expired.")
            
            print(f"WORKER: Uploading {filename}...", flush=True)
            msg = await app.send_video(
                chat_id=final_chat_id,
                video=stream,
                caption=caption,
                file_name=filename,
                supports_streaming=True,
                progress=lambda c, t: print(f"Up: {c/1024/1024:.1f}MB", flush=True) if c % (20*1024*1024) == 0 else None
            )
            
            clean_id = str(msg.chat.id).replace('-100', '')
            return {
                "message_id": msg.id,
                "chat_id": msg.chat.id,
                "file_id": msg.video.file_id,  # ✅ FIXED: Added file_id
                "link": f"https://t.me/c/{clean_id}/{msg.id}"
            }

# --- 3. QUEUE WORKER ---
JOB_QUEUE = queue.Queue()
JOBS = {} 
WORKER_THREAD = None
WORKER_LOCK = threading.Lock()

def worker_loop():
    """Background worker - FIXED to use single event loop"""
    print("SYSTEM: Queue Worker Started", flush=True)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    while True:
        job_id = None
        try:
            job_id, data = JOB_QUEUE.get()
            print(f"WORKER: Job {job_id}", flush=True)
            JOBS[job_id]['status'] = 'processing'
            
            # ✅ FIXED: Direct async call instead of nested function
            result = loop.run_until_complete(perform_upload(
                file_url=data['url'],
                chat_target=data['chat_id'],
                caption=data['caption'],
                filename=data.get('filename', 'video.mp4')
            ))
            
            JOBS[job_id]['status'] = 'done'
            JOBS[job_id]['result'] = result
            print(f"WORKER: ✅ Job {job_id} done!", flush=True)
            
        except Exception as e:
            print(f"WORKER ERROR: {e}", flush=True)
            if job_id:
                JOBS[job_id]['status'] = 'failed'
                JOBS[job_id]['error'] = str(e)
        finally:
            if job_id:
                JOB_QUEUE.task_done()

def ensure_worker_alive():
    """Start worker thread with thread safety"""
    global WORKER_THREAD
    with WORKER_LOCK:
        if WORKER_THREAD is None or not WORKER_THREAD.is_alive():
            print("SYSTEM: Starting worker thread...", flush=True)
            WORKER_THREAD = threading.Thread(target=worker_loop, daemon=True)
            WORKER_THREAD.start()

# --- ROUTES ---
@app.route('/')
def home(): 
    ensure_worker_alive()
    return jsonify({"status": "online", "queue": JOB_QUEUE.qsize(), "jobs": len(JOBS)})

@app.route('/upload-telegram', methods=['POST'])
def upload_telegram():
    data = request.json
    if not data.get('url') or not data.get('chat_id'): 
        return jsonify({"error": "Missing url or chat_id"}), 400
    
    ensure_worker_alive()
    job_id = str(uuid.uuid4())
    JOBS[job_id] = {'status': 'queued', 'created': time.time()}
    JOB_QUEUE.put((job_id, data))
    return jsonify({"job_id": job_id, "status": "queued"})

@app.route('/job-status/<job_id>', methods=['GET'])
def job_status(job_id):
    ensure_worker_alive()
    job = JOBS.get(job_id)
    if not job: 
        return jsonify({"status": "not_found"}), 404
    return jsonify(job)

# --- SEEDR ROUTES (FIXED ERROR HANDLING) ---
@app.route('/add-magnet', methods=['POST'])
def add_magnet():
    try:
        resp = requests.post(
            "https://www.seedr.cc/oauth_test/resource.php?json=1",
            data={
                "access_token": request.json.get('token'),
                "func": "add_torrent",
                "torrent_magnet": request.json.get('magnet')
            }
        )
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/list-files', methods=['POST'])
def list_files():
    data = request.json
    token = data.get('token')
    folder_id = str(data.get('folder_id', "0"))
    
    if folder_id == "0": 
        url = "https://www.seedr.cc/api/folder"
    else: 
        url = f"https://www.seedr.cc/api/folder/{folder_id}"
    
    params = {"access_token": token}
    try:
        resp = requests.get(url, params=params, headers=HEADERS_STREAM)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get-link', methods=['POST'])
def get_link():
    try:
        resp = requests.post(
            "https://www.seedr.cc/oauth_test/resource.php?json=1",
            data={
                "access_token": request.json.get('token'),
                "func": "fetch_file",
                "folder_file_id": str(request.json.get('file_id'))
            }
        )
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    ensure_worker_alive()
    app.run(host='0.0.0.0', port=10000)
