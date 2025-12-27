import os
import threading
import asyncio
import requests
import queue
import uuid
import time
import re
from datetime import datetime, timedelta
from io import IOBase
from flask import Flask, request, jsonify
from pyrogram import Client
from pyrogram.errors import FloodWait, ChannelPrivate, ChatAdminRequired
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

app = Flask(__name__)

# --- CONFIGURATION ---
API_ID = os.environ.get("TG_API_ID")
API_HASH = os.environ.get("TG_API_HASH")
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")
ADMIN_CHAT_ID = os.environ.get("ADMIN_CHAT_ID", "7197806663")

if API_ID:
    try:
        API_ID = int(API_ID)
    except:
        pass

HEADERS_STREAM = {
    "User-Agent": "Seedr Android/1.0",
    "Content-Type": "application/x-www-form-urlencoded"
}

# Message collection storage
MESSAGE_SESSIONS = {}  # {poster_message_id: {data}}
SESSION_LOCK = threading.Lock()

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

# --- 2. METADATA EXTRACTION ---
def extract_metadata_from_magnet(magnet_link):
    """Extract metadata from magnet name"""
    try:
        match = re.search(r'dn=([^&]+)', magnet_link)
        if not match:
            return {}
        
        name = match.group(1)
        name = name.replace('+', ' ').replace('%20', ' ').replace('%28', '(').replace('%29', ')')
        
        metadata = {}
        
        # Extract year
        year_match = re.search(r'(19|20)\d{2}', name)
        if year_match:
            metadata['year'] = year_match.group(0)
        
        # Extract quality/resolution
        quality_match = re.search(r'(480p|720p|1080p|2160p|4k)', name, re.IGNORECASE)
        if quality_match:
            metadata['resolution'] = quality_match.group(0).lower()
        
        # Extract language
        languages = ['Tamil', 'Telugu', 'Hindi', 'English', 'Malayalam', 'Kannada']
        for lang in languages:
            if re.search(lang, name, re.IGNORECASE):
                metadata['language'] = lang
                break
        
        # Extract source type
        if re.search(r'(WEB-DL|BluRay|WEBRip|BRRip)', name, re.IGNORECASE):
            metadata['quality_type'] = 'HD PRINT'
        elif re.search(r'(HDTV|CAM|HDCAM|TS|TC|PreDVD)', name, re.IGNORECASE):
            metadata['quality_type'] = 'THEATRE PRINT'
        
        # Clean title
        title = re.sub(r'(19|20)\d{2}', '', name)
        title = re.sub(r'(480p|720p|1080p|2160p|4k)', '', title, flags=re.IGNORECASE)
        title = re.sub(r'(WEB-DL|BluRay|WEBRip|HDTV|CAM|x264|x265|HEVC|AAC|DTS|5\.1|Tamil|Telugu|Hindi|English)', '', title, flags=re.IGNORECASE)
        title = re.sub(r'[._\-]+', ' ', title).strip()
        metadata['title'] = title
        
        return metadata
        
    except Exception as e:
        print(f"Metadata extraction error: {e}", flush=True)
        return {}

def detect_quality_from_size(size_bytes):
    """Detect quality from file size"""
    size_mb = size_bytes / (1024 * 1024)
    
    if size_mb < 900:
        return '480p'
    elif 900 <= size_mb < 1500:
        return '720p'
    elif 1500 <= size_mb <= 2048:
        return '1080p'
    else:
        return None  # Too large

# --- 3. ASYNC UPLOAD LOGIC ---
async def perform_upload(file_url, chat_target, caption, filename, file_size_mb=0):
    """Upload video with retry"""
    
    if file_size_mb > 2048:
        raise Exception(f"File too large: {file_size_mb:.1f}MB (max 2048MB)")
    
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            async with Client(
                "bot_session", 
                api_id=API_ID, 
                api_hash=API_HASH, 
                bot_token=BOT_TOKEN, 
                workdir="/tmp"
            ) as app:
                print(f"WORKER: Bot connected! (Attempt {retry_count + 1}/{max_retries})", flush=True)
                
                final_chat_id = None
                chat_str = str(chat_target).strip()
                
                if chat_str.startswith("@"):
                    chat = await app.get_chat(chat_str)
                    final_chat_id = chat.id
                    print(f"WORKER: ✅ Resolved to ID: {final_chat_id}", flush=True)
                elif chat_str.lstrip("-").isdigit():
                    final_chat_id = int(chat_str)
                else:
                    raise Exception(f"Invalid chat format: {chat_str}")
                
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
        
        except FloodWait as e:
            print(f"WORKER: FloodWait {e.value}s, waiting...", flush=True)
            await asyncio.sleep(e.value)
            retry_count += 1
            
        except Exception as e:
            retry_count += 1
            if retry_count >= max_retries:
                raise Exception(f"Upload failed after {max_retries} retries: {e}")
            print(f"WORKER: Retry {retry_count}/{max_retries} due to: {e}", flush=True)
            await asyncio.sleep(5)

# --- 4. SEND NOTIFICATION ---
async def send_admin_notification(message, reply_markup=None):
    """Send notification to admin"""
    if not ADMIN_CHAT_ID:
        print(f"NOTIFICATION: {message}", flush=True)
        return
    
    try:
        async with Client(
            "bot_session", 
            api_id=API_ID, 
            api_hash=API_HASH, 
            bot_token=BOT_TOKEN, 
            workdir="/tmp"
        ) as app:
            await app.send_message(
                chat_id=int(ADMIN_CHAT_ID),
                text=message,
                reply_markup=reply_markup
            )
            print(f"NOTIFICATION SENT", flush=True)
    except Exception as e:
        print(f"NOTIFICATION ERROR: {e}", flush=True)

# --- 5. QUEUE WORKER ---
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
            
            file_size_mb = data.get('file_size_mb', 0)
            
            if file_size_mb > 2048:
                movie_name = data.get('caption', 'Unknown')
                notification_msg = f"⚠️ **Skipped Upload**\n\n" \
                                   f"Movie: {movie_name}\n" \
                                   f"File size: {file_size_mb:.1f}MB\n" \
                                   f"Reason: Exceeds 2GB Seedr limit"
                loop.run_until_complete(send_admin_notification(notification_msg))
                raise Exception(f"File too large: {file_size_mb:.1f}MB")
            
            result = loop.run_until_complete(perform_upload(
                file_url=data['url'],
                chat_target=data['chat_id'],
                caption=data.get('caption', ''),
                filename=data.get('filename', 'video.mp4'),
                file_size_mb=file_size_mb
            ))
            
            JOBS[job_id]['status'] = 'done'
            JOBS[job_id]['result'] = result
            JOBS[job_id]['completed'] = time.time()
            print(f"WORKER: ✅ Job {job_id} done!", flush=True)
            
        except Exception as e:
            error_msg = str(e)
            print(f"WORKER ERROR: {error_msg}", flush=True)
            
            if "failed after" in error_msg or "too large" in error_msg:
                loop.run_until_complete(send_admin_notification(f"Job {job_id}: {error_msg}"))
            
            if job_id:
                JOBS[job_id]['status'] = 'failed'
                JOBS[job_id]['error'] = error_msg
                JOBS[job_id]['failed'] = time.time()
        finally:
            if job_id:
                JOB_QUEUE.task_done()
            
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
        "sessions": len(MESSAGE_SESSIONS),
        "worker_alive": WORKER_THREAD.is_alive() if WORKER_THREAD else False
    })

# ✅ FIX #1: UPDATED SESSION ROUTES
@app.route('/start-session', methods=['POST'])
def start_session():
    """Start message collection session"""
    data = request.json
    poster_msg_id = str(data.get('poster_message_id'))  # ✅ Force string
    
    with SESSION_LOCK:
        MESSAGE_SESSIONS[poster_msg_id] = {
            'created': time.time(),
            'timeout': time.time() + 300,  # 5 minutes
            'metadata': data.get('metadata', {}),
            'magnets': [],
            'status': 'collecting'
        }
    
    print(f"SESSION: Started \"{poster_msg_id}\"", flush=True)
    return jsonify({"status": "session_started", "poster_msg_id": poster_msg_id})

@app.route('/add-magnet-to-session', methods=['POST'])
def add_magnet_to_session():
    """Add magnet to session"""
    data = request.json
    poster_msg_id = str(data.get('poster_message_id'))  # ✅ Force string
    magnet = data.get('magnet')
    
    with SESSION_LOCK:
        if poster_msg_id not in MESSAGE_SESSIONS:
            print(f"SESSION ERROR: \"{poster_msg_id}\" not found. Available: {list(MESSAGE_SESSIONS.keys())}", flush=True)
            return jsonify({"error": "Session not found", "session_id": poster_msg_id}), 404
        
        session = MESSAGE_SESSIONS[poster_msg_id]
        
        if time.time() > session['timeout']:
            del MESSAGE_SESSIONS[poster_msg_id]
            return jsonify({"error": "timeout", "message": "⏱️ Workflow timeout. Please send magnets and type 'done' to restart."}), 408
        
        if len(session['magnets']) >= 3:
            return jsonify({"error": "max_magnets", "message": "⚠️ Maximum 3 qualities allowed."}), 400
        
        session['magnets'].append(magnet)
        print(f"SESSION: Added magnet {len(session['magnets'])}/3 to \"{poster_msg_id}\"", flush=True)
    
    return jsonify({"status": "magnet_added", "count": len(session['magnets'])})

@app.route('/get-session/<poster_msg_id>', methods=['GET'])
def get_session(poster_msg_id):
    """Get session data"""
    poster_msg_id = str(poster_msg_id)  # ✅ Force string
    
    with SESSION_LOCK:
        if poster_msg_id not in MESSAGE_SESSIONS:
            return jsonify({"error": "Session not found"}), 404
        
        session = MESSAGE_SESSIONS[poster_msg_id]
        
        if time.time() > session['timeout']:
            del MESSAGE_SESSIONS[poster_msg_id]
            return jsonify({"error": "timeout"}), 408
        
        return jsonify(session)

@app.route('/complete-session', methods=['POST'])
def complete_session():
    """Mark session as complete and return data"""
    data = request.json
    poster_msg_id = str(data.get('poster_message_id'))  # ✅ Force string
    
    with SESSION_LOCK:
        if poster_msg_id not in MESSAGE_SESSIONS:
            print(f"SESSION ERROR: \"{poster_msg_id}\" not found. Available: {list(MESSAGE_SESSIONS.keys())}", flush=True)
            return jsonify({"error": "Session not found", "session_id": poster_msg_id}), 404
        
        session = MESSAGE_SESSIONS[poster_msg_id]
        
        if len(session['magnets']) < 1:
            return jsonify({"error": "no_magnets", "message": "⚠️ No magnet links found. Please send at least 1 magnet link."}), 400
        
        result = {
            'metadata': session['metadata'],
            'magnets': session['magnets'],
            'count': len(session['magnets'])
        }
        
        del MESSAGE_SESSIONS[poster_msg_id]
        print(f"SESSION: Completed \"{poster_msg_id}\" with {len(session['magnets'])} magnets", flush=True)
        
        return jsonify(result)

# ✅ NEW: DEBUG ENDPOINT
@app.route('/debug/sessions', methods=['GET'])
def debug_sessions():
    """Debug: Show all active sessions"""
    with SESSION_LOCK:
        return jsonify({
            "active_sessions": list(MESSAGE_SESSIONS.keys()),
            "session_data": {k: {
                "magnets": len(v['magnets']),
                "created": v['created'],
                "timeout": v['timeout'],
                "time_left": int(v['timeout'] - time.time())
            } for k, v in MESSAGE_SESSIONS.items()}
        })

@app.route('/upload-telegram', methods=['POST'])
def upload_telegram():
    """Upload video"""
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
    
    return jsonify({"job_id": job_id, "status": "queued"})

@app.route('/job-status/<job_id>', methods=['GET'])
def job_status(job_id):
    """Check job status"""
    ensure_worker_alive()
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"status": "not_found"}), 404
    return jsonify(job)

@app.route('/extract-metadata', methods=['POST'])
def extract_metadata():
    """Extract metadata from magnet"""
    try:
        magnet = request.json.get('magnet', '')
        metadata = extract_metadata_from_magnet(magnet)
        return jsonify(metadata)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/detect-quality-from-size', methods=['POST'])
def detect_quality_api():
    """Detect quality from file size"""
    try:
        size_bytes = int(request.json.get('size_bytes', 0))
        quality = detect_quality_from_size(size_bytes)
        return jsonify({"quality": quality})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# --- SEEDR ROUTES WITH RETRY ---
@app.route('/add-magnet', methods=['POST'])
def add_magnet():
    """Add magnet to Seedr with retry"""
    max_retries = 3
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
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
            result = resp.json()
            
            if 'error' in result or result.get('result') == False:
                raise Exception(result.get('error', 'Unknown Seedr error'))
            
            return jsonify(result)
            
        except Exception as e:
            last_error = str(e)
            retry_count += 1
            if retry_count < max_retries:
                print(f"SEEDR: Add magnet retry {retry_count}/{max_retries}", flush=True)
                time.sleep(10)
            else:
                asyncio.run(send_admin_notification(
                    f"⚠️ **Seedr Add Magnet Failed**\n\n"
                    f"Retries: {max_retries}/{max_retries}\n"
                    f"Error: {last_error}\n"
                    f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M UTC')}"
                ))
                return jsonify({"error": last_error, "retries": max_retries}), 500

@app.route('/list-files', methods=['POST'])
def list_files():
    """List Seedr files with retry"""
    max_retries = 3
    retry_count = 0
    last_error = None
    
    while retry_count < max_retries:
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
            result = resp.json()
            
            # Check if files exist
            if 'folders' in result and len(result.get('folders', [])) == 0 and len(result.get('files', [])) == 0:
                if retry_count < max_retries - 1:
                    raise Exception("No files found yet")
            
            return jsonify(result)
            
        except Exception as e:
            last_error = str(e)
            retry_count += 1
            if retry_count < max_retries:
                print(f"SEEDR: List files retry {retry_count}/{max_retries}", flush=True)
                time.sleep(10)
            else:
                return jsonify({"error": last_error, "retries": max_retries}), 500

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

# ✅ FIX #3: UPDATED DELETE FOLDER ROUTE
@app.route('/delete-folder', methods=['POST'])
def delete_folder():
    """Delete folder from Seedr with verification"""
    try:
        folder_id = request.json.get('folder_id')
        token = request.json.get('token')
        
        # Validate folder_id
        if not folder_id or folder_id == 'null' or folder_id == 'None' or str(folder_id).lower() == 'none':
            print(f"SEEDR ERROR: Invalid folder_id: {folder_id}", flush=True)
            return jsonify({"error": "Invalid folder_id", "received": str(folder_id)}), 400
        
        # Convert to string
        folder_id = str(folder_id)
        
        print(f"SEEDR: Attempting to delete folder \"{folder_id}\"", flush=True)
        
        # ✅ FIX: Use proper JSON format for delete_arr
        import json
        
        resp = requests.post(
            "https://www.seedr.cc/oauth_test/resource.php?json=1",
            data={
                "access_token": token,
                "func": "delete",
                "delete_arr": json.dumps([int(folder_id)])  # ✅ Convert to int and use json.dumps
            },
            timeout=30
        )
        
        result = resp.json()
        print(f"SEEDR: Delete response: {result}", flush=True)
        
        # ✅ Verify deletion by listing folders again
        verify_resp = requests.get(
            "https://www.seedr.cc/api/folder",
            params={"access_token": token},
            timeout=30
        )
        
        verify_data = verify_resp.json()
        remaining_folders = [f['id'] for f in verify_data.get('folders', [])]
        
        if int(folder_id) in remaining_folders:
            print(f"SEEDR WARNING: Folder {folder_id} still exists after delete!", flush=True)
            # Try deleting again
            resp2 = requests.post(
                "https://www.seedr.cc/oauth_test/resource.php?json=1",
                data={
                    "access_token": token,
                    "func": "delete",
                    "delete_arr": json.dumps([int(folder_id)])
                },
                timeout=30
            )
            print(f"SEEDR: Second delete attempt: {resp2.json()}", flush=True)
        else:
            print(f"SEEDR: ✅ Folder {folder_id} successfully deleted!", flush=True)
        
        return jsonify(result)
        
    except Exception as e:
        print(f"SEEDR ERROR: {e}", flush=True)
        return jsonify({"error": str(e)}), 500

# --- CLEANUP EXPIRED SESSIONS ---
def cleanup_sessions():
    """Remove expired sessions"""
    while True:
        try:
            time.sleep(60)  # Check every minute
            current_time = time.time()
            
            with SESSION_LOCK:
                expired = [k for k, v in MESSAGE_SESSIONS.items() if current_time > v['timeout']]
                for session_id in expired:
                    del MESSAGE_SESSIONS[session_id]
                    print(f"SESSION: Cleaned up expired \"{session_id}\"", flush=True)
        except Exception as e:
            print(f"CLEANUP ERROR: {e}", flush=True)

# Start cleanup thread
cleanup_thread = threading.Thread(target=cleanup_sessions, daemon=True)
cleanup_thread.start()

if __name__ == '__main__':
    print("=" * 50, flush=True)
    print("🚀 Seedr-Telegram Bridge Starting", flush=True)
    print("=" * 50, flush=True)
    ensure_worker_alive()
    app.run(host='0.0.0.0', port=10000)
