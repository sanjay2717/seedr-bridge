import os
import threading
import asyncio
import requests
import queue
import uuid
import time
import re
import json
from datetime import datetime, timedelta, timezone
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
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "*/*"
}

# Message collection storage
MESSAGE_SESSIONS = {}
SESSION_LOCK = threading.Lock()

# --- PIKPAK ACCOUNT MANAGEMENT ---
PIKPAK_ACCOUNTS = [
    {
        "email": os.environ.get("PIKPAK_EMAIL_1"),
        "password": os.environ.get("PIKPAK_PASSWORD_1"),
        "access_token": None,
        "refresh_token": None,
        "daily_used": 0,
        "last_reset": None
    },
    {
        "email": os.environ.get("PIKPAK_EMAIL_2"),
        "password": os.environ.get("PIKPAK_PASSWORD_2"),
        "access_token": None,
        "refresh_token": None,
        "daily_used": 0,
        "last_reset": None
    },
    {
        "email": os.environ.get("PIKPAK_EMAIL_3"),
        "password": os.environ.get("PIKPAK_PASSWORD_3"),
        "access_token": None,
        "refresh_token": None,
        "daily_used": 0,
        "last_reset": None
    },
    {
        "email": os.environ.get("PIKPAK_EMAIL_4"),
        "password": os.environ.get("PIKPAK_PASSWORD_4"),
        "access_token": None,
        "refresh_token": None,
        "daily_used": 0,
        "last_reset": None
    }
]

ACCOUNT_LOCK = threading.Lock()

def reset_daily_counters():
    """Reset daily usage at midnight UTC"""
    now = datetime.now(timezone.utc)
    
    with ACCOUNT_LOCK:
        for account in PIKPAK_ACCOUNTS:
            if not account.get('email'):
                continue
                
            last_reset = account.get('last_reset')
            
            # Reset if never reset or different day
            if not last_reset or last_reset.date() < now.date():
                account['daily_used'] = 0
                account['last_reset'] = now
                print(f"PIKPAK: Reset counter for {account['email']}", flush=True)

def get_available_account():
    """Get account with available daily quota"""
    reset_daily_counters()
    
    with ACCOUNT_LOCK:
        for i, account in enumerate(PIKPAK_ACCOUNTS):
            if not account.get('email'):
                continue
            
            if account['daily_used'] < 5:
                print(f"PIKPAK: Using account {i+1} ({account['email']}) - Used: {account['daily_used']}/5", flush=True)
                return account, i
        
        print(f"PIKPAK ERROR: All accounts exhausted for today!", flush=True)
        return None, None

def increment_account_usage(account_index):
    """Increment usage counter for account"""
    with ACCOUNT_LOCK:
        if 0 <= account_index < len(PIKPAK_ACCOUNTS):
            PIKPAK_ACCOUNTS[account_index]['daily_used'] += 1
            print(f"PIKPAK: Account {account_index+1} now at {PIKPAK_ACCOUNTS[account_index]['daily_used']}/5", flush=True)

def pikpak_login(account):
    """Login to PikPak and get access token"""
    try:
        print(f"PIKPAK: Logging in as {account['email']}...", flush=True)
        
        resp = requests.post(
            "https://user.mypikpak.com/v1/auth/signin",
            json={
                "email": account['email'],
                "password": account['password']
            },
            headers={"Content-Type": "application/json"},
            timeout=30
        )
        
        data = resp.json()
        
        if 'access_token' not in data:
            raise Exception(f"Login failed: {data.get('error_description', 'Unknown error')}")
        
        account['access_token'] = data['access_token']
        account['refresh_token'] = data.get('refresh_token')
        
        print(f"PIKPAK: ✅ Login successful for {account['email']}", flush=True)
        return account['access_token']
        
    except Exception as e:
        print(f"PIKPAK LOGIN ERROR: {e}", flush=True)
        return None

def get_pikpak_headers(account):
    """Get headers with valid access token"""
    if not account.get('access_token'):
        pikpak_login(account)
    
    return {
        "Authorization": f"Bearer {account['access_token']}",
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0"
    }

# --- SMART STREAMER ---
class SmartStream(IOBase):
    def __init__(self, url, name):
        super().__init__()
        self.url = url
        self.name = name
        self.mode = 'rb'
        print(f"STREAM: Connecting to {url[:60]}...", flush=True)
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

# --- METADATA EXTRACTION ---
def extract_metadata_from_magnet(magnet_link):
    """Extract metadata from magnet name"""
    try:
        match = re.search(r'dn=([^&]+)', magnet_link)
        if not match:
            return {}
        
        name = match.group(1)
        name = name.replace('+', ' ').replace('%20', ' ').replace('%28', '(').replace('%29', ')')
        
        metadata = {}
        
        year_match = re.search(r'(19|20)\d{2}', name)
        if year_match:
            metadata['year'] = year_match.group(0)
        
        quality_match = re.search(r'(480p|720p|1080p|2160p|4k)', name, re.IGNORECASE)
        if quality_match:
            metadata['resolution'] = quality_match.group(0).lower()
        
        languages = ['Tamil', 'Telugu', 'Hindi', 'English', 'Malayalam', 'Kannada']
        for lang in languages:
            if re.search(lang, name, re.IGNORECASE):
                metadata['language'] = lang
                break
        
        if re.search(r'(WEB-DL|BluRay|WEBRip|BRRip)', name, re.IGNORECASE):
            metadata['quality_type'] = 'HD PRINT'
        elif re.search(r'(HDTV|CAM|HDCAM|TS|TC|PreDVD)', name, re.IGNORECASE):
            metadata['quality_type'] = 'THEATRE PRINT'
        
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
        return None

# --- ASYNC UPLOAD LOGIC ---
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
                        raise Exception("File size is 0. Download link expired.")
                    
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

# --- SEND NOTIFICATION ---
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

# --- QUEUE WORKER ---
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
                                   f"Reason: Exceeds 2GB limit"
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
    reset_daily_counters()
    
    account_status = []
    for i, acc in enumerate(PIKPAK_ACCOUNTS):
        if acc.get('email'):
            account_status.append({
                f"account_{i+1}": f"{acc['daily_used']}/5 used"
            })
    
    return jsonify({
        "status": "online",
        "service": "PikPak Bridge (4 Accounts)",
        "queue": JOB_QUEUE.qsize(),
        "jobs": len(JOBS),
        "sessions": len(MESSAGE_SESSIONS),
        "worker_alive": WORKER_THREAD.is_alive() if WORKER_THREAD else False,
        "accounts": account_status
    })

# --- SESSION ROUTES ---
@app.route('/start-session', methods=['POST'])
def start_session():
    """Start message collection session"""
    data = request.json
    poster_msg_id = str(data.get('poster_message_id'))
    
    with SESSION_LOCK:
        MESSAGE_SESSIONS[poster_msg_id] = {
            'created': time.time(),
            'timeout': time.time() + 300,
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
    poster_msg_id = str(data.get('poster_message_id'))
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
    poster_msg_id = str(poster_msg_id)
    
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
    poster_msg_id = str(data.get('poster_message_id'))
    
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

@app.route('/debug/accounts', methods=['GET'])
def debug_accounts():
    """Debug: Show account usage"""
    reset_daily_counters()
    
    account_info = []
    for i, acc in enumerate(PIKPAK_ACCOUNTS):
        if acc.get('email'):
            account_info.append({
                "account": i + 1,
                "email": acc['email'],
                "daily_used": acc['daily_used'],
                "limit": 5,
                "available": 5 - acc['daily_used'],
                "has_token": bool(acc.get('access_token'))
            })
    
    return jsonify({
        "accounts": account_info,
        "total_available": sum(5 - acc['daily_used'] for acc in PIKPAK_ACCOUNTS if acc.get('email'))
    })

# --- TELEGRAM UPLOAD ROUTES ---
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

# --- PIKPAK API ROUTES ---

@app.route('/add-magnet', methods=['POST'])
def add_magnet():
    """Add magnet to PikPak with account rotation"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            magnet = request.json.get('magnet')
            
            # Get available account
            account, account_index = get_available_account()
            
            if not account:
                # All accounts exhausted
                asyncio.run(send_admin_notification(
                    f"⚠️ **All PikPak Accounts Exhausted**\n\n"
                    f"All 4 accounts have reached their daily limit (5 torrents each).\n"
                    f"Limits reset at midnight UTC.\n"
                    f"Current time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                ))
                return jsonify({
                    "error": "All accounts exhausted",
                    "message": "All 4 PikPak accounts have reached daily limit. Try after midnight UTC."
                }), 429
            
            print(f"PIKPAK: Adding magnet with account {account_index+1}...", flush=True)
            
            headers = get_pikpak_headers(account)
            
            # Add offline download task
            resp = requests.post(
                "https://api-drive.mypikpak.com/drive/v1/files",
                headers=headers,
                json={
                    "kind": "drive#file",
                    "name": "",
                    "upload_type": "UPLOAD_TYPE_URL",
                    "url": {
                        "url": magnet
                    },
                    "folder_type": "DOWNLOAD"
                },
                timeout=30
            )
            
            result = resp.json()
            
            if 'error' in result:
                error_code = result.get('error_code', 0)
                
                # Token expired
                if error_code == 16:
                    print(f"PIKPAK: Token expired, re-logging in...", flush=True)
                    pikpak_login(account)
                    raise Exception("Token expired, retry")
                
                raise Exception(f"PikPak error: {result.get('error_description', result.get('error', 'Unknown'))}")
            
            task = result.get('task', {})
            task_id = task.get('id')
            file_id = result.get('file', {}).get('id')
            
            if not task_id:
                raise Exception("No task ID in response")
            
            # Increment usage
            increment_account_usage(account_index)
            
            print(f"PIKPAK: ✅ Magnet added (Task ID: {task_id}, Account: {account_index+1})", flush=True)
            
            return jsonify({
                "result": True,
                "id": file_id or task_id,
                "task_id": task_id,
                "account_used": account_index + 1,
                "code": 200
            })
            
        except Exception as e:
            last_error = str(e)
            retry_count += 1
            
            if retry_count < max_retries:
                print(f"PIKPAK: Retry {retry_count}/{max_retries} due to: {last_error}", flush=True)
                time.sleep(10)
            else:
                asyncio.run(send_admin_notification(
                    f"⚠️ **PikPak Add Failed**\n\n"
                    f"Retries: {max_retries}/{max_retries}\n"
                    f"Error: {last_error}\n"
                    f"Time: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}"
                ))
                return jsonify({"error": last_error, "retries": max_retries}), 500

@app.route('/list-files', methods=['POST'])
def list_files():
    """List PikPak files"""
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            folder_id = request.json.get('folder_id', '0')
            
            # Get any account (for listing)
            account, _ = get_available_account()
            if not account:
                account = next((a for a in PIKPAK_ACCOUNTS if a.get('email')), None)
            
            if not account:
                return jsonify({"error": "No accounts configured"}), 500
            
            headers = get_pikpak_headers(account)
            
            # List files
            if folder_id == '0':
                # List root (all files)
                resp =requests.get(
"https://api-drive.mypikpak.com/drive/v1/files",
headers=headers,
params={
"thumbnail_size": "SIZE_LARGE",
"limit": 100,
"filters": json.dumps({"phase": {"eq": "PHASE_TYPE_COMPLETE"}})
},
timeout=30
)
else:
# List specific folder
resp = requests.get(
"https://api-drive.mypikpak.com/drive/v1/files",
headers=headers,
params={
"parent_id": folder_id,
"thumbnail_size": "SIZE_LARGE",
"limit": 100
},
timeout=30
)
        result = resp.json()
        
        if 'error' in result:
            error_code = result.get('error_code', 0)
            if error_code == 16:
                pikpak_login(account)
                raise Exception("Token expired, retry")
            raise Exception(f"List error: {result.get('error', 'Unknown')}")
        
        files_data = result.get('files', [])
        
        if folder_id == '0':
            # Return folders (completed downloads)
            folders = [{
                'id': f['id'],
                'name': f['name']
            } for f in files_data if f.get('kind') == 'drive#folder']
            
            return jsonify({"folders": folders, "files": []})
        else:
            # Return files in folder
            files = [{
                'folder_file_id': f['id'],
                'name': f['name'],
                'size': f.get('size', 0)
            } for f in files_data if f.get('kind') == 'drive#file']
            
            return jsonify({"files": files, "folders": []})
        
    except Exception as e:
        retry_count += 1
        if retry_count < max_retries:
            print(f"PIKPAK: List retry {retry_count}/{max_retries}", flush=True)
            time.sleep(10)
        else:
            return jsonify({"error": str(e)}), 500
@app.route('/get-link', methods=['POST'])
def get_link():
"""Get download link from PikPak"""
try:
file_id = request.json.get('file_id')
    # Get any account
    account, _ = get_available_account()
    if not account:
        account = next((a for a in PIKPAK_ACCOUNTS if a.get('email')), None)
    
    if not account:
        return jsonify({"error": "No accounts configured"}), 500
    
    headers = get_pikpak_headers(account)
    
    print(f"PIKPAK: Getting download link for file {file_id}", flush=True)
    
    # Get file info
    resp = requests.get(
        f"https://api-drive.mypikpak.com/drive/v1/files/{file_id}",
        headers=headers,
        timeout=30
    )
    
    result = resp.json()
    
    if 'error' in result:
        raise Exception(f"Get link error: {result.get('error', 'Unknown')}")
    
    # Get web content link
    web_content_link = result.get('web_content_link')
    
    if not web_content_link:
        raise Exception("No download link available")
    
    print(f"PIKPAK: ✅ Got download link", flush=True)
    
    return jsonify({"url": web_content_link})
    
except Exception as e:
    print(f"PIKPAK ERROR: {e}", flush=True)
    return jsonify({"error": str(e)}), 500
@app.route('/delete-folder', methods=['POST'])
def delete_folder():
"""Delete file/folder from PikPak"""
try:
folder_id = str(request.json.get('folder_id'))
    if not folder_id or folder_id == 'null' or folder_id == 'None':
        return jsonify({"error": "Invalid folder_id"}), 400
    
    # Get any account
    account, _ = get_available_account()
    if not account:
        account = next((a for a in PIKPAK_ACCOUNTS if a.get('email')), None)
    
    if not account:
        return jsonify({"error": "No accounts configured"}), 500
    
    headers = get_pikpak_headers(account)
    
    print(f"PIKPAK: Attempting to delete file/folder \"{folder_id}\"", flush=True)
    
    # Delete file
    resp = requests.delete(
        f"https://api-drive.mypikpak.com/drive/v1/files/{folder_id}",
        headers=headers,
        timeout=30
    )
    
    # PikPak returns 204 No Content on successful delete
    if resp.status_code == 204:
        print(f"PIKPAK: ✅ File/folder {folder_id} successfully deleted!", flush=True)
        return jsonify({"result": True, "code": 200})
    
    result = resp.json() if resp.text else {}
    
    if 'error' in result:
        print(f"PIKPAK: Delete error: {result}", flush=True)
    
    print(f"PIKPAK: Delete response: {result}", flush=True)
    
    return jsonify({"result": True, "code": 200})
    
# ... [previous code] ...

except Exception as e:
    print(f"PIKPAK ERROR: {e}", flush=True)
    return jsonify({"error": str(e)}), 500

# --- CLEANUP EXPIRED SESSIONS ---
def cleanup_sessions():
    """Remove expired sessions"""
    while True:
        try:
            time.sleep(60)
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
    print("=" * 60, flush=True)
    print("🚀 PikPak Telegram Bridge Starting (4-Account Rotation)", flush=True)
    print("=" * 60, flush=True)
    # Initialize accounts
    for i, acc in enumerate(PIKPAK_ACCOUNTS):
        if acc.get('email'):
            print(f"Account {i+1}: {acc['email']}", flush=True)

    ensure_worker_alive()
    app.run(host='0.0.0.0', port=10000)
