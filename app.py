import os
import threading
import asyncio
import requests
import queue
import uuid
import time
import re
import json
import hashlib
from datetime import datetime, timedelta
from io import IOBase
from flask import Flask, request, jsonify, render_template, send_from_directory
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
    "Content-Type": "application/x-www-form-urlencoded"
}

EMERGENCY_STOP = False

# Message collection storage
MESSAGE_SESSIONS = {}
SESSION_LOCK = threading.Lock()

# Activity log storage (in memory)
ACTIVITY_LOG = []
MAX_ACTIVITY_LOG = 50

# Daily stats storage
DAILY_STATS = {
    "date": None,
    "downloads": 0,
    "uploads": 0,
    "failed": 0,
    "total_bytes": 0,
    "total_time": 0
}

def log_activity(status, message):
    """Log activity for admin dashboard"""
    global ACTIVITY_LOG
    
    from datetime import datetime
    
    activity = {
        "time": datetime.now().strftime("%H:%M"),
        "status": status,  # "success", "failed", "info"
        "message": message
    }
    
    ACTIVITY_LOG.insert(0, activity)
    
    # Keep only last 50 entries
    if len(ACTIVITY_LOG) > MAX_ACTIVITY_LOG:
        ACTIVITY_LOG = ACTIVITY_LOG[:MAX_ACTIVITY_LOG]

def update_daily_stats(stat_type, value=1):
    """Update daily statistics"""
    global DAILY_STATS
    
    from datetime import datetime
    today = datetime.now().strftime("%Y-%m-%d")
    
    # Reset if new day
    if DAILY_STATS["date"] != today:
        DAILY_STATS = {
            "date": today,
            "downloads": 0,
            "uploads": 0,
            "failed": 0,
            "total_bytes": 0,
            "total_time": 0
        }
    
    if stat_type in DAILY_STATS:
        DAILY_STATS[stat_type] += value

# ============================================================
# PIKPAK CONFIGURATION
# ============================================================

PIKPAK_CLIENT_ID = "YUMx5nI8ZU8Ap8pm"
PIKPAK_CLIENT_SECRET = "dbw2OtmVEeuUvIptb1Coyg"
PIKPAK_CLIENT_VERSION = "2.0.0"
PIKPAK_PACKAGE_NAME = "mypikpak.com"

PIKPAK_API_USER = "https://user.mypikpak.com"
PIKPAK_API_DRIVE = "https://api-drive.mypikpak.com"

# 15 Secret Salts for captcha_sign generation
PIKPAK_SALTS = [
    "C9qPpZLN8ucRTaTiUMWYS9cQvWOE",
    "+r6CQVxjzJV6LCV",
    "F",
    "pFJRC",
    "9WXYIDGrwTCz2OiVlgZa90qpECPD6olt",
    "/750aCr4lm/Sly/c",
    "RB+DT/gZCrbV",
    "",  # Empty salt #8
    "CyLsf7hdkIRxRm215hl",
    "7xHvLi2tOYP0Y92b",
    "ZGTXXxu8E/MIWaEDB+Sm/",
    "1UI3",
    "E7fP5Pfijd+7K+t6Tg/NhuLq0eEUVChpJSkrKxpO",
    "ihtqpG6FMt65+Xk+tWUH2",
    "NhXXU9rg4XXdzo7u5o"
]

# Load PikPak accounts from environment
def load_pikpak_accounts():
    accounts = []
    for i in range(1, 5):  # Accounts 1-4
        email = os.environ.get(f"PIKPAK_{i}_EMAIL")
        if email:
            accounts.append({
                "id": i,
                "email": email,
                "password": os.environ.get(f"PIKPAK_{i}_PASSWORD", ""),
                "device_id": os.environ.get(f"PIKPAK_{i}_DEVICE_ID", ""),
                "my_pack_id": os.environ.get(f"PIKPAK_{i}_MY_PACK_ID", ""),
                "access_token": None,
                "refresh_token": None,
                "user_id": None,
                "token_expires_at": 0,
                "downloads_today": 0,
                "last_download_date": None
            })
    return accounts

PIKPAK_ACCOUNTS = load_pikpak_accounts()
PIKPAK_TOKENS_FILE = "/tmp/pikpak_tokens.json"
PIKPAK_LOCK = threading.Lock()

# ============================================================
# PIKPAK TOKEN STORAGE
# ============================================================

def load_pikpak_tokens():
    """Load tokens from file"""
    try:
        with open(PIKPAK_TOKENS_FILE, 'r') as f:
            return json.load(f)
    except:
        return {}

def save_pikpak_tokens(tokens):
    """Save tokens to file"""
    try:
        with open(PIKPAK_TOKENS_FILE, 'w') as f:
            json.dump(tokens, f)
    except Exception as e:
        print(f"PIKPAK: Failed to save tokens: {e}", flush=True)

def get_account_tokens(account_id):
    """Get tokens for specific account"""
    tokens = load_pikpak_tokens()
    return tokens.get(f"account_{account_id}", {})

def set_account_tokens(account_id, token_data):
    """Save tokens for specific account"""
    tokens = load_pikpak_tokens()
    tokens[f"account_{account_id}"] = token_data
    save_pikpak_tokens(tokens)

# ============================================================
# PIKPAK CAPTCHA SIGN GENERATION
# ============================================================

def generate_captcha_sign(device_id):
    """
    Generate PikPak captcha_sign using MD5 + 15 salts
    Returns: (captcha_sign, timestamp)
    """
    timestamp = str(int(time.time() * 1000))
    
    # Build base string
    base_string = (
        PIKPAK_CLIENT_ID + 
        PIKPAK_CLIENT_VERSION + 
        PIKPAK_PACKAGE_NAME + 
        device_id + 
        timestamp
    )
    
    # Chain hash through 15 salts
    result = base_string
    for salt in PIKPAK_SALTS:
        result = hashlib.md5((result + salt).encode()).hexdigest()
    
    captcha_sign = "1." + result
    
    return captcha_sign, timestamp

# ============================================================
# PIKPAK API HELPERS
# ============================================================

def get_pikpak_captcha(action, device_id, user_id=None, captcha_sign=None, timestamp=None, username=None):
    """
    Get captcha token for PikPak API operation
    
    action: "POST:/v1/auth/signin", "GET:/drive/v1/files", etc.
    username: Required only for login action
    """
    url = f"{PIKPAK_API_USER}/v1/shield/captcha/init"
    
    headers = {
        "Content-Type": "application/json",
        "x-device-id": device_id
    }
    
    # Generate captcha_sign if not provided
    if not captcha_sign or not timestamp:
        captcha_sign, timestamp = generate_captcha_sign(device_id)
    
    # Build meta based on action type
    if "signin" in action:
        # Login requires username in meta
        meta = {
            "username": username
        }
    else:
        # API calls require full meta
        meta = {
            "captcha_sign": captcha_sign,
            "client_version": PIKPAK_CLIENT_VERSION,
            "package_name": PIKPAK_PACKAGE_NAME,
            "timestamp": timestamp,
            "user_id": user_id or ""
        }
    
    body = {
        "client_id": PIKPAK_CLIENT_ID,
        "action": action,
        "device_id": device_id,
        "meta": meta
    }
    
    try:
        response = requests.post(url, headers=headers, json=body, timeout=30)
        data = response.json()
        
        if "captcha_token" in data:
            return data["captcha_token"]
        else:
            print(f"PIKPAK: Captcha error: {data}", flush=True)
            raise Exception(f"Captcha failed: {data.get('error', 'Unknown')}")
    
    except Exception as e:
        print(f"PIKPAK: Captcha request failed: {e}", flush=True)
        raise

def pikpak_login(account):
    """
    Login to PikPak account
    Returns: token data dict
    """
    print(f"PIKPAK: Logging in account {account['id']} ({account['email']})", flush=True)
    
    device_id = account["device_id"]
    email = account["email"]
    password = account["password"]
    
    # Step 1: Get captcha for login
    captcha_token = get_pikpak_captcha(
        action="POST:/v1/auth/signin",
        device_id=device_id,
        username=email
    )
    
    # Step 2: Login
    url = f"{PIKPAK_API_USER}/v1/auth/signin"
    
    headers = {
        "Content-Type": "application/json",
        "x-device-id": device_id,
        "x-captcha-token": captcha_token
    }
    
    body = {
        "client_id": PIKPAK_CLIENT_ID,
        "client_secret": PIKPAK_CLIENT_SECRET,
        "username": email,
        "password": password
    }
    
    response = requests.post(url, headers=headers, json=body, timeout=30)
    data = response.json()
    
    if "access_token" in data:
        token_data = {
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "user_id": data["sub"],
            "expires_at": time.time() + data.get("expires_in", 7200) - 300  # 5 min buffer
        }
        
        # Save tokens
        set_account_tokens(account["id"], token_data)
        
        print(f"PIKPAK: ✅ Login successful for account {account['id']}", flush=True)
        return token_data
    else:
        print(f"PIKPAK: ❌ Login failed: {data}", flush=True)
        raise Exception(f"Login failed: {data.get('error', 'Unknown')}")

def refresh_pikpak_token(account):
    """
    Refresh expired access_token
    """
    print(f"PIKPAK: Refreshing token for account {account['id']}", flush=True)
    
    device_id = account["device_id"]
    tokens = get_account_tokens(account["id"])
    refresh_token = tokens.get("refresh_token")
    user_id = tokens.get("user_id")
    
    if not refresh_token:
        print(f"PIKPAK: No refresh token, doing full login", flush=True)
        return pikpak_login(account)
    
    # Get captcha for token refresh
    captcha_sign, timestamp = generate_captcha_sign(device_id)
    captcha_token = get_pikpak_captcha(
        action="POST:/v1/auth/token",
        device_id=device_id,
        user_id=user_id,
        captcha_sign=captcha_sign,
        timestamp=timestamp
    )
    
    url = f"{PIKPAK_API_USER}/v1/auth/token"
    
    headers = {
        "Content-Type": "application/json",
        "x-device-id": device_id,
        "x-captcha-token": captcha_token
    }
    
    body = {
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": PIKPAK_CLIENT_ID
    }
    
    response = requests.post(url, headers=headers, json=body, timeout=30)
    data = response.json()
    
    if "access_token" in data:
        token_data = {
            "access_token": data["access_token"],
            "refresh_token": data.get("refresh_token", refresh_token),
            "user_id": user_id,
            "expires_at": time.time() + data.get("expires_in", 7200) - 300
        }
        
        set_account_tokens(account["id"], token_data)
        
        print(f"PIKPAK: ✅ Token refreshed for account {account['id']}", flush=True)
        return token_data
    else:
        print(f"PIKPAK: ❌ Refresh failed, doing full login: {data}", flush=True)
        return pikpak_login(account)

def ensure_logged_in(account):
    """
    Ensure account has valid access_token
    Login or refresh if needed
    Returns: token data dict
    """
    tokens = get_account_tokens(account["id"])
    
    if not tokens.get("access_token"):
        print(f"PIKPAK: No token for account {account['id']}, logging in", flush=True)
        return pikpak_login(account)
    
    if time.time() >= tokens.get("expires_at", 0):
        print(f"PIKPAK: Token expired for account {account['id']}, refreshing", flush=True)
        return refresh_pikpak_token(account)
    
    return tokens

def select_available_account(exclude_accounts=None):
    """
    Select PikPak account with capacity
    Implements rotation and daily limit (5/day)
    exclude_accounts: list of account IDs to skip (exhausted ones)
    """
    if exclude_accounts is None:
        exclude_accounts = []
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    try:
        tokens_data = load_pikpak_tokens()
        usage = tokens_data.get("daily_usage", {})
    except:
        usage = {}
    
    print(f"PIKPAK: Checking accounts. Today: {today}", flush=True)
    print(f"PIKPAK: Excluding accounts: {exclude_accounts}", flush=True)
    print(f"PIKPAK: Total accounts loaded: {len(PIKPAK_ACCOUNTS)}", flush=True)
    
    if len(PIKPAK_ACCOUNTS) == 0:
        raise Exception("No PikPak accounts configured! Check environment variables.")
    
    for account in PIKPAK_ACCOUNTS:
        # Skip excluded accounts
        if account["id"] in exclude_accounts:
            print(f"PIKPAK: Account {account['id']}: SKIPPED (exhausted)", flush=True)
            continue
        
        account_key = f"account_{account['id']}"
        account_usage = usage.get(account_key, {})
        
        # Reset if new day OR no usage data exists
        if account_usage.get("date") != today:
            downloads_today = 0
        else:
            downloads_today = account_usage.get("count", 0)
        
        print(f"PIKPAK: Account {account['id']}: {downloads_today}/5 downloads today", flush=True)
        
        # Check limit (5 per day)
        if downloads_today < 5:
            print(f"PIKPAK: ✅ Selected account {account['id']}", flush=True)
            return account
    
    raise Exception("All PikPak accounts exhausted for today")


def mark_account_exhausted(account_id):
    """Mark account as exhausted for today (hit PikPak's limit)"""
    today = datetime.now().strftime("%Y-%m-%d")
    tokens = load_pikpak_tokens()
    
    if "daily_usage" not in tokens:
        tokens["daily_usage"] = {}
    
    account_key = f"account_{account_id}"
    tokens["daily_usage"][account_key] = {"date": today, "count": 5}  # Mark as full
    save_pikpak_tokens(tokens)
    
    print(f"PIKPAK: ⚠️ Account {account_id} marked as exhausted", flush=True)

def increment_account_usage(account_id):
    """Increment daily download counter for account"""
    today = datetime.now().strftime("%Y-%m-%d")
    tokens = load_pikpak_tokens()
    
    if "daily_usage" not in tokens:
        tokens["daily_usage"] = {}
    
    account_key = f"account_{account_id}"
    if tokens["daily_usage"].get(account_key, {}).get("date") != today:
        tokens["daily_usage"][account_key] = {"date": today, "count": 0}
    
    tokens["daily_usage"][account_key]["count"] += 1
    save_pikpak_tokens(tokens)
    
    print(f"PIKPAK: Account {account_id} usage: {tokens['daily_usage'][account_key]['count']}/5", flush=True)

# ============================================================
# PIKPAK DRIVE OPERATIONS
# ============================================================

def pikpak_add_magnet(magnet_link, account, tokens):
    """
    Add magnet link to PikPak
    Returns: task info with file_id (folder)
    """
    print(f"PIKPAK: Adding magnet to account {account['id']}", flush=True)
    
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
    # Get captcha for add magnet
    captcha_sign, timestamp = generate_captcha_sign(device_id)
    captcha_token = get_pikpak_captcha(
        action="POST:/drive/v1/files",
        device_id=device_id,
        user_id=user_id,
        captcha_sign=captcha_sign,
        timestamp=timestamp
    )
    
    url = f"{PIKPAK_API_DRIVE}/drive/v1/files"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "x-device-id": device_id,
        "x-captcha-token": captcha_token
    }
    
    body = {
        "kind": "drive#file",
        "name": "",
        "upload_type": "UPLOAD_TYPE_URL",
        "url": {
            "url": magnet_link
        },
        "folder_type": "DOWNLOAD"
    }
    
    response = requests.post(url, headers=headers, json=body, timeout=30)
    data = response.json()
    
    if "task" in data:
        print(f"PIKPAK: ✅ Magnet added: {data['task'].get('file_name', 'Unknown')}", flush=True)
        return data["task"]
    else:
        print(f"PIKPAK: ❌ Add magnet failed: {data}", flush=True)
        raise Exception(f"Add magnet failed: {data.get('error', 'Unknown')}")

def pikpak_poll_download(file_id, account, tokens, timeout=120):
    """
    Poll until download completes
    Returns: True when PHASE_TYPE_COMPLETE
    """
    print(f"PIKPAK: Polling download status for {file_id}", flush=True)
    
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
    start_time = time.time()
    poll_interval = 5  # seconds
    
    while time.time() - start_time < timeout:
        try:
            # Get fresh captcha
            captcha_sign, timestamp = generate_captcha_sign(device_id)
            captcha_token = get_pikpak_captcha(
                action="GET:/drive/v1/files/{id}",
                device_id=device_id,
                user_id=user_id,
                captcha_sign=captcha_sign,
                timestamp=timestamp
            )
            
            url = f"{PIKPAK_API_DRIVE}/drive/v1/files/{file_id}"
            
            headers = {
                "Authorization": f"Bearer {access_token}",
                "x-device-id": device_id,
                "x-captcha-token": captcha_token
            }
            
            response = requests.get(url, headers=headers, timeout=30)
            data = response.json()
            
            phase = data.get("phase", "")
            progress = data.get("progress", 0)
            
            print(f"PIKPAK: Status: {phase} ({progress}%)", flush=True)
            
            if phase == "PHASE_TYPE_COMPLETE":
                print(f"PIKPAK: ✅ Download complete!", flush=True)
                return True
            elif phase == "PHASE_TYPE_ERROR":
                raise Exception(f"Download failed: {data.get('message', 'Unknown error')}")
            
            time.sleep(poll_interval)
            
        except Exception as e:
            if "Download failed" in str(e):
                raise
            print(f"PIKPAK: Poll error (retrying): {e}", flush=True)
            time.sleep(poll_interval)
    
    raise Exception(f"Download timeout after {timeout} seconds")

def pikpak_get_file_info(file_id, account, tokens):
    """
    Get information about a file/folder
    """
    print(f"PIKPAK: Getting file info for {file_id}", flush=True)

    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]

    # Get fresh captcha
    captcha_sign, timestamp = generate_captcha_sign(device_id)
    captcha_token = get_pikpak_captcha(
        action="GET:/drive/v1/files/{id}",
        device_id=device_id,
        user_id=user_id,
        captcha_sign=captcha_sign,
        timestamp=timestamp
    )

    url = f"{PIKPAK_API_DRIVE}/drive/v1/files/{file_id}"

    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-device-id": device_id,
        "x-captcha-token": captcha_token
    }

    response = requests.get(url, headers=headers, timeout=30)
    data = response.json()

    if "kind" in data:
        return data
    else:
        raise Exception(f"Failed to get file info: {data.get('error', 'Unknown error')}")

def pikpak_list_files(parent_id, account, tokens):
    """
    List files in folder
    Returns: list of files
    """
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
    # Get fresh captcha
    captcha_sign, timestamp = generate_captcha_sign(device_id)
    captcha_token = get_pikpak_captcha(
        action="GET:/drive/v1/files",
        device_id=device_id,
        user_id=user_id,
        captcha_sign=captcha_sign,
        timestamp=timestamp
    )
    
    url = f"{PIKPAK_API_DRIVE}/drive/v1/files"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-device-id": device_id,
        "x-captcha-token": captcha_token
    }
    
    params = {
        "parent_id": parent_id,
        "limit": 100
    }
    
    response = requests.get(url, headers=headers, params=params, timeout=30)
    data = response.json()
    
    return data.get("files", [])

def find_video_file(files):
    """
    Find video file from list
    Returns: video file dict or None
    """
    for file in files:
        if (file.get("file_category") == "VIDEO" or
            file.get("mime_type", "").startswith("video/") or
            file.get("file_extension") in [".mp4", ".mkv", ".avi", ".mov", ".wmv"]):
            return file
    return None

def is_video_file(file_info):
    """
    Check if a file is a video based on its metadata.
    """
    if not file_info or not isinstance(file_info, dict):
        return False
    
    return (
        file_info.get("file_category") == "VIDEO" or
        file_info.get("mime_type", "").startswith("video/") or
        file_info.get("file_extension") in [".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm"]
    )

def pikpak_get_download_link(file_id, account, tokens):
    """
    Get download link for file
    Returns: download URL
    """
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
    # Get fresh captcha
    captcha_sign, timestamp = generate_captcha_sign(device_id)
    captcha_token = get_pikpak_captcha(
        action="GET:/drive/v1/files/{id}",
        device_id=device_id,
        user_id=user_id,
        captcha_sign=captcha_sign,
        timestamp=timestamp
    )
    
    url = f"{PIKPAK_API_DRIVE}/drive/v1/files/{file_id}"
    
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-device-id": device_id,
        "x-captcha-token": captcha_token
    }
    
    response = requests.get(url, headers=headers, timeout=30)
    data = response.json()
    
    download_url = data.get("web_content_link", "")
    
    if download_url:
        return download_url
    else:
        raise Exception(f"No download link in response: {data}")

def pikpak_delete_file(file_id, account, tokens):
    """
    Delete file/folder from PikPak
    Returns: True on success
    """
    print(f"PIKPAK: Deleting file {file_id}", flush=True)
    
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
    # Get fresh captcha
    captcha_sign, timestamp = generate_captcha_sign(device_id)
    captcha_token = get_pikpak_captcha(
        action="POST:/drive/v1/files:batchTrash",
        device_id=device_id,
        user_id=user_id,
        captcha_sign=captcha_sign,
        timestamp=timestamp
    )
    
    url = f"{PIKPAK_API_DRIVE}/drive/v1/files:batchTrash"
    
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {access_token}",
        "x-device-id": device_id,
        "x-captcha-token": captcha_token
    }
    
    body = {
        "ids": [file_id]
    }
    
    response = requests.post(url, headers=headers, json=body, timeout=30)
    data = response.json()
    
    print(f"PIKPAK: ✅ Delete response: {data}", flush=True)
    return True

# ============================================================
# SMART STREAMER (unchanged)
# ============================================================

class SmartStream(IOBase):
    """High-speed streaming class with large buffer and prefetch"""
    
    BUFFER_SIZE = 4 * 1024 * 1024  # 4MB buffer for faster reads
    
    def __init__(self, url, name):
        super().__init__()
        self.url = url
        self.name = name
        self.mode = 'rb'
        self._closed = False
        
        print(f"STREAM: Connecting to {url[:60]}...", flush=True)
        
        try:
            head = requests.head(url, allow_redirects=True, timeout=15, headers=HEADERS_STREAM)
            self.total_size = int(head.headers.get('content-length', 0))
            print(f"STREAM: Size {self.total_size} bytes ({self.total_size/1024/1024:.1f}MB)", flush=True)
        except Exception as e:
            print(f"STREAM WARNING: HEAD failed: {e}", flush=True)
            self.total_size = 0
        
        # Use session with optimized settings
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        # Start streaming with large buffer
        self.response = self.session.get(
            url,
            stream=True,
            timeout=(10, 600),
            headers=HEADERS_STREAM
        )
        self.response.raise_for_status()
        
        # Direct iterator for faster reads
        self.iterator = self.response.iter_content(chunk_size=self.BUFFER_SIZE)
        self.current_pos = 0
        self._leftover = b''
    
    def read(self, size=-1):
        if self._closed:
            raise ValueError("I/O operation on closed file")
        
        if size == -1 or size is None:
            size = self.BUFFER_SIZE
        
        # Use leftover data first
        if self._leftover:
            if len(self._leftover) >= size:
                data = self._leftover[:size]
                self._leftover = self._leftover[size:]
                self.current_pos += len(data)
                return data
            else:
                data = self._leftover
                self._leftover = b''
        else:
            data = b''
        
        # Read more chunks until we have enough
        try:
            while len(data) < size:
                chunk = next(self.iterator, None)
                if chunk is None:
                    break
                data += chunk
        except StopIteration:
            pass
        except Exception as e:
            print(f"STREAM ERROR: {e}", flush=True)
        
        # Store leftover for next read
        if len(data) > size:
            self._leftover = data[size:]
            data = data[:size]
        
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
    
    def readable(self):
        return True
    
    def writable(self):
        return False
    
    def seekable(self):
        return False
    
    def close(self):
        if not self._closed:
            self._closed = True
            try:
                if hasattr(self, 'response'):
                    self.response.close()
                if hasattr(self, 'session'):
                    self.session.close()
            except:
                pass
    
    def fileno(self):
        raise OSError("Stream does not support fileno()")
    
    def __enter__(self):
        return self
    
    def __exit__(self, *args):
        self.close()

# ============================================================
# METADATA EXTRACTION (unchanged)
# ============================================================

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

def detect_quality(user_quality, magnet_link, size_bytes):
    """
    Detect video quality based on user input, magnet name, or file size.
    Priority: User Input > Magnet Name > File Size.
    """
    if user_quality and user_quality != 'auto':
        return user_quality.lower()

    # Try to get from magnet name first
    metadata = extract_metadata_from_magnet(magnet_link)
    if metadata.get('resolution'):
        return metadata['resolution']

    # Fallback to size
    return detect_quality_from_size(size_bytes)

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

# ============================================================
# ASYNC UPLOAD LOGIC (unchanged)
# ============================================================

async def perform_upload(file_url, chat_target, caption, filename, file_size_mb=0):
    """Upload video with optimized speed"""
    
    if file_size_mb > 2048:
        raise Exception(f"File too large: {file_size_mb:.1f}MB (max 2048MB)")
    
    max_retries = 3
    retry_count = 0
    last_log_time = [time.time()]
    last_log_bytes = [0]
    
    def progress_callback(current, total):
        now = time.time()
        elapsed = now - last_log_time[0]
        
        # Log every 5 seconds with speed info
        if elapsed >= 5:
            bytes_since = current - last_log_bytes[0]
            speed_mbps = (bytes_since / elapsed) / (1024 * 1024)
            percent = current * 100 // total if total > 0 else 0
            
            print(f"📊 {current/1024/1024:.1f}/{total/1024/1024:.1f}MB ({percent}%) - {speed_mbps:.1f} MB/s", flush=True)
            
            last_log_time[0] = now
            last_log_bytes[0] = current
    
    while retry_count < max_retries:
        try:
            async with Client(
                "bot_session",
                api_id=API_ID,
                api_hash=API_HASH,
                bot_token=BOT_TOKEN,
                workdir="/tmp"
            ) as tg_app:
                print(f"WORKER: Bot connected! (Attempt {retry_count + 1}/{max_retries})", flush=True)
                
                # Resolve chat ID
                final_chat_id = None
                chat_str = str(chat_target).strip()
                
                if chat_str.startswith("@"):
                    chat = await tg_app.get_chat(chat_str)
                    final_chat_id = chat.id
                    print(f"WORKER: ✅ Resolved {chat_str} to ID: {final_chat_id}", flush=True)
                elif chat_str.lstrip("-").isdigit():
                    final_chat_id = int(chat_str)
                else:
                    raise Exception(f"Invalid chat format: {chat_str}")
                
                # Stream and upload with optimized settings
                start_time = time.time()
                
                with SmartStream(file_url, filename) as stream:
                    if stream.total_size == 0:
                        raise Exception("File size is 0. Link may be expired.")
                    
                    print(f"WORKER: Uploading {filename} ({stream.total_size/1024/1024:.1f}MB)...", flush=True)
                    last_log_bytes[0] = 0
                    last_log_time[0] = time.time()
                    
                    msg = await tg_app.send_video(
                        chat_id=final_chat_id,
                        video=stream,
                        caption=caption,
                        file_name=filename,
                        supports_streaming=True,
                        progress=progress_callback
                    )
                    
                    elapsed = time.time() - start_time
                    avg_speed = (stream.total_size / elapsed) / (1024 * 1024)
                    
                    clean_id = str(msg.chat.id).replace('-100', '')
                    private_link = f"https://t.me/c/{clean_id}/{msg.id}"
                    
                    print(f"WORKER: ✅ Upload complete! {elapsed:.1f}s ({avg_speed:.1f} MB/s avg)", flush=True)
                    print(f"WORKER: Link: {private_link}", flush=True)
                    
                    return {
                        "success": True,
                        "message_id": msg.id,
                        "chat_id": msg.chat.id,
                        "file_id": msg.video.file_id,
                        "private_link": private_link,
                        "file_size": msg.video.file_size,
                        "duration": msg.video.duration,
                        "upload_time": elapsed,
                        "avg_speed_mbps": avg_speed
                    }
        
        except FloodWait as e:
            print(f"WORKER: FloodWait {e.value}s, waiting...", flush=True)
            await asyncio.sleep(e.value)
            retry_count += 1
            
        except Exception as e:
            retry_count += 1
            error_msg = str(e)
            print(f"WORKER: Error: {error_msg}", flush=True)
            
            if retry_count >= max_retries:
                raise Exception(f"Upload failed after {max_retries} retries: {error_msg}")
            
            print(f"WORKER: Retry {retry_count}/{max_retries}...", flush=True)
            await asyncio.sleep(3)

# ============================================================
# SEND NOTIFICATION (unchanged)
# ============================================================

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
        ) as tg_app:
            await tg_app.send_message(
                chat_id=int(ADMIN_CHAT_ID),
                text=message,
                reply_markup=reply_markup
            )
            print(f"NOTIFICATION SENT", flush=True)
    except Exception as e:
        print(f"NOTIFICATION ERROR: {e}", flush=True)

# ============================================================
# QUEUE WORKER (unchanged)
# ============================================================

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
            
            log_activity("success", f"Uploaded: {data.get('filename', 'video.mp4')}")
            update_daily_stats("uploads")
            update_daily_stats("total_bytes", result.get('file_size', 0))
            update_daily_stats("total_time", result.get('upload_time', 0))
            
        except Exception as e:
            error_msg = str(e)
            print(f"WORKER ERROR: {error_msg}", flush=True)
            
            if "failed after" in error_msg or "too large" in error_msg:
                loop.run_until_complete(send_admin_notification(f"Job {job_id}: {error_msg}"))
            
            if job_id:
                JOBS[job_id]['status'] = 'failed'
                log_activity("failed", f"Upload failed: {error_msg[:50]}")
                update_daily_stats("failed")
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

# ============================================================
# ADMIN DASHBOARD ROUTES
# ============================================================

@app.route('/admin')
def admin_dashboard():
    """Serve admin dashboard HTML"""
    return render_template('admin.html')

@app.route('/admin/api/status')
def admin_api_status():
    """Get complete admin dashboard data"""
    from datetime import datetime
    
    # Get account status
    tokens_data = load_pikpak_tokens()
    usage = tokens_data.get("daily_usage", {})
    today = datetime.now().strftime("%Y-%m-%d")
    
    accounts_list = []
    total_remaining = 0
    
    for account in PIKPAK_ACCOUNTS:
        account_key = f"account_{account['id']}"
        account_usage = usage.get(account_key, {})
        
        if account_usage.get("date") != today:
            downloads_today = 0
        else:
            downloads_today = account_usage.get("count", 0)
        
        remaining = 5 - downloads_today
        total_remaining += remaining
        
        accounts_list.append({
            "id": account["id"],
            "email": account["email"],
            "downloads_today": downloads_today,
            "downloads_remaining": remaining,
            "available": remaining > 0
        })
    
    # Get sessions
    sessions_list = []
    current_time = time.time()
    with SESSION_LOCK:
        for session_id, session_data in MESSAGE_SESSIONS.items():
            expires_in = int(session_data['timeout'] - current_time)
            if expires_in > 0:
                sessions_list.append({
                    "id": session_id,
                    "magnets": len(session_data.get('magnets', [])),
                    "expires_in": f"{expires_in // 60}m {expires_in % 60}s"
                })
    
    # Count jobs
    completed = sum(1 for j in JOBS.values() if j.get('status') == 'done')
    failed = sum(1 for j in JOBS.values() if j.get('status') == 'failed')
    processing = sum(1 for j in JOBS.values() if j.get('status') == 'processing')
    
    # Format report data
    avg_time = "0s"
    total_data = "0 MB"
    if DAILY_STATS["uploads"] > 0 and DAILY_STATS["total_time"] > 0:
        avg_seconds = DAILY_STATS["total_time"] / DAILY_STATS["uploads"]
        avg_time = f"{avg_seconds:.1f}s"
    if DAILY_STATS["total_bytes"] > 0:
        total_mb = DAILY_STATS["total_bytes"] / (1024 * 1024)
        if total_mb > 1024:
            total_data = f"{total_mb/1024:.1f} GB"
        else:
            total_data = f"{total_mb:.0f} MB"
    
    return jsonify({
        "system": {
            "status": "online",
            "version": "2.0.0",
            "queue": JOB_QUEUE.qsize(),
            "completed": completed,
            "failed": failed,
            "processing": processing,
            "sessions": len(sessions_list),
            "emergency_stop": EMERGENCY_STOP,
            "worker_alive": WORKER_THREAD.is_alive() if WORKER_THREAD else False
        },
        "accounts": {
            "list": accounts_list,
            "total_remaining": total_remaining
        },
        "sessions": sessions_list,
        "report": {
            "downloads": DAILY_STATS["downloads"],
            "uploads": DAILY_STATS["uploads"],
            "avg_time": avg_time,
            "total_data": total_data
        },
        "activity": ACTIVITY_LOG[:20]
    })

@app.route('/admin/api/reset-quota/<int:account_id>', methods=['POST'])
def admin_reset_quota(account_id):
    """Reset quota for specific account"""
    try:
        tokens = load_pikpak_tokens()
        
        if "daily_usage" not in tokens:
            tokens["daily_usage"] = {}
        
        account_key = f"account_{account_id}"
        tokens["daily_usage"][account_key] = {
            "date": datetime.now().strftime("%Y-%m-%d"),
            "count": 0
        }
        
        save_pikpak_tokens(tokens)
        
        log_activity("info", f"Account {account_id} quota reset manually")
        
        return jsonify({"success": True, "message": f"Account {account_id} quota reset"})
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/admin/api/test-magnet', methods=['POST'])
def admin_test_magnet():
    """Test magnet link without downloading"""
    try:
        magnet = request.json.get('magnet', '')
        
        if not magnet or not magnet.startswith('magnet:'):
            return jsonify({"valid": False, "error": "Invalid magnet format"})
        
        # Extract name from magnet
        name = "Unknown"
        dn_match = re.search(r'dn=([^&]+)', magnet)
        if dn_match:
            name = dn_match.group(1)
            name = name.replace('+', ' ').replace('%20', ' ')
        
        # Detect quality
        quality = detect_quality_from_magnet(magnet) or "Unknown"
        
        # Find available account
        available_account = None
        try:
            account = select_available_account()
            available_account = account["id"]
        except:
            available_account = "None available"
        
        return jsonify({
            "valid": True,
            "name": name,
            "quality": quality,
            "account": available_account
        })
    except Exception as e:
        return jsonify({"valid": False, "error": str(e)})

# ============================================================
# FLASK ROUTES
# ============================================================

@app.route('/')
def home(): 
    """Health check"""
    ensure_worker_alive()
    return jsonify({
        "status": "online",
        "service": "PikPak-Telegram Bridge",
        "queue": JOB_QUEUE.qsize(),
        "jobs": len(JOBS),
        "sessions": len(MESSAGE_SESSIONS),
        "pikpak_accounts": len(PIKPAK_ACCOUNTS),
        "worker_alive": WORKER_THREAD.is_alive() if WORKER_THREAD else False
    })

@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files"""
    return send_from_directory('static', filename)

# ============================================================
# SESSION ROUTES (unchanged)
# ============================================================

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
            print(f"SESSION ERROR: \"{poster_msg_id}\" not found.", flush=True)
            return jsonify({"error": "Session not found", "session_id": poster_msg_id}), 404
        
        session = MESSAGE_SESSIONS[poster_msg_id]
        
        if time.time() > session['timeout']:
            del MESSAGE_SESSIONS[poster_msg_id]
            return jsonify({"error": "timeout"}), 408
        
        if len(session['magnets']) >= 3:
            return jsonify({"error": "max_magnets"}), 400
        
        session['magnets'].append(magnet)
        print(f"SESSION: Added magnet {len(session['magnets'])}/3", flush=True)
    
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
    """Mark session as complete"""
    data = request.json
    poster_msg_id = str(data.get('poster_message_id'))
    
    with SESSION_LOCK:
        if poster_msg_id not in MESSAGE_SESSIONS:
            return jsonify({"error": "Session not found"}), 404
        
        session = MESSAGE_SESSIONS[poster_msg_id]
        
        if len(session['magnets']) < 1:
            return jsonify({"error": "no_magnets"}), 400
        
        result = {
            'metadata': session['metadata'],
            'magnets': session['magnets'],
            'count': len(session['magnets'])
        }
        
        del MESSAGE_SESSIONS[poster_msg_id]
        print(f"SESSION: Completed with {len(session['magnets'])} magnets", flush=True)
        
        return jsonify(result)

@app.route('/debug/sessions', methods=['GET'])
def debug_sessions():
    """Debug: Show all active sessions"""
    with SESSION_LOCK:
        return jsonify({
            "active_sessions": list(MESSAGE_SESSIONS.keys()),
            "session_data": {k: {
                "magnets": len(v['magnets']),
                "time_left": int(v['timeout'] - time.time())
            } for k, v in MESSAGE_SESSIONS.items()}
        })

# ============================================================
# EMERGENCY STOP FLAG
# ============================================================

@app.route('/emergency-stop', methods=['POST'])
def emergency_stop():
    """Emergency stop all PikPak operations"""
    global EMERGENCY_STOP
    EMERGENCY_STOP = True
    print("🚨 EMERGENCY STOP ACTIVATED!", flush=True)
    return jsonify({"status": "stopped", "message": "Emergency stop activated"})

@app.route('/emergency-resume', methods=['POST'])
def emergency_resume():
    """Resume PikPak operations"""
    global EMERGENCY_STOP
    EMERGENCY_STOP = False
    print("✅ EMERGENCY STOP DEACTIVATED - Resumed", flush=True)
    return jsonify({"status": "resumed", "message": "Operations resumed"})

@app.route('/emergency-status', methods=['GET'])
def emergency_status():
    """Check emergency stop status"""
    return jsonify({"emergency_stop": EMERGENCY_STOP})

# ============================================================
# PIKPAK ADD MAGNET ROUTE (FIXED)
# ============================================================

@app.route('/add-magnet', methods=['POST'])
def add_magnet():
    """
    Add magnet to PikPak and return download link
    Handles both single file and folder downloads
    Smart retry - only retries on daily_limit error
    """
    global EMERGENCY_STOP
    
    magnet = request.json.get('magnet')
    user_quality = request.json.get('quality', 'auto')
    
    if not magnet:
        return jsonify({"error": "Missing magnet parameter"}), 400
    
    exhausted_accounts = []
    max_total_retries = 8
    attempt = 0
    last_account_id = None
    
    while attempt < max_total_retries:
        attempt += 1
        
        # CHECK EMERGENCY STOP
        if EMERGENCY_STOP:
            print("🚨 EMERGENCY STOP - Aborting magnet add", flush=True)
            return jsonify({
                "error": "Emergency stop activated",
                "retry": False
            }), 503
        
        try:
            print(f"PIKPAK: === ADD MAGNET ATTEMPT {attempt}/{max_total_retries} ===", flush=True)
            
            # 1. Select available account
            account = select_available_account(exclude_accounts=exhausted_accounts)
            last_account_id = account["id"]
            
            # 2. Ensure logged in
            tokens = ensure_logged_in(account)
            
            # 3. Add magnet
            task = pikpak_add_magnet(magnet, account, tokens)
            folder_id = task.get("file_id")
            file_name = task.get("file_name", "Unknown")
            
            # 3.5 CHECK FOR EMPTY FILE_ID (prevents infinite loop)
            if not folder_id or str(folder_id).strip() == "":
                error_msg = "PikPak returned empty file_id - magnet may be invalid"
                print(f"PIKPAK: ❌ {error_msg}", flush=True)
                print(f"PIKPAK: ⚠️ STOPPING - No retry to save quota", flush=True)
                
                return jsonify({
                    "error": error_msg,
                    "retry": False,
                    "account_used": account["id"]
                }), 400
            
            # 4. Poll until complete
            pikpak_poll_download(folder_id, account, tokens, timeout=120)
            
            # 5. Refresh tokens
            tokens = ensure_logged_in(account)
            
            # 6. Get file info to determine if it's file or folder
            file_info = pikpak_get_file_info(folder_id, account, tokens)
            kind = file_info.get("kind", "")
            
            print(f"PIKPAK: File kind: {kind}", flush=True)
            
            video_file = None
            download_url = None
            
            if kind == "drive#folder":
                # It's a folder - list contents and find video
                print(f"PIKPAK: Detected FOLDER, listing contents...", flush=True)
                files = pikpak_list_files(folder_id, account, tokens)
                
                video_file = find_video_file(files)
                if not video_file:
                    error_msg = "No video file found in folder"
                    print(f"PIKPAK: ❌ {error_msg}", flush=True)
                    
                    return jsonify({
                        "error": error_msg,
                        "retry": False,
                        "account_used": account["id"]
                    }), 400
                
                # Refresh tokens and get download link
                tokens = ensure_logged_in(account)
                download_url = pikpak_get_download_link(video_file["id"], account, tokens)
                
            else:
                # It's a single file - check if it's a video
                print(f"PIKPAK: Detected SINGLE FILE", flush=True)
                
                if not is_video_file(file_info):
                    error_msg = "Downloaded file is not a video"
                    print(f"PIKPAK: ❌ {error_msg}", flush=True)
                    
                    return jsonify({
                        "error": error_msg,
                        "retry": False,
                        "account_used": account["id"]
                    }), 400
                
                video_file = file_info
                download_url = file_info.get("web_content_link", "")
                
                if not download_url:
                    tokens = ensure_logged_in(account)
                    download_url = pikpak_get_download_link(folder_id, account, tokens)
            
            # 7. Check file size
            file_size = int(video_file.get("size", 0))
            file_size_mb = file_size / 1024 / 1024
            
            if file_size_mb > 2048:
                error_msg = f"File too large: {file_size_mb:.0f}MB (max 2048MB)"
                print(f"PIKPAK: ❌ {error_msg}", flush=True)
                
                return jsonify({
                    "error": error_msg,
                    "retry": False,
                    "account_used": account["id"]
                }), 400
            
            # 8. Increment usage counter
            increment_account_usage(account["id"])
            
            # 9. Detect quality
            detected_quality = detect_quality(user_quality, magnet, file_size)
            
            print(f"PIKPAK: === ADD MAGNET SUCCESS ===", flush=True)
            log_activity("success", f"Downloaded: {video_file.get('name', file_name)}")
            update_daily_stats("downloads")
            print(f"PIKPAK: Quality detected: {detected_quality}", flush=True)
            
            # Return success response
            return jsonify({
                "result": True,
                "folder_id": folder_id,
                "file_id": video_file.get("id", folder_id),
                "file_name": video_file.get("name", file_name),
                "file_size": file_size,
                "url": download_url,
                "account_used": account["id"],
                "file_type": "folder" if kind == "drive#folder" else "file",
                "quality_detected": detected_quality
            })
            
        except Exception as e:
            error_msg = str(e)
            print(f"PIKPAK: Error on attempt {attempt}: {error_msg}", flush=True)
            
            # Check if it's a daily limit error - ONLY this should retry on different account
            if "task_daily_create_limit" in error_msg:
                print(f"PIKPAK: ⚠️ Account {account['id']} hit daily limit!", flush=True)
                mark_account_exhausted(account["id"])
                exhausted_accounts.append(account["id"])
                print(f"PIKPAK: 🔄 Rotating to next account...", flush=True)
                continue
            
            # Check if all accounts exhausted
            if "All PikPak accounts exhausted" in error_msg:
                return jsonify({"error": error_msg, "retry": False}), 500
            
            # For other errors - limited retry
            if attempt >= 3:
                print(f"PIKPAK: ❌ Max retries reached, stopping", flush=True)
                return jsonify({
                    "error": error_msg,
                    "retry": False,
                    "attempts": attempt,
                    "account_used": last_account_id
                }), 500
            
            print(f"PIKPAK: Retrying in 5 seconds...", flush=True)
            time.sleep(5)
    
    return jsonify({"error": "Max retries exceeded", "retry": False}), 500

@app.route('/list-files', methods=['POST'])
def list_files():
    """List PikPak folder contents"""
    try:
        folder_id = request.json.get('folder_id')
        if not folder_id:
            return jsonify({"error": "Missing folder_id"}), 400
        
        account = select_available_account()
        tokens = ensure_logged_in(account)
        
        files = pikpak_list_files(folder_id, account, tokens)
        
        return jsonify({
            "folders": [],
            "files": files
        })
        
    except Exception as e:
        print(f"PIKPAK: List files error: {e}", flush=True)
        return jsonify({"error": str(e)}), 500

@app.route('/get-link', methods=['POST'])
def get_link():
    """Get PikPak download link for file"""
    try:
        file_id = request.json.get('file_id')
        if not file_id:
            return jsonify({"error": "Missing file_id"}), 400
        
        account = select_available_account()
        tokens = ensure_logged_in(account)
        
        download_url = pikpak_get_download_link(file_id, account, tokens)
        
        return jsonify({"url": download_url})
        
    except Exception as e:
        print(f"PIKPAK: Get link error: {e}", flush=True)
        return jsonify({"error": str(e)}), 500

@app.route('/delete-folder', methods=['POST'])
def delete_folder():
    """Delete PikPak folder"""
    try:
        folder_id = request.json.get('folder_id')
        if not folder_id or folder_id == 'null' or folder_id == 'None':
            return jsonify({"error": "Invalid folder_id"}), 400
        
        account = select_available_account()
        tokens = ensure_logged_in(account)
        
        pikpak_delete_file(folder_id, account, tokens)
        
        return jsonify({"result": True, "deleted_id": folder_id})
        
    except Exception as e:
        print(f"PIKPAK: Delete error: {e}", flush=True)
        return jsonify({"error": str(e)}), 500

@app.route('/pikpak/status', methods=['GET'])
def pikpak_status():
    """Get PikPak accounts status"""
    try:
        tokens_data = load_pikpak_tokens()
        usage = tokens_data.get("daily_usage", {})
        today = datetime.now().strftime("%Y-%m-%d")
        
        accounts_status = []
        total_remaining = 0
        
        for account in PIKPAK_ACCOUNTS:
            account_key = f"account_{account['id']}"
            account_usage = usage.get(account_key, {})
            
            if account_usage.get("date") != today:
                downloads_today = 0
            else:
                downloads_today = account_usage.get("count", 0)
            
            remaining = 5 - downloads_today
            total_remaining += remaining
            
            accounts_status.append({
                "id": account["id"],
                "email": account["email"],
                "downloads_today": downloads_today,
                "downloads_remaining": remaining,
                "available": remaining > 0
            })
        
        return jsonify({
            "accounts": accounts_status,
            "total_remaining": total_remaining,
            "total_accounts": len(PIKPAK_ACCOUNTS)
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# ============================================================
# UPLOAD & JOB ROUTES (unchanged)
# ============================================================

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

# ============================================================
# CLEANUP EXPIRED SESSIONS
# ============================================================

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

cleanup_thread = threading.Thread(target=cleanup_sessions, daemon=True)
cleanup_thread.start()

# ============================================================
# MAIN
# ============================================================

if __name__ == '__main__':
    print("=" * 50, flush=True)
    print("🚀 PikPak-Telegram Bridge Starting", flush=True)
    print(f"📦 Loaded {len(PIKPAK_ACCOUNTS)} PikPak accounts", flush=True)
    print("=" * 50, flush=True)
    ensure_worker_alive()
    app.run(host='0.0.0.0', port=10000)
