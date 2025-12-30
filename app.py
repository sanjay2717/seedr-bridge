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
from urllib.parse import unquote
from flask import Flask, request, jsonify, render_template, send_from_directory
from pyrogram import Client
from pyrogram.errors import FloodWait, ChannelPrivate, ChatAdminRequired
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

app = Flask(__name__)

# ============================================================
# SERVER IDENTIFICATION (NEW)
# ============================================================
SERVER_ID = "primary"
SERVER_MODE = os.environ.get("SERVER_MODE", "primary")
SESSION_NAME = "bot_session_primary"
SERVER_URL = "https://seedr-bridge.onrender.com"

# ============================================================
# CONFIGURATION
# ============================================================
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
    
    activity = {
        "time": datetime.now().strftime("%H:%M"),
        "status": status,
        "message": f"[{SERVER_ID.upper()}] {message}"
    }
    
    ACTIVITY_LOG.insert(0, activity)
    
    if len(ACTIVITY_LOG) > MAX_ACTIVITY_LOG:
        ACTIVITY_LOG = ACTIVITY_LOG[:MAX_ACTIVITY_LOG]

def update_daily_stats(stat_type, value=1):
    """Update daily statistics"""
    global DAILY_STATS
    
    today = datetime.now().strftime("%Y-%m-%d")
    
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

PIKPAK_SALTS = [
    "C9qPpZLN8ucRTaTiUMWYS9cQvWOE",
    "+r6CQVxjzJV6LCV",
    "F",
    "pFJRC",
    "9WXYIDGrwTCz2OiVlgZa90qpECPD6olt",
    "/750aCr4lm/Sly/c",
    "RB+DT/gZCrbV",
    "",
    "CyLsf7hdkIRxRm215hl",
    "7xHvLi2tOYP0Y92b",
    "ZGTXXxu8E/MIWaEDB+Sm/",
    "1UI3",
    "E7fP5Pfijd+7K+t6Tg/NhuLq0eEUVChpJSkrKxpO",
    "ihtqpG6FMt65+Xk+tWUH2",
    "NhXXU9rg4XXdzo7u5o"
]

# ============================================================
# PIKPAK ACCOUNT LOADING (MODIFIED - Only accounts 1, 2, 3)
# ============================================================

def load_pikpak_accounts():
    """Load PikPak accounts 1, 2, 3 for Server 1 (Primary)"""
    accounts = []
    
    # SERVER 1: Only load accounts 1, 2, 3 (for 1080p)
    for i in [1, 2, 3]:
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
            print(f"PIKPAK [{SERVER_ID}]: Loaded account {i}: {email}", flush=True)
    
    print(f"PIKPAK [{SERVER_ID}]: Total accounts loaded: {len(accounts)}", flush=True)
    return accounts

PIKPAK_ACCOUNTS = load_pikpak_accounts()
PIKPAK_TOKENS_FILE = f"/tmp/pikpak_tokens_{SERVER_ID}.json"
PIKPAK_LOCK = threading.Lock()
MAGNET_ADD_LOCK = threading.Lock()
PIKPAK_STORAGE_CACHE = {}
PIKPAK_STORAGE_CACHE_TIME = {}

def get_account_storage(account_id):
    """Get storage info for account with caching"""
    global PIKPAK_STORAGE_CACHE, PIKPAK_STORAGE_CACHE_TIME
    
    cache_key = f"account_{account_id}"
    current_time = time.time()
    
    # Return cached if less than 5 minutes old
    if cache_key in PIKPAK_STORAGE_CACHE:
        if current_time - PIKPAK_STORAGE_CACHE_TIME.get(cache_key, 0) < 300:
            return PIKPAK_STORAGE_CACHE[cache_key]
    
    try:
        account = None
        for acc in PIKPAK_ACCOUNTS:
            if acc["id"] == account_id:
                account = acc
                break
        
        if not account:
            return {"used_gb": 0, "total_gb": 6, "percent": 0}
        
        tokens = ensure_logged_in(account)
        device_id = account["device_id"]
        user_id = tokens["user_id"]
        access_token = tokens["access_token"]
        
        captcha_sign, timestamp = generate_captcha_sign(device_id)
        captcha_token = get_pikpak_captcha(
            action="GET:/drive/v1/about",
            device_id=device_id,
            user_id=user_id,
            captcha_sign=captcha_sign,
            timestamp=timestamp
        )
        
        url = f"{PIKPAK_API_DRIVE}/drive/v1/about"
        headers = {
            "Authorization": f"Bearer {access_token}",
            "x-device-id": device_id,
            "x-captcha-token": captcha_token
        }
        
        response = requests.get(url, headers=headers, timeout=30)
        data = response.json()
        
        quota = data.get("quota", {})
        used_bytes = int(quota.get("usage", 0))
        total_bytes = int(quota.get("limit", 6 * 1024 * 1024 * 1024))
        
        used_gb = round(used_bytes / (1024 * 1024 * 1024), 2)
        total_gb = round(total_bytes / (1024 * 1024 * 1024), 2)
        percent = round((used_bytes / total_bytes) * 100, 1) if total_bytes > 0 else 0
        
        result = {"used_gb": used_gb, "total_gb": total_gb, "percent": percent}
        
        PIKPAK_STORAGE_CACHE[cache_key] = result
        PIKPAK_STORAGE_CACHE_TIME[cache_key] = current_time
        
        return result
        
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Storage check failed for account {account_id}: {e}", flush=True)
        return {"used_gb": 0, "total_gb": 6, "percent": 0}

# ============================================================
# STARTUP QUOTA CHECK (Add after load_pikpak_accounts)
# ============================================================

def check_all_accounts_quota():
    """Check quota for all accounts on startup (doesn't consume quota)"""
    print(f"PIKPAK [{SERVER_ID}]: Checking account quotas on startup...", flush=True)
    
    today = datetime.now().strftime("%Y-%m-%d")
    tokens = load_pikpak_tokens()
    
    if "daily_usage" not in tokens:
        tokens["daily_usage"] = {}
        save_pikpak_tokens(tokens)
    
    for account in PIKPAK_ACCOUNTS:
        try:
            # Login and get storage (this handles login, captcha, and caching)
            storage = get_account_storage(account['id'])
            
            print(f"PIKPAK [{SERVER_ID}]: Account {account['id']} - Storage: {storage['used_gb']}GB/{storage['total_gb']}GB ({storage['percent']}%)", flush=True)
            print(f"PIKPAK [{SERVER_ID}]: Account {account['id']} - Login successful ✅", flush=True)
            
        except Exception as e:
            print(f"PIKPAK [{SERVER_ID}]: Account {account['id']} - Check failed: {e}", flush=True)
    
    print(f"PIKPAK [{SERVER_ID}]: Startup quota check complete", flush=True)
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
        print(f"PIKPAK [{SERVER_ID}]: Failed to save tokens: {e}", flush=True)

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
    """Generate PikPak captcha_sign using MD5 + 15 salts"""
    timestamp = str(int(time.time() * 1000))
    
    base_string = (
        PIKPAK_CLIENT_ID + 
        PIKPAK_CLIENT_VERSION + 
        PIKPAK_PACKAGE_NAME + 
        device_id + 
        timestamp
    )
    
    result = base_string
    for salt in PIKPAK_SALTS:
        result = hashlib.md5((result + salt).encode()).hexdigest()
    
    captcha_sign = "1." + result
    
    return captcha_sign, timestamp

# ============================================================
# PIKPAK STORAGE HELPER
# ============================================================

def get_pikpak_storage_info(account, tokens):
    """Get storage info for account"""
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
    captcha_sign, timestamp = generate_captcha_sign(device_id)
    captcha_token = get_pikpak_captcha(
        action="GET:/drive/v1/about",
        device_id=device_id,
        user_id=user_id,
        captcha_sign=captcha_sign,
        timestamp=timestamp
    )
    
    url = f"{PIKPAK_API_DRIVE}/drive/v1/about"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "x-device-id": device_id,
        "x-captcha-token": captcha_token
    }
    
    response = requests.get(url, headers=headers, timeout=30)
    data = response.json()
    
    quota = data.get("quota", {})
    return {
        "usage": int(quota.get("usage", 0)),
        "limit": int(quota.get("limit", 0))
    }

# ============================================================
# PIKPAK API HELPERS
# ============================================================

def get_pikpak_captcha(action, device_id, user_id=None, captcha_sign=None, timestamp=None, username=None):
    """Get captcha token for PikPak API operation"""
    url = f"{PIKPAK_API_USER}/v1/shield/captcha/init"
    
    headers = {
        "Content-Type": "application/json",
        "x-device-id": device_id
    }
    
    if not captcha_sign or not timestamp:
        captcha_sign, timestamp = generate_captcha_sign(device_id)
    
    if "signin" in action:
        meta = {"username": username}
    else:
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
            print(f"PIKPAK [{SERVER_ID}]: Captcha error: {data}", flush=True)
            raise Exception(f"Captcha failed: {data.get('error', 'Unknown')}")
    
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Captcha request failed: {e}", flush=True)
        raise

def pikpak_login(account):
    """Login to PikPak account"""
    print(f"PIKPAK [{SERVER_ID}]: Logging in account {account['id']} ({account['email']})", flush=True)
    
    device_id = account["device_id"]
    email = account["email"]
    password = account["password"]
    
    captcha_token = get_pikpak_captcha(
        action="POST:/v1/auth/signin",
        device_id=device_id,
        username=email
    )
    
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
            "expires_at": time.time() + data.get("expires_in", 7200) - 300
        }
        
        set_account_tokens(account["id"], token_data)
        
        print(f"PIKPAK [{SERVER_ID}]: ✅ Login successful for account {account['id']}", flush=True)
        return token_data
    else:
        print(f"PIKPAK [{SERVER_ID}]: ❌ Login failed: {data}", flush=True)
        raise Exception(f"Login failed: {data.get('error', 'Unknown')}")

def refresh_pikpak_token(account):
    """Refresh expired access_token"""
    print(f"PIKPAK [{SERVER_ID}]: Refreshing token for account {account['id']}", flush=True)
    
    device_id = account["device_id"]
    tokens = get_account_tokens(account["id"])
    refresh_token = tokens.get("refresh_token")
    user_id = tokens.get("user_id")
    
    if not refresh_token:
        print(f"PIKPAK [{SERVER_ID}]: No refresh token, doing full login", flush=True)
        return pikpak_login(account)
    
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
        
        print(f"PIKPAK [{SERVER_ID}]: ✅ Token refreshed for account {account['id']}", flush=True)
        return token_data
    else:
        print(f"PIKPAK [{SERVER_ID}]: ❌ Refresh failed, doing full login: {data}", flush=True)
        return pikpak_login(account)

def ensure_logged_in(account):
    """Ensure account has valid access_token"""
    tokens = get_account_tokens(account["id"])
    
    if not tokens.get("access_token"):
        print(f"PIKPAK [{SERVER_ID}]: No token for account {account['id']}, logging in", flush=True)
        return pikpak_login(account)
    
    if time.time() >= tokens.get("expires_at", 0):
        print(f"PIKPAK [{SERVER_ID}]: Token expired for account {account['id']}, refreshing", flush=True)
        return refresh_pikpak_token(account)
    
    return tokens

def select_available_account(exclude_accounts=None):
    """Select PikPak account with capacity"""
    if exclude_accounts is None:
        exclude_accounts = []
    
    today = datetime.now().strftime("%Y-%m-%d")
    
    try:
        tokens_data = load_pikpak_tokens()
        usage = tokens_data.get("daily_usage", {})
    except:
        usage = {}
    
    print(f"PIKPAK [{SERVER_ID}]: Checking accounts. Today: {today}", flush=True)
    print(f"PIKPAK [{SERVER_ID}]: Excluding accounts: {exclude_accounts}", flush=True)
    print(f"PIKPAK [{SERVER_ID}]: Total accounts loaded: {len(PIKPAK_ACCOUNTS)}", flush=True)
    
    if len(PIKPAK_ACCOUNTS) == 0:
        raise Exception(f"No PikPak accounts configured for {SERVER_ID}! Check environment variables.")
    
    for account in PIKPAK_ACCOUNTS:
        if account["id"] in exclude_accounts:
            print(f"PIKPAK [{SERVER_ID}]: Account {account['id']}: SKIPPED (exhausted)", flush=True)
            continue
        
        account_key = f"account_{account['id']}"
        account_usage = usage.get(account_key, {})
        
        if account_usage.get("date") != today:
            downloads_today = 0
        else:
            downloads_today = account_usage.get("count", 0)
        
        print(f"PIKPAK [{SERVER_ID}]: Account {account['id']}: {downloads_today}/5 downloads today", flush=True)
        
        if downloads_today < 5:
            print(f"PIKPAK [{SERVER_ID}]: ✅ Selected account {account['id']}", flush=True)
            return account
    
    raise Exception(f"All PikPak accounts exhausted for today on {SERVER_ID}")

def mark_account_exhausted(account_id):
    """Mark account as exhausted for today"""
    today = datetime.now().strftime("%Y-%m-%d")
    tokens = load_pikpak_tokens()
    
    if "daily_usage" not in tokens:
        tokens["daily_usage"] = {}
    
    account_key = f"account_{account_id}"
    tokens["daily_usage"][account_key] = {"date": today, "count": 5}
    save_pikpak_tokens(tokens)
    
    print(f"PIKPAK [{SERVER_ID}]: ⚠️ Account {account_id} marked as exhausted", flush=True)

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
    
    print(f"PIKPAK [{SERVER_ID}]: Account {account_id} usage: {tokens['daily_usage'][account_key]['count']}/5", flush=True)

# ============================================================
# PIKPAK DRIVE OPERATIONS
# ============================================================

def pikpak_add_magnet(magnet_link, account, tokens):
    """Add magnet link to PikPak"""
    print(f"PIKPAK [{SERVER_ID}]: Adding magnet to account {account['id']}", flush=True)
    
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
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
    
    # Debug: Print full response
    print(f"PIKPAK [{SERVER_ID}]: API Response keys: {data.keys()}", flush=True)
    
    # Check multiple possible response formats
    if "task" in data:
        task = data["task"]
        file_id = task.get("file_id") or task.get("id") or ""
        file_name = task.get("file_name") or task.get("name") or "Unknown"
        print(f"PIKPAK [{SERVER_ID}]: ✅ Magnet added: {file_name} (file_id: {file_id})", flush=True)
        
        # Ensure file_id is in the response
        task["file_id"] = file_id
        task["file_name"] = file_name
        return task
    
    elif "file" in data:
        # Alternative response format
        file_data = data["file"]
        file_id = file_data.get("id") or file_data.get("file_id") or ""
        file_name = file_data.get("name") or file_data.get("file_name") or "Unknown"
        print(f"PIKPAK [{SERVER_ID}]: ✅ Magnet added (file format): {file_name} (file_id: {file_id})", flush=True)
        
        return {
            "file_id": file_id,
            "file_name": file_name,
            **file_data
        }
    
    elif "id" in data:
        # Direct response format
        file_id = data.get("id") or ""
        file_name = data.get("name") or "Unknown"
        print(f"PIKPAK [{SERVER_ID}]: ✅ Magnet added (direct format): {file_name} (file_id: {file_id})", flush=True)
        
        return {
            "file_id": file_id,
            "file_name": file_name,
            **data
        }
    
    else:
        print(f"PIKPAK [{SERVER_ID}]: ❌ Add magnet failed: {data}", flush=True)
        raise Exception(f"Add magnet failed: {data.get('error', 'Unknown')}")

def pikpak_poll_download(file_id, account, tokens, timeout=120):
    """Poll until download completes"""
    print(f"PIKPAK [{SERVER_ID}]: Polling download status for {file_id}", flush=True)
    
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
    start_time = time.time()
    poll_interval = 5
    
    while time.time() - start_time < timeout:
        if EMERGENCY_STOP:
            raise Exception("Emergency stop activated during polling")
        try:
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
            
            print(f"PIKPAK [{SERVER_ID}]: Status: {phase} ({progress}%)", flush=True)
            
            if phase == "PHASE_TYPE_COMPLETE":
                print(f"PIKPAK [{SERVER_ID}]: ✅ Download complete!", flush=True)
                return True
            elif phase == "PHASE_TYPE_ERROR":
                raise Exception(f"Download failed: {data.get('message', 'Unknown error')}")
            
            time.sleep(poll_interval)
            
        except Exception as e:
            if "Download failed" in str(e):
                raise
            print(f"PIKPAK [{SERVER_ID}]: Poll error (retrying): {e}", flush=True)
            time.sleep(poll_interval)
    
    raise Exception(f"Download timeout after {timeout} seconds")

def pikpak_get_file_info(file_id, account, tokens):
    """Get information about a file/folder"""
    print(f"PIKPAK [{SERVER_ID}]: Getting file info for {file_id}", flush=True)

    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]

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
    """List files in folder"""
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
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
    """Find video file from list"""
    for file in files:
        if (file.get("file_category") == "VIDEO" or
            file.get("mime_type", "").startswith("video/") or
            file.get("file_extension") in [".mp4", ".mkv", ".avi", ".mov", ".wmv"]):
            return file
    return None

def is_video_file(file_info):
    """Check if a file is a video based on its metadata."""
    if not file_info or not isinstance(file_info, dict):
        return False
    
    return (
        file_info.get("file_category") == "VIDEO" or
        file_info.get("mime_type", "").startswith("video/") or
        file_info.get("file_extension") in [".mp4", ".mkv", ".avi", ".mov", ".wmv", ".flv", ".webm"]
    )

def pikpak_get_download_link(file_id, account, tokens):
    """Get download link for file"""
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
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
    """Delete file/folder from PikPak and try to empty trash"""
    print(f"PIKPAK [{SERVER_ID}]: Deleting file {file_id}", flush=True)
    
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
    # Step 1: Move to trash
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
    
    body = {"ids": [file_id]}
    
    response = requests.post(url, headers=headers, json=body, timeout=30)
    data = response.json()
    
    print(f"PIKPAK [{SERVER_ID}]: ✅ Moved to trash: {data}", flush=True)
    
    # Step 2: Try to empty trash (non-blocking)
    try:
        captcha_sign2, timestamp2 = generate_captcha_sign(device_id)
        captcha_token2 = get_pikpak_captcha(
            action="PATCH:/drive/v1/files/trash:empty",
            device_id=device_id,
            user_id=user_id,
            captcha_sign=captcha_sign2,
            timestamp=timestamp2
        )
        
        empty_url = f"{PIKPAK_API_DRIVE}/drive/v1/files/trash:empty"
        headers2 = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {access_token}",
            "x-device-id": device_id,
            "x-captcha-token": captcha_token2
        }
        
        empty_response = requests.patch(empty_url, headers=headers2, json={}, timeout=30)
        print(f"PIKPAK [{SERVER_ID}]: ✅ Trash emptied successfully", flush=True)
        
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: ⚠️ Trash not emptied (continuing anyway): {e}", flush=True)
    
    return True
# ============================================================
# SMART STREAMER
# ============================================================

class SmartStream(IOBase):
    """High-speed streaming class with large buffer and prefetch"""
    
    BUFFER_SIZE = 4 * 1024 * 1024  # 4MB buffer
    
    def __init__(self, url, name):
        super().__init__()
        self.url = url
        self.name = name
        self.mode = 'rb'
        self._closed = False
        
        print(f"STREAM [{SERVER_ID}]: Connecting to {url[:60]}...", flush=True)
        
        try:
            head = requests.head(url, allow_redirects=True, timeout=15, headers=HEADERS_STREAM)
            self.total_size = int(head.headers.get('content-length', 0))
            print(f"STREAM [{SERVER_ID}]: Size {self.total_size} bytes ({self.total_size/1024/1024:.1f}MB)", flush=True)
        except Exception as e:
            print(f"STREAM [{SERVER_ID}] WARNING: HEAD failed: {e}", flush=True)
            self.total_size = 0
        
        self.session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=10,
            max_retries=3
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)
        
        self.response = self.session.get(
            url,
            stream=True,
            timeout=(10, 1800),
            headers=HEADERS_STREAM
        )
        self.response.raise_for_status()
        
        self.iterator = self.response.iter_content(chunk_size=self.BUFFER_SIZE)
        self.current_pos = 0
        self._leftover = b''
    
    def read(self, size=-1):
        if self._closed:
            raise ValueError("I/O operation on closed file")
        
        if size == -1 or size is None:
            size = self.BUFFER_SIZE
        
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
        
        try:
            while len(data) < size:
                chunk = next(self.iterator, None)
                if chunk is None:
                    break
                data += chunk
        except StopIteration:
            pass
        except Exception as e:
            print(f"STREAM [{SERVER_ID}] ERROR: {e}", flush=True)
        
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
# METADATA EXTRACTION
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
    """Detect video quality based on user input, magnet name, or file size."""
    if user_quality and user_quality != 'auto':
        return user_quality.lower()

    metadata = extract_metadata_from_magnet(magnet_link)
    if metadata.get('resolution'):
        return metadata['resolution']

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
# ASYNC UPLOAD LOGIC (MODIFIED - Uses SESSION_NAME)
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
        
        if elapsed >= 5:
            bytes_since = current - last_log_bytes[0]
            speed_mbps = (bytes_since / elapsed) / (1024 * 1024)
            percent = current * 100 // total if total > 0 else 0
            
            print(f"📊 [{SERVER_ID}] {current/1024/1024:.1f}/{total/1024/1024:.1f}MB ({percent}%) - {speed_mbps:.1f} MB/s", flush=True)
            
            last_log_time[0] = now
            last_log_bytes[0] = current
    
    while retry_count < max_retries:
        try:
            async with Client(
                SESSION_NAME,  # MODIFIED: Use SERVER-SPECIFIC session name
                api_id=API_ID,
                api_hash=API_HASH,
                bot_token=BOT_TOKEN,
                workdir="/tmp"
            ) as tg_app:
                print(f"WORKER [{SERVER_ID}]: Bot connected! (Attempt {retry_count + 1}/{max_retries})", flush=True)
                
                final_chat_id = None
                chat_str = str(chat_target).strip()
                
                if chat_str.startswith("@"):
                    chat = await tg_app.get_chat(chat_str)
                    final_chat_id = chat.id
                    print(f"WORKER [{SERVER_ID}]: ✅ Resolved {chat_str} to ID: {final_chat_id}", flush=True)
                elif chat_str.lstrip("-").isdigit():
                    final_chat_id = int(chat_str)
                else:
                    raise Exception(f"Invalid chat format: {chat_str}")
                
                start_time = time.time()
                
                with SmartStream(file_url, filename) as stream:
                    if stream.total_size == 0:
                        raise Exception("File size is 0. Link may be expired.")
                    
                    print(f"WORKER [{SERVER_ID}]: Uploading {filename} ({stream.total_size/1024/1024:.1f}MB)...", flush=True)
                    last_log_bytes[0] = 0
                    last_log_time[0] = time.time()
                    
                    msg = await tg_app.send_document(
                        chat_id=final_chat_id,
                        document=stream,
                        caption=caption,
                        file_name=filename,
                        force_document=True,
                        progress=progress_callback
                    )

                    elapsed = time.time() - start_time
                    avg_speed = (stream.total_size / elapsed) / (1024 * 1024)

                    clean_id = str(msg.chat.id).replace('-100', '')
                    private_link = f"https://t.me/c/{clean_id}/{msg.id}"

                    print(f"WORKER [{SERVER_ID}]: ✅ Upload complete! {elapsed:.1f}s ({avg_speed:.1f} MB/s avg)", flush=True)
                    print(f"WORKER [{SERVER_ID}]: Link: {private_link}", flush=True)

                    return {
                        "success": True,
                        "message_id": msg.id,
                        "chat_id": msg.chat.id,
                        "file_id": msg.document.file_id,
                        "private_link": private_link,
                        "file_size": msg.document.file_size,
                        "duration": 0,
                        "upload_time": elapsed,
                        "avg_speed_mbps": avg_speed,
                        "server": SERVER_ID
                    }
        
        except FloodWait as e:
            print(f"WORKER [{SERVER_ID}]: FloodWait {e.value}s, waiting...", flush=True)
            await asyncio.sleep(e.value)
            retry_count += 1
            
        except Exception as e:
            retry_count += 1
            error_msg = str(e)
            print(f"WORKER [{SERVER_ID}]: Error: {error_msg}", flush=True)
            
            if retry_count >= max_retries:
                raise Exception(f"Upload failed after {max_retries} retries: {error_msg}")
            
            print(f"WORKER [{SERVER_ID}]: Retry {retry_count}/{max_retries}...", flush=True)
            await asyncio.sleep(3)

# ============================================================
# SEND NOTIFICATION
# ============================================================

async def send_admin_notification(message, reply_markup=None):
    """Send notification to admin"""
    if not ADMIN_CHAT_ID:
        print(f"NOTIFICATION [{SERVER_ID}]: {message}", flush=True)
        return
    
    try:
        async with Client(
            SESSION_NAME,
            api_id=API_ID,
            api_hash=API_HASH,
            bot_token=BOT_TOKEN,
            workdir="/tmp"
        ) as tg_app:
            await tg_app.send_message(
                chat_id=int(ADMIN_CHAT_ID),
                text=f"[{SERVER_ID.upper()}] {message}",
                reply_markup=reply_markup
            )
            print(f"NOTIFICATION [{SERVER_ID}] SENT", flush=True)
    except Exception as e:
        print(f"NOTIFICATION [{SERVER_ID}] ERROR: {e}", flush=True)

# ============================================================
# QUEUE WORKER
# ============================================================

JOB_QUEUE = queue.Queue()
JOBS = {} 
WORKER_THREAD = None
WORKER_LOCK = threading.Lock()

def worker_loop():
    """Background worker"""
    print(f"SYSTEM [{SERVER_ID}]: Queue Worker Started", flush=True)
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    
    while True:
        job_id = None
        try:
            job_id, data = JOB_QUEUE.get()
            print(f"WORKER [{SERVER_ID}]: Job {job_id}", flush=True)
            JOBS[job_id]['status'] = 'processing'
            JOBS[job_id]['started'] = time.time()
            JOBS[job_id]['server'] = SERVER_ID
            
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
            print(f"WORKER [{SERVER_ID}]: ✅ Job {job_id} done!", flush=True)
            
            log_activity("success", f"Uploaded: {data.get('filename', 'video.mp4')}")
            update_daily_stats("uploads")
            update_daily_stats("total_bytes", result.get('file_size', 0))
            update_daily_stats("total_time", result.get('upload_time', 0))
            
        except Exception as e:
            error_msg = str(e)
            print(f"WORKER [{SERVER_ID}] ERROR: {error_msg}", flush=True)
            
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
            print(f"SYSTEM [{SERVER_ID}]: Starting worker thread...", flush=True)
            WORKER_THREAD = threading.Thread(target=worker_loop, daemon=True)
            WORKER_THREAD.start()

# ============================================================
# ADMIN DASHBOARD ROUTES
# ============================================================

@app.route('/admin')
def admin_dashboard():
    """Serve admin dashboard HTML"""
    return render_template('admin.html')

@app.route('/admin/api/storage/<int:account_id>')
def admin_get_storage(account_id):
    """Get storage usage for specific account"""
    try:
        storage = get_account_storage(account_id)
        return jsonify(storage)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/admin/api/clear-trash/<int:account_id>', methods=['POST'])
def admin_clear_trash(account_id):
    """Empty trash for specific account"""
    try:
        account = next((a for a in PIKPAK_ACCOUNTS if a["id"] == account_id), None)
        if not account:
            return jsonify({"error": "Account not found"}), 404
            
        print(f"PIKPAK [{SERVER_ID}]: Clearing trash for account {account_id}", flush=True)
        tokens = ensure_logged_in(account)
        
        # Empty trash
        device_id = account["device_id"]
        captcha_sign, timestamp = generate_captcha_sign(device_id)
        captcha_token = get_pikpak_captcha(
            action="PATCH:/drive/v1/files/trash:empty",
            device_id=device_id,
            user_id=tokens["user_id"],
            captcha_sign=captcha_sign,
            timestamp=timestamp
        )
        
        url = f"{PIKPAK_API_DRIVE}/drive/v1/files/trash:empty"
        headers = {
            "Authorization": f"Bearer {tokens['access_token']}",
            "x-device-id": device_id,
            "x-captcha-token": captcha_token
        }
        requests.patch(url, headers=headers, json={}, timeout=30)
        
        # Invalidate cache
        if f"account_{account_id}" in PIKPAK_STORAGE_CACHE:
            del PIKPAK_STORAGE_CACHE[f"account_{account_id}"]
        
        return jsonify({"success": True, "message": "Trash cleared"})
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Failed to clear trash for account {account_id}: {e}", flush=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/admin/api/clear-mypack/<int:account_id>', methods=['POST'])
def admin_clear_mypack(account_id):
    """Delete ALL files in the root folder (My Pack)"""
    try:
        account = next((a for a in PIKPAK_ACCOUNTS if a["id"] == account_id), None)
        if not account:
            return jsonify({"error": "Account not found"}), 404
            
        print(f"PIKPAK [{SERVER_ID}]: Clearing My Pack for account {account_id}", flush=True)
        tokens = ensure_logged_in(account)
        
        # List files in root
        files = pikpak_list_files("root", account, tokens)
        deleted_count = len(files)
        
        if files:
            # Batch trash
            device_id = account["device_id"]
            captcha_sign, timestamp = generate_captcha_sign(device_id)
            captcha_token = get_pikpak_captcha(
                action="POST:/drive/v1/files:batchTrash",
                device_id=device_id,
                user_id=tokens["user_id"],
                captcha_sign=captcha_sign,
                timestamp=timestamp
            )
            
            url = f"{PIKPAK_API_DRIVE}/drive/v1/files:batchTrash"
            headers = {
                "Content-Type": "application/json",
                "Authorization": f"Bearer {tokens['access_token']}",
                "x-device-id": device_id,
                "x-captcha-token": captcha_token
            }
            body = {"ids": [f["id"] for f in files]}
            requests.post(url, headers=headers, json=body, timeout=30)
            
            # Empty trash
            admin_clear_trash(account_id)
        
        return jsonify({"success": True, "message": "My Pack cleared", "files_deleted": deleted_count})
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Failed to clear My Pack for account {account_id}: {e}", flush=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/admin/api/clear-all-trash', methods=['POST'])
def admin_clear_all_trash():
    """Empty trash for ALL accounts"""
    count = 0
    for account in PIKPAK_ACCOUNTS:
        try:
            admin_clear_trash(account["id"])
            count += 1
        except:
            pass
    return jsonify({"success": True, "accounts_cleared": count})

@app.route('/admin/api/clear-all-mypack', methods=['POST'])
def admin_clear_all_mypack():
    """Clear My Pack for ALL accounts"""
    count = 0
    for account in PIKPAK_ACCOUNTS:
        try:
            admin_clear_mypack(account["id"])
            count += 1
        except:
            pass
    return jsonify({"success": True, "accounts_cleared": count})

@app.route('/admin/api/status')
def admin_api_status():
    """Get complete admin dashboard data"""
    
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
        
        storage = get_account_storage(account["id"])
        
        accounts_list.append({
            "id": account["id"],
            "email": account["email"],
            "downloads_today": downloads_today,
            "downloads_remaining": remaining,
            "available": remaining > 0,
            "storage_used_gb": storage["used_gb"],
            "storage_total_gb": storage["total_gb"],
            "storage_percent": storage["percent"]
        })
    
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
    
    completed = sum(1 for j in JOBS.values() if j.get('status') == 'done')
    failed = sum(1 for j in JOBS.values() if j.get('status') == 'failed')
    processing = sum(1 for j in JOBS.values() if j.get('status') == 'processing')
    
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
        "server": {
            "id": SERVER_ID,
            "mode": SERVER_MODE,
            "url": SERVER_URL,
            "handles": "1080p, 2160p, 4K (large files)"
        },
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
        magnet = request.json.get('magnet', '').strip()
        
        if not magnet:
            return jsonify({"valid": False, "error": "No magnet provided"})
        
        original_magnet = magnet
        if '-magnet:' in magnet:
            parts = magnet.split('-', 1)
            if len(parts) == 2:
                magnet = parts[1]
        
        if not magnet.startswith('magnet:?'):
            return jsonify({"valid": False, "error": "Invalid magnet format. Must start with 'magnet:?'"})
        
        name = "Unknown"
        dn_match = re.search(r'dn=([^&]+)', magnet)
        if dn_match:
            name = dn_match.group(1)
            name = name.replace('+', ' ').replace('%20', ' ').replace('%28', '(').replace('%29', ')')
        
        quality = "Unknown"
        
        if original_magnet.startswith('1080p-'):
            quality = "1080p"
        elif original_magnet.startswith('720p-'):
            quality = "720p"
        elif original_magnet.startswith('480p-'):
            quality = "480p"
        elif original_magnet.startswith('2160p-') or original_magnet.startswith('4k-'):
            quality = "4K"
        else:
            name_lower = name.lower()
            if '1080p' in name_lower:
                quality = "1080p"
            elif '720p' in name_lower:
                quality = "720p"
            elif '480p' in name_lower:
                quality = "480p"
            elif '2160p' in name_lower or '4k' in name_lower:
                quality = "4K"
        
        available_account = "None available"
        try:
            account = select_available_account()
            available_account = account["id"]
        except:
            pass
        
        return jsonify({
            "valid": True,
            "name": name,
            "quality": quality,
            "account": available_account,
            "server": SERVER_ID
        })
        
    except Exception as e:
        return jsonify({"valid": False, "error": str(e)})

# ============================================================
# FLASK ROUTES (MODIFIED - Added server info)
# ============================================================

@app.route('/')
def home(): 
    """Health check with server identification"""
    ensure_worker_alive()
    return jsonify({
        "status": "online",
        "server": {
            "id": SERVER_ID,
            "mode": SERVER_MODE,
            "url": SERVER_URL,
            "handles": "1080p, 2160p, 4K (large files)"
        },
        "service": "PikPak-Telegram Bridge",
        "queue": JOB_QUEUE.qsize(),
        "jobs": len(JOBS),
        "sessions": len(MESSAGE_SESSIONS),
        "pikpak_accounts": len(PIKPAK_ACCOUNTS),
        "pikpak_account_ids": [acc["id"] for acc in PIKPAK_ACCOUNTS],
        "worker_alive": WORKER_THREAD.is_alive() if WORKER_THREAD else False
    })

@app.route('/static/<path:filename>')
def serve_static(filename):
    """Serve static files"""
    return send_from_directory('static', filename)

# ============================================================
# SESSION ROUTES
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
    
    print(f"SESSION [{SERVER_ID}]: Started \"{poster_msg_id}\"", flush=True)
    return jsonify({"status": "session_started", "poster_msg_id": poster_msg_id, "server": SERVER_ID})

@app.route('/add-magnet-to-session', methods=['POST'])
def add_magnet_to_session():
    """Add magnet to session"""
    data = request.json
    poster_msg_id = str(data.get('poster_message_id'))
    magnet = data.get('magnet')
    
    with SESSION_LOCK:
        if poster_msg_id not in MESSAGE_SESSIONS:
            print(f"SESSION [{SERVER_ID}] ERROR: \"{poster_msg_id}\" not found.", flush=True)
            return jsonify({"error": "Session not found", "session_id": poster_msg_id}), 404
        
        session = MESSAGE_SESSIONS[poster_msg_id]
        
        if time.time() > session['timeout']:
            del MESSAGE_SESSIONS[poster_msg_id]
            return jsonify({"error": "timeout"}), 408
        
        if len(session['magnets']) >= 3:
            return jsonify({"error": "max_magnets"}), 400
        
        session['magnets'].append(magnet)
        print(f"SESSION [{SERVER_ID}]: Added magnet {len(session['magnets'])}/3", flush=True)
    
    return jsonify({"status": "magnet_added", "count": len(session['magnets']), "server": SERVER_ID})

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
        
        return jsonify({**session, "server": SERVER_ID})

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
            'count': len(session['magnets']),
            'server': SERVER_ID
        }
        
        del MESSAGE_SESSIONS[poster_msg_id]
        print(f"SESSION [{SERVER_ID}]: Completed with {len(session['magnets'])} magnets", flush=True)
        
        return jsonify(result)

@app.route('/debug/sessions', methods=['GET'])
def debug_sessions():
    """Debug: Show all active sessions"""
    with SESSION_LOCK:
        return jsonify({
            "server": SERVER_ID,
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
    print(f"🚨 [{SERVER_ID}] EMERGENCY STOP ACTIVATED!", flush=True)
    return jsonify({"status": "stopped", "message": "Emergency stop activated", "server": SERVER_ID})

@app.route('/emergency-resume', methods=['POST'])
def emergency_resume():
    """Resume PikPak operations"""
    global EMERGENCY_STOP
    EMERGENCY_STOP = False
    print(f"✅ [{SERVER_ID}] EMERGENCY STOP DEACTIVATED - Resumed", flush=True)
    return jsonify({"status": "resumed", "message": "Operations resumed", "server": SERVER_ID})

@app.route('/emergency-status', methods=['GET'])
def emergency_status():
    """Check emergency stop status"""
    return jsonify({"emergency_stop": EMERGENCY_STOP, "server": SERVER_ID})

# ============================================================
# PIKPAK ADD MAGNET ROUTE
# ============================================================

def get_magnet_name(magnet_link):
    """DEPRECATED: Use extract_magnet_info instead."""
    import re
    from urllib.parse import unquote
    match = re.search(r'dn=([^&]+)', magnet_link)
    if match:
        return unquote(match.group(1)).replace('+', ' ')
    return None

def extract_magnet_info(magnet_link):
    """Extracts name and BTIH hash from a magnet link."""
    import re
    from urllib.parse import unquote
    
    name_match = re.search(r'dn=([^&]+)', magnet_link)
    name = unquote(name_match.group(1)).replace('+', ' ') if name_match else None
    
    hash_match = re.search(r'xt=urn:btih:([^&]+)', magnet_link, re.IGNORECASE)
    btih_hash = hash_match.group(1).lower() if hash_match else None
    
    return {"name": name, "hash": btih_hash}

def normalize_name(name):
    """Normalizes a name for fuzzy matching by lowercasing and removing symbols."""
    if not name:
        return ""
    # Lowercase, remove non-alphanumeric chars (except spaces), and collapse whitespace
    return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', name.lower())).strip()

def check_duplicate(magnet_link, account, tokens, user_quality):
    """
    Checks for duplicates in PikPak before adding a magnet.
    Returns file info dictionary if a duplicate is found, otherwise None.
    """
    try:
        print(f"PIKPAK [{SERVER_ID}]: Running smart duplicate check...", flush=True)
        magnet_info = extract_magnet_info(magnet_link)
        
        if not magnet_info["name"] or not magnet_info["hash"]:
            print(f"PIKPAK [{SERVER_ID}]: Could not extract sufficient info from magnet for duplicate check.", flush=True)
            return None
            
        normalized_magnet_name = normalize_name(magnet_info["name"])
        print(f"PIKPAK [{SERVER_ID}]: Normalized magnet name: '{normalized_magnet_name}' | Hash: '{magnet_info['hash'][:10]}...'", flush=True)

        # List files in the root directory (My Pack)
        files = pikpak_list_files(None, account, tokens) # None means root
        print(f"PIKPAK [{SERVER_ID}]: Found {len(files)} files in 'My Pack'. Starting comparison.", flush=True)

        for file in files:
            file_name = file.get('name')
            normalized_file_name = normalize_name(file_name)

            # 1. Fuzzy Name Matching
            if normalized_file_name == normalized_magnet_name:
                print(f"PIKPAK [{SERVER_ID}]: Fuzzy name match found: '{file_name}' (ID: {file['id']}). Verifying hash...", flush=True)
                
                # 2. Verify with hash
                try:
                    file_info = pikpak_get_file_info(file['id'], account, tokens)
                    params_url = file_info.get("params", {}).get("url", "")
                    
                    if magnet_info["hash"] in params_url.lower():
                        print(f"PIKPAK [{SERVER_ID}]: ✅ Found duplicate! Hash matches. File ID: {file['id']}", flush=True)
                        log_activity("info", f"Found duplicate: {file_name}")
                        
                        # We have a confirmed duplicate, prepare the response
                        download_url = pikpak_get_download_link(file['id'], account, tokens)
                        file_size = int(file.get('size', 0))
                        detected_quality = detect_quality(user_quality, magnet_link, file_size)
                        
                        return {
                           "result": True,
                           "folder_id": file['id'],
                           "file_id": file['id'],
                           "file_name": file['name'],
                           "file_size": file_size,
                           "url": download_url,
                           "account_used": account["id"],
                           "file_type": "file",
                           "quality_detected": detected_quality,
                           "server": SERVER_ID,
                           "quota_saved": True
                        }
                except Exception as e:
                    print(f"PIKPAK [{SERVER_ID}]: Verification failed for candidate '{file_name}': {e}", flush=True)
                    continue # Try next file
        
        print(f"PIKPAK [{SERVER_ID}]: No duplicate found after checking all files.", flush=True)
        return None

    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Smart duplicate check failed (continuing): {e}", flush=True)
        return None

@app.route('/add-magnet', methods=['POST'])
def add_magnet():
    """Add magnet to PikPak and return download link"""
    global EMERGENCY_STOP
    with MAGNET_ADD_LOCK:
        # Add random delay to prevent simultaneous requests
        import random
        time.sleep(random.uniform(1, 3))
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
            
            if EMERGENCY_STOP:
                print(f"🚨 [{SERVER_ID}] EMERGENCY STOP - Aborting magnet add", flush=True)
                return jsonify({
                    "error": "Emergency stop activated",
                    "retry": False,
                    "server": SERVER_ID
                }), 503
            
            try:
                print(f"PIKPAK [{SERVER_ID}]: === ADD MAGNET ATTEMPT {attempt}/{max_total_retries} ===", flush=True)
                
                # 1. Select account
                account = select_available_account(exclude_accounts=exhausted_accounts)
                last_account_id = account["id"]
                tokens = ensure_logged_in(account)

                # 2. Smart Duplicate Check
                duplicate_file = check_duplicate(magnet, account, tokens, user_quality)
                if duplicate_file:
                    # If duplicate found, return its info and skip adding
                    return jsonify(duplicate_file)

                # 3. Proceed with normal download if not found...
                print(f"PIKPAK [{SERVER_ID}]: No duplicate found. Proceeding to add magnet.", flush=True)
                task = pikpak_add_magnet(magnet, account, tokens)
                folder_id = task.get("file_id")
                file_name = task.get("file_name", "Unknown")
                
                if not folder_id or str(folder_id).strip() == "":
                    error_msg = "PikPak returned empty file_id - magnet may be invalid"
                    print(f"PIKPAK [{SERVER_ID}]: ❌ {error_msg}", flush=True)
                    print(f"PIKPAK [{SERVER_ID}]: ⚠️ STOPPING - No retry to save quota", flush=True)
                    
                    return jsonify({
                        "error": error_msg,
                        "retry": False,
                        "account_used": account["id"],
                        "server": SERVER_ID
                    }), 400
                
                pikpak_poll_download(folder_id, account, tokens, timeout=900)
                
                tokens = ensure_logged_in(account)
                
                file_info = pikpak_get_file_info(folder_id, account, tokens)
                kind = file_info.get("kind", "")
                
                print(f"PIKPAK [{SERVER_ID}]: File kind: {kind}", flush=True)
                
                video_file = None
                download_url = None
                
                if kind == "drive#folder":
                    print(f"PIKPAK [{SERVER_ID}]: Detected FOLDER, listing contents...", flush=True)
                    files = pikpak_list_files(folder_id, account, tokens)
                    
                    video_file = find_video_file(files)
                    if not video_file:
                        error_msg = "No video file found in folder"
                        print(f"PIKPAK [{SERVER_ID}]: ❌ {error_msg}", flush=True)
                        
                        return jsonify({
                            "error": error_msg,
                            "retry": False,
                            "account_used": account["id"],
                            "server": SERVER_ID
                        }), 400
                    
                    tokens = ensure_logged_in(account)
                    download_url = pikpak_get_download_link(video_file["id"], account, tokens)
                    
                else:
                    print(f"PIKPAK [{SERVER_ID}]: Detected SINGLE FILE", flush=True)
                    
                    if not is_video_file(file_info):
                        error_msg = "Downloaded file is not a video"
                        print(f"PIKPAK [{SERVER_ID}]: ❌ {error_msg}", flush=True)
                        
                        return jsonify({
                            "error": error_msg,
                            "retry": False,
                            "account_used": account["id"],
                            "server": SERVER_ID
                        }), 400
                    
                    video_file = file_info
                    download_url = file_info.get("web_content_link", "")
                    
                    if not download_url:
                        tokens = ensure_logged_in(account)
                        download_url = pikpak_get_download_link(folder_id, account, tokens)
                
                file_size = int(video_file.get("size", 0))
                file_size_mb = file_size / 1024 / 1024
                
                if file_size_mb > 2048:
                    error_msg = f"File too large: {file_size_mb:.0f}MB (max 2048MB)"
                    print(f"PIKPAK [{SERVER_ID}]: ❌ {error_msg}", flush=True)
                    
                    return jsonify({
                        "error": error_msg,
                        "retry": False,
                        "account_used": account["id"],
                        "server": SERVER_ID
                    }), 400
                
                increment_account_usage(account["id"])
                
                detected_quality = detect_quality(user_quality, magnet, file_size)
                
                print(f"PIKPAK [{SERVER_ID}]: === ADD MAGNET SUCCESS ===", flush=True)
                log_activity("success", f"Downloaded: {video_file.get('name', file_name)}")
                update_daily_stats("downloads")
                print(f"PIKPAK [{SERVER_ID}]: Quality detected: {detected_quality}", flush=True)
                
                return jsonify({
                    "result": True,
                    "folder_id": folder_id,
                    "file_id": video_file.get("id", folder_id),
                    "file_name": video_file.get("name", file_name),
                    "file_size": file_size,
                    "url": download_url,
                    "account_used": account["id"],
                    "file_type": "folder" if kind == "drive#folder" else "file",
                    "quality_detected": detected_quality,
                    "server": SERVER_ID
                })
                
            except Exception as e:
                error_msg = str(e)
                print(f"PIKPAK [{SERVER_ID}]: Error on attempt {attempt}: {error_msg}", flush=True)
                
                if "task_daily_create_limit" in error_msg:
                    print(f"PIKPAK [{SERVER_ID}]: ⚠️ Account {last_account_id} hit daily limit!", flush=True)
                    if last_account_id:
                        mark_account_exhausted(last_account_id)
                        exhausted_accounts.append(last_account_id)
                    print(f"PIKPAK [{SERVER_ID}]: 🔄 Rotating to next account...", flush=True)
                    continue
                
                if "All PikPak accounts exhausted" in error_msg:
                    return jsonify({"error": error_msg, "retry": False, "server": SERVER_ID}), 500
                
                if "Read timed out" in error_msg or "Download timeout" in error_msg:
                     print(f"PIKPAK [{SERVER_ID}]: ❌ Timeout error, stopping", flush=True)
                     return jsonify({
                        "error": error_msg,
                        "retry": False,
                        "attempts": attempt,
                        "account_used": last_account_id,
                        "server": SERVER_ID
                    }), 500

                if attempt >= 3:
                    print(f"PIKPAK [{SERVER_ID}]: ❌ Max retries reached, stopping", flush=True)
                    return jsonify({
                        "error": error_msg,
                        "retry": False,
                        "attempts": attempt,
                        "account_used": last_account_id,
                        "server": SERVER_ID
                    }), 500
                
                print(f"PIKPAK [{SERVER_ID}]: Retrying in 5 seconds...", flush=True)
                time.sleep(5)
    
    return jsonify({"error": "Max retries exceeded", "retry": False, "server": SERVER_ID}), 500

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
            "files": files,
            "server": SERVER_ID
        })
        
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: List files error: {e}", flush=True)
        return jsonify({"error": str(e), "server": SERVER_ID}), 500

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
        
        return jsonify({"url": download_url, "server": SERVER_ID})
        
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Get link error: {e}", flush=True)
        return jsonify({"error": str(e), "server": SERVER_ID}), 500

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
        
        return jsonify({"result": True, "deleted_id": folder_id, "server": SERVER_ID})
        
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Delete error: {e}", flush=True)
        return jsonify({"error": str(e), "server": SERVER_ID}), 500

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
            "server": SERVER_ID,
            "accounts": accounts_status,
            "total_remaining": total_remaining,
            "total_accounts": len(PIKPAK_ACCOUNTS)
        })
        
    except Exception as e:
        return jsonify({"error": str(e), "server": SERVER_ID}), 500

# ============================================================
# UPLOAD & JOB ROUTES
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
        'created': time.time(),
        'server': SERVER_ID
    }
    JOB_QUEUE.put((job_id, data))
    
    print(f"UPLOAD [{SERVER_ID}]: Queued job {job_id}", flush=True)
    
    return jsonify({"job_id": job_id, "status": "queued", "server": SERVER_ID})

@app.route('/job-status/<job_id>', methods=['GET'])
def job_status(job_id):
    """Check job status"""
    ensure_worker_alive()
    job = JOBS.get(job_id)
    if not job:
        return jsonify({"status": "not_found", "server": SERVER_ID}), 404
    return jsonify({**job, "server": SERVER_ID})

@app.route('/extract-metadata', methods=['POST'])
def extract_metadata():
    """Extract metadata from magnet"""
    try:
        magnet = request.json.get('magnet', '')
        metadata = extract_metadata_from_magnet(magnet)
        return jsonify({**metadata, "server": SERVER_ID})
    except Exception as e:
        return jsonify({"error": str(e), "server": SERVER_ID}), 500

@app.route('/detect-quality-from-size', methods=['POST'])
def detect_quality_api():
    """Detect quality from file size"""
    try:
        size_bytes = int(request.json.get('size_bytes', 0))
        quality = detect_quality_from_size(size_bytes)
        return jsonify({"quality": quality, "server": SERVER_ID})
    except Exception as e:
        return jsonify({"error": str(e), "server": SERVER_ID}), 500

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
                    print(f"SESSION [{SERVER_ID}]: Cleaned up expired \"{session_id}\"", flush=True)
        except Exception as e:
            print(f"CLEANUP [{SERVER_ID}] ERROR: {e}", flush=True)

cleanup_thread = threading.Thread(target=cleanup_sessions, daemon=True)
cleanup_thread.start()

# ============================================================
# MAIN
# ============================================================

if __name__ == '__main__':
    print("=" * 60, flush=True)
    print(f"🚀 PikPak-Telegram Bridge - SERVER: {SERVER_ID.upper()}", flush=True)
    print(f"📍 URL: {SERVER_URL}", flush=True)
    print(f"🎬 Handles: 1080p, 2160p, 4K (large files)", flush=True)
    print(f"📦 Loaded {len(PIKPAK_ACCOUNTS)} PikPak accounts: {[a['id'] for a in PIKPAK_ACCOUNTS]}", flush=True)
    print(f"🔑 Session: {SESSION_NAME}", flush=True)
    print("=" * 60, flush=True)
    check_all_accounts_quota() 
    ensure_worker_alive()
    app.run(host='0.0.0.0', port=10000)