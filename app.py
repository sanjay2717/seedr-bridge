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
from pyrogram import Client, enums
from pyrogram.errors import FloodWait, ChannelPrivate, ChatAdminRequired
from pyrogram.types import InlineKeyboardMarkup, InlineKeyboardButton

app = Flask(__name__)

# ============================================================
# SERVER IDENTIFICATION
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

class EmergencyStopError(Exception):
    """Custom exception for emergency stop."""
    pass

EMERGENCY_STOP = False

# Message collection storage
MESSAGE_SESSIONS = {}
SESSION_LOCK = threading.Lock()

# Activity log storage
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

from supabase_client import db

# ============================================================
# DB CONFIG
# ============================================================
DB_SERVER_ID = 1 # This is SERVER 1 (Primary)

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

PIKPAK_TOKENS_FILE = f"/tmp/pikpak_tokens_{SERVER_ID}.json"
PIKPAK_LOCK = threading.Lock()
MAGNET_ADD_LOCK = threading.Lock()
PIKPAK_STORAGE_CACHE = {}
PIKPAK_STORAGE_CACHE_TIME = {}

def get_account_storage(account):
    """Get storage info for account with caching"""
    global PIKPAK_STORAGE_CACHE, PIKPAK_STORAGE_CACHE_TIME
    
    account_id = account.get("id")
    cache_key = f"account_{account_id}"
    current_time = time.time()
    
    # Return cached if less than 5 minutes old
    if cache_key in PIKPAK_STORAGE_CACHE:
        if current_time - PIKPAK_STORAGE_CACHE_TIME.get(cache_key, 0) < 300:
            return PIKPAK_STORAGE_CACHE[cache_key]
    
    try:
        tokens = ensure_logged_in(account)
        # The account object passed in should already have device_id mapped
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
        
        # Extract the download quota from the API response
        cloud_quota = data.get("quotas", {}).get("cloud_download", {})
        real_usage = int(cloud_quota.get("usage", 0))
        real_limit = int(cloud_quota.get("limit", 5))
        
        # Sync DB with Real Usage + Storage Stats
        try:
            db.sync_account_stats(
                account_id=account_id,
                download_usage=real_usage,
                storage_used=used_bytes,
                storage_limit=total_bytes
            )
            print(f"PIKPAK [{SERVER_ID}]: Synced ALL stats for account {account_id}: Quota={real_usage}/{real_limit}, Storage={used_gb}GB/{total_gb}GB", flush=True)
        except Exception as e:
            print(f"PIKPAK [{SERVER_ID}]: Failed to sync stats: {e}", flush=True)
            
        result = {
            "used_gb": used_gb,
            "total_gb": total_gb,
            "percent": percent,
            "downloads_used": real_usage,
            "downloads_limit": real_limit
        }
        
        PIKPAK_STORAGE_CACHE[cache_key] = result
        PIKPAK_STORAGE_CACHE_TIME[cache_key] = current_time
        
        return result
        
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Storage check failed for account {account_id}: {e}", flush=True)
        return {"used_gb": 0, "total_gb": 6, "percent": 0}

def check_all_accounts_quota():
    """Check quota for all accounts on startup using the DB"""
    print(f"PIKPAK [{SERVER_ID}]: Checking account quotas on startup...", flush=True)
    
    try:
        accounts = db.get_all_server_accounts(DB_SERVER_ID)
        if not accounts:
            print(f"PIKPAK [{SERVER_ID}]: No accounts found in DB for server {DB_SERVER_ID}.", flush=True)
            return

        for account in accounts:
            try:
                # Map device ID for compatibility
                account['device_id'] = account.get('current_device_id')
                
                # Login and get storage
                storage = get_account_storage(account)
                
                print(f"PIKPAK [{SERVER_ID}]: Account {account['id']} - Storage: {storage['used_gb']}GB/{storage['total_gb']}GB ({storage['percent']}%)", flush=True)
                print(f"PIKPAK [{SERVER_ID}]: Account {account['id']} - Login successful ✅", flush=True)
                
            except Exception as e:
                print(f"PIKPAK [{SERVER_ID}]: Account {account.get('id', 'N/A')} - Check failed: {e}", flush=True)
    
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Failed to fetch accounts from DB for startup check: {e}", flush=True)

    print(f"PIKPAK [{SERVER_ID}]: Startup quota check complete", flush=True)

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

def get_best_account(exclude_ids=None):
    """
    Selects the best available PikPak account from the database.
    This function is a wrapper around the Supabase DB call.
    """
    if exclude_ids is None:
        exclude_ids = []

    print(f"PIKPAK [{SERVER_ID}]: Getting best account from DB, excluding IDs: {exclude_ids}", flush=True)
    
    # The DB function 'get_best_account_v2' is expected to handle the logic
    # of finding an active, non-exhausted account.
    account_data = db.get_best_account(DB_SERVER_ID, exclude_ids)
    
    if not account_data:
        raise Exception(f"All PikPak accounts exhausted for today on {SERVER_ID}")

    # Map DB field `current_device_id` to `device_id` for compatibility
    account_data['device_id'] = account_data.get('current_device_id')
    
    print(f"PIKPAK [{SERVER_ID}]: ✅ Selected account {account_data['id']} from DB", flush=True)
    return account_data

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
    
    if "task" in data:
        task = data["task"]
        file_id = task.get("file_id") or task.get("id") or ""
        file_name = task.get("file_name") or task.get("name") or "Unknown"
        print(f"PIKPAK [{SERVER_ID}]: ✅ Magnet added: {file_name} (file_id: {file_id})", flush=True)
        task["file_id"] = file_id
        task["file_name"] = file_name
        return task
    elif "file" in data:
        file_data = data["file"]
        file_id = file_data.get("id") or file_data.get("file_id") or ""
        file_name = file_data.get("name") or file_data.get("file_name") or "Unknown"
        print(f"PIKPAK [{SERVER_ID}]: ✅ Magnet added (file format): {file_name} (file_id: {file_id})", flush=True)
        return {"file_id": file_id, "file_name": file_name, **file_data}
    elif "id" in data:
        file_id = data.get("id") or ""
        file_name = data.get("name") or "Unknown"
        print(f"PIKPAK [{SERVER_ID}]: ✅ Magnet added (direct format): {file_name} (file_id: {file_id})", flush=True)
        return {"file_id": file_id, "file_name": file_name, **data}
    else:
        print(f"PIKPAK [{SERVER_ID}]: ❌ Add magnet failed: {data}", flush=True)
        raise Exception(f"Add magnet failed: {data.get('error', 'Unknown')}")

def pikpak_poll_download(file_id, account, tokens, timeout=120, filename=None, file_hash=None):
    """Poll until download completes with recovery. Returns the final file_id."""
    print(f"PIKPAK [{SERVER_ID}]: Polling download for {file_id} ({filename})", flush=True)
    
    device_id = account["device_id"]
    user_id = tokens["user_id"]
    access_token = tokens["access_token"]
    
    start_time = time.time()
    poll_interval = 5
    last_recovery_time = time.time()
    original_file_id = file_id
    
    while time.time() - start_time < timeout:
        if EMERGENCY_STOP:
            raise EmergencyStopError("Emergency stop activated during polling")
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

            if response.status_code == 404 or data.get("error") == "file_not_found":
                print(f"PIKPAK [{SERVER_ID}]: File {file_id} not found (404), waiting...", flush=True)
                
                # Trigger recovery search every 30 seconds
                if time.time() - last_recovery_time > 30:
                    last_recovery_time = time.time()
                    print(f"PIKPAK [{SERVER_ID}]: Triggering Recovery Search for '{filename}'", flush=True)
                    
                    try:
                        parent_id = account.get("my_pack_id") or ""
                        all_files = pikpak_list_files(parent_id, account, tokens)
                        found_file = None
                        
                        # 1. Match by hash
                        if file_hash:
                            for f in all_files:
                                try:
                                    f_info = pikpak_get_file_info(f['id'], account, tokens)
                                    params_url = f_info.get("params", {}).get("url", "")
                                    if file_hash.upper() in params_url.upper():
                                        found_file = f_info
                                        print(f"PIKPAK [{SERVER_ID}]: Recovery: Found match by HASH: {f_info.get('name')}", flush=True)
                                        break
                                except:
                                    continue
                        
                        # 2. Match by name (if no hash match)
                        if not found_file and filename:
                            normalized_target_name = normalize_name(filename)
                            for f in all_files:
                                if normalize_name(f.get('name')) == normalized_target_name:
                                    found_file = pikpak_get_file_info(f['id'], account, tokens)
                                    print(f"PIKPAK [{SERVER_ID}]: Recovery: Found match by NAME: {found_file.get('name')}", flush=True)
                                    break
                        
                        if found_file:
                            new_file_id = found_file.get('id')
                            phase = found_file.get("phase", "")
                            
                            if phase == "PHASE_TYPE_COMPLETE":
                                print(f"PIKPAK [{SERVER_ID}]: ✅ Download complete! (Found via recovery)", flush=True)
                                if new_file_id != original_file_id:
                                    print(f"PIKPAK [{SERVER_ID}]: ID switched from {original_file_id} to {new_file_id}", flush=True)
                                return new_file_id
                            
                            if new_file_id and new_file_id != file_id:
                                print(f"PIKPAK [{SERVER_ID}]: Recovery: Switched polling from {file_id} to {new_file_id}", flush=True)
                                file_id = new_file_id
                                continue
                                
                    except Exception as recovery_exc:
                        print(f"PIKPAK [{SERVER_ID}]: Recovery search failed: {recovery_exc}", flush=True)

                time.sleep(poll_interval)
                continue
            
            phase = data.get("phase", "")
            progress = data.get("progress", 0)
            
            print(f"PIKPAK [{SERVER_ID}]: Status: {phase} ({progress}%) for {file_id}", flush=True)
            
            # Robust completion check
            if phase == "PHASE_TYPE_COMPLETE" or data.get('progress') == 100:
                print(f"PIKPAK [{SERVER_ID}]: ✅ Download complete! (Phase: {phase}, Progress: {progress}%)", flush=True)
                if file_id != original_file_id:
                     print(f"PIKPAK [{SERVER_ID}]: ID switched from {original_file_id} to {file_id}", flush=True)
                return file_id
            elif phase == "PHASE_TYPE_ERROR":
                raise Exception(f"Download failed: {data.get('message', 'Unknown error')}")
            else:
                # Debug log for unexpected responses
                if not phase or phase != "PHASE_TYPE_COMPLETE":
                    print(f"DEBUG RESPONSE: {data}", flush=True)

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
        "limit": 100,
        "filters": '{"trashed":{"eq":false}}'
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
# ASYNC UPLOAD LOGIC
# ============================================================

async def perform_upload(file_url, chat_target, caption, filename, file_size_mb=0):
    """Upload video with optimized speed"""
    # Check emergency stop before starting
    if EMERGENCY_STOP:
        raise EmergencyStopError("Emergency stop activated - upload cancelled")
    
    if file_size_mb > 2048:
        raise Exception(f"File too large: {file_size_mb:.1f}MB (max 2048MB)")
    
    max_retries = 3
    retry_count = 0
    last_log_time = [time.time()]
    last_log_bytes = [0]
    
    def progress_callback(current, total):
        # Check emergency stop during upload
        if EMERGENCY_STOP:
            raise EmergencyStopError("Emergency stop activated during upload")
        
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
                SESSION_NAME,
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
                        progress=progress_callback,
                        parse_mode=enums.ParseMode.HTML,
                        thumb="thumbnail.jpg" if os.path.exists("thumbnail.jpg") else None
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
        
        except EmergencyStopError:
            # Re-raise to be caught by worker_loop without retrying
            raise
            
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
            
        except EmergencyStopError as e:
            error_msg = "cancelled by emergency stop"
            print(f"WORKER [{SERVER_ID}] CANCELLED: Job {job_id} cancelled by emergency stop.", flush=True)
            if job_id:
                JOBS[job_id]['status'] = 'failed'
                log_activity("failed", f"Upload cancelled: Emergency Stop")
                update_daily_stats("failed")
                JOBS[job_id]['error'] = error_msg
                JOBS[job_id]['failed'] = time.time()

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
    """Get storage usage for a specific account from DB"""
    try:
        all_accounts = db.get_all_server_accounts(DB_SERVER_ID)
        account = next((acc for acc in all_accounts if acc['id'] == account_id), None)
        
        if not account:
            return jsonify({"error": "Account not found"}), 404

        account['device_id'] = account.get('current_device_id')
        storage = get_account_storage(account)
        return jsonify(storage)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/admin/api/clear-trash/<int:account_id>', methods=['POST'])
def admin_clear_trash(account_id):
    """Empty trash for a specific account using DB"""
    try:
        all_accounts = db.get_all_server_accounts(DB_SERVER_ID)
        account = next((acc for acc in all_accounts if acc['id'] == account_id), None)

        if not account:
            return jsonify({"error": "Account not found"}), 404
        
        account['device_id'] = account.get('current_device_id')
        
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
        cache_key = f"account_{account_id}"
        if cache_key in PIKPAK_STORAGE_CACHE:
            del PIKPAK_STORAGE_CACHE[cache_key]
        if cache_key in PIKPAK_STORAGE_CACHE_TIME:
            del PIKPAK_STORAGE_CACHE_TIME[cache_key]

        get_account_storage(account)
        
        return jsonify({"success": True, "message": "Trash cleared"})
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Failed to clear trash for account {account_id}: {e}", flush=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/admin/api/clear-mypack/<int:account_id>', methods=['POST'])
def admin_clear_mypack(account_id):
    """Delete ALL files in the My Pack folder for an account from DB"""
    try:
        all_accounts = db.get_all_server_accounts(DB_SERVER_ID)
        account = next((acc for acc in all_accounts if acc['id'] == account_id), None)

        if not account:
            return jsonify({"error": "Account not found"}), 404
            
        account['device_id'] = account.get('current_device_id')

        print(f"PIKPAK [{SERVER_ID}]: Clearing My Pack for account {account_id}", flush=True)
        
        # Invalidate cache FIRST before any operations
        cache_key = f"account_{account_id}"
        if cache_key in PIKPAK_STORAGE_CACHE:
            del PIKPAK_STORAGE_CACHE[cache_key]
        if cache_key in PIKPAK_STORAGE_CACHE_TIME:
            del PIKPAK_STORAGE_CACHE_TIME[cache_key]
        
        tokens = ensure_logged_in(account)
        
        # Use my_pack_id if available, otherwise fall back to "root"
        parent_id = account.get("my_pack_id") or "root"
        print(f"PIKPAK [{SERVER_ID}]: Clearing folder: {parent_id}", flush=True)
        
        files = pikpak_list_files(parent_id, account, tokens)
        deleted_count = len(files)
        
        if files:
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
            
            # Clear trash after moving files
            admin_clear_trash(account_id)
        
        # Refresh storage stats after clearing
        get_account_storage(account)
        
        return jsonify({"success": True, "message": "My Pack cleared", "files_deleted": deleted_count})
    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Failed to clear My Pack for account {account_id}: {e}", flush=True)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/admin/api/clear-all-trash', methods=['POST'])
def admin_clear_all_trash():
    """Empty trash for ALL accounts by fetching from DB"""
    count = 0
    try:
        all_accounts = db.get_all_server_accounts(DB_SERVER_ID)
        for account in all_accounts:
            try:
                response = admin_clear_trash(account["id"])
                if response.get_json().get("success"):
                    count += 1
            except Exception as e:
                print(f"PIKPAK [{SERVER_ID}]: Failed to clear trash for account {account['id']}: {e}", flush=True)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    return jsonify({"success": True, "accounts_cleared": count})

@app.route('/admin/api/clear-all-mypack', methods=['POST'])
def admin_clear_all_mypack():
    """Clear My Pack for ALL accounts by fetching from DB"""
    count = 0
    try:
        all_accounts = db.get_all_server_accounts(DB_SERVER_ID)
        for account in all_accounts:
            try:
                response = admin_clear_mypack(account["id"])
                if response.get_json().get("success"):
                    count += 1
            except Exception as e:
                print(f"PIKPAK [{SERVER_ID}]: Failed to clear mypack for account {account['id']}: {e}", flush=True)
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500
    return jsonify({"success": True, "accounts_cleared": count})

@app.route('/admin/api/status')
def admin_api_status():
    """Get complete admin dashboard data from the database"""
    
    accounts_list = []
    total_remaining = 0
    
    try:
        db_accounts = db.get_all_server_accounts(DB_SERVER_ID)
        
        for account in db_accounts:
            account['device_id'] = account.get('current_device_id')
            
            downloads_today = account.get('quota_used', 0)
            remaining = 5 - downloads_today if downloads_today < 5 else 0
            total_remaining += remaining
            
            storage = get_account_storage(account)
            
            accounts_list.append({
                "id": account["id"],
                "email": account["email"],
                "status": account.get("status", "unknown"),
                "downloads_today": downloads_today,
                "downloads_remaining": remaining,
                "available": remaining > 0 and account.get("status") == "active",
                "storage_used_gb": storage.get("used_gb", 0),
                "storage_total_gb": storage.get("total_gb", 0),
                "storage_percent": storage.get("percent", 0)
            })
            
    except Exception as e:
        print(f"ADMIN [{SERVER_ID}]: Failed to fetch accounts for status: {e}", flush=True)

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
    """Reset quota for specific account by calling the DB"""
    try:
        success = db.reset_account_quota(account_id)
        if success:
            log_activity("info", f"Account {account_id} quota reset manually via DB")
            return jsonify({"success": True, "message": f"Account {account_id} quota reset"})
        else:
            return jsonify({"success": False, "error": "DB operation failed"}), 500
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

# ============================================================
# FLASK ROUTES
# ============================================================

@app.route('/')
def home(): 
    """Health check with server identification"""
    ensure_worker_alive()
    
    num_accounts = 0
    account_ids = []
    try:
        db_accounts = db.get_all_server_accounts(DB_SERVER_ID)
        num_accounts = len(db_accounts)
        account_ids = [acc['id'] for acc in db_accounts]
    except Exception as e:
        print(f"[{SERVER_ID}] DB connection error on health check: {e}", flush=True)

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
        "pikpak_accounts": num_accounts,
        "pikpak_account_ids": account_ids,
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
    """Add magnet to session with quality"""
    data = request.json
    poster_msg_id = str(data.get('poster_message_id'))
    magnet = data.get('magnet')
    quality = data.get('quality', 'auto')
    
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
        
        # Store as object with quality
        session['magnets'].append({
            "magnet": magnet,
            "quality": quality
        })
        print(f"SESSION [{SERVER_ID}]: Added magnet {len(session['magnets'])}/3 (quality: {quality})", flush=True)
    
    return jsonify({"status": "magnet_added", "count": len(session['magnets']), "quality": quality, "server": SERVER_ID})

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

def extract_hash(magnet_link):
    """Extracts the BTIH hash from a magnet link and normalizes it."""
    match = re.search(r'xt=urn:btih:([a-zA-Z0-9]+)', magnet_link, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None

def normalize_name(name):
    """Normalizes a name for fuzzy matching by lowercasing and removing symbols."""
    if not name:
        return ""
    return re.sub(r'\s+', ' ', re.sub(r'[^\w\s]', '', name.lower())).strip()

def check_duplicate(magnet_link, account, tokens, user_quality):
    """DEPRECATED: Fuzzy match check."""
    try:
        print(f"PIKPAK [{SERVER_ID}]: Running smart duplicate check...", flush=True)
        magnet_info = extract_magnet_info(magnet_link)
        
        if not magnet_info["name"] or not magnet_info["hash"]:
            return None
            
        normalized_magnet_name = normalize_name(magnet_info["name"])
        files = pikpak_list_files(None, account, tokens)

        for file in files:
            file_name = file.get('name')
            normalized_file_name = normalize_name(file_name)

            if normalized_file_name == normalized_magnet_name:
                try:
                    file_info = pikpak_get_file_info(file['id'], account, tokens)
                    params_url = file_info.get("params", {}).get("url", "")
                    
                    if magnet_info["hash"] in params_url.lower():
                        log_activity("info", f"Found duplicate: {file_name}")
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
                    continue
        return None

    except Exception as e:
        return None

def check_duplicate_by_hash(magnet_link, account, tokens, user_quality):
    """Checks for duplicates by BTIH hash."""
    try:
        print(f"PIKPAK [{SERVER_ID}]: Running robust duplicate check by hash...", flush=True)
        input_hash = extract_hash(magnet_link)
        
        if not input_hash:
            return None
            
        target_folder = account.get("my_pack_id") or None
        files = pikpak_list_files(target_folder, account, tokens)

        for file in files:
            file_name = file.get('name', 'Unknown')
            try:
                file_info = pikpak_get_file_info(file['id'], account, tokens)
                params_url = file_info.get("params", {}).get("url", "")
                
                is_match = input_hash in params_url.upper()
                
                if is_match:
                    print(f"PIKPAK [{SERVER_ID}]: ✅ Quota Saved! Found existing file by hash: '{file_name}'", flush=True)
                    log_activity("info", f"Found duplicate by hash: {file_name}")
                    
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
                continue
        return None

    except Exception as e:
        print(f"PIKPAK [{SERVER_ID}]: Robust duplicate check failed: {e}", flush=True)
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
                
                # 1. Select account from DB
                account = get_best_account(exclude_ids=exhausted_accounts)
                last_account_id = account["id"]
                tokens = ensure_logged_in(account)

                # 2. Robust Duplicate Check by Hash
                duplicate_file = check_duplicate_by_hash(magnet, account, tokens, user_quality)
                if duplicate_file:
                    return jsonify(duplicate_file)

                # 3. Proceed with normal download
                print(f"PIKPAK [{SERVER_ID}]: No duplicate found. Proceeding to add magnet.", flush=True)
                task = pikpak_add_magnet(magnet, account, tokens)
                initial_folder_id = task.get("file_id")
                file_name = task.get("file_name", "Unknown")
                file_hash = extract_hash(magnet)
                
                if not initial_folder_id or str(initial_folder_id).strip() == "":
                    return jsonify({"error": "PikPak returned empty file_id", "retry": False, "server": SERVER_ID}), 400
                
                # Poll for download and get the FINAL file/folder ID
                folder_id = pikpak_poll_download(initial_folder_id, account, tokens, timeout=900, filename=file_name, file_hash=file_hash)
                
                tokens = ensure_logged_in(account)
                file_info = pikpak_get_file_info(folder_id, account, tokens)
                kind = file_info.get("kind", "")
                
                video_file = None
                download_url = None
                
                if kind == "drive#folder":
                    files = pikpak_list_files(folder_id, account, tokens)
                    video_file = find_video_file(files)
                    if not video_file:
                        return jsonify({"error": "No video file found in folder", "retry": False, "server": SERVER_ID}), 400
                    tokens = ensure_logged_in(account)
                    download_url = pikpak_get_download_link(video_file["id"], account, tokens)
                else: 
                    if not is_video_file(file_info):
                        return jsonify({"error": "Downloaded file is not a video", "retry": False, "server": SERVER_ID}), 400
                    video_file = file_info
                    download_url = file_info.get("web_content_link", "")
                    if not download_url:
                        tokens = ensure_logged_in(account)
                        download_url = pikpak_get_download_link(folder_id, account, tokens)
                
                file_size = int(video_file.get("size", 0))
                # SUCCESS: Increment quota in DB
                db.increment_quota(account["id"])
                
                detected_quality = detect_quality(user_quality, magnet, file_size)
                
                print(f"PIKPAK [{SERVER_ID}]: === ADD MAGNET SUCCESS ===", flush=True)
                log_activity("success", f"Downloaded: {video_file.get('name', file_name)}")
                update_daily_stats("downloads")
                
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
                
                if "Emergency stop" in error_msg:
                    return jsonify({"error": error_msg, "retry": False}), 503

                if last_account_id and last_account_id not in exhausted_accounts:
                    if "Captcha failed" in error_msg:
                        db.rotate_device(last_account_id)
                        exhausted_accounts.append(last_account_id)
                        continue 
                    if "task_daily_create_limit" in error_msg:
                        exhausted_accounts.append(last_account_id)
                        continue 

                if "All PikPak accounts exhausted" in error_msg:
                    return jsonify({"error": error_msg, "retry": False, "server": SERVER_ID}), 500
                
                if attempt >= max_total_retries:
                    return jsonify({"error": f"Max retries reached: {error_msg}", "retry": False, "server": SERVER_ID}), 500
                
                time.sleep(3)
    
    return jsonify({"error": "Max retries exceeded", "retry": False, "server": SERVER_ID}), 500

@app.route('/admin/api/test-magnet', methods=['POST'])
def admin_test_magnet():
    """Test magnet link without downloading"""
    try:
        magnet = request.json.get('magnet', '').strip()
        
        if not magnet:
            return jsonify({"valid": False, "error": "No magnet provided"})
        
        if '-magnet:' in magnet:
            parts = magnet.split('-', 1)
            if len(parts) == 2:
                magnet = parts[1]
        
        if not magnet.startswith('magnet:?'):
            return jsonify({"valid": False, "error": "Invalid magnet format"})
        
        name = "Unknown"
        dn_match = re.search(r'dn=([^&]+)', magnet)
        if dn_match:
            name = dn_match.group(1).replace('+', ' ').replace('%20', ' ').replace('%28', '(').replace('%29', ')')

        available_account = "None available"
        try:
            account = get_best_account()
            available_account = account["id"]
        except Exception as e:
            available_account = f"Error: {e}"

        return jsonify({
            "valid": True,
            "name": name,
            "quality": "auto",
            "account": available_account,
            "server": SERVER_ID
        })
        
    except Exception as e:
        return jsonify({"valid": False, "error": str(e)})

@app.route('/list-files', methods=['POST'])
def list_files():
    """List PikPak folder contents"""
    try:
        folder_id = request.json.get('folder_id')
        if not folder_id:
            return jsonify({"error": "Missing folder_id"}), 400
        
        account = get_best_account()
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
        
        account = get_best_account()
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
        
        account = get_best_account()
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
        # Re-using admin_api_status logic as it is cleaner with DB
        return admin_api_status()
        
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

# ============================================================
# GOFILE INTEGRATION (LIGHTWEIGHT)
# ============================================================

@app.route('/save-gofile-result', methods=['POST'])
def save_gofile_result():
    """Save Gofile upload result from external service to DB"""
    data = request.json
    if not data or not data.get('file_id'):
        return jsonify({"success": False, "error": "Missing file_id"}), 400
    
    if 'server' not in data:
        data['server'] = 'global'
        
    result = db.add_gofile_upload(data)
    if result:
        return jsonify({"success": True})
    else:
        return jsonify({"success": False, "error": "DB insert failed"}), 500

@app.route('/gofile/status', methods=['GET'])
def gofile_status_list():
    """Get active Gofile uploads for dashboard"""
    try:
        active_files = db.get_active_gofile_uploads()
        return jsonify({
            "success": True,
            "count": len(active_files),
            "uploads": active_files
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/gofile/keep-alive', methods=['POST'])
def gofile_keep_alive():
    """Trigger lightweight keep-alive for all active Gofile uploads"""
    active_files = db.get_active_gofile_uploads()
    success_count = 0
    failed_count = 0
    
    print(f"GOFILE: Starting lightweight keep-alive for {len(active_files)} files...", flush=True)
    
    for file in active_files:
        file_id = file.get('file_id')
        link = file.get('direct_link') or file.get('download_page')
        
        if not link:
            continue
            
        try:
            # 1-byte range request to trigger activity without downloading
            headers = {"Range": "bytes=0-0"}
            response = requests.get(link, headers=headers, stream=True, timeout=5)
            
            if response.status_code in [200, 206]:
                db.update_gofile_keep_alive(file_id)
                success_count += 1
            else:
                print(f"GOFILE: Keep-alive failed for {file_id} (Status: {response.status_code})", flush=True)
                failed_count += 1
            
            response.close()
        except Exception as e:
            print(f"GOFILE: Keep-alive error for {file_id}: {e}", flush=True)
            failed_count += 1
            
    return jsonify({
        "success": True,
        "processed": len(active_files),
        "kept_alive": success_count,
        "failed": failed_count
    })


if __name__ == '__main__':
    print("=" * 60, flush=True)
    print(f"🚀 PikPak-Telegram Bridge - SERVER: {SERVER_ID.upper()}", flush=True)
    print(f"📍 URL: {SERVER_URL}", flush=True)
    print(f"🎬 Handles: 1080p, 2160p, 4K (large files)", flush=True)
    print(f"🔑 Session: {SESSION_NAME}", flush=True)
    print("=" * 60, flush=True)
    
    # Run startup checks
    try:
        db_accounts = db.get_all_server_accounts(DB_SERVER_ID)
        print(f"📦 Loaded {len(db_accounts)} PikPak accounts from DB", flush=True)
        check_all_accounts_quota()
    except Exception as e:
        print(f"⚠️ Startup DB Check Failed: {e}", flush=True)
        
    ensure_worker_alive()
    app.run(host='0.0.0.0', port=10000)