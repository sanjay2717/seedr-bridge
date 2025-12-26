import os
import threading
import asyncio
import requests
from io import IOBase
from flask import Flask, request, jsonify
from pyrogram import Client

app = Flask(__name__)

# --- CONFIGURATION ---
API_ID = os.environ.get("TG_API_ID")
API_HASH = os.environ.get("TG_API_HASH")
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")

# Ensure API_ID is integer
if API_ID:
    try:
        API_ID = int(API_ID)
    except:
        pass

# --- 1. THE SMART STREAMER (WORKING - DO NOT TOUCH) ---
class SmartStream(IOBase):
    def __init__(self, url, name):
        super().__init__()
        self.url = url
        self.name = name
        self.mode = 'rb'
        
        print(f"STREAM: Connecting to {url[:50]}...")
        
        try:
            head = requests.head(url, allow_redirects=True, timeout=10)
            self.total_size = int(head.headers.get('content-length', 0))
            print(f"STREAM: Size detected: {self.total_size} bytes")
        except Exception as e:
            print(f"STREAM WARNING: Could not get size: {e}")
            self.total_size = 0

        self.response = requests.get(url, stream=True, timeout=30)
        self.raw = self.response.raw
        self.raw.decode_content = True
        self.current_pos = 0
        self._closed = False

    def read(self, size=-1):
        if self._closed: raise ValueError("I/O operation on closed file")
        data = self.raw.read(size)
        if data: self.current_pos += len(data)
        return data

    def read1(self, size=-1): return self.read(size)
    def readinto(self, b):
        data = self.read(len(b))
        n = len(data)
        b[:n] = data
        return n

    def seek(self, offset, whence=0):
        if whence == 0: self.current_pos = offset
        elif whence == 1: self.current_pos += offset
        elif whence == 2: self.current_pos = self.total_size + offset
        return self.current_pos
    
    def tell(self): return self.current_pos
    def readable(self): return True
    def writable(self): return False
    def seekable(self): return True
    def closed(self): return self._closed
    def close(self):
        if not self._closed:
            self._closed = True
            if hasattr(self, 'response'): self.response.close()
    def __enter__(self): return self
    def __exit__(self, *args): self.close()
    def fileno(self): return None

# --- 2. UPLOAD WORKER (FIXED WITH PERSISTENT SESSION) ---
def upload_worker(file_url, chat_id, caption):
    print(f"WORKER: Starting upload to {chat_id}")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def perform_upload():
        # CRITICAL: Use workdir="/tmp" for persistent session
        async with Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workdir="/tmp") as app:
            print("WORKER: Bot connected!")
            
            # Accept both username format (@moviessquares) and numeric ID
            target_chat = chat_id if isinstance(chat_id, str) and chat_id.startswith('@') else int(chat_id)
            
            # Verify channel access
            try:
                print(f"WORKER: Verifying access to {target_chat}...")
                channel = await app.get_chat(target_chat)
                print(f"WORKER: ✅ Channel verified: {channel.title}")
            except Exception as e:
                print(f"WORKER ERROR: Cannot access channel: {e}")
                print("Make sure the bot is an Admin in @moviessquares!")
                return

            try:
                with SmartStream(file_url, "video.mp4") as stream:
                    if stream.total_size == 0:
                        print("WORKER WARNING: Could not determine file size.")
                    
                    print("WORKER: Streaming to Telegram...")
                    
                    await app.send_video(
                        chat_id=target_chat,
                        video=stream,
                        caption=caption,
                        supports_streaming=True,
                        progress=lambda c, t: print(f"📤 Upload: {c/1024/1024:.1f}/{t/1024/1024:.1f} MB") if c % (10*1024*1024) == 0 else None
                    )
                    print("WORKER: ✅ Upload Success!")
            except Exception as e:
                print(f"WORKER ERROR: {e}")
                import traceback
                traceback.print_exc()

    try:
        loop.run_until_complete(perform_upload())
    except Exception as e:
        print(f"LOOP ERROR: {e}")
        import traceback
        traceback.print_exc()
    finally:
        loop.close()

# --- ROUTES ---

@app.route('/')
def home(): 
    return "🎬 Seedr-Telegram Bridge for @moviessquares"

@app.route('/upload-telegram', methods=['POST'])
def upload_telegram():
    """
    Upload a video to @moviessquares
    
    Body:
    {
      "url": "https://rd8.seedr.cc/...",
      "chat_id": "@moviessquares",  // Can also use numeric ID
      "caption": "Movie Title (Year)"
    }
    """
    data = request.json
    file_url = data.get('url')
    chat_id = data.get('chat_id', '@moviessquares')  # Default to @moviessquares
    caption = data.get('caption', "🎬 Uploaded via Automation")
    
    if not file_url:
        return jsonify({"error": "Missing 'url' parameter"}), 400

    thread = threading.Thread(target=upload_worker, args=(file_url, chat_id, caption))
    thread.start()
    
    return jsonify({"status": "Upload started", "target": chat_id})

# --- TEST MESSAGE (INITIALIZE CHANNEL) ---
@app.route('/test-channel', methods=['POST'])
def test_channel():
    """
    Send a test message to verify bot access
    
    Body (optional):
    {
      "chat_id": "@moviessquares"  // Defaults to @moviessquares if not provided
    }
    """
    data = request.json or {}
    chat_id = data.get('chat_id', '@moviessquares')
    
    async def send_test():
        async with Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workdir="/tmp") as app:
            try:
                # Send test message
                msg = await app.send_message(chat_id, "🤖 Bot initialized successfully!\n✅ Ready to upload movies.")
                
                # Get channel info
                channel = await app.get_chat(chat_id)
                
                return {
                    "success": True,
                    "message": "Bot can access the channel!",
                    "channel_title": channel.title,
                    "channel_id": channel.id,
                    "channel_username": channel.username,
                    "message_id": msg.id
                }
            except Exception as e:
                import traceback
                return {
                    "success": False,
                    "error": str(e),
                    "solution": "Make sure the bot is an Admin in @moviessquares with 'Post Messages' permission",
                    "traceback": traceback.format_exc()
                }
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    result = loop.run_until_complete(send_test())
    loop.close()
    
    return jsonify(result)

# ==========================================
# WORKING SEEDR ROUTES (PRESERVED)
# ==========================================

HEADERS_ANDROID = {"User-Agent": "Seedr Android/1.0", "Content-Type": "application/x-www-form-urlencoded"}

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
    url = "https://www.seedr.cc/oauth_test/resource.php?json=1"
    payload = {"access_token": request.json.get('token'), "func": "add_torrent", "torrent_magnet": request.json.get('magnet')}
    try:
        resp = requests.post(url, data=payload)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)})

@app.route('/list-files', methods=['POST'])
def list_files():
    data = request.json
    token = data.get('token')
    folder_id = str(data.get('folder_id', "0"))
    if folder_id == "0": url = "https://www.seedr.cc/api/folder"
    else: url = f"https://www.seedr.cc/api/folder/{folder_id}"
    params = {"access_token": token}
    try:
        resp = requests.get(url, params=params, headers=HEADERS_ANDROID)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/get-link', methods=['POST'])
def get_link():
    url = "https://www.seedr.cc/oauth_test/resource.php?json=1"
    payload = {"access_token": request.json.get('token'), "func": "fetch_file", "folder_file_id": str(request.json.get('file_id'))}
    try:
        resp = requests.post(url, data=payload)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
