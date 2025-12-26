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

# --- 2. UPLOAD WORKER (FIXED PEER ID) ---
def upload_worker(file_url, chat_id, caption):
    print(f"WORKER: Starting upload to {chat_id}")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def perform_upload():
        async with Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True) as app:
            print("WORKER: Bot connected!")
            
            # --- FIX: CACHE THE PEER ID ---
            # We explicitly ask Telegram to resolve the chat ID before uploading.
            # This prevents the "Peer id invalid" error at the end.
            try:
                print(f"WORKER: Verifying channel access for {chat_id}...")
                target_chat = await app.get_chat(int(chat_id))
                print(f"WORKER: Channel verified: {target_chat.title}")
            except Exception as e:
                print(f"WORKER ERROR: Could not access channel: {e}")
                print("Make sure the Bot is an Admin in the channel!")
                return

            try:
                with SmartStream(file_url, "video.mp4") as stream:
                    if stream.total_size == 0:
                        print("WORKER WARNING: Could not determine file size.")
                    
                    print("WORKER: Streaming to Telegram...")
                    
                    # We use the integer ID explicitly
                    await app.send_video(
                        chat_id=int(chat_id),
                        video=stream,
                        caption=caption,
                        supports_streaming=True,
                        progress=lambda c, t: print(f"Upload: {c/1024/1024:.1f}/{t/1024/1024:.1f} MB") if c % (5*1024*1024) == 0 else None
                    )
                    print("WORKER: Upload Success!")
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
def home(): return "Seedr-Telegram Bridge Active."

@app.route('/upload-telegram', methods=['POST'])
def upload_telegram():
    data = request.json
    file_url = data.get('url')
    chat_id = data.get('chat_id')
    caption = data.get('caption', "Uploaded via Automation")
    
    if not file_url or not chat_id: return jsonify({"error": "Missing params"}), 400

    thread = threading.Thread(target=upload_worker, args=(file_url, chat_id, caption))
    thread.start()
    
    return jsonify({"status": "Upload started"})

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
