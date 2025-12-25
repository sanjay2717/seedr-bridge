import os
import threading
import asyncio
import requests
from flask import Flask, request, jsonify
from pyrogram import Client

app = Flask(__name__)

# --- CONFIGURATION ---
API_ID = os.environ.get("TG_API_ID")
API_HASH = os.environ.get("TG_API_HASH")
BOT_TOKEN = os.environ.get("TG_BOT_TOKEN")

# Safety Check
if API_ID:
    try:
        API_ID = int(API_ID)
    except:
        pass

HEADERS = {
    "User-Agent": "Seedr Android/1.0",
    "Content-Type": "application/x-www-form-urlencoded"
}

# --- 1. THE SMART STREAMER (FIXED SIZE DETECTION) ---
class CustomStream:
    def __init__(self, url, name):
        self.url = url
        self.name = name
        
        # We start the download immediately to get the TRUE headers
        print(f"STREAM: Connecting to {url[:20]}...")
        self.response = requests.get(url, stream=True, headers=HEADERS)
        self.raw = self.response.raw
        
        # Get the real size from the active connection
        self.total_size = int(self.response.headers.get('content-length', 0))
        print(f"STREAM: Size detected: {self.total_size} bytes")
        
        # Keep track of "Virtual Position" for Pyrogram
        self._pos = 0

    # Pyrogram calls this to check file size
    def seek(self, offset, whence=0):
        if whence == 0:
            self._pos = offset
        elif whence == 1:
            self._pos += offset
        elif whence == 2:
            self._pos = self.total_size + offset
        return self._pos

    # Pyrogram calls this to know where it is
    def tell(self):
        return self._pos

    # Pyrogram calls this to get data
    def read(self, size=-1):
        if size == -1:
            return self.raw.read()
        return self.raw.read(size)

# --- 2. UPLOAD WORKER ---
def upload_worker(file_url, chat_id, caption):
    print(f"WORKER: Starting upload to {chat_id}")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def perform_upload():
        async with Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True) as app:
            print("WORKER: Bot connected!")
            try:
                # Initialize the Smart Stream
                stream = CustomStream(file_url, "video.mp4")
                
                # Double check size before sending
                if stream.total_size == 0:
                    print("WORKER ERROR: File size is 0. Seedr link might be expired.")
                    return

                print("WORKER: Streaming to Telegram...")
                await app.send_video(
                    chat_id=int(chat_id),
                    video=stream,
                    caption=caption,
                    supports_streaming=True,
                    progress=lambda c, t: print(f"Progress: {c/1024/1024:.2f} MB") if c % (5*1024*1024) == 0 else None
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
    finally:
        loop.close()

# --- ROUTES ---

@app.route('/')
def home():
    return "Seedr-Telegram Bridge Active."

@app.route('/upload-telegram', methods=['POST'])
def upload_telegram():
    data = request.json
    file_url = data.get('url')
    chat_id = data.get('chat_id')
    caption = data.get('caption', "Uploaded via Automation")
    
    if not file_url or not chat_id:
        return jsonify({"error": "Missing params"}), 400

    thread = threading.Thread(target=upload_worker, args=(file_url, chat_id, caption))
    thread.start()
    return jsonify({"status": "Upload started"})

# ==========================================
# YOUR CONFIRMED WORKING SEEDR ROUTES
# ==========================================

@app.route('/auth/code', methods=['GET'])
def get_code():
    url = "https://www.seedr.cc/oauth_device/create"
    params = {"client_id": "seedr_xbmc"}
    try:
        resp = requests.get(url, params=params)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/auth/token', methods=['GET'])
def get_token():
    device_code = request.args.get('device_code')
    url = "https://www.seedr.cc/oauth_device/token"
    params = {"client_id": "seedr_xbmc", "grant_type": "device_token", "device_code": device_code}
    try:
        resp = requests.get(url, params=params)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/add-magnet', methods=['POST'])
def add_magnet():
    data = request.json
    token = data.get('token')
    magnet = data.get('magnet')
    if not token or not magnet: return jsonify({"error": "Missing params"}), 400
    url = "https://www.seedr.cc/oauth_test/resource.php?json=1"
    payload = {"access_token": token, "func": "add_torrent", "torrent_magnet": magnet}
    try:
        resp = requests.post(url, data=payload)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)})

@app.route('/list-files', methods=['POST'])
def list_files():
    data = request.json
    token = data.get('token')
    folder_id = data.get('folder_id', "0")
    if not token: return jsonify({"error": "Missing token"}), 400
    if str(folder_id) == "0": url = "https://www.seedr.cc/api/folder"
    else: url = f"https://www.seedr.cc/api/folder/{folder_id}"
    params = {"access_token": token}
    try:
        resp = requests.get(url, params=params, headers=HEADERS)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/get-link', methods=['POST'])
def get_link():
    data = request.json
    token = data.get('token')
    file_id = data.get('file_id')
    if not token or not file_id: return jsonify({"error": "Missing params"}), 400
    url = "https://www.seedr.cc/oauth_test/resource.php?json=1"
    payload = {"access_token": token, "func": "fetch_file", "folder_file_id": str(file_id)}
    try:
        print(f"Fetching link for file {file_id}...")
        resp = requests.post(url, data=payload)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
