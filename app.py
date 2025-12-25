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

if API_ID:
    try:
        API_ID = int(API_ID)
    except:
        pass

# --- 1. THE NATIVE STREAMER ---
# A pure wrapper that doesn't confuse Pyrogram with 'io' inheritance
class NativeStream:
    def __init__(self, url, name):
        self.url = url
        self.name = name
        self.mode = 'rb' # Binary mode flag
        
        print(f"STREAM: Initializing connection...")
        # Get size first
        try:
            head = requests.head(url, allow_redirects=True)
            self.total_size = int(head.headers.get('content-length', 0))
        except:
            self.total_size = 0
            
        # Start stream
        self.req = requests.get(url, stream=True)
        self.raw = self.req.raw
        self.current_pos = 0

    def read(self, size=-1):
        # Pass read commands directly to the raw socket
        data = self.raw.read(size)
        self.current_pos += len(data) if data else 0
        return data

    def seek(self, offset, whence=0):
        # Fake the seek so Pyrogram knows the file size
        if whence == 2: # SEEK_END
            self.current_pos = self.total_size + offset
        elif whence == 0:
            self.current_pos = offset
        return self.current_pos
    
    def tell(self):
        return self.current_pos

# --- 2. UPLOAD WORKER ---
def upload_worker(file_url, chat_id, caption):
    print(f"WORKER: Starting upload to {chat_id}")
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def perform_upload():
        async with Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True) as app:
            print("WORKER: Bot connected!")
            
            # --- FIX FOR PEER ID ERROR ---
            # We force the bot to look up the chat first to cache the ID
            try:
                print("WORKER: resolving chat ID...")
                await app.get_chat(int(chat_id))
            except Exception as e:
                print(f"WORKER WARNING: Could not verify chat ID: {e}")
                # We continue anyway, as it might just be a permission issue that upload can bypass

            try:
                # Initialize Native Stream
                stream = NativeStream(file_url, "video.mp4")
                print(f"WORKER: Stream Size: {stream.total_size}")

                print("WORKER: Uploading...")
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
# PRESERVED WORKING SEEDR ROUTES
# ==========================================

HEADERS = {
    "User-Agent": "Seedr Android/1.0",
    "Content-Type": "application/x-www-form-urlencoded"
}

@app.route('/auth/code', methods=['GET'])
def get_code():
    url = "https://www.seedr.cc/oauth_device/create"
    params = {"client_id": "seedr_xbmc"}
    try:
        resp = requests.get(url, params=params)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/auth/token', methods=['GET'])
def get_token():
    device_code = request.args.get('device_code')
    url = "https://www.seedr.cc/oauth_device/token"
    params = {
        "client_id": "seedr_xbmc",
        "grant_type": "device_token",
        "device_code": device_code
    }
    try:
        resp = requests.get(url, params=params)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/add-magnet', methods=['POST'])
def add_magnet():
    data = request.json
    token = data.get('token')
    magnet = data.get('magnet')
    if not token or not magnet:
        return jsonify({"error": "Missing params"}), 400
    url = "https://www.seedr.cc/oauth_test/resource.php?json=1"
    payload = {"access_token": token, "func": "add_torrent", "torrent_magnet": magnet}
    try:
        resp = requests.post(url, data=payload)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)})

@app.route('/list-files', methods=['POST'])
def list_files():
    data = request.json
    token = data.get('token')
    folder_id = data.get('folder_id', "0")
    if not token:
        return jsonify({"error": "Missing token"}), 400
    
    if str(folder_id) == "0":
        url = "https://www.seedr.cc/api/folder"
    else:
        url = f"https://www.seedr.cc/api/folder/{folder_id}"
        
    params = {"access_token": token}
    try:
        resp = requests.get(url, params=params, headers=HEADERS)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/get-link', methods=['POST'])
def get_link():
    data = request.json
    token = data.get('token')
    file_id = data.get('file_id')
    if not token or not file_id:
        return jsonify({"error": "Missing params"}), 400
    url = "https://www.seedr.cc/oauth_test/resource.php?json=1"
    payload = {
        "access_token": token,
        "func": "fetch_file",
        "folder_file_id": str(file_id)
    }
    try:
        print(f"Fetching link for file {file_id}...")
        resp = requests.post(url, data=payload)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
