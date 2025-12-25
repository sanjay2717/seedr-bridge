import os
import threading
import asyncio
import requests
import io
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

# --- 1. THE FIXED STREAMER ---
class CustomStream:
    def __init__(self, url, name):
        self.url = url
        self.name = name
        self.mode = 'rb'  # <--- THIS IS THE FIX (Binary Mode)
        
        print(f"STREAM: Connecting to {url[:30]}...")
        
        # Start the stream
        self.response = requests.get(url, stream=True)
        self.raw = self.response.raw
        
        # Get total size for the progress bar
        self.total_size = int(self.response.headers.get('content-length', 0))
        print(f"STREAM: File size is {self.total_size} bytes")
        
        self.current_pos = 0

    # Pyrogram calls this to check size
    def seek(self, offset, whence=0):
        if whence == 0:
            self.current_pos = offset
        elif whence == 1:
            self.current_pos += offset
        elif whence == 2:
            self.current_pos = self.total_size + offset
        return self.current_pos

    # Pyrogram calls this to read data
    def read(self, size=-1):
        return self.raw.read(size)
    
    # Pyrogram calls this to see where we are
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
            try:
                # Use the Fixed Stream
                stream = CustomStream(file_url, "video.mp4")
                
                if stream.total_size == 0:
                    print("WORKER ERROR: File size is 0. Check Seedr link.")
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

HEADERS_ANDROID = {
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
    # Kodi Method (Proven)
    url = "https://www.seedr.cc/oauth_test/resource.php?json=1"
    payload = {"access_token": request.json.get('token'), "func": "add_torrent", "torrent_magnet": request.json.get('magnet')}
    try:
        resp = requests.post(url, data=payload)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)})

@app.route('/list-files', methods=['POST'])
def list_files():
    # Android Method (URL Path Strategy) - Proven for Listing
    data = request.json
    token = data.get('token')
    folder_id = str(data.get('folder_id', "0"))
    
    if folder_id == "0": 
        url = "https://www.seedr.cc/api/folder"
    else: 
        url = f"https://www.seedr.cc/api/folder/{folder_id}"
        
    params = {"access_token": token}
    try:
        resp = requests.get(url, params=params, headers=HEADERS_ANDROID)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/get-link', methods=['POST'])
def get_link():
    # Kodi Method (Proven)
    url = "https://www.seedr.cc/oauth_test/resource.php?json=1"
    payload = {"access_token": request.json.get('token'), "func": "fetch_file", "folder_file_id": str(request.json.get('file_id'))}
    try:
        resp = requests.post(url, data=payload)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
