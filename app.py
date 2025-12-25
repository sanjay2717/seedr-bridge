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

# Safety conversion
if API_ID:
    try:
        API_ID = int(API_ID)
    except:
        pass

HEADERS = {
    "User-Agent": "Seedr Android/1.0",
    "Content-Type": "application/x-www-form-urlencoded"
}

# --- HELPER: STREAM CLASS (FIXED) ---
class HTTPStream:
    def __init__(self, url, filename):
        self.url = url
        self.name = filename
        self.mode = 'rb'  # <--- THIS IS THE FIX. Tells Pyrogram "I am a Binary File"
        
        print(f"STREAM: Connecting to Seedr...")
        self.response = requests.get(url, stream=True)
        if self.response.status_code != 200:
            print(f"STREAM ERROR: {self.response.status_code}")
            raise ValueError(f"Seedr connection failed")
            
        self.raw = self.response.raw
        self.raw.decode_content = True

    def read(self, chunk_size=-1):
        if chunk_size == -1:
            return self.raw.read()
        return self.raw.read(chunk_size)

# --- WORKER ---
def upload_worker(file_url, chat_id, caption):
    print(f"WORKER: Starting upload to {chat_id}")
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    async def perform_upload():
        async with Client("bot_session", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True) as app:
            print("WORKER: Bot connected!")
            try:
                # Create the stream wrapper
                stream = HTTPStream(file_url, "video.mp4")
                
                print("WORKER: Sending video...")
                await app.send_video(
                    chat_id=int(chat_id),
                    video=stream,
                    caption=caption,
                    supports_streaming=True,
                    progress=lambda c, t: print(f"Progress: {c/1024/1024:.1f} MB") if c % (10*1024*1024) == 0 else None
                )
                print("WORKER: Upload success!")
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

# --- SEEDR ROUTES (KEEPING ALL PREVIOUS FUNCTIONS) ---

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
    try:
        resp = requests.post("https://www.seedr.cc/oauth_test/resource.php?json=1", data={"access_token": request.json.get('token'), "func": "add_torrent", "torrent_magnet": request.json.get('magnet')})
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)})

@app.route('/list-files', methods=['POST'])
def list_files():
    data = request.json
    token = data.get('token')
    folder_id = data.get('folder_id', "0")
    if str(folder_id) == "0": url = "https://www.seedr.cc/api/folder"
    else: url = f"https://www.seedr.cc/api/folder/{folder_id}"
    try:
        resp = requests.get(url, params={"access_token": token}, headers=HEADERS)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/get-link', methods=['POST'])
def get_link():
    try:
        resp = requests.post("https://www.seedr.cc/oauth_test/resource.php?json=1", data={"access_token": request.json.get('token'), "func": "fetch_file", "folder_file_id": str(request.json.get('file_id'))})
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
