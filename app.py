import threading
import time
import requests
from flask import Flask, request, jsonify
from pyrogram import Client

app = Flask(__name__)

# --- TELEGRAM CONFIGURATION ---
API_ID = 29211789
API_HASH = "0500d6ebe45d42efe9ff73bf47cb3b4b"
BOT_TOKEN = "8319241228:AAHQotAqCjvOejaOVWtFUnLbxDAZilQotwA"

# --- SEEDR CONFIGURATION ---
HEADERS = {
    "User-Agent": "Seedr Android/1.0",
    "Content-Type": "application/x-www-form-urlencoded"
}

# --- HELPER: STREAM CLASS ---
# This tricks Pyrogram into thinking it's reading a local file,
# but actually it's streaming directly from the internet.
class HTTPStream:
    def __init__(self, url, filename):
        self.url = url
        self.name = filename
        self.response = requests.get(url, stream=True)
        self.raw = self.response.raw

    def read(self, chunk_size):
        return self.raw.read(chunk_size)

# --- WORKER: BACKGROUND UPLOAD ---
def upload_worker(file_url, chat_id, caption):
    print(f"Starting upload to {chat_id}...")
    
    # Initialize Bot
    bot = Client("my_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)
    
    try:
        bot.start()
        
        # Create the stream
        stream = HTTPStream(file_url, "video.mp4")
        
        # Send video (supports files up to 2GB via Bot API, 4GB via MTProto)
        bot.send_video(
            chat_id=int(chat_id),
            video=stream,
            caption=caption,
            supports_streaming=True
        )
        print("Upload success!")
        
    except Exception as e:
        print(f"Upload failed: {e}")
    finally:
        bot.stop()

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
        return jsonify({"error": "Missing url or chat_id"}), 400

    # Run upload in background so n8n doesn't timeout
    thread = threading.Thread(target=upload_worker, args=(file_url, chat_id, caption))
    thread.start()
    
    return jsonify({"status": "Upload started", "message": "Check your Telegram channel in a few minutes."})

# --- EXISTING SEEDR ROUTES (Keep these exactly as they were) ---

@app.route('/auth/code', methods=['GET'])
def get_code():
    try:
        resp = requests.get("https://www.seedr.cc/oauth_device/create", params={"client_id": "seedr_xbmc"})
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/auth/token', methods=['GET'])
def get_token():
    try:
        resp = requests.get("https://www.seedr.cc/oauth_device/token", params={
            "client_id": "seedr_xbmc", "grant_type": "device_token", "device_code": request.args.get('device_code')
        })
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/add-magnet', methods=['POST'])
def add_magnet():
    try:
        resp = requests.post("https://www.seedr.cc/oauth_test/resource.php?json=1", data={
            "access_token": request.json.get('token'), "func": "add_torrent", "torrent_magnet": request.json.get('magnet')
        })
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)})

@app.route('/list-files', methods=['POST'])
def list_files():
    folder_id = str(request.json.get('folder_id', "0"))
    url = f"https://www.seedr.cc/api/folder/{folder_id}" if folder_id != "0" else "https://www.seedr.cc/api/folder"
    try:
        resp = requests.get(url, params={"access_token": request.json.get('token')}, headers=HEADERS)
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

@app.route('/get-link', methods=['POST'])
def get_link():
    try:
        resp = requests.post("https://www.seedr.cc/oauth_test/resource.php?json=1", data={
            "access_token": request.json.get('token'), "func": "fetch_file", "folder_file_id": str(request.json.get('file_id'))
        })
        return jsonify(resp.json())
    except Exception as e: return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
