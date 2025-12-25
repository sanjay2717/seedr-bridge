from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# --- CONFIGURATION ---
HEADERS = {
    "User-Agent": "Seedr Android/1.0",
    "Content-Type": "application/x-www-form-urlencoded"
}

@app.route('/')
def home():
    return "Seedr Bridge Active."

# --- AUTH ENDPOINTS ---
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

# --- 1. ADD MAGNET ---
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

# --- 2. LIST FILES (Android Method) ---
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

# --- 3. GET DOWNLOAD LINK (NEW!) ---
@app.route('/get-link', methods=['POST'])
def get_link():
    data = request.json
    token = data.get('token')
    file_id = data.get('file_id') # This is the folder_file_id
    
    if not token or not file_id:
        return jsonify({"error": "Missing params"}), 400

    # We use the Kodi "Fetch File" command to get the link
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
