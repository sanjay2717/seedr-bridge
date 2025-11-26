from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

@app.route('/')
def home():
    return "Seedr Bridge Active"

# --- 1. ADD MAGNET ---
@app.route('/add-magnet', methods=['POST'])
def add_magnet():
    data = request.json
    token = data.get('token')
    magnet = data.get('magnet')
    
    if not token or not magnet:
        return jsonify({"error": "Missing params"}), 400
        
    url = "https://www.seedr.cc/oauth_test/resource.php"
    payload = {
        "access_token": token,
        "func": "add_torrent",
        "torrent_magnet": magnet
    }
    try:
        resp = requests.post(url, data=payload)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"result": False, "error": str(e)})

# --- 2. LIST FILES (Fixed Type Conversion) ---
@app.route('/list-files', methods=['POST'])
def list_files():
    data = request.json
    token = data.get('token')
    folder_id = data.get('folder_id', "0")
    
    if not token:
        return jsonify({"error": "Missing token"}), 400

    url = "https://www.seedr.cc/api/folder"
    
    # FORCE STRING CONVERSION HERE using str()
    payload = {
        "access_token": token,
        "folder_id": str(folder_id) 
    }
    
    try:
        print(f"Listing folder {folder_id}...")
        resp = requests.post(url, data=payload)
        return jsonify(resp.json())
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
