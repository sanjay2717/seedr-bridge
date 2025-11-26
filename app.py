from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

@app.route('/add-magnet', methods=['POST'])
def add_magnet_to_seedr():
    data = request.json
    token = data.get('token')
    magnet_link = data.get('magnet')

    if not token or not magnet_link:
        return jsonify({"success": False, "error": "Missing token or magnet"}), 400

    # 1. API Endpoint (The one that gave 200 OK earlier)
    url = "https://www.seedr.cc/api/folder/magnet/add"
    
    # 2. Correct Headers
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    # 3. Payload with BOTH possible parameter names (Safety Net)
    # Seedr sometimes wants 'magnet', sometimes 'torrent_magnet'. We send both.
    payload = {
        "magnet": magnet_link,
        "torrent_magnet": magnet_link,
        "folder_id": 0
    }

    print(f"Adding magnet via Standard API...")
    
    try:
        # Use 'data' to send form-urlencoded (standard for Seedr)
        response = requests.post(url, data=payload, headers=headers)
        
        # 4. Debugging: Print exactly what Seedr said
        print(f"Seedr Response Code: {response.status_code}")
        print(f"Seedr Response Body: {response.text}")
        
        return jsonify({
            "status_code": response.status_code,
            "seedr_response": response.json() if response.text else "No content"
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
