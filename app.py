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

    # KODI / XBMC API ENDPOINT
    # This is the special door for Device Tokens
    url = "https://www.seedr.cc/oauth_test/resource.php"
    
    # KODI PARAMETERS
    payload = {
        "access_token": token,        # Token goes in the body, not header
        "func": "add_torrent",        # The command function
        "torrent_magnet": magnet_link # The magnet link
    }
    
    # Headers to look like Kodi
    headers = {
        "User-Agent": "Seedr Kodi/1.0.3"
    }

    print(f"Sending command to Kodi endpoint...")
    
    try:
        response = requests.post(url, data=payload, headers=headers)
        
        # Check if response is empty (happens if token is bad)
        if not response.text:
             return jsonify({"success": False, "error": "Empty response from Seedr", "status": response.status_code}), 500

        try:
            r_json = response.json()
        except:
            # If not JSON, return text (e.g. "Error: invalid token")
            return jsonify({"success": False, "error": "Invalid JSON", "raw": response.text}), 500
            
        return jsonify({
            "status_code": response.status_code,
            "seedr_response": r_json
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
