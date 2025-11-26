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

    # API Endpoint
    # We stick with the one that gave us a 200 OK before
    url = "https://www.seedr.cc/api/folder/magnet/add"
    
    # PAYLOAD
    # We put the token INSIDE the data, not the header
    payload = {
        "access_token": token,  # <--- Moved here
        "magnet": magnet_link,
        "folder_id": 0
    }

    # HEADERS
    # We remove the "Authorization" header to avoid confusion
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }

    print(f"Adding magnet with Token-in-Body method...")
    
    try:
        # requests.post with 'data=' automatically formats as form-urlencoded
        response = requests.post(url, data=payload, headers=headers)
        
        print(f"Seedr Code: {response.status_code}")
        
        try:
            r_json = response.json()
        except:
            # If they send back HTML or text
            r_json = response.text
            
        return jsonify({
            "status_code": response.status_code,
            "seedr_response": r_json
        })
        
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
