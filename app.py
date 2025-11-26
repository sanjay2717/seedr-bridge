from flask import Flask, request, jsonify
import requests

app = Flask(__name__)

# SEEDR ENDPOINTS
LOGIN_URL = "https://www.seedr.cc/rest/login"
ADD_MAGNET_URL = "https://www.seedr.cc/rest/transfer/magnet"
LIST_FOLDER_URL = "https://www.seedr.cc/rest/folder"

@app.route('/')
def home():
    return "Seedr Bridge is Running!"

@app.route('/add-magnet', methods=['POST'])
def add_magnet_to_seedr():
    data = request.json
    username = data.get('username')
    password = data.get('password')
    magnet_link = data.get('magnet')

    if not username or not password or not magnet_link:
        return jsonify({"success": False, "error": "Missing inputs"}), 400

    # Start a Session (Like a Browser)
    session = requests.Session()
    
    # Step 1: Login
    # We send credentials as form data, just like the website login form
    login_data = {
        'username': username,
        'password': password,
        'type': 'login'
    }
    
    print(f"Attempting login for {username}...")
    login_resp = session.post(LOGIN_URL, data=login_data)
    
    # Check if login gave us a cookie
    if login_resp.status_code != 200 or 'PHPSESSID' not in session.cookies:
        return jsonify({
            "success": False, 
            "error": "Login Failed", 
            "details": login_resp.text
        }), 401

    print("Login Success! Adding magnet...")

    # Step 2: Add Magnet
    magnet_data = {'magnet': magnet_link}
    add_resp = session.post(ADD_MAGNET_URL, data=magnet_data)

    if add_resp.status_code == 200:
        return jsonify(add_resp.json())
    else:
        return jsonify({
            "success": False, 
            "error": "Failed to add magnet",
            "details": add_resp.text
        }), 400

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=10000)
