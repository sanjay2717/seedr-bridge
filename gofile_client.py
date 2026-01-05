"""
Handles all interactions with the Gofile.io API, including streaming uploads from PikPak URLs.
Updated for Gofile API v2 (2024+)
"""
import requests
import os
import json

# --- CONSTANTS ---
GOFILE_BASE_URL = "https://api.gofile.io"
GOFILE_UPLOAD_URL = "https://upload.gofile.io/uploadfile"
GOFILE_TOKEN = os.environ.get("GOFILE_TOKEN")


class GofileClient:
    """
    A client for interacting with the Gofile.io API (v2).
    """

    def __init__(self):
        """
        Initializes the GofileClient.
        Raises:
            ValueError: If the GOFILE_TOKEN environment variable is not set.
        """
        if not GOFILE_TOKEN:
            raise ValueError("GOFILE ERROR: GOFILE_TOKEN environment variable not set.")
        self.token = GOFILE_TOKEN
        self.headers = {"Authorization": f"Bearer {self.token}"}

    def get_account_id(self):
        """
        Retrieves the account ID associated with the API token.
        
        Returns:
            str: The account ID, or None on failure.
        """
        url = f"{GOFILE_BASE_URL}/accounts/getid"
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "ok":
                return data["data"]["id"]
            else:
                error_msg = data.get("data", {}).get("error", "Unknown error")
                print(f"GOFILE ERROR: Failed to get account ID - {error_msg}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed while getting account ID: {e}")
            return None
        except json.JSONDecodeError:
            print(f"GOFILE ERROR: Failed to decode JSON response from get_account_id.")
            return None

    def get_account_details(self, account_id):
        """
        Retrieves detailed information about the account.
        
        Args:
            account_id (str): The account ID.
        Returns:
            dict: Account details, or None on failure.
        """
        if not account_id:
            print("GOFILE ERROR: account_id is required for get_account_details.")
            return None
            
        url = f"{GOFILE_BASE_URL}/accounts/{account_id}"
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "ok":
                return data["data"]
            else:
                error_msg = data.get("data", {}).get("error", "Unknown error")
                print(f"GOFILE ERROR: Failed to get account details - {error_msg}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed while getting account details: {e}")
            return None
        except json.JSONDecodeError:
            print(f"GOFILE ERROR: Failed to decode JSON response from get_account_details.")
            return None

    def create_folder(self, parent_folder_id, folder_name):
        """
        Creates a new folder in a specified parent folder.
        Args:
            parent_folder_id (str): The ID of the parent folder.
            folder_name (str): The name of the new folder.
        Returns:
            dict: A dictionary containing the new folder's 'id' and 'code', or None on failure.
        """
        url = f"{GOFILE_BASE_URL}/contents/createFolder"
        payload = {
            "parentFolderId": parent_folder_id,
            "folderName": folder_name
        }
        try:
            response = requests.post(url, headers=self.headers, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "ok":
                return data["data"]
            else:
                error_msg = data.get("data", {}).get("error", "Unknown error")
                print(f"GOFILE ERROR: Failed to create folder '{folder_name}' - {error_msg}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed while creating folder: {e}")
            return None

    def upload_file_stream(self, file_url, folder_id, file_name):
        """
        Uploads a file by streaming it from a URL to prevent memory issues.
        This version uses an explicit iterator to ensure chunked streaming and adds progress logging.
        
        Args:
            file_url (str): The direct download URL of the file to upload.
            folder_id (str): The Gofile folder ID to upload into.
            file_name (str): The desired name for the file.
        Returns:
            dict: Upload metadata, or None on failure.
        """
        print(f"GOFILE INFO: Starting robust, memory-efficient stream upload for '{file_name}'.")
        
        try:
            # Step 1: Open a streaming connection to the source URL
            with requests.get(file_url, stream=True, timeout=(10, 1800)) as r:
                r.raise_for_status()

                # Step 2: Create an iterator that logs progress
                file_iterator = r.iter_content(chunk_size=4 * 1024 * 1024) # 4MB chunks
                
                total_bytes_streamed = 0
                has_logged_300mb = False

                def logging_iterator(iterator):
                    nonlocal total_bytes_streamed, has_logged_300mb
                    for chunk in iterator:
                        yield chunk
                        total_bytes_streamed += len(chunk)
                        if not has_logged_300mb and total_bytes_streamed >= 300 * 1024 * 1024:
                            print("GOFILE INFO: Upload streaming correctly. Over 300MB processed.")
                            has_logged_300mb = True
                
                # Step 3: Prepare the upload
                files = {'file': (file_name, logging_iterator(file_iterator), 'application/octet-stream')}
                data = {'folderId': folder_id}
                
                print(f"GOFILE INFO: Streaming to {GOFILE_UPLOAD_URL} with chunked encoding via iterator...")
                
                upload_response = requests.post(
                    GOFILE_UPLOAD_URL,
                    headers=self.headers,
                    data=data,
                    files=files,
                    timeout=3600 # Increased timeout for very large files
                )
                upload_response.raise_for_status()
                upload_data = upload_response.json()

                if upload_data.get("status") != "ok":
                    error_msg = upload_data.get("data", {}).get("error", "Unknown error")
                    print(f"GOFILE ERROR: File upload failed - {error_msg}")
                    return None

                result = upload_data["data"]
                print(f"GOFILE INFO: Upload successful for '{result.get('fileName')}'. File ID: {result.get('fileId')}")

                # Step 4: Create a direct link for the newly uploaded file
                file_id = result.get('fileId')
                direct_link_data = self.create_direct_link(file_id)
                direct_link = None
                if direct_link_data and 'link' in direct_link_data:
                    direct_link = direct_link_data['link']
                    print(f"GOFILE INFO: Successfully created direct link.")
                else:
                    print(f"GOFILE WARN: Could not create direct link for {file_id}.")

                # Step 5: Format response for DB compatibility
                return {
                    "file_id": result.get("fileId"),
                    "file_name": result.get("fileName"),
                    "download_page": result.get("downloadPage"),
                    "direct_link": direct_link,
                    "server": result.get('server', 'global'),
                    "file_size": int(r.headers.get('content-length', 0)) # Get size from original download headers
                }

        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed during file upload stream: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"GOFILE ERROR: Failed to decode JSON response during upload: {e}")
            return None
        except Exception as e:
            print(f"GOFILE ERROR: An unexpected error occurred during file upload: {e}")
            return None

    def create_direct_link(self, content_id):
        """
        Creates a direct download link for a file or folder.
        
        Args:
            content_id (str): The ID of the file/folder.
        Returns:
            dict: Direct link data including the URL, or None on failure.
        """
        url = f"{GOFILE_BASE_URL}/contents/{content_id}/directlinks"
        try:
            response = requests.post(url, headers=self.headers, json={}, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "ok":
                print(f"GOFILE INFO: Direct link created for {content_id}")
                # The response is a list of links, return the first one.
                return data["data"]["links"][0] if data["data"].get("links") else None
            else:
                error_msg = data.get("data", {}).get("error", "Unknown error")
                print(f"GOFILE ERROR: Failed to create direct link - {error_msg}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed while creating direct link: {e}")
            return None

    def keep_alive(self, direct_link):
        """
        Sends a request to a Gofile direct link to keep it active.
        
        Args:
            direct_link (str): The direct download link of the file.
        Returns:
            bool: True if the request was successful (status 200 or 206), False otherwise.
        """
        if not direct_link:
            print("GOFILE WARN: Keep-alive skipped, direct link is invalid.")
            return False
            
        print(f"GOFILE INFO: Sending keep-alive for {direct_link}")
        try:
            # Auth header is now required for keep-alive on private/premium files
            keep_alive_headers = {
                **self.headers,
                'Range': 'bytes=0-0'
            }
            response = requests.get(direct_link, headers=keep_alive_headers, timeout=30, stream=True)
            
            if response.status_code in [200, 206]:
                print("GOFILE INFO: Keep-alive successful.")
                return True
            else:
                print(f"GOFILE WARN: Keep-alive failed with status code {response.status_code}.")
                return False
        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Keep-alive request failed for {direct_link}: {e}")
            return False

    def check_file_status(self, content_id):
        """
        Checks the status of a specific file or folder content.
        
        Args:
            content_id (str): The ID of the content to check.
        Returns:
            dict: The content's status information, or None on failure.
        """
        url = f"{GOFILE_BASE_URL}/contents/{content_id}"
        try:
            response = requests.get(url, headers=self.headers, timeout=60)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "ok":
                return data["data"]
            else:
                error_msg = data.get("data", {}).get("error", "Unknown error")
                print(f"GOFILE ERROR: Failed to check status for content {content_id} - {error_msg}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed while checking status for {content_id}: {e}")
            return None
        except json.JSONDecodeError:
            print(f"GOFILE ERROR: Failed to decode JSON from check_file_status for {content_id}.")
            return None
