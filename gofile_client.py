"""
Handles all interactions with the Gofile.io API, including streaming uploads from PikPak URLs.
"""
import requests
import os
import time
import json

# --- CONSTANTS ---
GOFILE_BASE_URL = "https://api.gofile.io"
GOFILE_TOKEN = os.environ.get("GOFILE_TOKEN")

class GofileClient:
    """
    A client for interacting with the Gofile.io API.
    """

    def __init__(self):
        """
        Initializes the GofileClient.
        Raises:
            ValueError: If the GOFILE_TOKEN environment variable is not set.
        """
        if not GOFILE_TOKEN:
            raise ValueError("GOFILE ERROR: GOFILE_TOKEN environment variable not set.")
        self.headers = {"Authorization": f"Bearer {GOFILE_TOKEN}"}

    def get_best_server(self):
        """
        Gets the best available Gofile server for upload.
        Returns:
            str: The server name (e.g., "store1") or None if an error occurs.
        """
        url = f"{GOFILE_BASE_URL}/getServer"
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "ok":
                return data["data"]["server"]
            else:
                error_msg = data.get("data", {}).get("error", "Unknown error")
                print(f"GOFILE ERROR: Failed to get best server - {error_msg}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed while getting best server: {e}")
            return None
        except json.JSONDecodeError:
            print(f"GOFILE ERROR: Failed to decode JSON response from getServer.")
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
        except json.JSONDecodeError:
            print(f"GOFILE ERROR: Failed to decode JSON response from createFolder.")
            return None

    def upload_file_stream(self, file_url, folder_id, file_name):
        """
        Uploads a file by streaming it directly from a URL to Gofile.
        Args:
            file_url (str): The direct download URL of the file to upload.
            folder_id (str): The Gofile folder ID to upload the file into.
            file_name (str): The desired name for the file in Gofile.
        Returns:
            dict: Upload metadata (fileId, fileName, downloadPage, directLink) or None on failure.
        """
        print(f"GOFILE INFO: Starting stream upload for '{file_name}' from URL.")
        server = self.get_best_server()
        if not server:
            return None
        
        print(f"GOFILE INFO: Using server '{server}' for upload.")

        try:
            # Step 1: GET request to file_url with stream=True
            with requests.get(file_url, stream=True, timeout=1800) as pikpak_response:
                pikpak_response.raise_for_status()

                # Step 2: POST request to https://{server}.gofile.io/uploadFile
                upload_url = f"https://{server}.gofile.io/uploadFile"
                
                # The 'files' parameter handles the streaming multipart/form-data upload.
                files = {'file': (file_name, pikpak_response.raw)}
                data = {'folderId': folder_id}

                print(f"GOFILE INFO: Streaming to {upload_url}...")
                with requests.post(
                    upload_url,
                    headers=self.headers,
                    data=data,
                    files=files,
                    timeout=1800
                ) as upload_response:
                    upload_response.raise_for_status()
                    upload_data = upload_response.json()

                    if upload_data.get("status") == "ok":
                        result = upload_data["data"]
                        # Manually construct the direct download link
                        result['directLink'] = f"https://{server}.gofile.io/download/web/{result['fileId']}/{result['fileName']}"
                        print(f"GOFILE INFO: Upload successful for '{file_name}'.")
                        return result
                    else:
                        error_msg = upload_data.get("data", {}).get("error", "Unknown error")
                        print(f"GOFILE ERROR: File upload failed - {error_msg}")
                        return None

        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed during file upload stream: {e}")
            return None
        except json.JSONDecodeError:
            print(f"GOFILE ERROR: Failed to decode JSON response from uploadFile.")
            return None
        except Exception as e:
            print(f"GOFILE ERROR: An unexpected error occurred during file upload: {e}")
            return None

    def keep_alive(self, direct_link):
        """
        Sends a request to a Gofile direct link to keep it active.
        This is done by requesting the first byte of the file.
        Args:
            direct_link (str): The direct download link of the file.
        Returns:
            bool: True if the request was successful (status 206), False otherwise.
        """
        if not direct_link:
            print("GOFILE WARN: Keep-alive skipped, direct link is invalid.")
            return False
            
        print(f"GOFILE INFO: Sending keep-alive for {direct_link}")
        try:
            headers = {'Range': 'bytes=0-0'}
            # A short timeout is fine as we only need the headers and 1 byte.
            response = requests.get(direct_link, headers=headers, timeout=30, stream=True)
            # Status 206 Partial Content indicates success
            if response.status_code == 206:
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
            dict: A dictionary with the content's status information, or None on failure.
        """
        url = f"{GOFILE_BASE_URL}/contents/{content_id}?allDetails=true"
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
            print(f"GOFILE ERROR: Failed to decode JSON response from check_file_status for {content_id}.")
            return None
