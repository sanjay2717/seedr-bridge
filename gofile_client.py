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
        Uses the new global upload endpoint (auto server selection).
        
        Args:
            file_url (str): The direct download URL of the file to upload.
            folder_id (str): The Gofile folder ID to upload the file into.
            file_name (str): The desired name for the file in Gofile.
        Returns:
            dict: Upload metadata (fileId, fileName, size, downloadPage, server), or None on failure.
        """
        print(f"GOFILE INFO: Starting stream upload for '{file_name}' from URL.")
        print(f"GOFILE INFO: Using global endpoint: {GOFILE_UPLOAD_URL}")

        try:
            # 1. Stream from source URL (PikPak)
            with requests.get(file_url, stream=True, timeout=1800) as pikpak_response:
                pikpak_response.raise_for_status()

                # 2. Prepare upload data
                files = {'file': (file_name, pikpak_response.raw)}
                data = {'folderId': folder_id}

                print(f"GOFILE INFO: Streaming to Gofile...")
                
                # 3. Upload to global endpoint (server auto-selected)
                with requests.post(
                    GOFILE_UPLOAD_URL,
                    headers=self.headers,
                    data=data,
                    files=files,
                    timeout=1800
                ) as upload_response:
                    upload_response.raise_for_status()
                    upload_data = upload_response.json()

                    if upload_data.get("status") == "ok":
                        result = upload_data["data"]
                        
                        # Server is returned in response (new API)
                        server = result.get('server', 'unknown')
                        
                        print(f"GOFILE INFO: Upload successful for '{file_name}'.")
                        print(f"GOFILE INFO: Server: {server}, Size: {result.get('size')}")
                        
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

    def create_direct_link(self, content_id, expire_time=None, source_ips=None, domains_allowed=None):
        """
        Creates a direct download link for a file or folder.
        For folders, Gofile automatically creates a ZIP archive.
        
        Args:
            content_id (str): The ID of the file/folder.
            expire_time (int, optional): Unix timestamp for link expiration.
            source_ips (list, optional): List of allowed IP addresses.
            domains_allowed (list, optional): List of allowed domains.
        Returns:
            dict: Direct link data including the URL, or None on failure.
        """
        url = f"{GOFILE_BASE_URL}/contents/{content_id}/directlinks"
        
        payload = {}
        if expire_time:
            payload['expireTime'] = expire_time
        if source_ips:
            payload['sourceIpsAllowed'] = source_ips
        if domains_allowed:
            payload['domainsAllowed'] = domains_allowed
        
        try:
            response = requests.post(
                url, 
                headers=self.headers, 
                json=payload if payload else None,
                timeout=60
            )
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "ok":
                print(f"GOFILE INFO: Direct link created for {content_id}")
                return data["data"]
            else:
                error_msg = data.get("data", {}).get("error", "Unknown error")
                print(f"GOFILE ERROR: Failed to create direct link - {error_msg}")
                return None
                
        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed while creating direct link: {e}")
            return None
        except json.JSONDecodeError:
            print(f"GOFILE ERROR: Failed to decode JSON response from directlinks.")
            return None

    def delete_direct_link(self, content_id, direct_link_id):
        """
        Deletes a direct link.
        
        Args:
            content_id (str): The ID of the content.
            direct_link_id (str): The ID of the direct link to delete.
        Returns:
            bool: True if successful, False otherwise.
        """
        url = f"{GOFILE_BASE_URL}/contents/{content_id}/directlinks/{direct_link_id}"
        
        try:
            response = requests.delete(url, headers=self.headers, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "ok":
                print(f"GOFILE INFO: Direct link {direct_link_id} deleted")
                return True
            else:
                error_msg = data.get("data", {}).get("error", "Unknown error")
                print(f"GOFILE ERROR: Failed to delete direct link - {error_msg}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed while deleting direct link: {e}")
            return False
        except json.JSONDecodeError:
            print(f"GOFILE ERROR: Failed to decode JSON response.")
            return False

    def keep_alive(self, direct_link):
        """
        Sends a request to a Gofile direct link to keep it active.
        This is done by requesting the first byte of the file.
        
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
            headers = {
                **self.headers,
                'Range': 'bytes=0-0'
            }
            response = requests.get(direct_link, headers=headers, timeout=30, stream=True)
            
            # Status 200 or 206 indicates success
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
        Note: This endpoint only works with folder IDs per new API.
        
        Args:
            content_id (str): The ID of the content to check.
        Returns:
            dict: A dictionary with the content's status information, or None on failure.
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
            print(f"GOFILE ERROR: Failed to decode JSON response from check_file_status for {content_id}.")
            return None

    def delete_content(self, content_ids):
        """
        Permanently deletes files and/or folders.
        
        Args:
            content_ids (str or list): Single ID or list of content IDs to delete.
        Returns:
            bool: True if successful, False otherwise.
        """
        url = f"{GOFILE_BASE_URL}/contents"
        
        # Handle both single ID and list
        if isinstance(content_ids, list):
            ids_string = ",".join(content_ids)
        else:
            ids_string = content_ids
        
        payload = {"contentsId": ids_string}
        
        try:
            response = requests.delete(url, headers=self.headers, json=payload, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "ok":
                print(f"GOFILE INFO: Content deleted: {ids_string}")
                return True
            else:
                error_msg = data.get("data", {}).get("error", "Unknown error")
                print(f"GOFILE ERROR: Failed to delete content - {error_msg}")
                return False
                
        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed while deleting content: {e}")
            return False
        except json.JSONDecodeError:
            print(f"GOFILE ERROR: Failed to decode JSON response.")
            return False

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
            print(f"GOFILE ERROR: Failed to decode JSON response.")
            return None

    def get_account_details(self, account_id):
        """
        Retrieves detailed information about the account.
        
        Args:
            account_id (str): The account ID.
        Returns:
            dict: Account details including root folder ID, or None on failure.
        """
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
            print(f"GOFILE ERROR: Failed to decode JSON response.")
            return None