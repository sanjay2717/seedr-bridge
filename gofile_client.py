"""
Handles all interactions with the Gofile.io API, including streaming uploads from PikPak URLs.
Updated for Gofile API v2 (2024+)
Uses requests-toolbelt for true streaming multipart uploads (no RAM buffering)
"""
import requests
from requests_toolbelt import MultipartEncoder, MultipartEncoderMonitor
import os
import json

# --- CONSTANTS ---
GOFILE_BASE_URL = "https://api.gofile.io"
GOFILE_TOKEN = os.environ.get("GOFILE_TOKEN")

# Headers for streaming from PikPak
HEADERS_PIKPAK = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Content-Type": "application/x-www-form-urlencoded"
}


class StreamingIteratorWrapper:
    """
    Wraps a streaming iterator to make it compatible with MultipartEncoder.
    This provides a file-like object that reads from the stream without buffering.
    """
    
    def __init__(self, iterator, total_size=None):
        self.iterator = iterator
        self.total_size = total_size
        self._buffer = b''
        self._bytes_read = 0
        self._exhausted = False
        self._logged_300mb = False
    
    def read(self, size=-1):
        """Read bytes from the streaming iterator."""
        if self._exhausted and not self._buffer:
            return b''
        
        # If size is -1 or None, read everything (not recommended for large files)
        if size is None or size < 0:
            result = self._buffer
            self._buffer = b''
            for chunk in self.iterator:
                result += chunk
            self._exhausted = True
            self._bytes_read += len(result)
            return result
        
        # Read until we have enough bytes or iterator is exhausted
        while len(self._buffer) < size and not self._exhausted:
            try:
                chunk = next(self.iterator)
                self._buffer += chunk
            except StopIteration:
                self._exhausted = True
                break
        
        # Return requested bytes
        result = self._buffer[:size]
        self._buffer = self._buffer[size:]
        self._bytes_read += len(result)
        
        # Progress logging
        if not self._logged_300mb and self._bytes_read >= 300 * 1024 * 1024:
            print(f"GOFILE INFO: Streaming correctly. Over 300MB processed ({self._bytes_read / (1024*1024):.1f} MB)")
            self._logged_300mb = True
        
        return result
    
    def __len__(self):
        """Return total size if known (required for Content-Length header)."""
        if self.total_size:
            return self.total_size
        raise TypeError("Size unknown")


class GofileClient:
    """
    A client for interacting with the Gofile.io API (v2).
    Uses true streaming uploads to avoid memory issues.
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

    def _get_best_server(self):
        """
        Gets the best available server for upload.
        
        Returns:
            str: The server name (e.g., 'store1'), or None on failure.
        """
        url = f"{GOFILE_BASE_URL}/servers"
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "ok":
                servers = data.get("data", {}).get("servers", [])
                if servers:
                    # Return the first available server
                    server = servers[0].get("name")
                    print(f"GOFILE INFO: Selected upload server: {server}")
                    return server
            print("GOFILE WARN: Could not get best server, using default.")
            return None
        except Exception as e:
            print(f"GOFILE WARN: Failed to get best server: {e}")
            return None

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
        Uploads a file by TRUE STREAMING from a URL - NO RAM BUFFERING.
        Uses requests-toolbelt MultipartEncoder for chunked transfer encoding.
        
        Args:
            file_url (str): The direct download URL of the file to upload.
            folder_id (str): The Gofile folder ID to upload into.
            file_name (str): The desired name for the file.
        Returns:
            dict: Upload metadata, or None on failure.
        """
        print(f"GOFILE INFO: Starting TRUE streaming upload for '{file_name}' (no RAM buffering).")
        
        source_response = None
        try:
            # Step 1: Use Singapore regional upload proxy for Render Singapore
            upload_url = "https://upload-ap-sgp.gofile.io/uploadfile"
            
            print(f"GOFILE INFO: Using Singapore regional upload proxy: {upload_url}")
            
            # Step 2: Open a streaming connection to the source URL
            source_response = requests.get(file_url, stream=True, timeout=(10, 1800), headers=HEADERS_PIKPAK)
            source_response.raise_for_status()
            
            # Get file size from headers if available
            file_size = source_response.headers.get('content-length')
            file_size = int(file_size) if file_size else None
            
            if file_size:
                print(f"GOFILE INFO: Source file size: {file_size / (1024*1024):.2f} MB")
            else:
                print(f"GOFILE INFO: Source file size unknown, using chunked transfer.")
            
            # Step 3: Create streaming iterator (4MB chunks for efficiency)
            file_iterator = source_response.iter_content(chunk_size=4 * 1024 * 1024)
            
            # Step 4: Wrap iterator in file-like object for MultipartEncoder
            streaming_wrapper = StreamingIteratorWrapper(file_iterator, file_size)
            
            # Step 5: Create MultipartEncoder for TRUE streaming upload
            # This sends data as it reads - NO BUFFERING IN RAM
            fields = {
                'folderId': folder_id,
                'file': (file_name, streaming_wrapper, 'application/octet-stream')
            }
            
            encoder = MultipartEncoder(fields=fields)
            
            # Step 6: Create progress monitor (optional but useful for debugging)
            bytes_uploaded = [0]
            last_logged = [0]
            
            def progress_callback(monitor):
                bytes_uploaded[0] = monitor.bytes_read
                # Log every 100MB
                if bytes_uploaded[0] - last_logged[0] >= 100 * 1024 * 1024:
                    print(f"GOFILE INFO: Uploaded {bytes_uploaded[0] / (1024*1024):.1f} MB...")
                    last_logged[0] = bytes_uploaded[0]
            
            monitor = MultipartEncoderMonitor(encoder, progress_callback)
            
            # Step 7: Prepare headers - Content-Type MUST include boundary
            upload_headers = {
                'Authorization': f'Bearer {self.token}',
                'Content-Type': monitor.content_type
            }
            
            # If we know the size, we can set Content-Length for better compatibility
            # Otherwise, requests-toolbelt will use chunked transfer encoding
            
            print(f"GOFILE INFO: Starting chunked upload to Gofile...")
            
            # Step 8: Perform the upload - this streams directly, no buffering!
            upload_response = requests.post(
                upload_url,
                headers=upload_headers,
                data=monitor,
                timeout=7200  # 2 hours for very large files
            )
            
            upload_response.raise_for_status()
            upload_data = upload_response.json()

            if upload_data.get("status") != "ok":
                error_msg = upload_data.get("data", {}).get("error", "Unknown error")
                print(f"GOFILE ERROR: File upload failed - {error_msg}")
                return None

            result = upload_data["data"]
            print(f"GOFILE INFO: Upload successful for '{result.get('fileName')}'. File ID: {result.get('fileId')}")

            # Step 9: Create a direct link for the newly uploaded file
            file_id = result.get('fileId')
            direct_link_data = self.create_direct_link(file_id)
            direct_link = None
            if direct_link_data and 'link' in direct_link_data:
                direct_link = direct_link_data['link']
                print(f"GOFILE INFO: Successfully created direct link.")
            else:
                print(f"GOFILE WARN: Could not create direct link for {file_id}.")

            # Step 10: Format response for DB compatibility
            return {
                "file_id": result.get("fileId"),
                "file_name": result.get("fileName"),
                "download_page": result.get("downloadPage"),
                "direct_link": direct_link,
                "server": result.get('server', 'ap-sgp'),
                "file_size": file_size or result.get('size', 0)
            }

        except requests.exceptions.Timeout as e:
            print(f"GOFILE ERROR: Upload timed out: {e}")
            return None
        except requests.exceptions.ConnectionError as e:
            print(f"GOFILE ERROR: Connection error during upload: {e}")
            return None
        except requests.exceptions.RequestException as e:
            print(f"GOFILE ERROR: Request failed during file upload stream: {e}")
            return None
        except json.JSONDecodeError as e:
            print(f"GOFILE ERROR: Failed to decode JSON response during upload: {e}")
            return None
        except Exception as e:
            print(f"GOFILE ERROR: An unexpected error occurred during file upload: {e}")
            import traceback
            traceback.print_exc()
            return None
        finally:
            # Always close the source connection
            if source_response:
                source_response.close()

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