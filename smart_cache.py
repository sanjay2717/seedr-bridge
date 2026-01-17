"""
Smart Cache System for PikPak Deduplication
Checks database before downloading to avoid duplicate downloads.
"""

import re
import time
import requests
from typing import Optional, Dict, List
from supabase_client import db

# ============================================
# CONFIGURATION (Import from app.py or define here)
# ============================================

PIKPAK_API_DRIVE = "https://api-drive.mypikpak.com"
PIKPAK_CLIENT_ID = "YNxT9w7GMdWvEOKa"  # Update if different


# ============================================
# HELPER FUNCTIONS
# ============================================

def extract_hash(magnet_link: str) -> Optional[str]:
    """
    Extracts the BTIH hash from a magnet link and normalizes it.
    """
    match = re.search(r'xt=urn:btih:([a-zA-Z0-9]+)', magnet_link, re.IGNORECASE)
    if match:
        return match.group(1).upper()
    return None


# ============================================
# SMART CACHE CHECK (Use Before Download)
# ============================================

def check_smart_cache(magnet_link: str) -> Optional[Dict]:
    """
    Check if file already exists in any PikPak account.
    Call this BEFORE pikpak_add_magnet().
    
    Args:
        magnet_link: The magnet URL
        
    Returns:
        Dict with account_id, file_id, file_name if found
        None if not in cache
    """
    magnet_hash = extract_hash(magnet_link)
    
    if not magnet_hash:
        print("⚠️ Smart Cache: Could not extract hash from magnet")
        return None
    
    print(f"🔍 Smart Cache: Checking for hash {magnet_hash[:16]}...")
    
    # Check database
    cached = db.check_smart_cache(magnet_hash)
    
    if cached:
        print(f"✅ Smart Cache HIT!")
        print(f"   📁 File: {cached.get('file_name', 'Unknown')}")
        print(f"   👤 Account: {cached.get('account_id')}")
        print(f"   🆔 File ID: {cached.get('file_id')}")
        return cached
    
    print(f"❌ Smart Cache MISS: Hash not found in database")
    return None


def save_to_smart_cache(
    magnet_link: str,
    file_id: str,
    account_id: int,
    file_name: str = None,
    file_size: int = None,
    file_hash: str = None,
    parent_id: str = None
) -> bool:
    """
    Save file to smart cache after successful download.
    Call this AFTER successful pikpak_add_magnet().
    
    Args:
        magnet_link: Original magnet URL
        file_id: PikPak file ID
        account_id: Which account has this file
        file_name: Name of the file
        file_size: Size in bytes
        file_hash: PikPak's file hash (from params.url)
        parent_id: Parent folder ID
        
    Returns:
        True if saved, False if error
    """
    magnet_hash = extract_hash(magnet_link)
    
    if not magnet_hash:
        print("⚠️ Smart Cache: Could not extract hash, not saving")
        return False
    
    data = {
        'magnet_hash': magnet_hash,
        'file_id': file_id,
        'account_id': account_id,
        'file_name': file_name,
        'file_size': file_size,
        'file_hash': file_hash,
        'parent_id': parent_id
    }
    
    result = db.save_to_smart_cache(data)
    
    if result:
        print(f"💾 Smart Cache: Saved {file_name or file_id}")
        return True
    
    return False


# ============================================
# FULL SYNC FUNCTION
# ============================================

def pikpak_list_files_paginated(parent_id: str, account: Dict, tokens: Dict) -> List[Dict]:
    """
    List ALL files in a folder with pagination support.
    
    Args:
        parent_id: Folder ID to list
        account: Account dict with device_id
        tokens: Auth tokens with access_token
        
    Returns:
        List of all file objects
    """
    all_files = []
    page_token = None
    page_count = 0
    
    headers = {
        "Authorization": f"Bearer {tokens['access_token']}",
        "X-Client-ID": PIKPAK_CLIENT_ID,
        "X-Device-ID": account.get('device_id', ''),
        "Content-Type": "application/json"
    }
    
    while True:
        page_count += 1
        print(f"   📄 Fetching page {page_count}...")
        
        params = {
            "parent_id": parent_id,
            "limit": 100,
            "filters": '{"trashed":{"eq":false}}'
        }
        
        if page_token:
            params["page_token"] = page_token
        
        try:
            url = f"{PIKPAK_API_DRIVE}/drive/v1/files"
            response = requests.get(url, headers=headers, params=params, timeout=30)
            data = response.json()
            
            files = data.get("files", [])
            all_files.extend(files)
            
            # Check for next page
            page_token = data.get("next_page_token")
            
            if not page_token or not files:
                break
                
            # Rate limiting
            time.sleep(0.5)
            
        except Exception as e:
            print(f"   ❌ Error fetching page {page_count}: {e}")
            break
    
    print(f"   ✅ Total files found: {len(all_files)}")
    return all_files


def pikpak_get_file_hash(file_id: str, account: Dict, tokens: Dict) -> Optional[str]:
    """
    Get file hash from PikPak file info.
    
    Returns:
        Hash string or None
    """
    headers = {
        "Authorization": f"Bearer {tokens['access_token']}",
        "X-Client-ID": PIKPAK_CLIENT_ID,
        "X-Device-ID": account.get('device_id', ''),
        "Content-Type": "application/json"
    }
    
    try:
        url = f"{PIKPAK_API_DRIVE}/drive/v1/files/{file_id}"
        response = requests.get(url, headers=headers, timeout=30)
        data = response.json()
        
        # Try to extract hash from params.url
        params_url = data.get("params", {}).get("url", "")
        
        if params_url:
            # Extract hash from magnet link in params.url
            hash_match = re.search(r'btih:([a-fA-F0-9]+)', params_url)
            if hash_match:
                return hash_match.group(1).upper()
        
        # Fallback to hash or md5 field
        return data.get("hash") or data.get("md5")
        
    except Exception as e:
        print(f"   ⚠️ Could not get hash for {file_id}: {e}")
        return None


def sync_account_to_cache(
    account: Dict,
    tokens: Dict,
    login_func,
    skip_hash_lookup: bool = False
) -> Dict:
    """
    Sync a single account's files to smart cache.
    
    Args:
        account: Account dict with id, email, my_pack_id, device_id
        tokens: Auth tokens
        login_func: Function to call for login (from app.py)
        skip_hash_lookup: If True, skip individual file hash lookups (faster but less accurate)
        
    Returns:
        Dict with stats: synced, trashed, errors
    """
    account_id = account['id']
    my_pack_id = account.get('my_pack_id')
    
    stats = {'synced': 0, 'trashed': 0, 'errors': 0}
    
    if not my_pack_id:
        print(f"   ⚠️ No my_pack_id for account {account_id}, skipping")
        return stats
    
    print(f"📂 Syncing Account {account_id}: {account.get('email', 'Unknown')[:20]}...")
    
    try:
        # Get all files from PikPak
        api_files = pikpak_list_files_paginated(my_pack_id, account, tokens)
        api_file_ids = set()
        
        files_to_upsert = []
        
        for file in api_files:
            file_id = file.get('id')
            
            if not file_id:
                continue
                
            api_file_ids.add(file_id)
            
            # Get hash for this file
            file_hash = None
            magnet_hash = None
            
            if not skip_hash_lookup:
                file_hash = pikpak_get_file_hash(file_id, account, tokens)
                if file_hash:
                    magnet_hash = file_hash
                time.sleep(0.2)  # Rate limiting
            
            # If no hash found, use file_id as fallback magnet_hash
            if not magnet_hash:
                magnet_hash = f"FILEID_{file_id}"
            
            files_to_upsert.append({
                'magnet_hash': magnet_hash,
                'file_hash': file_hash,
                'file_id': file_id,
                'account_id': account_id,
                'file_name': file.get('name'),
                'file_size': int(file.get('size', 0)),
                'parent_id': file.get('parent_id'),
                'is_trash': False
            })
        
        # Bulk upsert to database
        if files_to_upsert:
            db.bulk_upsert_cache(files_to_upsert)
            stats['synced'] = len(files_to_upsert)
        
        # Find deleted files (in DB but not in API)
        db_file_ids = set(db.get_cached_files_by_account(account_id))
        deleted_file_ids = db_file_ids - api_file_ids
        
        if deleted_file_ids:
            db.mark_cache_as_trash(account_id, list(deleted_file_ids))
            stats['trashed'] = len(deleted_file_ids)
        
        print(f"   ✅ Synced: {stats['synced']} | Trashed: {stats['trashed']}")
        
    except Exception as e:
        print(f"   ❌ Error syncing account {account_id}: {e}")
        stats['errors'] = 1
    
    return stats


def sync_all_accounts_to_cache(login_func, get_account_func) -> Dict:
    """
    Sync ALL accounts to smart cache.
    Call this on startup or after clear-trash.
    
    Args:
        login_func: Your pikpak_login function from app.py
        get_account_func: Function to get all accounts
        
    Returns:
        Dict with total stats
    """
    print("=" * 50)
    print("🔄 SMART CACHE: Starting Full Sync")
    print("=" * 50)
    
    total_stats = {'synced': 0, 'trashed': 0, 'errors': 0, 'accounts': 0}
    
    try:
        # Get all active accounts from DB
        # Fetch from both servers (1 and 2)
        all_accounts = []
        all_accounts.extend(db.get_all_server_accounts(1))
        all_accounts.extend(db.get_all_server_accounts(2))
        
        print(f"📊 Found {len(all_accounts)} accounts to sync")
        
        for account in all_accounts:
            if account.get('status') != 'active':
                print(f"   ⏭️ Skipping inactive account {account['id']}")
                continue
            
            # Ensure device_id is set
            account['device_id'] = account.get('current_device_id') or account.get('device_id')
            
            try:
                # Login to account
                tokens = login_func(account)
                
                if not tokens:
                    print(f"   ❌ Login failed for account {account['id']}")
                    total_stats['errors'] += 1
                    continue
                
                # Sync this account
                stats = sync_account_to_cache(
                    account=account,
                    tokens=tokens,
                    login_func=login_func,
                    skip_hash_lookup=True  # Faster sync, set False for accurate hashes
                )
                
                total_stats['synced'] += stats['synced']
                total_stats['trashed'] += stats['trashed']
                total_stats['errors'] += stats['errors']
                total_stats['accounts'] += 1
                
                # Rate limiting between accounts
                time.sleep(1)
                
            except Exception as e:
                print(f"   ❌ Error with account {account['id']}: {e}")
                total_stats['errors'] += 1
        
        print("=" * 50)
        print(f"🎉 SMART CACHE: Sync Complete!")
        print(f"   📊 Accounts: {total_stats['accounts']}")
        print(f"   ✅ Synced: {total_stats['synced']} files")
        print(f"   🗑️ Trashed: {total_stats['trashed']} files")
        print(f"   ❌ Errors: {total_stats['errors']}")
        print("=" * 50)
        
    except Exception as e:
        print(f"❌ SMART CACHE: Sync failed - {e}")
    
    return total_stats


# ============================================
# CACHE MAINTENANCE
# ============================================

def clear_trashed_cache(account_id: int = None) -> int:
    """
    Permanently remove trashed entries from cache.
    
    Args:
        account_id: Optional - only clear for specific account
        
    Returns:
        Number of records deleted
    """
    return db.clear_trash_from_cache(account_id)


def get_cache_stats() -> Dict:
    """
    Get smart cache statistics.
    """
    return db.get_smart_cache_stats()