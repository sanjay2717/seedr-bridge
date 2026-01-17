"""
Smart Cache System for PikPak Deduplication
Checks database before downloading to avoid duplicate downloads.
"""

import re
import time
import random
import requests
from typing import Optional, Dict, List, Callable
from supabase_client import db

# ============================================
# CONFIGURATION
# ============================================

PIKPAK_API_DRIVE = "https://api-drive.mypikpak.com"
PIKPAK_CLIENT_ID = "YUMx5nI8ZU8Ap8pm"


# ============================================
# HELPER FUNCTIONS
# ============================================

def extract_hash(magnet_link: str) -> Optional[str]:
    """
    Extracts the BTIH hash from a magnet link and normalizes it.
    """
    if not magnet_link:
        return None
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
    file_id: str,
    account_id: int,
    magnet_link: str = None,
    magnet_hash: str = None,
    file_name: str = None,
    file_size: int = None,
    file_hash: str = None,
    parent_id: str = None
) -> bool:
    """
    Save file to smart cache after successful download.
    Call this AFTER successful pikpak_add_magnet().
    
    Args:
        file_id: PikPak file ID (required)
        account_id: Which account has this file (required)
        magnet_link: Original magnet URL (optional if magnet_hash provided)
        magnet_hash: Direct hash (optional if magnet_link provided)
        file_name: Name of the file
        file_size: Size in bytes
        file_hash: PikPak's file hash
        parent_id: Parent folder ID
        
    Returns:
        True if saved, False if error
    """
    # Get hash from either source
    if magnet_hash:
        final_hash = magnet_hash.upper()
    elif magnet_link:
        final_hash = extract_hash(magnet_link)
    else:
        print("⚠️ Smart Cache: No hash provided, not saving")
        return False
    
    if not final_hash:
        print("⚠️ Smart Cache: Could not extract hash, not saving")
        return False
    
    data = {
        'magnet_hash': final_hash,
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
# PIKPAK API FUNCTIONS (For Sync)
# ============================================

def pikpak_list_files_paginated(parent_id: str, account: Dict, tokens: Dict, captcha_func: Callable = None) -> List[Dict]:
    """
    List ALL files in a folder with pagination support.
    
    Args:
        parent_id: Folder ID to list
        account: Account dict with device_id
        tokens: Auth tokens with access_token
        captcha_func: Function to generate captcha token (action, device_id, user_id)
        
    Returns:
        List of all file objects
    """
    all_files = []
    page_token = None
    page_count = 0
    
    device_id = account.get('device_id', '')
    user_id = tokens.get('user_id', '')
    
    # Generate captcha if function provided
    captcha_token = ""
    if captcha_func:
        captcha_token = captcha_func("GET:/drive/v1/files", device_id, user_id)
    
    headers = {
        "Authorization": f"Bearer {tokens['access_token']}",
        "X-Client-ID": PIKPAK_CLIENT_ID,
        "X-Device-ID": device_id,
        "X-Captcha-Token": captcha_token,
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


def pikpak_get_file_info(file_id: str, account: Dict, tokens: Dict, captcha_func: Callable = None) -> Optional[Dict]:
    """
    Get detailed file info from PikPak API.
    This returns the full file object including params.url with magnet hash.
    
    Args:
        file_id: PikPak file ID
        account: Account dict with device_id
        tokens: Auth tokens with access_token
        captcha_func: Function to generate captcha token
        
    Returns:
        Full file info dict or None on error
    """
    device_id = account.get('device_id', '')
    user_id = tokens.get('user_id', '')
    
    captcha_token = ""
    if captcha_func:
        captcha_token = captcha_func(f"GET:/drive/v1/files/{file_id}", device_id, user_id)

    headers = {
        "Authorization": f"Bearer {tokens['access_token']}",
        "X-Client-ID": PIKPAK_CLIENT_ID,
        "X-Device-ID": device_id,
        "X-Captcha-Token": captcha_token,
        "Content-Type": "application/json"
    }
    
    try:
        url = f"{PIKPAK_API_DRIVE}/drive/v1/files/{file_id}"
        response = requests.get(url, headers=headers, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        else:
            print(f"   ⚠️ API returned {response.status_code} for file {file_id}")
            return None
            
    except Exception as e:
        print(f"   ⚠️ Could not get info for {file_id}: {e}")
        return None


def extract_hash_from_file_info(file_info: Dict) -> Optional[str]:
    """
    Extract magnet hash from PikPak file info response.
    
    Args:
        file_info: Full file info dict from pikpak_get_file_info
        
    Returns:
        Magnet hash (uppercase) or None
    """
    if not file_info:
        return None
    
    # Try params.url first (contains magnet link)
    params_url = file_info.get("params", {}).get("url", "")
    
    if params_url:
        # Extract BTIH from magnet link
        magnet_hash = extract_hash(params_url)
        if magnet_hash:
            return magnet_hash
    
    # Fallback: Try hash field
    file_hash = file_info.get("hash")
    if file_hash:
        return file_hash.upper()
    
    # Fallback: Try md5 field
    md5 = file_info.get("md5")
    if md5:
        return md5.upper()
    
    return None


# ============================================
# SYNC FUNCTIONS
# ============================================

def sync_account_to_cache(
    account: Dict,
    tokens: Dict,
    login_func: Callable = None,
    captcha_func: Callable = None
) -> Dict:
    """
    Sync a single account's files to smart cache.
    Fetches detailed info for each file to get magnet hash.
    
    Args:
        account: Account dict with id, email, my_pack_id, device_id
        tokens: Auth tokens
        login_func: Function to call for login (not used, kept for compatibility)
        captcha_func: Function to generate captcha token
        
    Returns:
        Dict with stats: synced, skipped, trashed, errors
    """
    account_id = account['id']
    my_pack_id = account.get('my_pack_id')
    
    stats = {'synced': 0, 'skipped': 0, 'trashed': 0, 'errors': 0}
    
    if not my_pack_id:
        print(f"   ⚠️ No my_pack_id for account {account_id}, skipping")
        return stats
    
    print(f"📂 Syncing Account {account_id}: {account.get('email', 'Unknown')[:20]}...")
    
    try:
        # Step 1: Get all files from PikPak (basic info only)
        api_files = pikpak_list_files_paginated(my_pack_id, account, tokens, captcha_func)
        api_file_ids = set()
        
        print(f"   🔍 Fetching detailed info for {len(api_files)} files...")
        
        # Step 2: For each file, get detailed info and extract hash
        for idx, file in enumerate(api_files):
            file_id = file.get('id')
            file_name = file.get('name', 'Unknown')
            
            if not file_id:
                continue
            
            api_file_ids.add(file_id)
            
            # Progress indicator
            if (idx + 1) % 10 == 0:
                print(f"   📊 Progress: {idx + 1}/{len(api_files)}")
            
            try:
                # Get detailed file info
                file_info = pikpak_get_file_info(file_id, account, tokens, captcha_func)
                
                if not file_info:
                    print(f"      ⚠️ Could not get info: {file_name[:30]}")
                    stats['errors'] += 1
                    continue
                
                # Extract magnet hash
                magnet_hash = extract_hash_from_file_info(file_info)
                
                if not magnet_hash:
                    print(f"      ⏭️ No hash found: {file_name[:30]}")
                    stats['skipped'] += 1
                    continue
                
                # Get file hash (separate from magnet hash)
                file_hash = file_info.get("hash") or file_info.get("md5")
                
                # Save to cache
                saved = save_to_smart_cache(
                    file_id=file_id,
                    account_id=account_id,
                    magnet_hash=magnet_hash,
                    file_name=file_name,
                    file_size=int(file.get('size', 0)),
                    file_hash=file_hash,
                    parent_id=file.get('parent_id')
                )
                
                if saved:
                    stats['synced'] += 1
                else:
                    stats['errors'] += 1
                
                # Rate limiting (avoid API throttling)
                time.sleep(random.uniform(1.0, 2.0))
                
            except Exception as e:
                print(f"      ❌ Error processing {file_name[:30]}: {e}")
                stats['errors'] += 1
                continue
        
        # Step 3: Find deleted files (in DB but not in API)
        db_file_ids = set(db.get_cached_files_by_account(account_id))
        deleted_file_ids = db_file_ids - api_file_ids
        
        if deleted_file_ids:
            db.mark_cache_as_trash(account_id, list(deleted_file_ids))
            stats['trashed'] = len(deleted_file_ids)
            print(f"   🗑️ Marked {len(deleted_file_ids)} deleted files as trash")
        
        print(f"   ✅ Done! Synced: {stats['synced']} | Skipped: {stats['skipped']} | Trashed: {stats['trashed']} | Errors: {stats['errors']}")
        
    except Exception as e:
        print(f"   ❌ Error syncing account {account_id}: {e}")
        stats['errors'] += 1
    
    return stats


def sync_all_accounts_to_cache(login_func: Callable, get_account_func: Callable = None, captcha_func: Callable = None) -> Dict:
    """
    Sync ALL accounts to smart cache.
    Call this on startup or after clear-trash.
    
    Args:
        login_func: Your pikpak_login function from app.py
        get_account_func: Not used, kept for compatibility
        captcha_func: Function to generate captcha token
        
    Returns:
        Dict with total stats
    """
    print("=" * 60)
    print("🔄 SMART CACHE: Starting Full Sync (With Hash Lookup)")
    print("=" * 60)
    
    total_stats = {
        'synced': 0,
        'skipped': 0,
        'trashed': 0,
        'errors': 0,
        'accounts': 0,
        'accounts_failed': 0
    }
    
    try:
        # Get all active accounts from DB (both servers)
        all_accounts = []
        all_accounts.extend(db.get_all_server_accounts(1))
        all_accounts.extend(db.get_all_server_accounts(2))
        
        print(f"📊 Found {len(all_accounts)} total accounts")
        
        for account in all_accounts:
            account_id = account.get('id')
            
            # Skip inactive accounts
            if account.get('status') != 'active':
                print(f"   ⏭️ Skipping inactive account {account_id}")
                continue
            
            # Ensure device_id is set
            account['device_id'] = account.get('current_device_id') or account.get('device_id')
            
            if not account['device_id']:
                print(f"   ⚠️ No device_id for account {account_id}, skipping")
                total_stats['accounts_failed'] += 1
                continue
            
            try:
                # Login to account
                print(f"\n🔐 Logging into Account {account_id}...")
                tokens = login_func(account)
                
                if not tokens or not tokens.get('access_token'):
                    print(f"   ❌ Login failed for account {account_id}")
                    total_stats['accounts_failed'] += 1
                    continue
                
                # Sync this account
                stats = sync_account_to_cache(
                    account=account,
                    tokens=tokens,
                    login_func=login_func,
                    captcha_func=captcha_func
                )
                
                total_stats['synced'] += stats['synced']
                total_stats['skipped'] += stats['skipped']
                total_stats['trashed'] += stats['trashed']
                total_stats['errors'] += stats['errors']
                total_stats['accounts'] += 1
                
                # Rate limiting between accounts
                wait_time = random.randint(5, 10)
                print(f"   ⏳ Waiting {wait_time}s before next account...")
                time.sleep(wait_time)
                
            except Exception as e:
                print(f"   ❌ Error with account {account_id}: {e}")
                total_stats['accounts_failed'] += 1
        
        print("\n" + "=" * 60)
        print(f"🎉 SMART CACHE: Sync Complete!")
        print(f"   📊 Accounts Synced: {total_stats['accounts']}")
        print(f"   ❌ Accounts Failed: {total_stats['accounts_failed']}")
        print(f"   ✅ Files Synced: {total_stats['synced']}")
        print(f"   ⏭️ Files Skipped: {total_stats['skipped']}")
        print(f"   🗑️ Files Trashed: {total_stats['trashed']}")
        print(f"   ⚠️ Errors: {total_stats['errors']}")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ SMART CACHE: Sync failed - {e}")
    
    return total_stats


def sync_single_account(account_id: int, login_func: Callable, captcha_func: Callable = None) -> Dict:
    """
    Sync a single account by ID.
    Useful for testing or targeted sync.
    
    Args:
        account_id: Account ID to sync
        login_func: Your pikpak_login function
        captcha_func: Function to generate captcha token
        
    Returns:
        Dict with stats
    """
    print(f"🔄 Syncing single account: {account_id}")
    
    # Get account from DB
    all_accounts = db.get_all_server_accounts(1) + db.get_all_server_accounts(2)
    account = next((a for a in all_accounts if a['id'] == account_id), None)
    
    if not account:
        print(f"❌ Account {account_id} not found")
        return {'error': 'Account not found'}
    
    account['device_id'] = account.get('current_device_id') or account.get('device_id')
    
    # Login
    tokens = login_func(account)
    if not tokens:
        print(f"❌ Login failed for account {account_id}")
        return {'error': 'Login failed'}
    
    # Sync
    return sync_account_to_cache(account, tokens, login_func, captcha_func)


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