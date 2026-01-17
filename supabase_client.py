import os
from datetime import datetime, timezone
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import Optional, Dict, List

# Load environment variables
load_dotenv()

class SupabaseDB:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")
        
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in .env")
        
        self.client: Client = create_client(url, key)

    def get_best_account(self, target_server_id: int, exclude_ids: list = None) -> Optional[Dict]:
        """
        Fetches best account, skipping specific IDs if provided.
        """
        if exclude_ids is None:
            exclude_ids = []
            
        try:
            # Fetch TOP 5 candidates (Active, Quota < 5, Server Match)
            # Ordered by: Least Used -> Oldest Used
            response = self.client.table('accounts')\
                .select('*')\
                .eq('server_id', target_server_id)\
                .eq('status', 'active')\
                .lt('quota_used', 5)\
                .order('quota_used', desc=False)\
                .order('last_used_at', desc=False)\
                .limit(5)\
                .execute()
            
            candidates = response.data or []
            
            # Python-side filtering (Pick first one NOT in exclude_ids)
            for acc in candidates:
                if acc['id'] not in exclude_ids:
                    return acc
            
            return None
            
        except Exception as e:
            print(f"❌ DB Error (get_best_account): {e}")
            return None

    def increment_quota(self, account_id: int) -> bool:
        """
        Call this AFTER a successful download to update quota usage.
        """
        try:
            # First fetch current usage
            current = self.client.table('accounts').select('quota_used').eq('id', account_id).execute()
            if current.data:
                new_val = current.data[0]['quota_used'] + 1
                
                # Update usage and timestamp
                self.client.table('accounts').update({
                    'quota_used': new_val,
                    'last_used_at': datetime.now(timezone.utc).isoformat(),
                    'updated_at': datetime.now(timezone.utc).isoformat()
                }).eq('id', account_id).execute()
                print(f"✅ Quota updated for Account ID {account_id}: {new_val}/5")
                return True
            return False
        except Exception as e:
            print(f"❌ DB Error (increment_quota): {e}")
            return False

    def rotate_device(self, account_id: int) -> Optional[str]:
        """
        Call this ONLY if PikPak returns a Device ID error.
        It auto-flags the old ID and assigns a fresh one.
        """
        try:
            # Calls our SQL function
            response = self.client.rpc('rotate_account_device', {'target_account_id': account_id}).execute()
            new_id = response.data
            print(f"🔄 Device Rotated! New ID: {new_id}")
            return new_id
        except Exception as e:
            print(f"❌ DB Error (rotate_device): {e}")
            return None

    def get_all_server_accounts(self, server_id: int):
        """Fetch ALL accounts for this server (for Admin Dashboard)"""
        try:
            response = self.client.table('accounts')\
                .select('*')\
                .eq('server_id', server_id)\
                .order('id', desc=False)\
                .execute()
            return response.data or []
        except Exception as e:
            print(f"❌ DB Error (get_all_server_accounts): {e}")
            return []

    def reset_account_quota(self, account_id: int):
        """Reset quota for specific account"""
        try:
            self.client.table('accounts').update({
                'quota_used': 0,
                'last_used_at': datetime.now(timezone.utc).isoformat()
            }).eq('id', account_id).execute()
            return True
        except Exception as e:
            print(f"❌ DB Error (reset_account_quota): {e}")
            return False

    def sync_quota(self, account_id: int, usage: int):
        """Sync local DB quota with real PikPak usage"""
        try:
            self.client.table('accounts').update({
                'quota_used': usage,
                'last_used_at': datetime.now(timezone.utc).isoformat()
            }).eq('id', account_id).execute()
        except Exception as e:
            print(f"❌ DB Error (sync_quota): {e}")

    def update_storage_stats(self, account_id: int, used_bytes: int, limit_bytes: int):
        """
        Updates the disk space columns in Supabase.
        """
        try:
            # Avoid division by zero
            percent = 0.0
            if limit_bytes > 0:
                percent = round((used_bytes / limit_bytes) * 100, 2)

            self.client.table('accounts').update({
                'storage_used_bytes': used_bytes,
                'storage_limit_bytes': limit_bytes,
                'storage_percent': percent,
                'updated_at': datetime.now(timezone.utc).isoformat()
            }).eq('id', account_id).execute()
            
            print(f"📊 Storage Synced for Account {account_id}: {percent}% Full")
            
        except Exception as e:
            print(f"❌ DB Error (update_storage_stats): {e}")

    def sync_account_stats(self, account_id: int, download_usage: int, storage_used: int, storage_limit: int):
        """
        Syncs both download quota AND storage stats in one DB call.
        """
        try:
            percent = 0.0
            if storage_limit > 0:
                percent = round((storage_used / storage_limit) * 100, 2)

            self.client.table('accounts').update({
                'quota_used': download_usage,
                'storage_used_bytes': storage_used,
                'storage_limit_bytes': storage_limit,
                'storage_percent': percent,
                'last_used_at': datetime.now(timezone.utc).isoformat(),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }).eq('id', account_id).execute()
            
            print(f"✅ Synced ALL stats for Account {account_id}: Q={download_usage}, S={percent}%")
            return True
            
        except Exception as e:
            print(f"❌ DB Error (sync_account_stats): {e}")
            return False

    # ============================================
    # GOFILE UPLOAD METHODS
    # ============================================

    def add_gofile_upload(self, data: Dict) -> Optional[Dict]:
        """
        Insert a new Gofile upload record.
        
        Required in data: file_id, server
        Optional: folder_id, folder_code, file_name, file_size, 
                  movie_name, quality, direct_link, pikpak_file_id
        """
        try:
            response = self.client.table('gofile_uploads').insert({
                'file_id': data['file_id'],
                'server': data['server'],
                'folder_id': data.get('folder_id'),
                'folder_code': data.get('folder_code'),
                'file_name': data.get('file_name'),
                'file_size': data.get('file_size'),
                'movie_name': data.get('movie_name'),
                'quality': data.get('quality'),
                'direct_link': data.get('direct_link'),
                'pikpak_file_id': data.get('pikpak_file_id')
            }).execute()
            
            print(f"✅ Gofile recorded: {data.get('file_name', data['file_id'])}")
            return response.data[0] if response.data else None
            
        except Exception as e:
            print(f"❌ DB Error (add_gofile_upload): {e}")
            return None

    def get_active_gofile_uploads(self) -> list:
        """
        Get all active Gofile uploads.
        """
        try:
            response = self.client.table('gofile_uploads')\
                .select('*')\
                .eq('status', 'active')\
                .order('created_at', desc=True)\
                .execute()
            
            return response.data or []
            
        except Exception as e:
            print(f"❌ DB Error (get_active_gofile_uploads): {e}")
            return []

    def update_gofile_keep_alive(self, file_id: str, status: str = None, server: str = None) -> bool:
        """
        Update Gofile record: timestamp always updated, status/server optional.
        
        Args:
            file_id: Gofile content ID
            status: Optional - 'active', 'expired', 'deleted'
            server: Optional - new server if migrated
        """
        try:
            update_data = {
                'last_keep_alive': datetime.now(timezone.utc).isoformat()
            }
            
            if status is not None:
                update_data['status'] = status
            
            if server is not None:
                update_data['server'] = server
            
            self.client.table('gofile_uploads')\
                .update(update_data)\
                .eq('file_id', file_id)\
                .execute()
            
            print(f"🔄 Gofile updated: {file_id}")
            return True
            
        except Exception as e:
            print(f"❌ DB Error (update_gofile_keep_alive): {e}")
            return False

    def mark_gofile_upload_as_expired(self, file_id: str) -> bool:
        """
        Marks a Gofile upload as 'expired'.
        """
        try:
            self.client.table('gofile_uploads').update({
                'status': 'expired',
                'updated_at': datetime.now(timezone.utc).isoformat()
            }).eq('file_id', file_id).execute()
            
            print(f"🔄 Gofile status set to EXPIRED for: {file_id}")
            return True
            
        except Exception as e:
            print(f"❌ DB Error (mark_gofile_upload_as_expired): {e}")
            return False

    def get_gofile_by_file_id(self, file_id: str) -> Optional[Dict]:
        """
        Get single Gofile record by file_id.
        """
        try:
            response = self.client.table('gofile_uploads')\
                .select('*')\
                .eq('file_id', file_id)\
                .limit(1)\
                .execute()
            
            return response.data[0] if response.data else None
            
        except Exception as e:
            print(f"❌ DB Error (get_gofile_by_file_id): {e}")
            return None

    # ============================================
    # SMART CACHE METHODS (PikPak Deduplication)
    # ============================================

    def check_smart_cache(self, magnet_hash: str) -> Optional[Dict]:
        """
        Check if file exists in any account's cache.
        
        Args:
            magnet_hash: The info_hash extracted from magnet link
            
        Returns:
            Dict with account_id, file_id, file_name if found, else None
        """
        try:
            response = self.client.table('pikpak_files')\
                .select('*')\
                .eq('magnet_hash', magnet_hash)\
                .eq('is_trash', False)\
                .limit(1)\
                .execute()
            
            if response.data and len(response.data) > 0:
                print(f"✅ Smart Cache HIT: {response.data[0].get('file_name', 'Unknown')}")
                return response.data[0]
            
            return None
            
        except Exception as e:
            print(f"❌ DB Error (check_smart_cache): {e}")
            return None

    def save_to_smart_cache(self, data: Dict) -> Optional[Dict]:
        """
        Save file to smart cache after successful download.
        Uses UPSERT to handle duplicates gracefully.
        
        Required in data: magnet_hash, file_id, account_id
        Optional: file_hash, file_name, file_size, parent_id
        """
        try:
            record = {
                'magnet_hash': data['magnet_hash'],
                'file_id': data['file_id'],
                'account_id': data['account_id'],
                'file_hash': data.get('file_hash'),
                'file_name': data.get('file_name'),
                'file_size': data.get('file_size'),
                'parent_id': data.get('parent_id'),
                'is_trash': False,
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            response = self.client.table('pikpak_files')\
                .upsert(record, on_conflict='magnet_hash,account_id,file_id')\
                .execute()
            
            print(f"💾 Smart Cache saved: {data.get('file_name', data['file_id'])}")
            return response.data[0] if response.data else None
            
        except Exception as e:
            print(f"❌ DB Error (save_to_smart_cache): {e}")
            return None

    def mark_cache_as_trash(self, account_id: int, file_ids: List[str]) -> bool:
        """
        Mark files as trashed (soft delete) in smart cache.
        Called when files are deleted from PikPak.
        
        Args:
            account_id: The account that had these files
            file_ids: List of PikPak file IDs to mark as trash
        """
        if not file_ids:
            return True
            
        try:
            self.client.table('pikpak_files')\
                .update({
                    'is_trash': True,
                    'updated_at': datetime.now(timezone.utc).isoformat()
                })\
                .eq('account_id', account_id)\
                .in_('file_id', file_ids)\
                .execute()
            
            print(f"🗑️ Smart Cache: Marked {len(file_ids)} files as trash for Account {account_id}")
            return True
            
        except Exception as e:
            print(f"❌ DB Error (mark_cache_as_trash): {e}")
            return False

    def get_cached_files_by_account(self, account_id: int) -> List[str]:
        """
        Get all cached file_ids for an account.
        Used during sync to detect deleted files.
        
        Args:
            account_id: The account to query
            
        Returns:
            List of file_id strings
        """
        try:
            response = self.client.table('pikpak_files')\
                .select('file_id')\
                .eq('account_id', account_id)\
                .eq('is_trash', False)\
                .execute()
            
            return [row['file_id'] for row in (response.data or [])]
            
        except Exception as e:
            print(f"❌ DB Error (get_cached_files_by_account): {e}")
            return []

    def bulk_upsert_cache(self, files: List[Dict]) -> bool:
        """
        Bulk upsert multiple files at once.
        More efficient for sync operations.
        
        Args:
            files: List of dicts with magnet_hash, file_id, account_id, etc.
        """
        if not files:
            return True
            
        try:
            # Add timestamp to all records
            for f in files:
                f['updated_at'] = datetime.now(timezone.utc).isoformat()
                if 'is_trash' not in f:
                    f['is_trash'] = False
            
            self.client.table('pikpak_files')\
                .upsert(files, on_conflict='magnet_hash,account_id,file_id')\
                .execute()
            
            print(f"💾 Smart Cache: Bulk upserted {len(files)} files")
            return True
            
        except Exception as e:
            print(f"❌ DB Error (bulk_upsert_cache): {e}")
            return False

    def get_smart_cache_stats(self) -> Dict:
        """
        Get statistics about the smart cache.
        Useful for admin dashboard.
        """
        try:
            # Total cached files
            total = self.client.table('pikpak_files')\
                .select('id', count='exact')\
                .eq('is_trash', False)\
                .execute()
            
            # Trashed files
            trashed = self.client.table('pikpak_files')\
                .select('id', count='exact')\
                .eq('is_trash', True)\
                .execute()
            
            return {
                'active_files': total.count or 0,
                'trashed_files': trashed.count or 0,
                'total_files': (total.count or 0) + (trashed.count or 0)
            }
            
        except Exception as e:
            print(f"❌ DB Error (get_smart_cache_stats): {e}")
            return {'active_files': 0, 'trashed_files': 0, 'total_files': 0}

    def clear_trash_from_cache(self, account_id: int = None) -> int:
        """
        Permanently delete trashed entries from cache.
        
        Args:
            account_id: Optional - only clear for specific account
            
        Returns:
            Number of records deleted
        """
        try:
            query = self.client.table('pikpak_files')\
                .delete()\
                .eq('is_trash', True)
            
            if account_id:
                query = query.eq('account_id', account_id)
            
            response = query.execute()
            count = len(response.data) if response.data else 0
            
            print(f"🧹 Smart Cache: Permanently deleted {count} trashed entries")
            return count
            
        except Exception as e:
            print(f"❌ DB Error (clear_trash_from_cache): {e}")
            return 0

# Singleton instance
db = SupabaseDB()