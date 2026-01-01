import os
from datetime import datetime, timezone
from supabase import create_client, Client
from dotenv import load_dotenv
from typing import Optional, Dict

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

# Singleton instance
db = SupabaseDB()