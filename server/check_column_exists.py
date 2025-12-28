#!/usr/bin/env python3
"""
Quick script to check if rakuten_registration_status column exists in product_management table.
"""

import sys
import os

# Add server directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.db import _get_dsn
import psycopg2

def main():
    """Check if the column exists."""
    print("🔍 Checking if 'rakuten_registration_status' column exists...")
    
    try:
        dsn = _get_dsn()
        if not dsn:
            print("❌ Database connection string not found. Please check your environment variables.")
            return False
        
        with psycopg2.connect(dsn) as conn:
            with conn.cursor() as cur:
                # Check if column exists
                cur.execute("""
                    SELECT column_name, data_type, is_nullable
                    FROM information_schema.columns 
                    WHERE table_name='product_management' 
                    AND column_name='rakuten_registration_status'
                """)
                result = cur.fetchone()
                
                if result:
                    column_name, data_type, is_nullable = result
                    print(f"✅ Column exists!")
                    print(f"   Name: {column_name}")
                    print(f"   Type: {data_type}")
                    print(f"   Nullable: {is_nullable}")
                    
                    # Check current values
                    cur.execute("""
                        SELECT 
                            COUNT(*) as total,
                            COUNT(rakuten_registration_status) FILTER (WHERE rakuten_registration_status = 'true') as success_count,
                            COUNT(rakuten_registration_status) FILTER (WHERE rakuten_registration_status = 'false') as failed_count,
                            COUNT(rakuten_registration_status) FILTER (WHERE rakuten_registration_status IS NULL) as unregistered_count
                        FROM product_management
                    """)
                    stats = cur.fetchone()
                    if stats:
                        total, success, failed, unregistered = stats
                        print(f"\n📊 Current status distribution:")
                        print(f"   Total products: {total}")
                        print(f"   Success: {success}")
                        print(f"   Failed: {failed}")
                        print(f"   Unregistered: {unregistered}")
                    
                    return True
                else:
                    print("❌ Column does NOT exist!")
                    print("   Please run the migration to add the column:")
                    print("   python server/migrate_add_registration_status.py")
                    return False
                    
    except Exception as e:
        print(f"❌ Error checking column: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

