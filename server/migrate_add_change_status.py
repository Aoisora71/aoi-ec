#!/usr/bin/env python3
"""
Migration script to add change_status column to product_management table.
Run this script once to update your database schema.
"""

import sys
import os

# Add server directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from modules.db import fix_product_management_schema, get_db_connection_context
from modules.db import _get_dsn
import psycopg2

def main():
    """Run the migration to add change_status column."""
    print("Starting migration to add change_status column...")
    
    try:
        # Ensure the schema is fixed (this includes adding the new column)
        fix_product_management_schema()
        print("Migration completed successfully!")
        print("   The 'change_status' column has been added to the product_management table.")
        print("   This column can be used to track modification status (e.g., 'modified', 'pending', 'approved')")
        
        # Verify the column exists
        dsn = _get_dsn()
        if dsn:
            with get_db_connection_context(dsn=dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT column_name, data_type 
                        FROM information_schema.columns 
                        WHERE table_name='product_management' 
                        AND column_name='change_status'
                    """)
                    result = cur.fetchone()
                    if result:
                        print(f"Verified: Column '{result[0]}' exists with type '{result[1]}'")
                    else:
                        print("Warning: Column not found after migration. Please check manually.")
        
    except Exception as e:
        print(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

