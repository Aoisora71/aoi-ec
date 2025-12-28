# -*- coding: utf-8 -*-
"""
Rakuten Inventory Management API Client

This module provides the API client for managing inventory on Rakuten,
including inventory updates for product variants, along with utility functions
for configuration and a CLI script for updating inventory.
"""
import base64
import json
import argparse
import requests
from typing import Dict, Any, Optional
from pathlib import Path
import sys
import io

# Fix Windows console encoding issues (only for CLI script, not when imported as module)
# This should only run when the script is executed directly, not when imported
# Moving this to main() function to avoid interfering with logging when imported


class RakutenInventoryAPI:
    """Rakuten Inventory Management API Client"""
    
    INVENTORY_BASE_URL = "https://api.rms.rakuten.co.jp/es/2.1/inventories/manage-numbers"
    
    def __init__(self, service_secret: Optional[str] = None, license_key: Optional[str] = None):
        """
        Initialize the Rakuten Inventory API client
        
        Args:
            service_secret: Rakuten service secret (optional, will be loaded from server/rakuten_config.json if not provided)
            license_key: Rakuten license key (optional, will be loaded from server/rakuten_config.json if not provided)
        """
        # If credentials not provided, load from config file
        if service_secret is None or license_key is None:
            # Construct path to server/rakuten_config.json
            # This file is in server/modules/, so go up one level to server/
            module_dir = Path(__file__).parent
            server_dir = module_dir.parent
            config_file = server_dir / "rakuten_config.json"
            
            try:
                with open(config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    rakuten_config = config.get("rakuten", {})
                    
                    if service_secret is None:
                        service_secret = rakuten_config.get("service_secret")
                        if service_secret is None:
                            raise ValueError("service_secret not provided and not found in config file")
                    
                    if license_key is None:
                        license_key = rakuten_config.get("license_key")
                        if license_key is None:
                            raise ValueError("license_key not provided and not found in config file")
            except FileNotFoundError:
                raise FileNotFoundError(f"Config file not found: {config_file}. Please provide service_secret and license_key as parameters, or ensure the config file exists.")
        
        self.service_secret = service_secret
        self.license_key = license_key
        self.auth_header = self._create_auth_header()
    
    def _create_auth_header(self) -> str:
        """Create ESA Base64 encoded authorization header"""
        credentials = f"{self.service_secret}:{self.license_key}"
        # Encode to bytes, then Base64 encode, then decode to string
        encoded = base64.b64encode(credentials.encode('utf-8')).decode('ascii')
        return f"ESA {encoded}"
    
    def upsert_inventory(
        self, 
        manage_number: str, 
        variant_id: str,
        mode: str,
        quantity: int,
        operation_lead_time: Optional[Dict[str, Any]] = None,
        ship_from_ids: Optional[list] = None
    ) -> Dict[str, Any]:
        """
        Register or update inventory information for a product variant
        
        Args:
            manage_number: Product control number (max 32 characters)
            variant_id: SKU control number (max 32 characters)
            mode: Update mode - "ABSOLUTE" or "RELATIVE" (use "ABSOLUTE" for new registrations)
            quantity: Quantity in stock (max 99999)
            operation_lead_time: Optional dict with:
                - normalDeliveryTimeId: number (in-stock shipping lead time ID)
                - backOrderDeliveryTimeId: number (out-of-stock shipping lead time ID)
            ship_from_ids: Optional list of delivery lead time IDs
            
        Returns:
            Response dictionary
        """
        url = f"{self.INVENTORY_BASE_URL}/{manage_number}/variants/{variant_id}"
        
        headers = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }
        
        # Build request body
        body = {
            "mode": mode,
            "quantity": quantity
        }
        
        if operation_lead_time:
            body["operationLeadTime"] = operation_lead_time
        
        if ship_from_ids:
            body["shipFromIds"] = ship_from_ids
        
        try:
            response = requests.put(url, headers=headers, json=body, timeout=30)
            
            # Check status code - 204 No Content means success
            if response.status_code == 204:
                return {"success": True, "data": None, "message": "Inventory updated successfully"}
            
            # For error status codes, raise to be caught by exception handler
            response.raise_for_status()
            
            # If we get here, it's a 2xx but not 204
            try:
                return {"success": True, "data": response.json(), "message": "Request processed"}
            except (ValueError, json.JSONDecodeError):
                return {"success": True, "data": None, "message": "Request processed (no response body)"}
                
        except requests.exceptions.HTTPError as e:
            error_response = None
            error_text = None
            
            # Try to get JSON error response
            if hasattr(e, 'response') and e.response is not None:
                # Get raw text first
                try:
                    error_text = e.response.text
                except:
                    error_text = None
                
                # Try to parse as JSON
                if error_text:
                    try:
                        error_response = e.response.json()
                    except (ValueError, json.JSONDecodeError):
                        # If JSON parsing fails, store as raw text
                        error_response = {"raw_response": error_text, "note": "Response is not valid JSON"}
                else:
                    # Empty response
                    error_response = {"note": "Response body is empty"}
                
                # Get response headers
                response_headers = dict(e.response.headers)
            else:
                response_headers = None
            
            return {
                "success": False,
                "error": str(e),
                "status_code": e.response.status_code if hasattr(e, 'response') and e.response else None,
                "error_data": error_response,
                "error_text": error_text,
                "response_headers": response_headers,
                "url": url
            }
        except Exception as e:
            return {"success": False, "error": str(e)}


def register_inventory_from_product_management(item_number: str) -> Dict[str, Any]:
    """
    Register inventory to Rakuten using data from product_management table.
    
    Args:
        item_number: Product item_number from product_management table (used as manage_number)
        
    Returns:
        Response dictionary with success status and details
    """
    from .db import get_product_management_by_item_number
    
    # Get product data from database
    product_data = get_product_management_by_item_number(item_number)
    if not product_data:
        return {
            "success": False,
            "error": f"Product with item_number '{item_number}' not found in product_management table"
        }
    
    # Get inventory data
    inventory_data = product_data.get("inventory")
    if not inventory_data:
        return {
            "success": False,
            "error": f"No inventory data found for product '{item_number}'"
        }
    
    # Parse inventory if it's a string
    if isinstance(inventory_data, str):
        try:
            inventory_data = json.loads(inventory_data)
        except json.JSONDecodeError:
            return {
                "success": False,
                "error": f"Invalid inventory JSON format for product '{item_number}'"
            }
    
    # Validate inventory structure
    if not isinstance(inventory_data, dict):
        return {
            "success": False,
            "error": f"Inventory data must be a dictionary for product '{item_number}'"
        }
    
    manage_number = inventory_data.get("manage_number") or item_number
    variants = inventory_data.get("variants", [])
    
    if not variants or not isinstance(variants, list):
        return {
            "success": False,
            "error": f"No variants found in inventory data for product '{item_number}'"
        }
    
    # Create API client (will load credentials from config if not provided)
    api = RakutenInventoryAPI()
    
    # Register inventory for each variant
    results = []
    errors = []
    
    for variant in variants:
        if not isinstance(variant, dict):
            errors.append({"variant_id": "unknown", "error": "Invalid variant format"})
            continue
        
        variant_id = variant.get("variant_id")
        quantity = variant.get("quantity")
        mode = variant.get("mode", "ABSOLUTE")
        
        if not variant_id:
            errors.append({"variant_id": "unknown", "error": "Missing variant_id in variant data"})
            continue
        
        if quantity is None:
            errors.append({"variant_id": variant_id, "error": "Missing quantity"})
            continue
        
        # Register this variant's inventory
        result = api.upsert_inventory(
            manage_number=manage_number,
            variant_id=str(variant_id),
            mode=mode,
            quantity=int(quantity)
        )
        
        if result.get("success"):
            results.append({
                "variant_id": variant_id,
                "quantity": quantity,
                "status": "success"
            })
        else:
            error_msg = result.get("error", "Unknown error")
            errors.append({
                "variant_id": variant_id,
                "error": error_msg,
                "error_details": format_error_message(result) if result.get("error_data") else None
            })
    
    # Return summary
    if errors and not results:
        # All failed
        return {
            "success": False,
            "error": f"Failed to register inventory for all {len(variants)} variants",
            "errors": errors,
            "registered_count": 0,
            "total_count": len(variants)
        }
    elif errors:
        # Some failed
        return {
            "success": True,
            "message": f"Registered inventory for {len(results)}/{len(variants)} variants",
            "registered_count": len(results),
            "failed_count": len(errors),
            "total_count": len(variants),
            "results": results,
            "errors": errors
        }
    else:
        # All succeeded
        return {
            "success": True,
            "message": f"Successfully registered inventory for all {len(results)} variants",
            "registered_count": len(results),
            "total_count": len(variants),
            "results": results
        }


# ============================================================================
# Utility Functions
# ============================================================================

def load_config(config_file: str = "config.json") -> Dict[str, Any]:
    """
    Load configuration from JSON file
    
    Args:
        config_file: Path to configuration file (default: config.json)
        
    Returns:
        Configuration dictionary
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file is invalid JSON
    """
    with open(config_file, 'r', encoding='utf-8') as f:
        return json.load(f)


def create_inventory_api_from_config(config_file: str = "config.json") -> RakutenInventoryAPI:
    """
    Create a RakutenInventoryAPI instance from configuration file
    
    Args:
        config_file: Path to configuration file (default: config.json)
        
    Returns:
        Initialized RakutenInventoryAPI instance
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        KeyError: If required configuration keys are missing
    """
    config = load_config(config_file)
    credentials = config["rakuten"]
    
    return RakutenInventoryAPI(
        service_secret=credentials["service_secret"],
        license_key=credentials["license_key"]
    )


def format_error_message(result: Dict[str, Any]) -> str:
    """
    Format API error response into a readable message
    
    Args:
        result: API response dictionary (with success=False)
        
    Returns:
        Formatted error message string
    """
    lines = []
    lines.append(f"Error: {result.get('error', 'Unknown error')}")
    
    if result.get("status_code"):
        lines.append(f"Status Code: {result['status_code']}")
    
    if result.get("error_data"):
        if isinstance(result["error_data"], dict):
            if "errors" in result["error_data"]:
                for error in result["error_data"]["errors"]:
                    code = error.get("code", "UNKNOWN")
                    message = error.get("message", "No message")
                    lines.append(f"  [{code}] {message}")
            else:
                lines.append(f"Error Data: {json.dumps(result['error_data'], indent=2, ensure_ascii=False)}")
        else:
            lines.append(f"Error Data: {result['error_data']}")
        
    if result.get("url"):
        lines.append(f"URL: {result['url']}")

    return "\n".join(lines)


# ============================================================================
# CLI Script
# ============================================================================

def main():
    """Main execution function for CLI script"""
    # Fix Windows console encoding issues (only for CLI, not when imported)
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    
    parser = argparse.ArgumentParser(
        description='Update inventory for Rakuten product variant',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Basic usage - set absolute quantity
  python -m modules.rakuten_inventory torimesi sku-1 100
  
  # With delivery lead times
  python -m modules.rakuten_inventory torimesi sku-1 100 --normal-delivery-time 4 --back-order-delivery-time 5
  
  # With ship from IDs
  python -m modules.rakuten_inventory torimesi sku-1 100 --ship-from-ids 3 4 5
  
  # Relative update (add/subtract from current quantity)
  python -m modules.rakuten_inventory torimesi sku-1 10 --mode RELATIVE
        """
    )
    
    parser.add_argument('manage_number', help='Product control number (max 32 characters)')
    parser.add_argument('variant_id', help='SKU control number (max 32 characters)')
    parser.add_argument('quantity', type=int, help='Quantity in stock (max 99999)')
    parser.add_argument('--mode', choices=['ABSOLUTE', 'RELATIVE'], default='ABSOLUTE',
                       help='Update mode: ABSOLUTE (set value) or RELATIVE (add/subtract)')
    parser.add_argument('--normal-delivery-time', type=int,
                       help='In-stock shipping lead time ID')
    parser.add_argument('--back-order-delivery-time', type=int,
                       help='Out-of-stock shipping lead time ID')
    parser.add_argument('--ship-from-ids', type=int, nargs='+',
                       help='List of delivery lead time IDs (max 1 element, space-separated)')
    
    args = parser.parse_args()
    
    # Validate quantity
    if args.quantity < 0 or args.quantity > 99999:
        print("Error: Quantity must be between 0 and 99999")
        sys.exit(1)
    
    try:
        # Create API client from config
        api = create_inventory_api_from_config()
        
        # Build operation lead time dict if provided
        operation_lead_time = None
        if args.normal_delivery_time or args.back_order_delivery_time:
            operation_lead_time = {}
            if args.normal_delivery_time:
                operation_lead_time["normalDeliveryTimeId"] = args.normal_delivery_time
            if args.back_order_delivery_time:
                operation_lead_time["backOrderDeliveryTimeId"] = args.back_order_delivery_time
        
        print(f"\nUpdating inventory for product: {args.manage_number}, variant: {args.variant_id}")
        print("-" * 80)
        print(f"Mode: {args.mode}")
        print(f"Quantity: {args.quantity}")
        if operation_lead_time:
            print(f"Operation Lead Time: {operation_lead_time}")
        if args.ship_from_ids:
            print(f"Ship From IDs: {args.ship_from_ids}")
        print("-" * 80)
        
        # Upsert inventory
        result = api.upsert_inventory(
            manage_number=args.manage_number,
            variant_id=args.variant_id,
            mode=args.mode,
            quantity=args.quantity,
            operation_lead_time=operation_lead_time,
            ship_from_ids=args.ship_from_ids
        )
        
        # Print result
        if result["success"]:
            print("✓ Success!")
            print(f"Message: {result['message']}")
            print("\n" + "=" * 80)
            print("🎉 INVENTORY SUCCESSFULLY UPDATED! 🎉")
            print("=" * 80)
        else:
            print("✗ Failed!")
            print(format_error_message(result))
            sys.exit(1)
            
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except KeyError as e:
        print(f"Error: Missing configuration key: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

