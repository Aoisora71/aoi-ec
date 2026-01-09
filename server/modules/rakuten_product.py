# -*- coding: utf-8 -*-
"""
Rakuten Product Management API Client

This module provides the API client for managing products on Rakuten,
including product registration and deletion, along with utility functions
for configuration, validation, and data handling.
"""
import base64
import json
import os
import random
import requests
import re
import unicodedata
import logging
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
import sys
import io

# Configure logging
logger = logging.getLogger(__name__)

# Import translation and filter functions from deepl_trans
from .deepl_trans import (
    clean_text_for_rakuten,
    _translate_variant_value_with_context,
    _is_color_key,
    _is_size_key,
    _extract_size_from_text,
    _shorten_selector_value,
    _trim_text_to_byte_limit,
    _clean_selector_text,
)

# Constants
RAKUTEN_SELECTOR_VALUE_LIMIT = 40  # Maximum 40 values per variant selector

# Fix Windows console encoding issues (only for CLI script, not when imported as module)
# This should only run when the script is executed directly, not when imported
# Moving this to main() function to avoid interfering with logging when imported


class RakutenProductAPI:
    """Rakuten Product Registration API Client"""
    
    BASE_URL = "https://api.rms.rakuten.co.jp/es/2.0/items/manage-numbers"
    
    def __init__(self, service_secret: Optional[str] = None, license_key: Optional[str] = None):
        """
        Initialize the Rakuten API client
        
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
    
    def register_product(self, manage_number: str, product_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Register or update a product on Rakuten
        
        Args:
            manage_number: Product control number (max 32 characters)
            product_data: Product data dictionary in Rakuten API format
            
        Returns:
            Response dictionary with success status and details
        """
        url = f"{self.BASE_URL}/{manage_number}"
        
        headers = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }
        
        try:
            response = requests.put(url, headers=headers, json=product_data, timeout=30)
            
            # Check status code - 204 No Content means success
            if response.status_code == 204:
                return {"success": True, "data": None, "message": "Product registered successfully"}
            
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

    def update_product_price(self, manage_number: str, variants: Dict[str, Dict[str, Any]], genre_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Partially update product price on Rakuten using PATCH endpoint.
        Only updates the standardPrice field in variants.
        
        Args:
            manage_number: Product control number (max 32 characters)
            variants: Dictionary of variants with only standardPrice field
                     Format: {variantId: {"standardPrice": "price"}}
            genre_id: Optional existing genreId to include (may be required by Rakuten even for PATCH)
            
        Returns:
            Response dictionary with success status and details
        """
        url = f"{self.BASE_URL}/{manage_number}"
        
        headers = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }
        
        # Prepare PATCH payload with only variants and standardPrice
        # Include genreId if provided (to satisfy Rakuten validation without changing it)
        patch_data = {
            "variants": variants
        }
        if genre_id:
            patch_data["genreId"] = str(genre_id)
        
        try:
            response = requests.patch(url, headers=headers, json=patch_data, timeout=30)
            
            # Check status code - 204 No Content means success
            if response.status_code == 204:
                return {"success": True, "data": None, "message": "Product price updated successfully"}
            
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

    def delete_product(self, manage_number: str) -> Dict[str, Any]:
        """
        Delete a product from Rakuten
        
        Args:
            manage_number: Product control number
            
        Returns:
            Response dictionary with success status and details
        """
        url = f"{self.BASE_URL}/{manage_number}"
        
        headers = {
            "Authorization": self.auth_header,
        }
        
        try:
            response = requests.delete(url, headers=headers, timeout=30)
            
            # Check status code - 204 No Content means success
            if response.status_code == 204:
                return {"success": True, "data": None, "message": "Product deleted successfully"}
            
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

    def get_product(self, manage_number: str) -> Dict[str, Any]:
        """
        Get product information from Rakuten
        
        Args:
            manage_number: Product control number (max 32 characters)
            
        Returns:
            Response dictionary with success status and product data
        """
        url = f"{self.BASE_URL}/{manage_number}"
        
        headers = {
            "Authorization": self.auth_header,
        }
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            # Check status code - 200 OK means success
            if response.status_code == 200:
                try:
                    product_data = response.json()
                    return {
                        "success": True,
                        "data": product_data,
                        "message": "Product retrieved successfully"
                    }
                except (ValueError, json.JSONDecodeError) as e:
                    return {
                        "success": False,
                        "error": f"Failed to parse JSON response: {str(e)}",
                        "status_code": response.status_code,
                        "error_text": response.text
                    }
            
            # For error status codes, raise to be caught by exception handler
            response.raise_for_status()
            
            # If we get here, it's a 2xx but not 200
            try:
                return {"success": True, "data": response.json(), "message": "Request processed"}
            except (ValueError, json.JSONDecodeError):
                return {"success": True, "data": None, "message": "Request processed (no response body)"}
                
        except requests.exceptions.Timeout as e:
            logger.error(f"Timeout error while getting product {manage_number} from Rakuten: {str(e)}")
            return {
                "success": False,
                "error": "Request timeout: Rakuten API took too long to respond",
                "status_code": None,
                "error_data": {"note": "Request timed out after 30 seconds"},
                "url": url
            }
        except requests.exceptions.ConnectionError as e:
            logger.error(f"Connection error while getting product {manage_number} from Rakuten: {str(e)}")
            return {
                "success": False,
                "error": f"Connection error: {str(e)}",
                "status_code": None,
                "error_data": {"note": "Failed to connect to Rakuten API"},
                "url": url
            }
        except requests.exceptions.HTTPError as e:
            error_response = None
            error_text = None
            
            # Try to get JSON error response
            if hasattr(e, 'response') and e.response is not None:
                status_code = e.response.status_code
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
                status_code = None
                response_headers = None
            
            return {
                "success": False,
                "error": str(e),
                "status_code": status_code,
                "error_data": error_response,
                "error_text": error_text,
                "response_headers": response_headers,
                "url": url
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    def map_category(self, manage_number: str, category_ids: List[str], main_plural_category_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Map Rakuten category IDs to a product.
    
    Args:
            manage_number: Product control number (max 32 characters)
            category_ids: List of Rakuten category IDs (1-5 items, as strings)
            main_plural_category_id: Main plural category ID (required if categoryIds contains "1ページ複数商品形式" category)
        
    Returns:
            Response dictionary with success status and details
        """
        # Validate category_ids
        if not category_ids or len(category_ids) == 0:
            return {
                "success": False,
                "error": "At least one category ID is required"
            }
        
        if len(category_ids) > 5:
            return {
                "success": False,
                "error": f"Maximum 5 category IDs allowed, got {len(category_ids)}"
            }
        
        # Remove duplicates
        unique_category_ids = list(dict.fromkeys(category_ids))  # Preserves order
        if len(unique_category_ids) != len(category_ids):
            logger.warning(f"Duplicate category IDs removed: {category_ids} -> {unique_category_ids}")
        
        url = f"https://api.rms.rakuten.co.jp/es/2.0/categories/item-mappings/manage-numbers/{manage_number}"
        
        headers = {
            "Authorization": self.auth_header,
            "Content-Type": "application/json"
        }
        
        # Build request body
        request_body: Dict[str, Any] = {
            "categoryIds": unique_category_ids
        }
        
        # Add mainPluralCategoryId if provided
        if main_plural_category_id:
            request_body["mainPluralCategoryId"] = main_plural_category_id
        
        try:
            response = requests.put(url, headers=headers, json=request_body, timeout=30)
            
            # Check status code - 204 No Content means success
            if response.status_code == 204:
                logger.info(f"✅ Category mapping successful for product {manage_number}: {unique_category_ids}")
                return {
                    "success": True,
                    "data": None,
                    "message": f"Category mapping successful: {unique_category_ids}"
                }
            
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
            
            logger.error(f"❌ Category mapping failed for product {manage_number}: {error_response}")
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
            logger.error(f"❌ Category mapping exception for product {manage_number}: {e}")
            return {"success": False, "error": str(e)}


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
                    metadata = error.get("metadata", {})
                    property_path = metadata.get("propertyPath", "")
                    if property_path:
                        lines.append(f"  [{code}] {message} (at {property_path})")
                    else:
                        lines.append(f"  [{code}] {message}")
            else:
                lines.append(f"Error Data: {json.dumps(result['error_data'], indent=2, ensure_ascii=False)}")
        else:
            lines.append(f"Error Data: {result['error_data']}")
        
    if result.get("url"):
        lines.append(f"URL: {result['url']}")

    return "\n".join(lines)


def register_product_from_product_management(item_number: str) -> Dict[str, Any]:
    """
    Register a product to Rakuten using data from product_management table.
    Also maps the product to Rakuten category IDs if r_cat_id is set.
    
    For blocked products (block = true), only updates price information using PATCH endpoint.
    For non-blocked products, uses PUT endpoint to update all product information.
    
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
    
    # Check if product is blocked
    block = product_data.get("block")
    if isinstance(block, str):
        is_blocked = block.lower() == "t" or block.lower() == "true"
    elif isinstance(block, bool):
        is_blocked = block
    else:
        is_blocked = False
    
    # Create API client (will load credentials from config if not provided)
    api = RakutenProductAPI()
    
    # If blocked, always use PATCH endpoint to update price only
    if is_blocked:
        logger.info(f"⚠️  Product {item_number} is blocked - using PATCH to update price only")
        
        # Extract variants with only standardPrice
        variants = product_data.get("variants")
        if not variants:
            return {
                "success": False,
                "error": f"No variants found for blocked product '{item_number}'"
            }
        
        # Parse variants if it's a string
        if isinstance(variants, str):
            try:
                variants = json.loads(variants)
            except (ValueError, json.JSONDecodeError):
                return {
                    "success": False,
                    "error": f"Invalid variants JSON format for product '{item_number}'"
                }
        
        # Filter variants to only include standardPrice
        price_only_variants = {}
        if isinstance(variants, dict):
            for sku_id, variant_data in variants.items():
                if isinstance(variant_data, dict) and "standardPrice" in variant_data:
                    standard_price = variant_data["standardPrice"]
                    # Ensure standardPrice is an integer (Rakuten API requires integer)
                    try:
                        if isinstance(standard_price, float):
                            price_int = int(standard_price)
                        elif isinstance(standard_price, str):
                            cleaned_price = standard_price.split('.')[0]
                            price_int = int(cleaned_price) if cleaned_price else 0
                        elif isinstance(standard_price, int):
                            price_int = standard_price
                        else:
                            price_int = int(float(standard_price))
                        
                        # Only include variants with valid price
                        if price_int >= 0:
                            # Include selectorValues for variant identification if available
                            variant_update = {"standardPrice": str(price_int)}
                            if "selectorValues" in variant_data:
                                variant_update["selectorValues"] = variant_data["selectorValues"]
                            
                            price_only_variants[sku_id] = variant_update
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to convert standardPrice to integer for variant {sku_id}: {standard_price} - {e}")
                        continue
        
        if not price_only_variants:
            return {
                "success": False,
                "error": f"No valid price data found for blocked product '{item_number}'"
            }
        
        # Include existing genreId if available (Rakuten may require it even for PATCH)
        # We include it without changing it, just to satisfy validation
        genre_id = product_data.get("genre_id")
        if genre_id:
            logger.info(f"📤 Sending PATCH request to update price for blocked product {item_number} with {len(price_only_variants)} variant(s) (including existing genreId)")
        else:
            logger.info(f"📤 Sending PATCH request to update price for blocked product {item_number} with {len(price_only_variants)} variant(s)")
        
        result = api.update_product_price(item_number, price_only_variants, genre_id=str(genre_id) if genre_id else None)
        
        # For blocked products, skip category mapping (only updating price)
        if result.get("success"):
            logger.info(f"✅ Price updated successfully for blocked product {item_number}")
        else:
            logger.error(f"❌ Failed to update price for blocked product {item_number}: {result.get('error')}")
        
        return result
    
    # For non-blocked products, use PUT endpoint (full update)
    # Convert to Rakuten API format
    rakuten_json = convert_product_management_to_rakuten_json(product_data)
    
    # Register product to Rakuten
    result = api.register_product(item_number, rakuten_json)
    
    # If product registration was successful, map categories using r_cat_id
    if result.get("success"):
        r_cat_id = product_data.get("r_cat_id")
        category_ids: list[str] = []

        if isinstance(r_cat_id, (list, tuple)):
            # JSON array from DB: normalise each element to string
            category_ids = [
                str(cat_id).strip()
                for cat_id in r_cat_id
                if cat_id is not None and str(cat_id).strip()
            ]
        elif isinstance(r_cat_id, str):
            if r_cat_id.strip():
                # Parse r_cat_id - it can be a single ID or comma-separated list
                category_ids_str = r_cat_id.strip()
                
                # Try JSON array first
                parsed: list[str] = []
                try:
                    loaded = json.loads(category_ids_str)
                    if isinstance(loaded, list):
                        parsed = [
                            str(cat_id).strip()
                            for cat_id in loaded
                            if cat_id is not None and str(cat_id).strip()
                        ]
                except Exception:
                    parsed = []

                if not parsed:
                    # Fallback: split by comma and clean up
                    parsed = [
                        cat_id.strip()
                        for cat_id in category_ids_str.split(",")
                        if cat_id.strip()
                    ]

                category_ids = parsed
        elif r_cat_id is not None:
            # Single scalar (int, etc.)
            cat = str(r_cat_id).strip()
            if cat:
                category_ids = [cat]
            
        if category_ids:
            logger.info(f"📋 Mapping categories for product {item_number}: {category_ids}")
            
            # Map categories (mainPluralCategoryId is optional, set to None for now)
            category_result = api.map_category(
                manage_number=item_number,
                category_ids=category_ids,
                main_plural_category_id=None,  # Can be enhanced later if needed
            )
            
            if not category_result.get("success"):
                # Log warning but don't fail the entire registration
                logger.warning(
                    f"⚠️  Product {item_number} registered successfully, but category mapping failed: {category_result.get('error')}"
                )
                # Add category mapping error to result (non-fatal)
                result["category_mapping"] = {
                    "success": False,
                    "error": category_result.get("error"),
                    "error_data": category_result.get("error_data"),
                }
            else:
                logger.info(f"✅ Category mapping successful for product {item_number}")
                result["category_mapping"] = {
                    "success": True,
                    "category_ids": category_ids,
                }
        else:
            logger.debug(f"ℹ️  No category IDs to map for product {item_number} (r_cat_id is empty after parsing)")
    else:
        logger.warning(f"⚠️  Product registration failed for {item_number}, skipping category mapping")
    
    return result


def delete_product_from_product_management(item_number: str) -> Dict[str, Any]:
    """
    Delete a product from Rakuten using item_number from product_management table.
    
    Args:
        item_number: Product item_number from product_management table (used as manage_number)
    
    Returns:
        Response dictionary with success status and details
    """
    # Create API client (will load credentials from config if not provided)
    api = RakutenProductAPI()
    
    # Delete product from Rakuten
    result = api.delete_product(item_number)
    
    return result


def check_product_registration_status(item_number: str) -> Dict[str, Any]:
    """
    Check if a product is registered on Rakuten by attempting to retrieve it.
    
    Args:
        item_number: Product item_number from product_management table (used as manage_number)
        
    Returns:
        Response dictionary with success status and registration status:
        - success: True if product exists on Rakuten, False if it doesn't or error occurred
        - is_registered: True if product is registered, False if deleted/not found
        - status: "registered" if product exists, "deleted" if not found, "error" if error occurred
    """
    # Create API client (will load credentials from config if not provided)
    api = RakutenProductAPI()
    
    # Try to get product information from Rakuten
    result = api.get_product(item_number)
    
    if result.get("success"):
        # Product information was retrieved successfully - product is registered
        return {
            "success": True,
            "is_registered": True,
            "status": "registered",
            "message": "Product is registered on Rakuten",
            "data": result.get("data")
        }
    else:
        # Check status code to determine if product was deleted or if there's an error
        status_code = result.get("status_code")
        
        # 404 Not Found typically means product doesn't exist (was deleted)
        if status_code == 404:
            return {
                "success": True,  # Check operation succeeded, product is just not registered
                "is_registered": False,
                "status": "deleted",
                "message": "Product not found on Rakuten (likely deleted)",
                "error": result.get("error")
            }
        else:
            # Other error (network, authentication, etc.)
            return {
                "success": False,
                "is_registered": None,
                "status": "error",
                "message": "Error checking product status",
                "error": result.get("error"),
                "status_code": status_code,
                "error_data": result.get("error_data")
            }


def update_product_registration_status_from_rakuten(item_number: str) -> Dict[str, Any]:
    """
    Check product registration status on Rakuten and update database accordingly.
    - If product exists on Rakuten:
      - If hideItem is false: update status to "onsale"
      - If hideItem is true: update status to "stop"
    - If product doesn't exist (404): update status to "deleted"
    - If error occurred: don't update status
    
    Args:
        item_number: Product item_number from product_management table
        
    Returns:
        Response dictionary with success status and update details
    """
    from .db import update_rakuten_registration_status, get_product_management_by_item_number
    
    # First check if product exists in database
    product_data = get_product_management_by_item_number(item_number)
    if not product_data:
        return {
            "success": False,
            "error": f"Product with item_number '{item_number}' not found in product_management table"
        }
    
    current_status = product_data.get("rakuten_registration_status")
    
    # Check registration status on Rakuten
    check_result = check_product_registration_status(item_number)
    
    if check_result.get("status") == "registered":
        # Product exists on Rakuten - check hideItem value to determine status
        product_data_from_rakuten = check_result.get("data")
        hide_item = None
        
        if product_data_from_rakuten and isinstance(product_data_from_rakuten, dict):
            hide_item = product_data_from_rakuten.get("hideItem")
            # Handle boolean or string values
            if isinstance(hide_item, str):
                hide_item = hide_item.lower() in ("true", "t", "1")
            elif hide_item is None:
                hide_item = False
        
        # Determine status based on hideItem
        if hide_item is False:
            # hideItem is false -> product is on sale
            new_status = "onsale"
            status_message = "販売中"
        else:
            # hideItem is true -> product is stopped
            new_status = "stop"
            status_message = "販売停止"
        
        # Update status if it has changed
        if current_status != new_status:
            update_result = update_rakuten_registration_status(item_number, new_status)
            if update_result:
                return {
                    "success": True,
                    "status": "registered",
                    "previous_status": current_status,
                    "new_status": new_status,
                    "message": f"Product is registered on Rakuten, status updated to '{new_status}' ({status_message})",
                    "hideItem": hide_item
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to update registration status in database"
                }
        else:
            return {
                "success": True,
                "status": "registered",
                "previous_status": current_status,
                "new_status": new_status,
                "message": f"Product is registered on Rakuten, status already '{new_status}' ({status_message})",
                "hideItem": hide_item
            }
    
    elif check_result.get("status") == "deleted":
        # Product doesn't exist on Rakuten - always update to "deleted"
        if current_status != "deleted":
            update_result = update_rakuten_registration_status(item_number, "deleted")
            if update_result:
                return {
                    "success": True,
                    "status": "deleted",
                    "previous_status": current_status,
                    "new_status": "deleted",
                    "message": "Product not found on Rakuten, status updated to 'deleted'"
                }
            else:
                return {
                    "success": False,
                    "error": "Failed to update registration status in database"
                }
        else:
            # Already deleted, no need to update
            return {
                "success": True,
                "status": "deleted",
                "previous_status": current_status,
                "new_status": "deleted",
                "message": "Product not found on Rakuten, status already 'deleted'"
            }
    
    else:
        # Error occurred during check - don't update status
        return {
            "success": False,
            "error": check_result.get("error", "Unknown error checking product status"),
            "status_code": check_result.get("status_code"),
            "error_data": check_result.get("error_data"),
            "message": "Failed to check product status on Rakuten, database not updated"
        }


def update_multiple_products_registration_status_from_rakuten(item_numbers: List[str]) -> Dict[str, Any]:
    """
    Check registration status for multiple products on Rakuten and update database accordingly.
    Processes products sequentially.
    
    Args:
        item_numbers: List of product item_numbers from product_management table
        
    Returns:
        Response dictionary with overall results and per-product details
    """
    if not item_numbers:
        return {
            "success": False,
            "error": "item_numbers is required and cannot be empty"
        }
    
    results = []
    success_count = 0
    error_count = 0
    
    for item_number in item_numbers:
        if not item_number:
            continue
        
        try:
            result = update_product_registration_status_from_rakuten(item_number)
            result["item_number"] = item_number
            results.append(result)
            
            if result.get("success"):
                success_count += 1
            else:
                error_count += 1
                
        except Exception as e:
            error_count += 1
            results.append({
                "item_number": item_number,
                "success": False,
                "error": str(e)
            })
    
    return {
        "success": True,
        "total": len(item_numbers),
        "success_count": success_count,
        "error_count": error_count,
        "results": results
    }


# ============================================================================
# Variant Processing Functions
# ============================================================================

def clean_variant_selectors(variant_selectors: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Clean variant selectors to remove machine-dependent characters.
    Uses DeepL translation.
    
    Args:
        variant_selectors: List of variant selector dictionaries
        
    Returns:
        Cleaned variant selectors list
    """
    if not variant_selectors:
        return variant_selectors
    
    cleaned = []
    for selector in variant_selectors:
        if not isinstance(selector, dict):
            cleaned.append(selector)
            continue
        
        cleaned_selector = selector.copy()
        
        # Clean displayName
        if 'displayName' in cleaned_selector:
            cleaned_selector['displayName'] = clean_text_for_rakuten(cleaned_selector['displayName'])
        
        # Clean values and translate Chinese to Japanese using enhanced translation
        if 'values' in cleaned_selector and isinstance(cleaned_selector['values'], list):
            cleaned_values = []
            seen_values = set()  # Track seen values to avoid duplicates
            
            for value_obj in cleaned_selector['values']:
                if isinstance(value_obj, dict):
                    cleaned_value = value_obj.copy()
                    if 'displayValue' in cleaned_value:
                        original_value = cleaned_value['displayValue']
                        
                        # Use enhanced translation function
                        cleaned_text = _translate_variant_value_with_context(
                            value=original_value,
                            key=cleaned_selector.get("key"),
                            context=None,
                            max_bytes=32,
                        )
                        
                        if not cleaned_text or cleaned_text.strip() == '':
                            continue
                        
                        # Note: Chinese character filtering is already handled by _translate_variant_value_with_context()
                        cleaned_value['displayValue'] = cleaned_text
                        
                        # Check for duplicates - skip if we've seen this displayValue before
                        display_value_key = cleaned_value.get('displayValue', '').strip()
                        if display_value_key and display_value_key not in seen_values:
                            seen_values.add(display_value_key)
                            cleaned_values.append(cleaned_value)
                        # Skip duplicates silently
                else:
                    cleaned_values.append(value_obj)
                
                # CRITICAL: Limit to 40 values as we process them (check after each item)
                if len(cleaned_values) >= RAKUTEN_SELECTOR_VALUE_LIMIT:
                    break
            
            # CRITICAL: Rakuten API allows maximum 40 values per variant selector
            # Final safety check - limit to first 40 values (should already be limited above, but ensure)
            if len(cleaned_values) > RAKUTEN_SELECTOR_VALUE_LIMIT:
                cleaned_values = cleaned_values[:RAKUTEN_SELECTOR_VALUE_LIMIT]
            
            cleaned_selector['values'] = cleaned_values
        
        cleaned.append(cleaned_selector)
    
    return cleaned


def clean_variants(variants: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
    """
    Modern variant cleaner that enforces Rakuten's selector requirements.
    Uses DeepL translation.
        
    Returns:
        Tuple of (cleaned_variants, selector_usage) where selector_usage maps selector keys
        to the ordered list of unique display values (max 40 per selector).
    """
    if not variants or not isinstance(variants, dict):
        return (variants if isinstance(variants, dict) else {}, {})
    
    cleaned: Dict[str, Any] = {}
    selector_usage: Dict[str, List[str]] = {}
    seen_selector_combinations: set = set()  # Track seen selector value combinations to prevent duplicates
    
    for sku_id, variant_data in variants.items():
        if not isinstance(variant_data, dict):
            cleaned[sku_id] = variant_data
            continue
        
        cleaned_variant = variant_data.copy()
        skip_variant = False
        pending_usage_updates: List[Tuple[str, str]] = []
        
        selector_values = cleaned_variant.get('selectorValues')
        if isinstance(selector_values, dict):
            cleaned_selector_values: Dict[str, Any] = {}
            for key, value in selector_values.items():
                if isinstance(value, str):
                    # Use enhanced translation function
                    # Note: Chinese character filtering is already handled by _translate_variant_value_with_context()
                    cleaned_text = _translate_variant_value_with_context(
                        value=value,
                        key=key,
                        context=None,
                        max_bytes=32
                    )
                    
                    if not cleaned_text or cleaned_text.strip() == '':
                        skip_variant = True
                        break
                else:
                    cleaned_text = value
                
                cleaned_selector_values[key] = cleaned_text
            
            if skip_variant:
                continue
            
            # CRITICAL: Check for duplicate selector value combinations (e.g., {size=XL, color=グレ})
            selector_combination = tuple(sorted((k, str(v)) for k, v in cleaned_selector_values.items()))
            if selector_combination in seen_selector_combinations:
                logger.warning(
                    f"Skipping variant {sku_id} due to duplicate selector combination: {dict(selector_combination)}"
                )
                skip_variant = True
                continue
            
            # Validate values and enforce 40-value limit per selector
            for selector_key, selector_value in cleaned_selector_values.items():
                if not isinstance(selector_value, str):
                    continue
                
                used_values = selector_usage.setdefault(selector_key, [])
                if selector_value in used_values:
                    continue
                if len(used_values) >= RAKUTEN_SELECTOR_VALUE_LIMIT:
                    logger.warning(
                        f"Skipping variant {sku_id} because selector '{selector_key}' already has {RAKUTEN_SELECTOR_VALUE_LIMIT} unique values"
                    )
                    skip_variant = True
                    break
                pending_usage_updates.append((selector_key, selector_value))
            
            if skip_variant:
                continue
            
            # Mark this combination as seen (only if we're going to add it)
            seen_selector_combinations.add(selector_combination)
            
            for selector_key, selector_value in pending_usage_updates:
                selector_usage.setdefault(selector_key, []).append(selector_value)
            
            cleaned_variant['selectorValues'] = cleaned_selector_values
        else:
            cleaned_variant['selectorValues'] = selector_values or {}
        
        # Clean and validate attributes, especially "総個数" (total quantity)
        if 'attributes' in cleaned_variant and isinstance(cleaned_variant['attributes'], list):
            cleaned_attributes = []
            for attr in cleaned_variant['attributes']:
                if not isinstance(attr, dict):
                    cleaned_attributes.append(attr)
                    continue
                
                attr_name = attr.get('name', '')
                attr_values = attr.get('values', [])
                
                # Validate and fix "総個数" (total quantity) attribute
                if attr_name == '総個数' and isinstance(attr_values, list) and len(attr_values) > 0:
                    first_value = attr_values[0]
                    
                    # Convert to valid number if invalid
                    if isinstance(first_value, str):
                        cleaned_value = re.sub(r'[^\d.]', '', first_value)
                        try:
                            num_value = float(cleaned_value) if cleaned_value else 1.0
                            # Validate range: 1 to 999999999
                            if num_value < 1:
                                num_value = 1.0
                            elif num_value > 999999999:
                                num_value = 999999999.0
                            # Format with up to 7 decimal places (per Rakuten limit)
                            formatted_value = f"{num_value:.7f}".rstrip('0').rstrip('.')
                            attr['values'] = [formatted_value]
                        except ValueError:
                            attr['values'] = ["1"]
                    elif isinstance(first_value, (int, float)):
                        # Ensure within range
                        num_value = max(1, min(first_value, 999999999))
                        formatted_value = f"{num_value:.7f}".rstrip('0').rstrip('.')
                        attr['values'] = [formatted_value]
                    else:
                        attr['values'] = ["1"]
                else:
                    # Ensure values are strings and clean them
                    if isinstance(attr_values, list):
                        cleaned_attr_values = []
                        for val in attr_values:
                            if isinstance(val, str):
                                cleaned_attr_values.append(clean_text_for_rakuten(val))
                            else:
                                cleaned_attr_values.append(str(val))
                        attr['values'] = cleaned_attr_values
                
                cleaned_attributes.append(attr)
            
            cleaned_variant['attributes'] = cleaned_attributes
        
        # Ensure standardPrice is an integer (Rakuten API requires integer, not float)
        if 'standardPrice' in cleaned_variant:
            standard_price = cleaned_variant['standardPrice']
            try:
                # Convert to integer if it's a float or string
                if isinstance(standard_price, float):
                    cleaned_variant['standardPrice'] = int(standard_price)
                elif isinstance(standard_price, str):
                    # Remove any decimal points and convert to int
                    cleaned_price = standard_price.split('.')[0]
                    cleaned_variant['standardPrice'] = int(cleaned_price) if cleaned_price else 0
                elif isinstance(standard_price, int):
                    # Already an integer, keep as is
                    cleaned_variant['standardPrice'] = standard_price
                else:
                    # Try to convert to int
                    cleaned_variant['standardPrice'] = int(float(standard_price))
            except (ValueError, TypeError) as e:
                logger.warning(f"Failed to convert standardPrice to integer for variant {sku_id}: {standard_price} - {e}")
                # Remove invalid standardPrice
                cleaned_variant.pop('standardPrice', None)
        
        cleaned[sku_id] = cleaned_variant
    
    return cleaned, selector_usage


def _filter_variant_selectors_by_usage(
    selectors: List[Dict[str, Any]],
    selector_usage: Dict[str, List[str]],
) -> List[Dict[str, Any]]:
    if not selectors:
        return []
    
    filtered_selectors: List[Dict[str, Any]] = []
    for selector in selectors:
        key = selector.get("key")
        used_values = selector_usage.get(key)
        if not used_values:
            continue
        
        value_lookup = {}
        for value_obj in selector.get("values", []):
            display_value = value_obj.get("displayValue", "")
            if display_value:
                value_lookup.setdefault(display_value, value_obj)
        
        ordered_values: List[Dict[str, Any]] = []
        for display_value in used_values:
            if display_value in value_lookup:
                ordered_values.append(value_lookup[display_value])
            else:
                ordered_values.append({"displayValue": display_value})
            if len(ordered_values) >= RAKUTEN_SELECTOR_VALUE_LIMIT:
                break
        
        selector_copy = selector.copy()
        selector_copy["values"] = ordered_values
        filtered_selectors.append(selector_copy)
    
    return filtered_selectors


def _build_variant_selectors_from_usage(
    selector_usage: Dict[str, List[str]]
) -> List[Dict[str, Any]]:
    if not selector_usage:
        return []
    
    selectors: List[Dict[str, Any]] = []
    for key, values in selector_usage.items():
        display_name = "カラー" if _is_color_key(key) else key.capitalize()
        selector_values = [{"displayValue": v} for v in values[:RAKUTEN_SELECTOR_VALUE_LIMIT]]
        selectors.append({
            "key": key,
            "displayName": display_name,
            "values": selector_values,
        })
    return selectors


def fix_html_tags(text: str) -> str:
    """
    Fix invalid HTML tags in product description text.
    Ensures all HTML tags are properly closed and valid.
    
    Args:
        text: Text that may contain HTML tags
        
    Returns:
        Text with fixed HTML tags
    """
    if not text or not isinstance(text, str):
        return text
    
    # List of allowed HTML tags for Rakuten product descriptions
    self_closing_tags = {'br', 'hr', 'img', 'input', 'meta', 'link'}
    paired_tags = {'b', 'strong', 'i', 'em', 'u', 'p', 'div', 'span', 'ul', 'ol', 'li', 
                   'h1', 'h2', 'h3', 'h4', 'h5', 'h6', 'a', 'table', 'tr', 'td', 'th', 
                   'thead', 'tbody', 'tfoot'}
    allowed_tags = self_closing_tags | paired_tags
    
    # Fix common issues: <br&gt; -> <br>
    text = re.sub(r'<br&gt;', '<br>', text, flags=re.IGNORECASE)
    text = re.sub(r'&lt;br&gt;', '<br>', text, flags=re.IGNORECASE)
    text = re.sub(r'&lt;br&gt;', '<br>', text, flags=re.IGNORECASE)
    
    # Fix double-escaped tags
    text = re.sub(r'&lt;([a-zA-Z0-9]+)&gt;', r'<\1>', text)
    text = re.sub(r'<([a-zA-Z0-9]+)&gt;', r'<\1>', text)
    text = re.sub(r'&lt;([a-zA-Z0-9]+)>', r'<\1>', text)
    
    return text


def convert_product_management_to_rakuten_json(product_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Convert product_management table data to Rakuten API JSON format.
    
    Args:
        product_data: Product data dictionary from product_management table
        
    Returns:
        Dictionary in Rakuten API JSON format
    """
    # Check if product is blocked - if so, only send price and inventory information
    block = product_data.get("block")
    if isinstance(block, str):
        is_blocked = block.lower() == "t" or block.lower() == "true"
    elif isinstance(block, bool):
        is_blocked = block
    else:
        is_blocked = False
    
    # If blocked, return minimal JSON with only itemNumber and variants with price only
    if is_blocked:
        rakuten_json = {
            "itemNumber": product_data.get("item_number", ""),
        }
        
        # Only include variants with standardPrice (price information)
        if product_data.get("variants"):
            variants = product_data["variants"]
            # Parse if it's a string
            if isinstance(variants, str):
                try:
                    variants = json.loads(variants)
                except (ValueError, json.JSONDecodeError):
                    variants = {}
            
            # Filter variants to only include price information
            price_only_variants = {}
            if isinstance(variants, dict):
                for sku_id, variant_data in variants.items():
                    if isinstance(variant_data, dict):
                        # Only keep standardPrice and selectorValues (for variant identification)
                        price_only_variant = {}
                        if "selectorValues" in variant_data:
                            price_only_variant["selectorValues"] = variant_data["selectorValues"]
                        if "standardPrice" in variant_data:
                            standard_price = variant_data["standardPrice"]
                            # Ensure standardPrice is an integer (Rakuten API requires integer)
                            try:
                                if isinstance(standard_price, float):
                                    price_only_variant["standardPrice"] = int(standard_price)
                                elif isinstance(standard_price, str):
                                    cleaned_price = standard_price.split('.')[0]
                                    price_only_variant["standardPrice"] = int(cleaned_price) if cleaned_price else 0
                                elif isinstance(standard_price, int):
                                    price_only_variant["standardPrice"] = standard_price
                                else:
                                    price_only_variant["standardPrice"] = int(float(standard_price))
                            except (ValueError, TypeError):
                                logger.warning(f"Failed to convert standardPrice to integer for variant {sku_id}: {standard_price}")
                                continue
                        
                        # Only add variant if it has both selectorValues and standardPrice
                        if "selectorValues" in price_only_variant and "standardPrice" in price_only_variant:
                            price_only_variants[sku_id] = price_only_variant
            
            if price_only_variants:
                rakuten_json["variants"] = price_only_variants
        
        logger.info(f"⚠️  Product {product_data.get('item_number')} is blocked - only sending price information")
        return rakuten_json
    
    # Convert hide_item from boolean/string to boolean
    hide_item = product_data.get("hide_item")
    if isinstance(hide_item, str):
        hide_item_bool = hide_item.lower() == "t" or hide_item.lower() == "true"
    elif isinstance(hide_item, bool):
        hide_item_bool = hide_item
    else:
        hide_item_bool = False
    
    # Build the Rakuten API JSON structure
    # Clean text fields to remove machine-dependent characters
    title = clean_text_for_rakuten(product_data.get("title", "")) if product_data.get("title") else ""
    tagline = clean_text_for_rakuten(product_data.get("tagline", "")) if product_data.get("tagline") else ""
    sales_description = clean_text_for_rakuten(product_data.get("sales_description", "")) if product_data.get("sales_description") else ""
    
    # Clean productDescription (nested dictionary)
    product_description = product_data.get("product_description") or {"pc": "", "sp": ""}
    if isinstance(product_description, dict):
        cleaned_product_description = {}
        for key, value in product_description.items():
            if isinstance(value, str):
                # First clean text, then fix HTML tags
                cleaned_text = clean_text_for_rakuten(value)
                cleaned_product_description[key] = fix_html_tags(cleaned_text)
            else:
                cleaned_product_description[key] = value
        product_description = cleaned_product_description
    
    rakuten_json = {
        "itemNumber": product_data.get("item_number", ""),
        "title": title,
        "tagline": tagline,
        "productDescription": product_description,
        "salesDescription": sales_description,
        "itemType": product_data.get("item_type", "NORMAL"),
        "genreId": str(product_data.get("genre_id", "")) if product_data.get("genre_id") else "",
        "tags": product_data.get("tags") or [],
        "hideItem": hide_item_bool,
        "unlimitedInventoryFlag": bool(product_data.get("unlimited_inventory_flag", False)),
        "images": product_data.get("images") or [],
        "features": product_data.get("features") or {},
        "payment": product_data.get("payment") or {},
    }
    
    # Add optional fields if they exist
    if product_data.get("layout"):
        rakuten_json["layout"] = product_data["layout"]
    
    cleaned_variant_selectors: Optional[List[Dict[str, Any]]] = None
    selector_usage: Dict[str, List[str]] = {}
    
    # Clean and add variants (remove machine-dependent characters)
    if product_data.get("variants"):
        variants = product_data["variants"]
        # Parse if it's a string
        if isinstance(variants, str):
            try:
                variants = json.loads(variants)
            except (ValueError, json.JSONDecodeError):
                variants = {}
        # Clean the variants using enhanced translation
        cleaned_variants, selector_usage = clean_variants(variants)
        rakuten_json["variants"] = cleaned_variants
    
    # Clean and add variant_selectors (remove machine-dependent characters)
    if product_data.get("variant_selectors"):
        variant_selectors = product_data["variant_selectors"]
        # Parse if it's a string
        if isinstance(variant_selectors, str):
            try:
                variant_selectors = json.loads(variant_selectors)
            except (ValueError, json.JSONDecodeError):
                variant_selectors = []
        
        # Clean variant selectors using enhanced translation
        cleaned_variant_selectors = clean_variant_selectors(variant_selectors)
        
        # Rebuild from usage if we have selector_usage (ensures consistency)
        if selector_usage:
            cleaned_variant_selectors = _build_variant_selectors_from_usage(selector_usage)
        elif cleaned_variant_selectors:
            # Filter by usage if available
            cleaned_variant_selectors = _filter_variant_selectors_by_usage(cleaned_variant_selectors, selector_usage)
        
        rakuten_json["variantSelectors"] = cleaned_variant_selectors or []
    else:
        # Build from usage if we have it
        if selector_usage:
            rakuten_json["variantSelectors"] = _build_variant_selectors_from_usage(selector_usage)
        else:
            rakuten_json["variantSelectors"] = []
    
    return rakuten_json
