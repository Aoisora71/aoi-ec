# -*- coding: utf-8 -*-
"""
Validate Product Data Structure

This script validates that product JSON data matches the Rakuten API specification
and contains all required fields.

Usage:
    python validate_product_data.py <json_file>

Example:
    python validate_product_data.py origin_rakuten.json
"""
import json
import sys
from main import validate_product_data


def validate_from_file(filepath: str) -> bool:
    """Validate product data from JSON file"""
    print("=" * 80)
    print("PRODUCT DATA VALIDATION")
    print("=" * 80)
    
    # Load product data
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            product_data = json.load(f)
        print(f"\n✓ Product data loaded from: {filepath}")
    except FileNotFoundError:
        print(f"\n✗ File not found: {filepath}")
        return False
    except json.JSONDecodeError as e:
        print(f"\n✗ Invalid JSON: {e}")
        return False
    except Exception as e:
        print(f"\n✗ Failed to load product data: {e}")
        return False
    
    # Validate structure
    errors = validate_product_data(product_data)
    
    if errors:
        print("\n✗ Validation errors found:")
        for error in errors:
            print(f"  - {error}")
        return False
    else:
        print("\n✓ All required fields present")
        
        # Show data statistics
        print(f"\nData Statistics:")
        print(f"  Total top-level fields: {len(product_data)}")
        print(f"  Variants: {len(product_data.get('variants', {}))}")
        if "variantSelectors" in product_data:
            print(f"  Variant selectors: {len(product_data['variantSelectors'])}")
        
        print("\n" + "=" * 80)
        print("✓ Validation complete - Product data is ready for API submission")
        print("=" * 80)
        return True

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python validate_product_data.py <json_file>")
        print("Example: python validate_product_data.py product_template.json")
        sys.exit(1)
    
    filepath = sys.argv[1]
    success = validate_from_file(filepath)
    sys.exit(0 if success else 1)

