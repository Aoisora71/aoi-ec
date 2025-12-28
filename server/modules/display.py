from typing import List
from .models import Product
import json


def display_all_results_table(products: List[Product]) -> None:
    """
    Display all search results in a formatted table.
    """
    if not products:
        print("No products found to display.")
        return
    
    print(f"\n=== SEARCH RESULTS ({len(products)} products) ===")
    print("=" * 120)
    
    # Table header
    print(f"{'#':<3} {'ID':<12} {'Chinese Title':<40} {'Japanese Title':<40} {'Price':<8} {'Sold':<6} {'Shop':<20}")
    print("-" * 120)
    
    for i, product in enumerate(products, 1):
        goods_id = str(product.get("goodsId", ""))[:12]
        title_c = str(product.get("titleC", ""))[:40]
        title_t = str(product.get("titleT", ""))[:40]
        price = str(product.get("goodsPrice", "N/A"))
        sold = str(product.get("monthSold", "N/A"))
        shop_info = product.get("shopInfo", {})
        if isinstance(shop_info, dict):
            shop_name = str(shop_info.get("shopName", "Unknown"))[:20]
        else:
            shop_name = "Unknown"
        print(f"{i:<3} {goods_id:<12} {title_c:<40} {title_t:<40} {price:<8} {sold:<6} {shop_name:<20}")
    
    print("=" * 120)
    print(f"Total: {len(products)} products")
    
    # Show additional statistics
    print(f"\n=== STATISTICS ===")
    prices = []
    for p in products:
        try:
            price = float(p.get("goodsPrice", 0))
            if price > 0:
                prices.append(price)
        except (ValueError, TypeError):
            continue
    if prices:
        print(f"Price range: {min(prices):.2f} - {max(prices):.2f} RMB")
        print(f"Average price: {sum(prices)/len(prices):.2f} RMB")
    categories = {}
    for p in products:
        cat_id = p.get("topCategoryId")
        if cat_id:
            categories[cat_id] = categories.get(cat_id, 0) + 1
    if categories:
        print(f"Top categories:")
        for cat_id, count in sorted(categories.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  Category {cat_id}: {count} products")
    shops = {}
    for p in products:
        shop_info = p.get("shopInfo", {})
        if isinstance(shop_info, dict):
            shop_name = shop_info.get("shopName", "Unknown")
            if shop_name != "Unknown":
                shops[shop_name] = shops.get(shop_name, 0) + 1
    if shops:
        print(f"Top shops:")
        for shop_name, count in sorted(shops.items(), key=lambda x: x[1], reverse=True)[:5]:
            print(f"  {shop_name}: {count} products")


def display_all_search_result_items(products: List[Product], show_empty: bool = False) -> None:
    """
    Display all available fields and items in search results in a structured format.
    """
    if not products:
        print("No products found to display.")
        return
    
    print(f"\n=== SEARCH RESULT ITEMS ANALYSIS ===")
    print(f"Total products found: {len(products)}")
    print("=" * 50)
    
    all_fields = set()
    for product in products:
        all_fields.update(product.keys())
    sorted_fields = sorted(all_fields)
    
    print(f"\nAll available fields ({len(sorted_fields)} total):")
    print("-" * 30)
    for i, field in enumerate(sorted_fields, 1):
        print(f"{i:2d}. {field}")
    
    print(f"\nDetailed field analysis:")
    print("=" * 50)
    for field in sorted_fields:
        field_values = []
        non_empty_count = 0
        for product in products:
            value = product.get(field)
            if value is not None and value != "" and value != [] and value != {}:
                non_empty_count += 1
                if len(field_values) < 3:
                    field_values.append(value)
        if show_empty or non_empty_count > 0:
            print(f"\nField: {field}")
            print(f"  - Present in {non_empty_count}/{len(products)} products ({non_empty_count/len(products)*100:.1f}%)")
            if field_values:
                print(f"  - Sample values:")
                for i, val in enumerate(field_values, 1):
                    try:
                        display_val = str(val)
                        if len(display_val) > 100:
                            display_val = display_val[:97] + "..."
                        display_val = display_val.encode('utf-8', errors='replace').decode('utf-8')
                        print(f"    {i}. {display_val}")
                    except (UnicodeEncodeError, UnicodeDecodeError):
                        display_val = repr(val)
                        if len(display_val) > 100:
                            display_val = display_val[:97] + "..."
                        print(f"    {i}. {display_val}")
    
    print(f"\n=== SAMPLE PRODUCT STRUCTURE ===")
    if products:
        sample_product = products[0]
        print("First product structure:")
        try:
            print(json.dumps(sample_product, ensure_ascii=False, indent=2))
        except UnicodeEncodeError:
            print(json.dumps(sample_product, ensure_ascii=True, indent=2))



