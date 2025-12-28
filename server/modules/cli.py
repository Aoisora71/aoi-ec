import argparse
import os
import json
from typing import Any

from .config import (
    API_URL,
    DETAIL_API_URL,
    IMAGE_ID_API_URL,
    LOGISTICS_API_URL,
    TAGS_API_URL,
    CREATE_ORDER_API_URL,
    UPDATE_ORDER_STATUS_API_URL,
    CANCEL_ORDER_API_URL,
    ORDER_LIST_API_URL,
    ORDER_DETAIL_API_URL,
    STOCK_LIST_API_URL,
    CREATE_PORDER_API_URL,
    UPDATE_PORDER_STATUS_API_URL,
    CANCEL_PORDER_API_URL,
    PORDER_LIST_API_URL,
    PORDER_DETAIL_API_URL,
    LOGISTICS_TRACK_API_URL,
)
from .api_search import search_products, get_product_detail, get_image_id, keyword_search_products, parse_keyword_search_response, search_multiple_categories
from .display import display_all_results_table, display_all_search_result_items
from .db import save_products_to_db
from .console import SearchResultConsole
from .filters import collect_categories_from_products as get_available_categories
from .orders import (
    create_order, update_order_status, cancel_order, get_order_list, get_order_detail,
    get_stock_list, create_porder, update_porder_status, cancel_porder,
    get_porder_list, get_porder_detail, get_logistics_track,
)
from .enrich import enrich_products_with_detail
from .meta import get_logistics, get_tags
try:
    from .image_processing import process_product_images_from_db, process_all_product_images, ImageProcessor
except ImportError:
    # Handle the hyphen in filename
    import importlib.util
    import sys
    from pathlib import Path
    
    # Check if the image-processing.py file exists
    image_processing_file = Path(__file__).parent / "image-processing.py"
    if image_processing_file.exists():
        # Load the image-processing module
        spec = importlib.util.spec_from_file_location(
            "image_processing", 
            image_processing_file
        )
        image_processing = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(image_processing)
        
        # Make it available in the current namespace
        sys.modules['modules.image_processing'] = image_processing
        
        # Import the functions
        process_product_images_from_db = image_processing.process_product_images_from_db
        process_all_product_images = image_processing.process_all_product_images
        ImageProcessor = image_processing.ImageProcessor
    else:
        # Create dummy functions if the module doesn't exist
        def process_product_images_from_db(*args, **kwargs):
            return {"error": "Image processing module not available"}
        
        def process_all_product_images(*args, **kwargs):
            return {"error": "Image processing module not available"}
        
        class ImageProcessor:
            def __init__(self, *args, **kwargs):
                pass


def run(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Search products and fetch product details via API")
    subparsers = parser.add_subparsers(dest="command", required=False)

    # Search
    search_parser = subparsers.add_parser("search", help="Search products by keyword")
    search_parser.add_argument("keyword", nargs="?", default="laptop", help="Keyword to search (default: laptop)")
    search_parser.add_argument("--page", type=int, default=1, help="Page number (default: 1)")
    search_parser.add_argument("--page-size", type=int, default=10, help="Page size (default: 10)")
    search_parser.add_argument("--price-min", type=str, help="Minimum price filter (sent as price_min)")
    search_parser.add_argument("--price-max", type=str, help="Maximum price filter (sent as price_max)")
    search_parser.add_argument("--order-key", type=str, help="Sort field (sent as order_by[0][key])")
    search_parser.add_argument("--order-value", type=str, choices=["asc", "desc"], help="Sort direction (sent as order_by[0][value])")
    search_parser.add_argument("--shop-type", type=str, default="1688", help="Shop type (default: 1688)")
    search_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    search_parser.add_argument("--categories", nargs="*", help="Filter by categories (multi-select)")
    search_parser.add_argument("--subcategories", nargs="*", help="Filter by subcategories (multi-select)")
    search_parser.add_argument("--max-length", type=float, help="Maximum length (cm)")
    search_parser.add_argument("--max-width", type=float, help="Maximum width (cm)")
    search_parser.add_argument("--max-height", type=float, help="Maximum height (cm)")
    search_parser.add_argument("--max-weight", type=float, help="Maximum weight (grams)")
    search_parser.add_argument("--jpy-price-min", type=float, help="Minimum price in Japanese Yen")
    search_parser.add_argument("--jpy-price-max", type=float, help="Maximum price in Japanese Yen")
    search_parser.add_argument("--exchange-rate", type=float, default=20.0, help="RMB to JPY exchange rate (default: 20.0)")
    search_parser.add_argument("--strict", action="store_true", help="Strict mode: only return products meeting ALL criteria")
    search_parser.add_argument("--min-inventory", type=int, help="Minimum inventory level")
    search_parser.add_argument("--max-delivery-days", type=int, help="Maximum delivery days to Japan")
    search_parser.add_argument("--max-shipping-fee", type=float, help="Maximum shipping fee to Japan (RMB)")
    search_parser.add_argument("--with-detail", dest="with_detail", action="store_true", default=True, help="Also fetch detail (images, description) for results [default]")
    search_parser.add_argument("--no-detail", dest="with_detail", action="store_false", help="Do not fetch detail for results")
    search_parser.add_argument("--detail-limit", type=int, default=5, help="Max number of items to enrich with detail (default: 5)")
    search_parser.add_argument("--api-url", type=str, help="Override search API URL")
    search_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    search_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    search_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    search_parser.add_argument("--show-all-fields", action="store_true", help="Display all available fields in search results")
    search_parser.add_argument("--show-empty-fields", action="store_true", help="Include empty fields in field analysis")
    search_parser.add_argument("--display-all", action="store_true", help="Display all results in a formatted table")
    search_parser.add_argument("--save-to-postgres", action="store_true", help="Save results to PostgreSQL (env: DATABASE_URL or PG* vars)")
    search_parser.add_argument("--db-keyword", type=str, help="Override keyword stored with rows (defaults to search keyword)")

    # Keyword Search (new API)
    keyword_parser = subparsers.add_parser("keyword-search", help="Search products using the new keyword search API")
    keyword_parser.add_argument("keywords", nargs="?", default="dress", help="Keywords to search (default: dress)")
    keyword_parser.add_argument("--shop-type", type=str, default="1688", choices=["1688", "taobao", "tmall"], help="Shop type (default: 1688)")
    keyword_parser.add_argument("--page", type=int, default=1, help="Page number (default: 1)")
    keyword_parser.add_argument("--page-size", type=int, default=50, help="Page size (default: 50)")
    keyword_parser.add_argument("--price-start", type=str, help="Starting price filter")
    keyword_parser.add_argument("--price-end", type=str, help="Ending price filter")
    keyword_parser.add_argument("--sort-field", type=str, choices=["price", "monthSold", "rePurchaseRate"], help="Sort field")
    keyword_parser.add_argument("--sort-order", type=str, choices=["asc", "desc"], default="desc", help="Sort order (default: desc)")
    keyword_parser.add_argument("--region-opp", type=str, choices=["jpOpp", "krOpp"], help="Region option (jpOpp for Japanese hot, krOpp for Korean hot)")
    keyword_parser.add_argument("--filter", type=str, help="Filter options (e.g., certifiedFactory,shipIn48Hours)")
    keyword_parser.add_argument("--category-id", type=str, help="Category ID for filtering")
    keyword_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    keyword_parser.add_argument("--api-url", type=str, help="Override API URL")
    keyword_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    keyword_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    keyword_parser.add_argument("--verbose", action="store_true", help="Print request details")
    keyword_parser.add_argument("--json", action="store_true", help="Output raw JSON response")
    keyword_parser.add_argument("--parsed", action="store_true", help="Output parsed response (default)")

    # Multi-Category Search
    multi_cat_parser = subparsers.add_parser("multi-category-search", help="Search products across multiple categories")
    multi_cat_parser.add_argument("--category-ids", required=True, nargs="+", help="List of category IDs to search (space-separated)")
    multi_cat_parser.add_argument("--shop-type", type=str, default="1688", choices=["1688", "taobao", "tmall"], help="Shop type (default: 1688)")
    multi_cat_parser.add_argument("--page", type=int, default=1, help="Page number (default: 1)")
    multi_cat_parser.add_argument("--page-size", type=int, default=50, help="Page size per category (default: 50)")
    multi_cat_parser.add_argument("--max-products-per-category", type=int, default=100, help="Maximum products per category (default: 100)")
    multi_cat_parser.add_argument("--price-start", type=str, help="Starting price filter")
    multi_cat_parser.add_argument("--price-end", type=str, help="Ending price filter")
    multi_cat_parser.add_argument("--sort-field", type=str, choices=["price", "monthSold", "rePurchaseRate"], help="Sort field")
    multi_cat_parser.add_argument("--sort-order", type=str, choices=["asc", "desc"], default="desc", help="Sort order (default: desc)")
    multi_cat_parser.add_argument("--region-opp", type=str, choices=["jpOpp", "krOpp"], help="Region option (jpOpp for Japanese hot, krOpp for Korean hot)")
    multi_cat_parser.add_argument("--filter", type=str, help="Filter options (e.g., certifiedFactory,shipIn48Hours)")
    multi_cat_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    multi_cat_parser.add_argument("--api-url", type=str, help="Override API URL")
    multi_cat_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    multi_cat_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    multi_cat_parser.add_argument("--verbose", action="store_true", help="Print request details")
    multi_cat_parser.add_argument("--json", action="store_true", help="Output raw JSON response")
    multi_cat_parser.add_argument("--parsed", action="store_true", help="Output parsed response (default)")
    multi_cat_parser.add_argument("--save-to-postgres", action="store_true", help="Save results to PostgreSQL")

    # Detail
    detail_parser = subparsers.add_parser("detail", help="Get product detail by goodsId")
    detail_parser.add_argument("--goods-id", required=True, help="goodsId to fetch detail for")
    detail_parser.add_argument("--shop-type", type=str, default="1688", help="Product type: 1688/taobao")
    detail_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    detail_parser.add_argument("--detail-api-url", type=str, help="Override detail API URL")
    detail_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    detail_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    detail_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    detail_parser.add_argument("--description-only", action="store_true", help="Print only the HTML description")
    detail_parser.add_argument("--images-only", action="store_true", help="Print only the image URLs array")
    detail_parser.add_argument("--images-and-description", action="store_true", help="Print both images and description together")
    detail_parser.add_argument("--normalize", action="store_true", help="Normalize detail payload per spec")

    # Image
    image_parser = subparsers.add_parser("image", help="Get image ID by uploading base64 encoded image")
    image_parser.add_argument("--image-base64", required=True, help="Base64 encoded image data")
    image_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    image_parser.add_argument("--image-api-url", type=str, help="Override image ID API URL")
    image_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    image_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    image_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    image_parser.add_argument("--image-id-only", action="store_true", help="Print only the image ID")
    image_parser.add_argument("--link-only", action="store_true", help="Print only the search link")

    # Logistics names/tags
    logistics_parser = subparsers.add_parser("logistics", help="Get current useful logistics information")
    logistics_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    logistics_parser.add_argument("--logistics-api-url", type=str, help="Override logistics API URL")
    logistics_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    logistics_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    logistics_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    logistics_parser.add_argument("--names-only", action="store_true", help="Print only logistics names")
    logistics_parser.add_argument("--ids-only", action="store_true", help="Print only logistics IDs")

    tags_parser = subparsers.add_parser("tags", help="Get current useful tags information")
    tags_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    tags_parser.add_argument("--tags-api-url", type=str, help="Override tags API URL")
    tags_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    tags_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    tags_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    tags_parser.add_argument("--types-only", action="store_true", help="Print only tag types")
    tags_parser.add_argument("--translations-only", action="store_true", help="Print only Japanese translations")

    # Orders
    order_parser = subparsers.add_parser("order", help="Create an order with multiple products")
    order_parser.add_argument("--purchase-order", required=True, help="Customer's original order number")
    order_parser.add_argument("--status", required=True, choices=["10", "20"], help="Order Status: 10-Provisional Order, 20-Official Order")
    order_parser.add_argument("--goods", required=True, help="JSON string containing goods data array")
    order_parser.add_argument("--logistics-id", type=str, help="Customer's desired logistics mode")
    order_parser.add_argument("--remark", type=str, help="Order remarks")
    order_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    order_parser.add_argument("--order-api-url", type=str, help="Override create order API URL")
    order_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    order_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    order_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    order_parser.add_argument("--order-sn-only", action="store_true", help="Print only the order number")
    order_parser.add_argument("--status-only", action="store_true", help="Print only the order status")

    update_parser = subparsers.add_parser("update-status", help="Update order status")
    update_parser.add_argument("--order-sn", required=True, help="rakumart system order number")
    update_parser.add_argument("--status", required=True, choices=["10", "20"], help="New status: 10-Provisional, 20-Official")
    update_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    update_parser.add_argument("--update-api-url", type=str, help="Override update order status API URL")
    update_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    update_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    update_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    update_parser.add_argument("--order-sn-only", action="store_true", help="Print only the order number from response")

    cancel_parser = subparsers.add_parser("cancel", help="Cancel an order")
    cancel_parser.add_argument("--order-sn", required=True, help="rakumart system order number")
    cancel_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    cancel_parser.add_argument("--cancel-api-url", type=str, help="Override cancel order API URL")
    cancel_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    cancel_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    cancel_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    cancel_parser.add_argument("--raw", action="store_true", help="Print raw API response as JSON")

    order_list_parser = subparsers.add_parser("orders", help="Fetch paginated order list")
    order_list_parser.add_argument("--page", type=int, default=1, help="Page number (default: 1)")
    order_list_parser.add_argument("--page-size", type=int, default=10, help="Items per page (default: 10)")
    order_list_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    order_list_parser.add_argument("--orders-api-url", type=str, help="Override order list API URL")
    order_list_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    order_list_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    order_list_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    order_list_parser.add_argument("--summary", action="store_true", help="Print only a summary table of orders")

    order_detail_parser = subparsers.add_parser("order-detail", help="Fetch order detail by order_sn")
    order_detail_parser.add_argument("--order-sn", required=True, help="rakumart system order number")
    order_detail_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    order_detail_parser.add_argument("--order-detail-api-url", type=str, help="Override order detail API URL")
    order_detail_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    order_detail_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    order_detail_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    order_detail_parser.add_argument("--items-only", action="store_true", help="Print only order_detail items array")

    stock_list_parser = subparsers.add_parser("stock", help="Fetch stock list")
    stock_list_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    stock_list_parser.add_argument("--stock-api-url", type=str, help="Override stock list API URL")
    stock_list_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    stock_list_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    stock_list_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    stock_list_parser.add_argument("--raw", action="store_true", help="Print raw API response as JSON")

    porder_parser = subparsers.add_parser("porder", help="Create a delivery order (Porder)")
    porder_parser.add_argument("--status", required=True, choices=["10", "20"], help="Porder status: 10-Provisional, 20-Official")
    porder_parser.add_argument("--logistics-id", required=True, help="Logistics ID")
    porder_parser.add_argument("--porder-detail", required=True, help="JSON array for porder_detail list")
    porder_parser.add_argument("--client-remark", type=str, help="Client remark for the whole porder")
    porder_parser.add_argument("--receiver-address", type=str, help="JSON object for receiver_address")
    porder_parser.add_argument("--importer-address", type=str, help="JSON object for importer_address")
    porder_parser.add_argument("--porder-file", type=str, help="JSON array for porder_file list")
    porder_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    porder_parser.add_argument("--porder-api-url", type=str, help="Override create porder API URL")
    porder_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    porder_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    porder_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    porder_parser.add_argument("--porder-sn-only", action="store_true", help="Print only the porder_sn from response")

    upd_porder_parser = subparsers.add_parser("porder-update-status", help="Update porder status")
    upd_porder_parser.add_argument("--porder-sn", required=True, help="rakumart system porder number")
    upd_porder_parser.add_argument("--status", required=True, choices=["10", "20"], help="Porder status: 10-Provisional, 20-Official")
    upd_porder_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    upd_porder_parser.add_argument("--porder-update-api-url", type=str, help="Override update porder status API URL")
    upd_porder_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    upd_porder_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    upd_porder_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")

    cancel_porder_parser = subparsers.add_parser("porder-cancel", help="Cancel a porder")
    cancel_porder_parser.add_argument("--porder-sn", required=True, help="rakumart system porder number")
    cancel_porder_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    cancel_porder_parser.add_argument("--porder-cancel-api-url", type=str, help="Override cancel porder API URL")
    cancel_porder_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    cancel_porder_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    cancel_porder_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")

    porder_list_parser = subparsers.add_parser("porders", help="Fetch porder list")
    porder_list_parser.add_argument("--page", type=int, default=1, help="Page number (default: 1)")
    porder_list_parser.add_argument("--page-size", type=int, default=10, help="Items per page (default: 10)")
    porder_list_parser.add_argument("--porder-sn", type=str, help="Filter by porder_sn")
    porder_list_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    porder_list_parser.add_argument("--porders-api-url", type=str, help="Override porder list API URL")
    porder_list_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    porder_list_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    porder_list_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    porder_list_parser.add_argument("--summary", action="store_true", help="Print only a summary table of porders")

    porder_detail_parser = subparsers.add_parser("porder-detail", help="Fetch porder detail by porder_sn")
    porder_detail_parser.add_argument("--porder-sn", required=True, help="rakumart system porder number")
    porder_detail_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    porder_detail_parser.add_argument("--porder-detail-api-url", type=str, help="Override porder detail API URL")
    porder_detail_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    porder_detail_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    porder_detail_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    porder_detail_parser.add_argument("--items-only", action="store_true", help="Print only porder_detail items array")

    ltrack_parser = subparsers.add_parser("ltrack", help="Fetch international logistics tracking by express number")
    ltrack_parser.add_argument("--express-no", required=True, help="International logistics number")
    ltrack_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    ltrack_parser.add_argument("--ltrack-api-url", type=str, help="Override logistics track API URL")
    ltrack_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    ltrack_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    ltrack_parser.add_argument("--verbose", action="store_true", help="Print request payload and endpoint")
    ltrack_parser.add_argument("--timeline-only", action="store_true", help="Print only time/address timeline entries")

    console_parser = subparsers.add_parser("console", help="Interactive console for search results")
    console_parser.add_argument("keyword", nargs="?", default="laptop", help="Keyword to search (default: laptop)")
    console_parser.add_argument("--page", type=int, default=1, help="Page number (default: 1)")
    console_parser.add_argument("--page-size", type=int, default=20, help="Page size (default: 20)")
    console_parser.add_argument("--shop-type", type=str, default="1688", help="Shop type (default: 1688)")
    console_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    console_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    console_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    console_parser.add_argument("--api-url", type=str, help="Override search API URL")
    console_parser.add_argument("--with-detail", dest="with_detail", action="store_true", default=True, help="Also fetch detail for results [default]")
    console_parser.add_argument("--no-detail", dest="with_detail", action="store_false", help="Do not fetch detail for results")
    console_parser.add_argument("--detail-limit", type=int, default=10, help="Max number of items to enrich with detail (default: 10)")

    # Image processing
    process_parser = subparsers.add_parser("process-images", help="Process product images with AI")
    process_parser.add_argument("--product-id", type=str, help="Process images for specific product ID")
    process_parser.add_argument("--limit", type=int, help="Limit number of products to process")
    process_parser.add_argument("--test-image", type=str, help="Test processing on a single image URL")
    process_parser.add_argument("--output-dir", type=str, default="processed_images", help="Directory to save processed images (default: processed_images)")
    process_parser.add_argument("--no-save", action="store_true", help="Don't save processed images to local storage")
    process_parser.add_argument("--verbose", action="store_true", help="Verbose output")

    # Product name optimization removed

    # Categories summary
    categories_parser = subparsers.add_parser("categories", help="Search and summarize categories from results")
    categories_parser.add_argument("keyword", nargs="?", default="laptop", help="Keyword to search (default: laptop)")
    categories_parser.add_argument("--page", type=int, default=1, help="Page number (default: 1)")
    categories_parser.add_argument("--page-size", type=int, default=20, help="Page size (default: 20)")
    categories_parser.add_argument("--shop-type", type=str, default="1688", help="Shop type (default: 1688)")
    categories_parser.add_argument("--timeout", type=int, default=15, help="HTTP timeout seconds (default: 15)")
    categories_parser.add_argument("--app-key", type=str, help="Override APP_KEY for this call")
    categories_parser.add_argument("--app-secret", type=str, help="Override APP_SECRET for this call")
    categories_parser.add_argument("--api-url", type=str, help="Override search API URL")

    args = parser.parse_args(argv)

    if getattr(args, "command", None) is None:
        argv2 = argv if argv is not None else os.sys.argv[1:]
        if any(tok == "detail" or tok.startswith("--goods-id") for tok in argv2):
            args = parser.parse_args(["detail", *argv2])
        elif any(tok == "image" or tok.startswith("--image-base64") for tok in argv2):
            args = parser.parse_args(["image", *argv2])
        elif any(tok == "logistics" for tok in argv2):
            args = parser.parse_args(["logistics", *argv2])
        elif any(tok == "tags" for tok in argv2):
            args = parser.parse_args(["tags", *argv2])
        elif any(tok == "order" or tok.startswith("--purchase-order") for tok in argv2):
            args = parser.parse_args(["order", *argv2])
        elif any(tok == "update-status" or tok.startswith("--order-sn") for tok in argv2):
            args = parser.parse_args(["update-status", *argv2])
        elif any(tok == "cancel" or tok.startswith("--order-sn") for tok in argv2):
            args = parser.parse_args(["cancel", *argv2])
        elif any(tok == "orders" or tok.startswith("--page") for tok in argv2):
            args = parser.parse_args(["orders", *argv2])
        elif any(tok == "order-detail" or tok.startswith("--order-sn") for tok in argv2):
            args = parser.parse_args(["order-detail", *argv2])
        elif any(tok == "stock" for tok in argv2):
            args = parser.parse_args(["stock", *argv2])
        elif any(tok == "porder" or tok.startswith("--porder-detail") for tok in argv2):
            args = parser.parse_args(["porder", *argv2])
        elif any(tok == "porder-update-status" or tok.startswith("--porder-sn") and "porder-update-status" in argv2 for tok in argv2):
            args = parser.parse_args(["porder-update-status", *argv2])
        elif any(tok == "porder-cancel" or tok.startswith("--porder-sn") and "porder-cancel" in argv2 for tok in argv2):
            args = parser.parse_args(["porder-cancel", *argv2])
        elif any(tok == "porders" or tok.startswith("--page") for tok in argv2):
            args = parser.parse_args(["porders", *argv2])
        elif any(tok == "porder-detail" or tok.startswith("--porder-sn") for tok in argv2):
            args = parser.parse_args(["porder-detail", *argv2])
        elif any(tok == "ltrack" or tok.startswith("--express-no") for tok in argv2):
            args = parser.parse_args(["ltrack", *argv2])
        elif any(tok == "categories" for tok in argv2):
            args = parser.parse_args(["categories", *argv2])
        elif any(tok == "process-images" for tok in argv2):
            args = parser.parse_args(["process-images", *argv2])
        # optimize-names command removed
        else:
            args = parser.parse_args(["search", *argv2])

    # Delegate to the same handlers as in main.py
    # Rather than duplicating the large routing, import main and reuse its block is heavy.
    # Here we replicate minimal handling for search/detail/image/console and keep others via orders module.
    if args.command == "search":
        products = search_products(
            args.keyword,
            page=args.page,
            page_size=args.page_size,
            price_min=getattr(args, "price_min", None),
            price_max=getattr(args, "price_max", None),
            order_key=getattr(args, "order_key", None),
            order_value=getattr(args, "order_value", None),
            categories=getattr(args, "categories", None),
            subcategories=getattr(args, "subcategories", None),
            max_length=getattr(args, "max_length", None),
            max_width=getattr(args, "max_width", None),
            max_height=getattr(args, "max_height", None),
            max_weight=getattr(args, "max_weight", None),
            jpy_price_min=getattr(args, "jpy_price_min", None),
            jpy_price_max=getattr(args, "jpy_price_max", None),
            exchange_rate=getattr(args, "exchange_rate", 20.0),
            strict_mode=getattr(args, "strict", False),
            min_inventory=getattr(args, "min_inventory", None),
            max_delivery_days=getattr(args, "max_delivery_days", None),
            max_shipping_fee=getattr(args, "max_shipping_fee", None),
            request_timeout_seconds=args.timeout,
            shop_type=getattr(args, "shop_type", "1688"),
            app_key=getattr(args, "app_key", None),
            app_secret=getattr(args, "app_secret", None),
            api_url=getattr(args, "api_url", None),
        )
        if getattr(args, "with_detail", True) and products:
            limit = max(0, int(getattr(args, "detail_limit", 0)))
            enrich_products_with_detail(
                products,
                get_detail_fn=lambda **kwargs: get_product_detail(
                    goods_id=kwargs.get("goods_id"),
                    shop_type=kwargs.get("shop_type"),
                    request_timeout_seconds=kwargs.get("request_timeout_seconds"),
                    app_key=getattr(args, "app_key", None),
                    app_secret=getattr(args, "app_secret", None),
                    api_url=None,
                ),
                shop_type=getattr(args, "shop_type", "1688"),
                request_timeout_seconds=args.timeout,
                limit=limit,
            )
        if getattr(args, "show_all_fields", False):
            display_all_search_result_items(products, show_empty=getattr(args, "show_empty_fields", False))
        elif getattr(args, "display_all", False):
            display_all_results_table(products)
        else:
            for p in products:
                print(json.dumps(p, ensure_ascii=False, indent=2))
        if getattr(args, "save_to_postgres", False) and products:
            saved = save_products_to_db(products, keyword=(getattr(args, "db_keyword", None) or args.keyword))
            print(f"Saved {saved} products to PostgreSQL.")
        return 0
    elif args.command == "keyword-search":
        # Prepare sort parameter if provided
        sort_param = None
        if getattr(args, "sort_field", None):
            sort_param = {args.sort_field: args.sort_order}
        
        # Make the keyword search API call
        response = keyword_search_products(
            keywords=args.keywords,
            shop_type=args.shop_type,
            page=args.page,
            page_size=args.page_size,
            price_start=getattr(args, "price_start", None),
            price_end=getattr(args, "price_end", None),
            sort=sort_param,
            region_opp=getattr(args, "region_opp", None),
            filter=getattr(args, "filter", None),
            category_id=getattr(args, "category_id", None),
            request_timeout_seconds=args.timeout,
            app_key=getattr(args, "app_key", None),
            app_secret=getattr(args, "app_secret", None),
            api_url=getattr(args, "api_url", None),
        )
        
        if args.verbose:
            print(f"API URL: {API_URL}")
            print(f"Keywords: {args.keywords}")
            print(f"Shop Type: {args.shop_type}")
            print(f"Page: {args.page}, Page Size: {args.page_size}")
            if sort_param:
                print(f"Sort: {sort_param}")
            print()
        
        if args.json:
            # Output raw JSON response
            print(json.dumps(response, ensure_ascii=False, indent=2))
        else:
            # Output parsed response (default)
            parsed_response = parse_keyword_search_response(response)
            print(json.dumps(parsed_response, ensure_ascii=False, indent=2))
        
        return 0
    elif args.command == "multi-category-search":
        # Prepare sort parameter if provided
        sort_param = None
        if getattr(args, "sort_field", None):
            sort_param = {args.sort_field: args.sort_order}
        
        # Make the multi-category search API call
        response = search_multiple_categories(
            category_ids=args.category_ids,
            shop_type=args.shop_type,
            page=args.page,
            page_size=args.page_size,
            max_products_per_category=args.max_products_per_category,
            price_start=getattr(args, "price_start", None),
            price_end=getattr(args, "price_end", None),
            sort=sort_param,
            region_opp=getattr(args, "region_opp", None),
            filter=getattr(args, "filter", None),
            request_timeout_seconds=args.timeout,
            app_key=getattr(args, "app_key", None),
            app_secret=getattr(args, "app_secret", None),
            api_url=getattr(args, "api_url", None),
        )
        
        if args.verbose:
            print(f"API URL: {API_URL}")
            print(f"Category IDs: {args.category_ids}")
            print(f"Shop Type: {args.shop_type}")
            print(f"Page: {args.page}, Page Size: {args.page_size}")
            print(f"Max Products per Category: {args.max_products_per_category}")
            if sort_param:
                print(f"Sort: {sort_param}")
            print()
        
        if args.json:
            # Output raw JSON response
            print(json.dumps(response, ensure_ascii=False, indent=2))
        else:
            # Output parsed response (default)
            print(json.dumps(response, ensure_ascii=False, indent=2))
        
        # Save to database if requested
        if getattr(args, "save_to_postgres", False) and response.get("success", False):
            products = response.get("data", {}).get("result", {}).get("products", [])
            if products:
                saved = save_products_to_db(products, keyword="multi-category-search")
                print(f"Saved {saved} products to PostgreSQL.")
        
        return 0
    elif args.command == "categories":
        products = search_products(
            args.keyword,
            page=args.page,
            page_size=args.page_size,
            request_timeout_seconds=args.timeout,
            shop_type=getattr(args, "shop_type", "1688"),
            app_key=getattr(args, "app_key", None),
            app_secret=getattr(args, "app_secret", None),
            api_url=getattr(args, "api_url", None),
        )
        if not products:
            print(" No products found.")
            return 0
        categories_info = get_available_categories(products)
        print(f" Found {len(products)} products with the following categories:")
        print("\nCategories:")
        for cat in categories_info["categories"]:
            print(f"  - {cat}")
        print("\nSubcategories:")
        for subcat in categories_info["subcategories"]:
            print(f"  - {subcat}")
        return 0
    elif args.command == "detail":
        detail = get_product_detail(
            goods_id=args.goods_id,
            shop_type=args.shop_type,
            request_timeout_seconds=args.timeout,
            app_key=getattr(args, "app_key", None),
            app_secret=getattr(args, "app_secret", None),
            api_url=getattr(args, "detail_api_url", None),
            normalize=getattr(args, "normalize", False),
        )
        if detail is None:
            print(" No detail returned.")
            return 1
        if getattr(args, "images_and_description", False):
            out: dict[str, Any] = {
                "images": detail.get("images", []),
                "description": detail.get("description", ""),
            }
            print(json.dumps(out, ensure_ascii=False, indent=2))
        elif getattr(args, "description_only", False):
            print(detail.get("description", ""))
        elif getattr(args, "images_only", False):
            print(json.dumps(detail.get("images", []), ensure_ascii=False, indent=2))
        else:
            print(json.dumps(detail, ensure_ascii=False, indent=2))
        return 0
    elif args.command == "image":
        result = get_image_id(
            image_base64=args.image_base64,
            request_timeout_seconds=args.timeout,
            app_key=getattr(args, "app_key", None),
            app_secret=getattr(args, "app_secret", None),
            api_url=getattr(args, "image_api_url", None),
        )
        if result is None:
            print(" No image ID returned.")
            return 1
        if getattr(args, "image_id_only", False):
            print(result.get("imageId", ""))
        elif getattr(args, "link_only", False):
            print(result.get("link", ""))
        else:
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    elif args.command == "console":
        products = search_products(
            args.keyword,
            page=args.page,
            page_size=args.page_size,
            request_timeout_seconds=args.timeout,
            shop_type=getattr(args, "shop_type", "1688"),
            app_key=getattr(args, "app_key", None),
            app_secret=getattr(args, "app_secret", None),
            api_url=getattr(args, "api_url", None),
        )
        if getattr(args, "with_detail", True) and products:
            limit = max(0, int(getattr(args, "detail_limit", 0)))
            enrich_products_with_detail(
                products,
                get_detail_fn=lambda **kwargs: get_product_detail(
                    goods_id=kwargs.get("goods_id"),
                    shop_type=kwargs.get("shop_type"),
                    request_timeout_seconds=kwargs.get("request_timeout_seconds"),
                    api_url=None,
                ),
                shop_type=getattr(args, "shop_type", "1688"),
                request_timeout_seconds=args.timeout,
                limit=limit,
            )
        if not products:
            print("No products found.")
            return 0
        console = SearchResultConsole(products)
        console.cmdloop()
        return 0
    elif args.command == "logistics":
        data = get_logistics(
            request_timeout_seconds=args.timeout,
            app_key=getattr(args, "app_key", None),
            app_secret=getattr(args, "app_secret", None),
            api_url=getattr(args, "logistics_api_url", None),
        )
        if not data:
            print(" No data returned.")
            return 1
        if getattr(args, "names_only", False):
            try:
                rows = data.get("data", [])
                for row in rows:
                    print(row.get("name"))
            except Exception:
                print(json.dumps(data, ensure_ascii=False, indent=2))
        elif getattr(args, "ids_only", False):
            try:
                rows = data.get("data", [])
                for row in rows:
                    print(row.get("id"))
            except Exception:
                print(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0
    elif args.command == "tags":
        data = get_tags(
            request_timeout_seconds=args.timeout,
            app_key=getattr(args, "app_key", None),
            app_secret=getattr(args, "app_secret", None),
            api_url=getattr(args, "tags_api_url", None),
        )
        if not data:
            print(" No data returned.")
            return 1
        if getattr(args, "types_only", False):
            try:
                rows = data.get("data", [])
                for row in rows:
                    print(row.get("type"))
            except Exception:
                print(json.dumps(data, ensure_ascii=False, indent=2))
        elif getattr(args, "translations_only", False):
            try:
                rows = data.get("data", [])
                for row in rows:
                    print(row.get("japanese"))
            except Exception:
                print(json.dumps(data, ensure_ascii=False, indent=2))
        else:
            print(json.dumps(data, ensure_ascii=False, indent=2))
        return 0
    elif args.command == "process-images":
        save_locally = not getattr(args, "no_save", False)
        output_dir = getattr(args, "output_dir", "processed_images")
        
        if getattr(args, "test_image", None):
            # Test processing on a single image
            processor = ImageProcessor()
            result = processor.process_image(args.test_image, save_locally=save_locally, output_dir=output_dir)
            
            if result:
                print(f"Processing completed successfully!")
                print(f"Original URL: {result['original_url']}")
                print(f"Processed size: {result['processed_size']} bytes")
                print(f"Text detections: {result['text_detections']}")
                print(f"Face detections: {result['face_detections']}")
                print(f"Logo detections: {result['logo_detections']}")
                if result['local_path']:
                    print(f"Saved to: {result['local_path']}")
                else:
                    print("Image not saved to local storage (--no-save flag used)")
            else:
                print("Failed to process image")
                return 1
        elif getattr(args, "product_id", None):
            # Process images for a specific product
            result = process_product_images_from_db(args.product_id, save_locally=save_locally, output_dir=output_dir)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        else:
            # Process images for all products
            limit = getattr(args, "limit", None)
            result = process_all_product_images(limit=limit)
            print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    # optimize-names command removed
    else:
        # Defer to main.py handlers for the rest to avoid duplication here
        # For now, return non-zero to indicate not handled
        return 2


