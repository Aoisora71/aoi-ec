from typing import List, Optional, Dict, Any
import time
import json
import requests
import logging
from .sign import md5_sign
from .http import safe_post_json
from .config import APP_KEY, APP_SECRET, API_URL, DETAIL_API_URL, IMAGE_ID_API_URL

logger = logging.getLogger(__name__)


def generate_sign(app_key: str, app_secret: str, timestamp: str) -> str:
    # Rakumart open API expects MD5(app_key + app_secret + timestamp)
    return md5_sign(app_key, app_secret, timestamp)


def keyword_search_products(
    keywords: str,
    shop_type: str = "1688",
    page: int = 1,
    page_size: int = 50,
    price_start: Optional[str] = None,
    price_end: Optional[str] = None,
    sort: Optional[Dict[str, str]] = None,
    region_opp: Optional[str] = None,
    filter: Optional[str] = None,
    category_id: Optional[str] = None,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Search products by keywords using the Rakumart API.
    
    Args:
        keywords: Search keywords (required)
        shop_type: Shop type (1688, taobao, tmall) - default is 1688
        page: Page number - default is 1
        page_size: Number of items per page - default is 50
        price_start: Starting price filter
        price_end: Ending price filter
        sort: Sort options (e.g., {"rePurchaseRate": "desc"})
        region_opp: Region option (jpOpp for Japanese hot, krOpp for Korean hot)
        filter: Filter options (certifiedFactory, shipIn48Hours, etc.)
        category_id: Category ID for filtering
        request_timeout_seconds: Request timeout
        app_key: API key (uses config default if not provided)
        app_secret: API secret (uses config default if not provided)
        api_url: API URL (uses config default if not provided)
    
    Returns:
        Dictionary containing the API response with product data
    """
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp) if resolved_app_key and resolved_app_secret else ""
    
    # Prepare multipart/form-data payload
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "keywords": (None, keywords),
        "shop_type": (None, shop_type),
        "page": (None, str(page)),
        "pageSize": (None, str(page_size)),
    }
    
    # Add optional parameters
    if price_start is not None:
        files["priceStart"] = (None, str(price_start))
        logger.info(f"Adding priceStart parameter: {price_start}")
    if price_end is not None:
        files["priceEnd"] = (None, str(price_end))
        logger.info(f"Adding priceEnd parameter: {price_end}")
    if sort is not None:
        files["sort"] = (None, json.dumps(sort))
        logger.info(f"Adding sort parameter: {sort}")
    if region_opp is not None:
        files["regionOpp"] = (None, region_opp)
        logger.info(f"Adding regionOpp parameter: {region_opp}")
    if filter is not None:
        files["filter"] = (None, filter)
        logger.info(f"Adding filter parameter: {filter}")
    if category_id is not None:
        files["categoryId"] = (None, str(category_id))
        logger.info(f"Adding categoryId parameter: {category_id}")
    
    # Make the API request
    logger.info(f"Making API request to {resolved_api_url} with parameters: {list(files.keys())}")
    data = safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)
    
    if data is None:
        logger.error("API request returned None - connection failed")
        return {"success": False, "error": "API request failed"}
    
    logger.info(f"API response received: success={data.get('success')}, has_data={'data' in data}")
    
    # Log the raw response structure for debugging
    if not data.get("success", False):
        logger.error(f"API returned error: {data}")
    else:
        logger.info(f"API response structure: {list(data.keys())}")
        if "data" in data:
            logger.info(f"Data structure: {list(data['data'].keys()) if isinstance(data['data'], dict) else type(data['data'])}")
    
    return data


def search_multiple_categories(
    category_ids: List[str],
    shop_type: str = "1688",
    page: int = 1,
    page_size: int = 50,
    price_start: Optional[str] = None,
    price_end: Optional[str] = None,
    sort: Optional[Dict[str, str]] = None,
    region_opp: Optional[str] = None,
    filter: Optional[str] = None,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
    max_products_per_category: int = 100,
) -> Dict[str, Any]:
    """
    Search products across multiple categories using the Rakumart API.
    
    Args:
        category_ids: List of category IDs to search
        shop_type: Shop type (1688, taobao, tmall) - default is 1688
        page: Page number - default is 1
        page_size: Number of items per page per category - default is 50
        price_start: Starting price filter
        price_end: Ending price filter
        sort: Sort options (e.g., {"rePurchaseRate": "desc"})
        region_opp: Region option (jpOpp for Japanese hot, krOpp for Korean hot)
        filter: Filter options (certifiedFactory, shipIn48Hours, etc.)
        request_timeout_seconds: Request timeout
        app_key: API key (uses config default if not provided)
        app_secret: API secret (uses config default if not provided)
        api_url: API URL (uses config default if not provided)
        max_products_per_category: Maximum products to fetch per category
    
    Returns:
        Dictionary containing combined results from all categories
    """
    if not category_ids:
        logger.error("No category IDs provided for multi-category search")
        return {
            "success": False,
            "error": "No category IDs provided",
            "data": None
        }
    
    logger.info(f"Starting multi-category search for {len(category_ids)} categories: {category_ids}")
    
    all_products = []
    category_results = {}
    total_found = 0
    successful_categories = 0
    failed_categories = []
    
    print(f"Searching {len(category_ids)} categories...")
    logger.info(f"Searching {len(category_ids)} categories...")
    
    for i, category_id in enumerate(category_ids, 1):
        print(f"Searching category {i}/{len(category_ids)}: {category_id}")
        
        try:
            # Search each category
            response = keyword_search_products(
                keywords="",  # Empty keywords for category search
                shop_type=shop_type,
                page=page,
                page_size=min(page_size, max_products_per_category),
                price_start=price_start,
                price_end=price_end,
                sort=sort,
                region_opp=region_opp,
                filter=filter,
                category_id=category_id,
                request_timeout_seconds=request_timeout_seconds,
                app_key=app_key,
                app_secret=app_secret,
                api_url=api_url,
            )
            
            if response.get("success", False):
                parsed = parse_keyword_search_response(response)
                if parsed["success"]:
                    products = parsed["data"]["result"]["products"]
                    category_total = parsed["data"]["result"]["total"]
                    
                    # Add category information to each product
                    for product in products:
                        product["source_category_id"] = category_id
                    
                    all_products.extend(products)
                    category_results[category_id] = {
                        "total": category_total,
                        "returned": len(products),
                        "products": products
                    }
                    total_found += category_total
                    successful_categories += 1
                    
                    print(f"  ✓ Found {len(products)} products (total: {category_total})")
                else:
                    print(f"  ✗ Failed to parse response: {parsed.get('error', 'Unknown error')}")
                    failed_categories.append(category_id)
            else:
                print(f"  ✗ API request failed: {response.get('error', 'Unknown error')}")
                failed_categories.append(category_id)
                
        except Exception as e:
            error_msg = str(e)
            print(f"  ✗ Exception occurred: {error_msg}")
            logger.error(f"Exception searching category {category_id}: {error_msg}", exc_info=True)
            failed_categories.append(category_id)
        
        # Add small delay between requests to avoid rate limiting
        if i < len(category_ids):
            time.sleep(0.5)
    
    # Combine results
    logger.info(f"Multi-category search completed: {successful_categories} successful, {len(failed_categories)} failed, {len(all_products)} total products")
    
    combined_response = {
        "success": successful_categories > 0,
        "data": {
            "data_type": "goodsList",
            "result": {
                "total": total_found,
                "products": all_products,
                "category_breakdown": category_results,
                "summary": {
                    "total_categories_searched": len(category_ids),
                    "successful_categories": successful_categories,
                    "failed_categories": len(failed_categories),
                    "total_products_found": len(all_products),
                    "failed_category_ids": failed_categories
                }
            }
        }
    }
    
    if failed_categories:
        warning_msg = f"Failed to search {len(failed_categories)} categories: {failed_categories}"
        combined_response["warnings"] = warning_msg
        logger.warning(warning_msg)
    
    if successful_categories == 0:
        error_msg = f"All {len(category_ids)} categories failed to search"
        logger.error(error_msg)
        combined_response["error"] = error_msg
    
    return combined_response


def parse_keyword_search_response(response: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse the keyword search API response according to the documented structure.
    
    Args:
        response: Raw API response dictionary
    
    Returns:
        Parsed response with standardized structure
    """
    logger.info(f"Parsing API response: success={response.get('success')}")
    
    if not response.get("success", False):
        error_msg = response.get("error", "Unknown error")
        logger.error(f"API response indicates failure: {error_msg}")
        return {
            "success": False,
            "error": error_msg,
            "data": None
        }
    
    try:
        data = response.get("data", {})
        logger.info(f"Response data structure: {list(data.keys()) if isinstance(data, dict) else type(data)}")
        
        # Handle different possible response structures
        products = []
        total = 0
        
        # Try different possible paths for products
        if isinstance(data, dict):
            # Structure 1: data.result.result
            if "result" in data and isinstance(data["result"], dict):
                result = data["result"]
                total = result.get("total", 0)
                products = result.get("result", [])
                logger.info(f"Found products via data.result.result: {len(products)} products")
            
            # Structure 2: data.result (direct array)
            elif "result" in data and isinstance(data["result"], list):
                products = data["result"]
                total = len(products)
                logger.info(f"Found products via data.result (direct array): {len(products)} products")
            
            # Structure 3: data.products
            elif "products" in data and isinstance(data["products"], list):
                products = data["products"]
                total = data.get("total", len(products))
                logger.info(f"Found products via data.products: {len(products)} products")
            
            # Structure 4: data.data.result
            elif "data" in data and isinstance(data["data"], dict):
                inner_data = data["data"]
                if "result" in inner_data and isinstance(inner_data["result"], list):
                    products = inner_data["result"]
                    total = inner_data.get("total", len(products))
                    logger.info(f"Found products via data.data.result: {len(products)} products")
        
        # If still no products, log the full structure for debugging
        if not products:
            logger.warning("No products found in response. Full response structure:")
            logger.warning(json.dumps(response, ensure_ascii=False, indent=2))
        
        parsed_response = {
            "success": True,
            "data": {
                "data_type": data.get("data_type") if isinstance(data, dict) else None,
                "result": {
                    "total": total,
                    "products": []
                }
            }
        }
        
        # Parse products
        for i, product in enumerate(products):
            if not isinstance(product, dict):
                logger.warning(f"Product {i} is not a dict: {type(product)}")
                continue
                
            # Extract image URL - check multiple possible field names
            img_url = (
                product.get("imgUrl") or 
                product.get("imageUrl") or 
                product.get("image_url") or 
                product.get("img_url") or
                product.get("image") or
                (product.get("images", [])[0] if isinstance(product.get("images"), list) and len(product.get("images", [])) > 0 else None)
            )
            
            # Extract description - check multiple possible field names
            detail_description = (
                product.get("detailDescription") or 
                product.get("detail_description") or
                product.get("description") or
                product.get("desc") or
                product.get("detailDesc")
            )
            
            parsed_product = {
                "shopType": product.get("shopType"),
                "goodsId": product.get("goodsId"),
                "titleC": product.get("titleC"),
                "titleT": product.get("titleT"),
                "traceInfo": product.get("traceInfo"),
                "goodsPrice": product.get("goodsPrice"),
                "imgUrl": img_url,  # Use the extracted image URL
                "monthSold": product.get("monthSold"),
                "isJxhy": product.get("isJxhy"),
                "goodsTags": product.get("goodsTags"),
                "shopCity": product.get("shopCity"),
                "repurchaseRate": product.get("repurchaseRate"),
                "sellerIdentities": product.get("sellerIdentities"),
                "createDate": product.get("createDate"),
                "tradeScore": product.get("tradeScore"),
                "rating": product.get("rating"),
                "topCategoryId": product.get("topCategoryId"),
                "secondCategoryId": product.get("secondCategoryId"),
                "shopInfo": product.get("shopInfo"),
                "detailDescription": detail_description,  # Use the extracted description
                "dimensions": product.get("dimensions"),
                "size": product.get("size"),
            }
            
            # Preserve original product data for debugging and additional fields
            # This ensures we don't lose any data that might be useful later
            for key, value in product.items():
                if key not in parsed_product and value is not None:
                    # Only add non-None values to avoid clutter
                    parsed_product[key] = value
            parsed_response["data"]["result"]["products"].append(parsed_product)
        
        logger.info(f"Successfully parsed {len(parsed_response['data']['result']['products'])} products")
        return parsed_response
        
    except (KeyError, TypeError) as e:
        logger.error(f"Failed to parse response: {str(e)}")
        logger.error(f"Response structure: {json.dumps(response, ensure_ascii=False, indent=2)}")
        return {
            "success": False,
            "error": f"Failed to parse response: {str(e)}",
            "data": None
        }


def search_products(
    keyword: str,
    page: int = 1,
    page_size: int = 20,
    shop_type: str = "1688",
    price_min: Optional[str] = None,
    price_max: Optional[str] = None,
    order_key: Optional[str] = None,
    order_value: Optional[str] = None,
    categories: Optional[List[str]] = None,
    subcategories: Optional[List[str]] = None,
    max_length: Optional[float] = None,
    max_width: Optional[float] = None,
    max_height: Optional[float] = None,
    max_weight: Optional[float] = None,
    jpy_price_min: Optional[float] = None,
    jpy_price_max: Optional[float] = None,
    exchange_rate: float = 20.0,
    strict_mode: bool = False,
    min_inventory: Optional[int] = None,
    max_delivery_days: Optional[int] = None,
    max_shipping_fee: Optional[float] = None,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
    apply_filters_fn=None,
) -> List[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp) if resolved_app_key and resolved_app_secret else ""
    payload: Dict[str, Any] = {
        "app_key": resolved_app_key,
        "timestamp": timestamp,
        "sign": sign,
        "keywords": keyword,
        "shop_type": shop_type,
        "page": str(page),
        "pageSize": str(page_size),
    }
    if price_min is not None:
        payload["price_min"] = str(price_min)
    if price_max is not None:
        payload["price_max"] = str(price_max)
    if order_key is not None:
        payload["order_by[0][key]"] = order_key
    if order_value is not None:
        payload["order_by[0][value]"] = order_value
    if categories is not None:
        for i, category in enumerate(categories):
            payload[f"categories[{i}]"] = str(category)
    if subcategories is not None:
        for i, subcategory in enumerate(subcategories):
            payload[f"subcategories[{i}]"] = str(subcategory)

    if max_length is not None:
        payload["max_length"] = str(max_length)
    if max_width is not None:
        payload["max_width"] = str(max_width)
    if max_height is not None:
        payload["max_height"] = str(max_height)
    if min_inventory is not None:
        payload["min_inventory"] = str(min_inventory)
    if max_delivery_days is not None:
        payload["max_delivery_days"] = str(max_delivery_days)
    if max_shipping_fee is not None:
        payload["max_shipping_fee"] = str(max_shipping_fee)

    data = safe_post_json(resolved_api_url, data=payload, timeout=request_timeout_seconds)
    if data is None:
        return []

    if not data.get("success", False):
        print(" API request failed:", data)
        return []

    try:
        products = data["data"]["result"]["result"]
    except (KeyError, TypeError):
        print(" Unexpected API response structure:")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return []

    if apply_filters_fn:
        products = apply_filters_fn(
            products,
            categories=categories,
            subcategories=subcategories,
            max_length=max_length,
            max_width=max_width,
            max_height=max_height,
            max_weight=max_weight,
            jpy_price_min=jpy_price_min,
            jpy_price_max=jpy_price_max,
            exchange_rate=exchange_rate,
            strict_mode=strict_mode,
            min_inventory=min_inventory,
            max_delivery_days=max_delivery_days,
            max_shipping_fee=max_shipping_fee,
        )

    return products


def _normalize_detail_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Best-effort normalization of product detail data shape per API spec."""
    out: Dict[str, Any] = {}
    # Top-level basics
    out["fromUrl"] = payload.get("fromUrl")
    out["fromPlatform"] = payload.get("fromPlatform")
    out["fromPlatform_logo"] = payload.get("fromPlatform_logo")
    out["shopId"] = str(payload.get("shopId")) if payload.get("shopId") is not None else None
    out["shopName"] = payload.get("shopName")
    out["goodsId"] = str(payload.get("goodsId")) if payload.get("goodsId") is not None else None
    out["titleC"] = payload.get("titleC")
    out["titleT"] = payload.get("titleT")
    out["video"] = payload.get("video")
    out["images"] = payload.get("images") if isinstance(payload.get("images"), list) else []
    out["address"] = payload.get("address")
    out["description"] = payload.get("description")

    goods_info = payload.get("goodsInfo") if isinstance(payload.get("goodsInfo"), dict) else {}
    out_goods: Dict[str, Any] = {}
    out_goods["unit"] = goods_info.get("unit")
    out_goods["minOrderQuantity"] = goods_info.get("minOrderQuantity")
    out_goods["priceRangesType"] = goods_info.get("priceRangesType")

    # priceRanges: list of {priceMin, priceMax, startQuantity}
    price_ranges = []
    for pr in goods_info.get("priceRanges", []) or []:
        if not isinstance(pr, dict):
            continue
        price_ranges.append({
            "priceMin": pr.get("priceMin"),
            "priceMax": pr.get("priceMax"),
            "startQuantity": pr.get("startQuantity"),
        })
    out_goods["priceRanges"] = price_ranges

    # specification: list of {keyC, keyT, valueC[{name,picUrl}], valueT[{name,picUrl}]}
    specs_out = []
    for spec in goods_info.get("specification", []) or []:
        if not isinstance(spec, dict):
            continue
        vals_c = []
        for v in spec.get("valueC", []) or []:
            if isinstance(v, dict):
                vals_c.append({"name": v.get("name"), "picUrl": v.get("picUrl")})
        vals_t = []
        for v in spec.get("valueT", []) or []:
            if isinstance(v, dict):
                vals_t.append({"name": v.get("name"), "picUrl": v.get("picUrl")})
        specs_out.append({
            "keyC": spec.get("keyC"),
            "keyT": spec.get("keyT"),
            "valueC": vals_c,
            "valueT": vals_t,
        })
    out_goods["specification"] = specs_out

    # goodsInventory: list items with keyC/keyT and valueC/valueT arrays of sku entries
    inv_out = []
    for inv in goods_info.get("goodsInventory", []) or []:
        if not isinstance(inv, dict):
            continue
        def _coerce_entries(entries):
            res = []
            for e in entries or []:
                if not isinstance(e, dict):
                    continue
                res.append({
                    "startQuantity": e.get("startQuantity"),
                    "price": e.get("price"),
                    "amountOnSale": e.get("amountOnSale"),
                    "skuId": e.get("skuId"),
                    "specId": e.get("specId"),
                })
            return res
        inv_out.append({
            "keyC": inv.get("keyC"),
            "keyT": inv.get("keyT"),
            "valueC": _coerce_entries(inv.get("valueC")),
            "valueT": _coerce_entries(inv.get("valueT")),
        })
    out_goods["goodsInventory"] = inv_out

    # detail: list of key/value pairs
    detail_rows = []
    for row in goods_info.get("detail", []) or []:
        if isinstance(row, dict):
            detail_rows.append({
                "keyC": row.get("keyC"),
                "valueC": row.get("valueC"),
                "keyT": row.get("keyT"),
                "valueT": row.get("valueT"),
            })
    out_goods["detail"] = detail_rows

    out["goodsInfo"] = out_goods
    return out


def get_product_detail(
    goods_id: str,
    shop_type: str = "1688",
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
    *,
    normalize: bool = False,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or DETAIL_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp) if resolved_app_key and resolved_app_secret else ""
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "shopType": (None, shop_type),
        "goodsId": (None, str(goods_id)),
    }
    data = safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)
    if data is None:
        return None
    if not data.get("success", False):
        print(" Detail API failed:", data)
        return None
    try:
        detail = data["data"]
    except (KeyError, TypeError):
        print(" Unexpected detail API response structure:")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return None

    if normalize:
        try:
            return _normalize_detail_payload(detail)
        except Exception:
            # Fall back to raw detail in case of unexpected structures
            return detail
    return detail


def get_image_id(
    image_base64: str,
    request_timeout_seconds: int = 15,
    app_key: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_url: Optional[str] = None,
) -> Optional[dict]:
    timestamp = str(int(time.time()))
    resolved_app_key = app_key or APP_KEY
    resolved_app_secret = app_secret or APP_SECRET
    resolved_api_url = api_url or IMAGE_ID_API_URL
    sign = generate_sign(resolved_app_key, resolved_app_secret, timestamp) if resolved_app_key and resolved_app_secret else ""
    files = {
        "app_key": (None, resolved_app_key),
        "timestamp": (None, timestamp),
        "sign": (None, sign),
        "imageBase64": (None, image_base64),
    }
    data = safe_post_json(resolved_api_url, files=files, timeout=request_timeout_seconds)
    if data is None:
        return None
    if not data.get("success", False):
        print(" Image ID API failed:", data)
        return None
    try:
        return data["data"]
    except (KeyError, TypeError):
        print(" Unexpected image ID API response structure:")
        print(json.dumps(data, ensure_ascii=False, indent=2))
        return None


