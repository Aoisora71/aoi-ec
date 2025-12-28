from typing import List, Dict, Any, Optional
from .utils import get_product_price_in_jpy


def filter_products_by_size(products: List[dict], max_length: Optional[float] = None,
                            max_width: Optional[float] = None, max_height: Optional[float] = None,
                            strict_mode: bool = False) -> List[dict]:
    if not any([max_length, max_width, max_height]):
        return products
    filtered: List[dict] = []
    for product in products:
        dimensions = product.get("dimensions", {}) or product.get("size", {}) or product.get("specs", {})
        length = dimensions.get("length") or dimensions.get("l") or dimensions.get("长")
        width = dimensions.get("width") or dimensions.get("w") or dimensions.get("宽")
        height = dimensions.get("height") or dimensions.get("h") or dimensions.get("高")
        if strict_mode and (length is None and max_length is not None or
                            width is None and max_width is not None or
                            height is None and max_height is not None):
            continue
        if max_length is not None and length is not None and float(length) > max_length:
            continue
        if max_width is not None and width is not None and float(width) > max_width:
            continue
        if max_height is not None and height is not None and float(height) > max_height:
            continue
        filtered.append(product)
    return filtered


def filter_products_by_inventory(products: List[dict], min_inventory: int,
                                 strict_mode: bool = False) -> List[dict]:
    if not min_inventory:
        return products
    filtered: List[dict] = []
    for product in products:
        inventory = product.get("inventory") or product.get("stock") or product.get("quantity")
        if inventory is None:
            if not strict_mode:
                filtered.append(product)
            continue
        try:
            if int(inventory) >= min_inventory:
                filtered.append(product)
        except (TypeError, ValueError):
            if not strict_mode:
                filtered.append(product)
    return filtered


def filter_products_by_delivery(products: List[dict], max_delivery_days: int,
                                strict_mode: bool = False) -> List[dict]:
    if not max_delivery_days:
        return products
    filtered: List[dict] = []
    for product in products:
        delivery = product.get("delivery_days") or product.get("shipping_days") or product.get("delivery_time")
        if delivery is None:
            if not strict_mode:
                filtered.append(product)
            continue
        try:
            if int(delivery) <= max_delivery_days:
                filtered.append(product)
        except (TypeError, ValueError):
            if not strict_mode:
                filtered.append(product)
    return filtered


def filter_products_by_shipping_fee(products: List[dict], max_shipping_fee: float,
                                    strict_mode: bool = False) -> List[dict]:
    if not max_shipping_fee and max_shipping_fee != 0:
        return products
    filtered: List[dict] = []
    for product in products:
        shipping_fee = product.get("shipping_fee") or product.get("shipping_cost") or product.get("delivery_fee")
        if shipping_fee is None:
            if not strict_mode:
                filtered.append(product)
            continue
        try:
            if float(shipping_fee) <= float(max_shipping_fee):
                filtered.append(product)
        except (TypeError, ValueError):
            if not strict_mode:
                filtered.append(product)
    return filtered


def filter_products_by_weight(products: List[dict], max_weight: float,
                              strict_mode: bool = False) -> List[dict]:
    if not max_weight and max_weight != 0:
        return products
    filtered: List[dict] = []
    for product in products:
        weight = (product.get("weight") or product.get("product_weight") or
                  product.get("net_weight") or product.get("gross_weight"))
        if weight is None:
            if not strict_mode:
                filtered.append(product)
            continue
        try:
            if float(weight) <= float(max_weight):
                filtered.append(product)
        except (TypeError, ValueError):
            if not strict_mode:
                filtered.append(product)
    return filtered


def filter_products_by_jpy_price(products: List[dict], jpy_price_min: Optional[float] = None,
                                 jpy_price_max: Optional[float] = None, exchange_rate: float = 20.0,
                                 strict_mode: bool = False) -> List[dict]:
    if not any([jpy_price_min, jpy_price_max]):
        return products
    filtered: List[dict] = []
    for product in products:
        jpy_price = get_product_price_in_jpy(product, exchange_rate)
        if jpy_price is None:
            if not strict_mode:
                filtered.append(product)
            continue
        if jpy_price_min and jpy_price < jpy_price_min:
            continue
        if jpy_price_max and jpy_price > jpy_price_max:
            continue
        filtered.append(product)
    return filtered


def filter_products_by_categories(products: List[dict], categories: Optional[List[str]] = None,
                                  subcategories: Optional[List[str]] = None) -> List[dict]:
    if not any([categories, subcategories]):
        return products
    filtered: List[dict] = []
    for product in products:
        category_info = product.get("category") or product.get("categoryInfo") or {}
        if not category_info:
            filtered.append(product)
            continue
        if categories:
            main_cat = category_info.get("category") or category_info.get("mainCategory") or category_info.get("一级类目")
            if not main_cat or main_cat not in categories:
                continue
        if subcategories:
            sub_cat = category_info.get("subcategory") or category_info.get("subCategory") or category_info.get("二级类目")
            if not sub_cat or sub_cat not in subcategories:
                continue
        filtered.append(product)
    return filtered


def apply_product_filters(products: List[dict], categories: Optional[List[str]] = None,
                          subcategories: Optional[List[str]] = None,
                          max_length: Optional[float] = None, max_width: Optional[float] = None,
                          max_height: Optional[float] = None, max_weight: Optional[float] = None,
                          jpy_price_min: Optional[float] = None, jpy_price_max: Optional[float] = None,
                          exchange_rate: float = 20.0, strict_mode: bool = False,
                          min_inventory: Optional[int] = None, max_delivery_days: Optional[int] = None,
                          max_shipping_fee: Optional[float] = None) -> List[dict]:
    filtered_products = products
    filtered_products = filter_products_by_categories(filtered_products, categories, subcategories)
    filtered_products = filter_products_by_jpy_price(filtered_products, jpy_price_min, jpy_price_max, exchange_rate, strict_mode)
    filtered_products = filter_products_by_size(filtered_products, max_length, max_width, max_height, strict_mode)
    if min_inventory is not None:
        filtered_products = filter_products_by_inventory(filtered_products, min_inventory, strict_mode)
    if max_delivery_days is not None:
        filtered_products = filter_products_by_delivery(filtered_products, max_delivery_days, strict_mode)
    if max_shipping_fee is not None:
        filtered_products = filter_products_by_shipping_fee(filtered_products, max_shipping_fee, strict_mode)
    if max_weight is not None:
        filtered_products = filter_products_by_weight(filtered_products, max_weight, strict_mode)
    return filtered_products


def collect_categories_from_products(products: List[dict]) -> Dict[str, List[str]]:
    categories = set()
    subcategories = set()
    for product in products:
        category_info = product.get("category") or product.get("categoryInfo") or {}
        if isinstance(category_info, dict):
            main_cat = category_info.get("category") or category_info.get("mainCategory") or category_info.get("一级类目")
            if main_cat:
                categories.add(main_cat)
            sub_cat = category_info.get("subcategory") or category_info.get("subCategory") or category_info.get("二级类目")
            if sub_cat:
                subcategories.add(sub_cat)
        elif isinstance(category_info, str):
            categories.add(category_info)
    return {
        "categories": sorted(list(categories)),
        "subcategories": sorted(list(subcategories)),
    }



