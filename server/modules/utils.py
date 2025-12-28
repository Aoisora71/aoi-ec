from typing import Optional, Dict, Any


def convert_rmb_to_jpy(rmb_price: float, exchange_rate: float = 20.0) -> float:
    """Convert RMB price to JPY."""
    return rmb_price * exchange_rate


def convert_jpy_to_rmb(jpy_price: float, exchange_rate: float = 20.0) -> float:
    """Convert JPY price to RMB."""
    return jpy_price / exchange_rate


def get_product_price_in_jpy(product: Dict[str, Any], exchange_rate: float = 20.0) -> Optional[float]:
    """Extract and convert product price to JPY. Returns None if unavailable."""
    price_fields = ["goodsPrice", "price", "productPrice", "salePrice", "marketPrice"]
    for field in price_fields:
        price = product.get(field)
        if price is None:
            continue
        try:
            if isinstance(price, str):
                import re
                price_clean = re.sub(r'[^\d.,]', '', price)
                if not price_clean:
                    continue
                price_clean = price_clean.replace(',', '.')
                price_val = float(price_clean)
            else:
                price_val = float(price)
            return convert_rmb_to_jpy(price_val, exchange_rate)
        except (ValueError, TypeError):
            continue
    return None


