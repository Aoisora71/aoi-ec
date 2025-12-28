from typing import List, Optional


def enrich_products_with_detail(
    products: List[dict],
    get_detail_fn,
    shop_type: str,
    request_timeout_seconds: int,
    limit: Optional[int] = None,
) -> None:
    """
    Enrich the given products list in-place by fetching detail for up to `limit` items.
    If limit is None or 0, enrich all items. Missing goodsId are skipped.
    The `get_detail_fn` must be a callable with signature (goods_id, shop_type, request_timeout_seconds, **kwargs).
    """
    if not products:
        return

    if limit is None or limit == 0:
        num_to_enrich = len(products)
    else:
        num_to_enrich = max(0, min(limit, len(products)))

    for idx in range(num_to_enrich):
        item = products[idx]
        goods_id = str(item.get("goodsId", ""))
        if not goods_id:
            continue
        detail = get_detail_fn(
            goods_id=goods_id,
            shop_type=item.get("shopType", shop_type),
            request_timeout_seconds=request_timeout_seconds,
            normalize=True,
        )
        if detail:
            # Preserve existing fields for backward compatibility
            item["detailImages"] = detail.get("images", [])
            item["detailDescription"] = detail.get("description", "")
            # Add normalized payload for richer display
            item["detailNormalized"] = detail


