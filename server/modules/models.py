from typing import TypedDict, NotRequired, List, Dict, Any


class ShopInfo(TypedDict, total=False):
    shopName: str
    address: NotRequired[str]
    wangwang: NotRequired[str]


class Product(TypedDict, total=False):
    goodsId: str
    titleC: str
    titleT: str
    goodsPrice: str | float
    monthSold: int
    shopInfo: ShopInfo | Dict[str, Any]
    repurchaseRate: NotRequired[str | float]
    tradeScore: NotRequired[str | float]
    topCategoryId: NotRequired[str]
    secondCategoryId: NotRequired[str]
    createDate: NotRequired[str]
    detailImages: NotRequired[List[str]]
    detailDescription: NotRequired[str]
    dimensions: NotRequired[Dict[str, Any]]
    size: NotRequired[Dict[str, Any]]
    specs: NotRequired[Dict[str, Any]]


class OrderItemProp(TypedDict, total=False):
    key: str
    value: str


class OrderItemTag(TypedDict, total=False):
    type: str
    no: str
    goods_no: NotRequired[str]


class OrderItemOption(TypedDict, total=False):
    name: str
    num: int | str


class OrderItem(TypedDict, total=False):
    link: str
    price: str | float
    num: int | str
    pic: NotRequired[str]
    remark: NotRequired[str]
    fba: NotRequired[str]
    asin: NotRequired[str]
    props: NotRequired[List[OrderItemProp]]
    option: NotRequired[List[OrderItemOption]]
    tags: NotRequired[List[OrderItemTag]]


class Order(TypedDict, total=False):
    purchase_order: str
    status: str
    goods: List[OrderItem]
    logistics_id: NotRequired[str]
    remark: NotRequired[str]


class PorderDetailTag(TypedDict, total=False):
    type: NotRequired[str]
    no: NotRequired[str]
    goods_no: NotRequired[str]
    text_line_one: NotRequired[str]
    text_line_two: NotRequired[str]


class PorderDetailItem(TypedDict, total=False):
    order_sn: str
    sorting: NotRequired[int | str]
    num: int | str
    client_remark: NotRequired[str]
    porder_detail_tag: NotRequired[List[PorderDetailTag]]


class Address(TypedDict, total=False):
    name: NotRequired[str]
    phone: NotRequired[str]
    address: NotRequired[str]
    zipcode: NotRequired[str]


class Porder(TypedDict, total=False):
    status: str
    logistics_id: str
    porder_detail: List[PorderDetailItem]
    client_remark: NotRequired[str]
    receiver_address: NotRequired[Address]
    importer_address: NotRequired[Address]
    porder_file: NotRequired[List[dict]]


