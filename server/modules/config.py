import os
import pathlib

# Load environment variables from a .env file if python-dotenv is available
try:
    from dotenv import load_dotenv, find_dotenv  # type: ignore
except Exception:
    load_dotenv = None  # type: ignore
else:
    try:
        # Prefer loading .env from the project root (one level above this file)
        package_dir = pathlib.Path(__file__).resolve().parent
        project_root = package_dir.parent
        root_env = project_root / ".env"
        if root_env.exists():
            load_dotenv(dotenv_path=str(root_env), override=False)
        else:
            # Locate nearest .env (walking up) and load if found
            _dotenv_path = find_dotenv(usecwd=True)
            if _dotenv_path:
                load_dotenv(_dotenv_path, override=False)
            else:
                # Fallback to default behavior
                load_dotenv()
    except Exception:
        # Non-fatal: continue without .env
        pass

# Credentials
APP_KEY = os.getenv("APP_KEY", "56832_68d09f0d2a2c6")
APP_SECRET = os.getenv("APP_SECRET", "eEtXGkyf9HFIsZ2i!ZOv")

# Search/Detail/Image APIs
API_URL = os.getenv("API_URL", "https://apiwww.rakumart.com/open/goods/keywordsSearch")
DETAIL_API_URL = os.getenv("DETAIL_API_URL", "https://apiwww.rakumart.com/open/goods/detail")
IMAGE_ID_API_URL = os.getenv("IMAGE_ID_API_URL", "https://apiwww.rakumart.com/open/goods/getImageId")

# Logistics/Tags
LOGISTICS_API_URL = os.getenv("LOGISTICS_API_URL", "https://apiwww.rakumart.com/open/currentUsefulLogistics")
TAGS_API_URL = os.getenv("TAGS_API_URL", "https://apiwww.rakumart.com/open/currentUsefulTags")
LOGISTICS_TRACK_API_URL = os.getenv("LOGISTICS_TRACK_API_URL", "https://apiwww.rakumart.com/open/logisticsTrack")

# Orders
CREATE_ORDER_API_URL = os.getenv("CREATE_ORDER_API_URL", "https://apiwww.rakumart.com/open/createOrder")
UPDATE_ORDER_STATUS_API_URL = os.getenv("UPDATE_ORDER_STATUS_API_URL", "https://apiwww.rakumart.com/open/updateOrderStatus")
CANCEL_ORDER_API_URL = os.getenv("CANCEL_ORDER_API_URL", "https://apiwww.rakumart.com/open/cancelOrder")
ORDER_LIST_API_URL = os.getenv("ORDER_LIST_API_URL", "https://apiwww.rakumart.com/open/orderList")
ORDER_DETAIL_API_URL = os.getenv("ORDER_DETAIL_API_URL", "https://apiwww.rakumart.com/open/orderDetail")
STOCK_LIST_API_URL = os.getenv("STOCK_LIST_API_URL", "https://apiwww.rakumart.com/open/stockList")

# Porders
CREATE_PORDER_API_URL = os.getenv("CREATE_PORDER_API_URL", "https://apiwww.rakumart.com/open/createPorder")
UPDATE_PORDER_STATUS_API_URL = os.getenv("UPDATE_PORDER_STATUS_API_URL", "https://apiwww.rakumart.com/open/updatePorderStatus")
CANCEL_PORDER_API_URL = os.getenv("CANCEL_PORDER_API_URL", "https://apiwww.rakumart.com/open/cancelPorder")
PORDER_LIST_API_URL = os.getenv("PORDER_LIST_API_URL", "https://apiwww.rakumart.com/open/porderList")
PORDER_DETAIL_API_URL = os.getenv("PORDER_DETAIL_API_URL", "https://apiwww.rakumart.com/open/porderDetail")

# OpenAI removed


