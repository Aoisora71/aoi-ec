from typing import Iterable, Optional, Dict, Any, Sequence, List
from contextlib import contextmanager
import os
import json
import datetime as dt
import logging
import re
import hashlib
import secrets

from .openai_utils import generate_marketing_copy

# Try to import bcrypt for password hashing
try:
    import bcrypt
    BCRYPT_AVAILABLE = True
except ImportError:
    BCRYPT_AVAILABLE = False
    logger.warning("bcrypt not available. Password hashing will use SHA-256 (less secure). Install bcrypt for better security: pip install bcrypt")

# Optional .env loading
try:  # pragma: no cover
    from dotenv import load_dotenv  # type: ignore
except Exception:  # pragma: no cover
    load_dotenv = None  # type: ignore
else:  # pragma: no cover
    try:
        load_dotenv()
    except Exception:
        pass

try:
    import psycopg2
    import psycopg2.extras
except Exception as exc:  # pragma: no cover
    psycopg2 = None  # type: ignore
    _import_error = exc
else:
    _import_error = None

# Configure logging
logger = logging.getLogger(__name__)

_UNSET = object()

# Connection pool for database connections
_connection_pool: Optional[Any] = None
_pool_initialized: bool = False


def _normalize_domestic_shipping_costs(
    costs: Optional[Dict[str, Any]],
    fallback: float,
) -> Dict[str, float]:
    """
    Ensure domestic shipping costs dict contains all required size keys with float values.
    """
    try:
        base_cost = float(costs.get("regular")) if costs and "regular" in costs else float(fallback)
    except (TypeError, ValueError):
        base_cost = float(fallback)
    
    default = {
        "regular": base_cost,
        "size60": base_cost,
        "size80": base_cost,
        "size100": base_cost,
    }
    
    if not isinstance(costs, dict):
        return default
    
    normalized = default.copy()
    for key in normalized.keys():
        value = costs.get(key)
        if value is None:
            continue
        try:
            normalized[key] = float(value)
        except (TypeError, ValueError):
            logger.warning(f"⚠️  Invalid value for domestic shipping cost '{key}': {value}. Using default {normalized[key]}.")
    return normalized


def _extract_relative_image_path(url: str) -> str:
    """
    Extract relative path from full S3 URL for storage in database.
    Adds "img" prefix to directory name if it starts with a number (to match Rakuten Cabinet folder structure).
    
    Example:
        Input:  "https://licel-product-image.s3.ap-southeast-2.amazonaws.com/products/01306503/01306503_4.jpg"
        Output: "/img01306503/01306503_4.jpg"
    
    Args:
        url: Full S3 URL or relative path
    
    Returns:
        Relative path starting with "/" (e.g., "/img01306503/01306503_4.jpg")
    """
    if not url:
        return url
    
    # Extract the path from the URL
    relative_path = None
    
    if url.startswith("/"):
        # Already a relative path
        relative_path = url
    elif "/products/" in url:
        # Extract everything after "/products"
        relative_path = url.split("/products/", 1)[1]
        # Ensure it starts with "/"
        relative_path = "/" + relative_path if not relative_path.startswith("/") else relative_path
    else:
        # Fallback: try to extract path from URL using urlparse
        from urllib.parse import urlparse
        parsed = urlparse(url)
        path = parsed.path
        # Remove leading "/products" if present
        if path.startswith("/products"):
            path = path[len("/products"):]
        # Ensure it starts with "/"
        relative_path = "/" + path.lstrip("/") if path else url
    
    if not relative_path:
        return url
    
    # Add "img" prefix to directory name if it starts with a number
    # Pattern: "/01306503/01306503_4.jpg" -> "/img01306503/01306503_4.jpg"
    # Split the path into parts
    parts = relative_path.lstrip("/").split("/", 1)
    if len(parts) >= 1:
        directory_name = parts[0]
        file_name = parts[1] if len(parts) > 1 else ""
        
        # If directory name starts with a number (image_key like "01306503"), prefix with "img"
        if directory_name and directory_name[0].isdigit():
            directory_name = "img" + directory_name
        
        # Reconstruct the path
        if file_name:
            relative_path = f"/{directory_name}/{file_name}"
        else:
            relative_path = f"/{directory_name}"
    
    return relative_path


def _get_dsn() -> Optional[str]:
    url = os.getenv("DATABASE_URL")
    if url:
        return url
    host = os.getenv("PGHOST")
    if not host:
        return None
    user = os.getenv("PGUSER")
    password = os.getenv("PGPASSWORD")
    dbname = os.getenv("PGDATABASE")
    port = os.getenv("PGPORT", "5432")
    return f"host={host} port={port} dbname={dbname} user={user} password={password}"


def _ensure_import() -> None:
    if psycopg2 is None:  # pragma: no cover
        raise RuntimeError(f"psycopg2 not available: {_import_error}")


def init_connection_pool(
    minconn: int = 1,
    maxconn: int = 20,
    dsn: Optional[str] = None,
    connect_timeout: int = 10
) -> None:
    """
    Initialize the database connection pool.
    
    Args:
        minconn: Minimum number of connections in the pool
        maxconn: Maximum number of connections in the pool
        dsn: Optional database connection string. If not provided, uses environment variables.
        connect_timeout: Connection timeout in seconds (default: 10)
    
    Raises:
        RuntimeError: If database is not configured or connection fails
    """
    global _connection_pool, _pool_initialized
    
    if _pool_initialized and _connection_pool is not None:
        logger.info("Connection pool already initialized")
        return
    
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError(
            "PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.\n"
            "Example: DATABASE_URL=postgresql://user:password@host:5432/dbname"
        )
    
    try:
        # Add connect_timeout to the connection string if it's a URL
        if dsn_final.startswith("postgresql://") or dsn_final.startswith("postgres://"):
            # For URL format, add connect_timeout as query parameter
            if "?" in dsn_final:
                dsn_final = f"{dsn_final}&connect_timeout={connect_timeout}"
            else:
                dsn_final = f"{dsn_final}?connect_timeout={connect_timeout}"
        else:
            # For key=value format, append connect_timeout
            if "connect_timeout" not in dsn_final:
                dsn_final = f"{dsn_final} connect_timeout={connect_timeout}"
        
        # Create ThreadedConnectionPool for thread-safe connection pooling
        _connection_pool = psycopg2.pool.ThreadedConnectionPool(
            minconn=minconn,
            maxconn=maxconn,
            dsn=dsn_final
        )
        _pool_initialized = True
        logger.info(f"Database connection pool initialized (min={minconn}, max={maxconn})")
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Failed to initialize connection pool: {error_msg}")
        _connection_pool = None
        _pool_initialized = False
        raise RuntimeError(
            f"Failed to initialize database connection pool: {error_msg}\n"
            f"Please check your DATABASE_URL or PG* environment variables."
        )


def close_connection_pool() -> None:
    """
    Close all connections in the pool and clean up.
    """
    global _connection_pool, _pool_initialized
    
    if _connection_pool is not None:
        try:
            _connection_pool.closeall()
            logger.info("Database connection pool closed")
        except Exception as e:
            logger.error(f"Error closing connection pool: {e}")
        finally:
            _connection_pool = None
            _pool_initialized = False


def _get_connection_from_pool(dsn: Optional[str] = None) -> Any:
    """
    Get a connection from the pool, or create a direct connection if pool is not available.
    
    Args:
        dsn: Optional database connection string. If not provided, uses environment variables.
    
    Returns:
        Database connection object
    
    Raises:
        RuntimeError: If database is not configured or connection fails
    """
    global _connection_pool, _pool_initialized
    
    # If pool is initialized and available, use it
    if _pool_initialized and _connection_pool is not None:
        try:
            return _connection_pool.getconn()
        except Exception as e:
            logger.warning(f"Failed to get connection from pool: {e}. Falling back to direct connection.")
            # Fall through to direct connection
    
    # Fallback to direct connection if pool is not available
    return get_db_connection(dsn=dsn)


def _return_connection_to_pool(conn: Any) -> None:
    """
    Return a connection to the pool, or close it if pool is not available.
    
    Args:
        conn: Database connection object to return
    """
    global _connection_pool, _pool_initialized
    
    if conn is None:
        return
    
    # If pool is initialized and available, return connection to pool
    if _pool_initialized and _connection_pool is not None:
        try:
            _connection_pool.putconn(conn)
            return
        except Exception as e:
            logger.warning(f"Failed to return connection to pool: {e}. Closing connection directly.")
    
    # Fallback: close connection directly
    try:
        conn.close()
    except Exception:
        pass


@contextmanager
def get_db_connection_context(dsn: Optional[str] = None):
    """
    Context manager for database connections using the connection pool.
    Automatically returns the connection to the pool when done.
    
    Usage:
        with get_db_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM table")
        # Or with explicit DSN:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM table")
    
    Args:
        dsn: Optional database connection string. If not provided, uses environment variables.
    
    Yields:
        Database connection object
    """
    conn = None
    try:
        conn = _get_connection_from_pool(dsn=dsn)
        yield conn
    except Exception:
        # If there's an error, mark connection as bad and close it
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
            # Don't return bad connections to pool
            try:
                conn.close()
            except Exception:
                pass
            # If pool is available, try to recreate the connection
            global _connection_pool, _pool_initialized
            if _pool_initialized and _connection_pool is not None:
                try:
                    _connection_pool.putconn(conn, close=True)
                except Exception:
                    pass
        raise
    else:
        # Success - return connection to pool
        if conn is not None:
            _return_connection_to_pool(conn)


def get_db_connection(dsn: Optional[str] = None, connect_timeout: int = 10):
    """
    Get a database connection using the configured DSN
    
    Args:
        dsn: Optional database connection string. If not provided, uses environment variables.
        connect_timeout: Connection timeout in seconds (default: 10)
    
    Raises:
        RuntimeError: If database is not configured or connection fails
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError(
            "PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.\n"
            "Example: DATABASE_URL=postgresql://user:password@host:5432/dbname"
        )
    
    try:
        # Add connect_timeout to the connection string if it's a URL
        if dsn_final.startswith("postgresql://") or dsn_final.startswith("postgres://"):
            # For URL format, add connect_timeout as query parameter
            if "?" in dsn_final:
                dsn_final = f"{dsn_final}&connect_timeout={connect_timeout}"
            else:
                dsn_final = f"{dsn_final}?connect_timeout={connect_timeout}"
        else:
            # For key=value format, append connect_timeout
            if "connect_timeout" not in dsn_final:
                dsn_final = f"{dsn_final} connect_timeout={connect_timeout}"
        
        return psycopg2.connect(dsn_final)
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Database connection failed: {error_msg}")
        
        # Provide helpful error messages based on error type
        if "Connection timed out" in error_msg or "timeout" in error_msg.lower():
            raise RuntimeError(
                f"Database connection timed out to {dsn_final.split('@')[-1] if '@' in dsn_final else 'database'}.\n"
                f"Please check:\n"
                f"  1. Is PostgreSQL server running?\n"
                f"  2. Is the host/port correct? (Check DATABASE_URL or PGHOST env var)\n"
                f"  3. Is the server accessible from this machine? (Check firewall/network)\n"
                f"  4. Are the credentials correct?\n"
                f"Original error: {error_msg}"
            )
        elif "does not exist" in error_msg:
            raise RuntimeError(
                f"Database does not exist.\n"
                f"Please create the database or check the database name in your connection string.\n"
                f"Original error: {error_msg}"
            )
        elif "password authentication failed" in error_msg.lower() or "authentication failed" in error_msg.lower():
            raise RuntimeError(
                f"Database authentication failed.\n"
                f"Please check your username and password in DATABASE_URL or PGUSER/PGPASSWORD env vars.\n"
                f"Original error: {error_msg}"
            )
        else:
            raise RuntimeError(
                f"Database connection failed: {error_msg}\n"
                f"Please check your DATABASE_URL or PG* environment variables."
            )


def _to_numeric(value):
    try:
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        s = str(value)
        import re
        s = re.sub(r"[^0-9.\-]", "", s)
        return float(s) if s else None
    except Exception:
        return None


def _filter_detail_json_t_only(data: Optional[dict]) -> Optional[dict]:
    """
    Filter detail JSON to only keep fields ending with 'T' (like titleT, keyT, valueT)
    and remove fields ending with 'C' (like titleC, keyC, valueC).
    Also excludes specific fields: video, description, fromPlatform_logo, picUrl.
    Includes fields: address, fromUrl, fromPlatform, shopName (these are preserved in detail_json).
    Recursively processes nested dictionaries and lists.
    
    Example:
        Input: {"titleC": "Chinese", "titleT": "Japanese", "keyC": "key", "keyT": "キー", "fromUrl": "http://...", "picUrl": "http://..."}
        Output: {"titleT": "Japanese", "keyT": "キー", "fromUrl": "http://..."}
    """
    if data is None:
        return None
    
    if not isinstance(data, dict):
        return data
    
    # Fields to exclude from detail_json
    EXCLUDED_FIELDS = {
        'video',
        'description',
        'fromPlatform_logo',
        'picUrl', 
        'titleT'
         # Exclude picUrl from detail_json
    }
    
    # Fields to always preserve (even if they have nested 'C' fields)
    PRESERVE_FIELDS = {
        'specification',
        'specifications',
        'spec',
        'goodsinfo',  # Preserve goodsinfo object which contains specification
    }
    
    filtered = {}
    
    for key, value in data.items():
        # Always preserve specification-related fields
        if key in PRESERVE_FIELDS:
            if isinstance(value, list):
                # For lists, preserve the structure but filter nested dicts
                filtered_list = []
                for item in value:
                    if isinstance(item, dict):
                        # Filter nested dicts but preserve keyT and valueT structure
                        # This will recursively remove picUrl and other excluded fields
                        filtered_item = _filter_detail_json_t_only(item)
                        # Explicitly remove excluded fields as a safety measure
                        if filtered_item and isinstance(filtered_item, dict):
                            for excluded_field in EXCLUDED_FIELDS:
                                filtered_item.pop(excluded_field, None)
                        if filtered_item is not None and filtered_item:
                            filtered_list.append(filtered_item)
                    else:
                        filtered_list.append(item)
                if filtered_list:
                    filtered[key] = filtered_list
            elif isinstance(value, dict):
                # Recursively filter the dict, which will remove picUrl and other excluded fields
                filtered_value = _filter_detail_json_t_only(value)
                # Explicitly remove excluded fields as a safety measure
                if filtered_value and isinstance(filtered_value, dict):
                    for excluded_field in EXCLUDED_FIELDS:
                        filtered_value.pop(excluded_field, None)
                if filtered_value is not None and filtered_value:
                    filtered[key] = filtered_value
            else:
                filtered[key] = value
            continue
        
        # Skip fields ending with 'C' (case-sensitive)
        if key.endswith('C'):
            continue
        
        # Skip explicitly excluded fields
        if key in EXCLUDED_FIELDS:
            continue
        
        # Process value based on type
        if isinstance(value, dict):
            # Recursively filter nested dictionaries
            filtered_value = _filter_detail_json_t_only(value)
            # Only add if the filtered value is not None and not empty
            if filtered_value is not None and filtered_value:
                filtered[key] = filtered_value
        elif isinstance(value, list):
            # Process list items
            filtered_list = []
            for item in value:
                if isinstance(item, dict):
                    filtered_item = _filter_detail_json_t_only(item)
                    # Explicitly remove excluded fields from filtered item (in case they weren't caught)
                    if filtered_item and isinstance(filtered_item, dict):
                        for excluded_field in EXCLUDED_FIELDS:
                            filtered_item.pop(excluded_field, None)
                    # Only add non-empty filtered items
                    if filtered_item is not None and filtered_item:
                        filtered_list.append(filtered_item)
                else:
                    # Keep non-dict items as-is (strings, numbers, etc.)
                    filtered_list.append(item)
            # Only add list if it has items
            if filtered_list:
                filtered[key] = filtered_list
        else:
            # Keep primitive values (strings, numbers, booleans, None)
            # This includes fields ending with 'T' and fields that don't end with 'C' or 'T'
            # Note: We keep None values as they might be meaningful
            filtered[key] = value
    
    # Post-process to explicitly remove excluded fields from nested structures
    # This ensures picUrl and titleT are removed even from deeply nested structures
    def remove_excluded_fields_recursive(obj):
        """Recursively remove excluded fields from nested structures."""
        if isinstance(obj, dict):
            # Remove excluded fields from this dict
            for excluded_field in EXCLUDED_FIELDS:
                obj.pop(excluded_field, None)
            # Recursively process all values
            for value in obj.values():
                remove_excluded_fields_recursive(value)
        elif isinstance(obj, list):
            # Recursively process all items in the list
            for item in obj:
                remove_excluded_fields_recursive(item)
    
    if filtered:
        remove_excluded_fields_recursive(filtered)
    
    # Return None if the filtered dict is empty, otherwise return the filtered dict
    return filtered if filtered else None


def cleanup_empty_records(dsn: Optional[str] = None) -> int:
    """
    Remove records with empty essential data from the database
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                # Delete records with empty product_id or product_name
                cur.execute("""
                    DELETE FROM products_origin 
                    WHERE product_id IS NULL 
                       OR product_id = '' 
                       OR product_name IS NULL 
                       OR product_name = ''
                """)
                deleted_count = cur.rowcount
                conn.commit()
                logger.info(f"Cleaned up {deleted_count} empty records from database")
                return deleted_count
    except Exception as e:
        logger.error(f"Error cleaning up empty records: {e}")
        return 0


def _is_valid_product_data(product_tuple):
    """
    Validate that a product tuple has essential non-empty data
    """
    if not product_tuple or len(product_tuple) < 4:
        return False
    
    # Check essential fields (product_id, product_name)
    product_id = product_tuple[0]  # product_id is at index 0
    product_name = product_tuple[3]  # product_name is at index 3
    
    if not product_id or str(product_id).strip() == "":
        return False
    if not product_name or str(product_name).strip() == "":
        return False
        
    return True


def _load_category_weight_map(dsn: str) -> dict[str, Optional[float]]:
    """
    Build a mapping from Rakumart category ID to the configured weight in category_management.
    """
    mapping: dict[str, Optional[float]] = {}
    try:
        with get_db_connection_context(dsn=dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT category_ids, weight FROM category_management WHERE weight IS NOT NULL")
                for category_ids_value, weight in cur.fetchall():
                    if weight is None:
                        continue
                    category_ids_obj = category_ids_value
                    if isinstance(category_ids_obj, str):
                        try:
                            category_ids_obj = json.loads(category_ids_obj)
                        except json.JSONDecodeError:
                            category_ids_obj = []
                    if isinstance(category_ids_obj, (list, tuple)):
                        for cid in category_ids_obj:
                            if cid is None:
                                continue
                            mapping[str(cid)] = float(weight)
    except Exception as exc:
        logger.error(f"Failed to load category weight map: {exc}")
    return mapping


def _load_category_size_map(dsn: str) -> dict[str, Optional[float]]:
    """
    Build a mapping from Rakumart category ID to the configured size in category_management.
    """
    mapping: dict[str, Optional[float]] = {}
    try:
        with get_db_connection_context(dsn=dsn) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT category_ids, size FROM category_management WHERE size IS NOT NULL")
                for category_ids_value, size_value in cur.fetchall():
                    if size_value is None:
                        continue
                    category_ids_obj = category_ids_value
                    if isinstance(category_ids_obj, str):
                        try:
                            category_ids_obj = json.loads(category_ids_obj)
                        except json.JSONDecodeError:
                            category_ids_obj = []
                    if isinstance(category_ids_obj, (list, tuple)):
                        for cid in category_ids_obj:
                            if cid is None:
                                continue
                            mapping[str(cid)] = float(size_value)
    except Exception as exc:
        logger.error(f"Failed to load category size map: {exc}")
    return mapping


def _load_category_rakuten_map(dsn: str) -> dict[str, list[str]]:
    """
    Build a mapping from Rakumart category ID to the configured Rakuten category ID.
    Uses the first element of rakuten_category_ids for each category_id.
    """
    mapping: dict[str, list[str]] = {}
    try:
        with get_db_connection_context(dsn=dsn) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT category_ids, rakuten_category_ids "
                    "FROM category_management "
                    "WHERE rakuten_category_ids IS NOT NULL "
                    "  AND rakuten_category_ids <> '[]'::jsonb"
                )
                for category_ids_value, rakuten_ids_value in cur.fetchall():
                    category_ids_obj = category_ids_value
                    if isinstance(category_ids_obj, str):
                        try:
                            category_ids_obj = json.loads(category_ids_obj)
                        except json.JSONDecodeError:
                            category_ids_obj = []
                    if isinstance(category_ids_obj, (list, tuple)):
                        # Normalise Rakuten IDs list
                        rakuten_ids_obj = rakuten_ids_value
                        if isinstance(rakuten_ids_obj, str):
                            try:
                                rakuten_ids_obj = json.loads(rakuten_ids_obj)
                            except json.JSONDecodeError:
                                rakuten_ids_obj = []
                        if not isinstance(rakuten_ids_obj, (list, tuple)):
                            rakuten_ids_obj = []
                        clean_rakuten_ids = [
                            str(rid).strip()
                            for rid in rakuten_ids_obj
                            if rid is not None and str(rid).strip()
                        ]
                        if not clean_rakuten_ids:
                            continue
                        for cid in category_ids_obj:
                            if cid is None:
                                continue
                            key = str(cid).strip()
                            if not key:
                                continue
                            # Last one wins if duplicates; this is acceptable for our use-case
                            mapping[key] = clean_rakuten_ids
    except Exception as exc:
        logger.error(f"Failed to load category Rakuten map: {exc}")
    return mapping


def _ensure_products_origin_numeric_column(column_name: str, dsn: str) -> None:
    """
    Ensure the specified numeric column exists on products_origin.
    """
    with get_db_connection_context(dsn=dsn) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                  FROM information_schema.columns
                 WHERE table_name = 'products_origin'
                   AND column_name = %s
                """,
                (column_name,),
            )
            if cur.fetchone():
                return
            cur.execute(f"ALTER TABLE products_origin ADD COLUMN {column_name} numeric;")
        conn.commit()


SIZE_OPTION_MAP: dict[str, float] = {
    "DM": 30.0,
    "60": 60.0,
    "80": 80.0,
    "100": 100.0,
}


def _normalize_size_selection(
    size_option: Optional[str],
    size_value: Optional[float],
) -> tuple[Optional[str], Optional[float]]:
    """
    Normalize category size selection into stored option and numeric value.
    """
    option_normalized: Optional[str] = None
    if size_option:
        option = size_option.strip()
        if option:
            option_upper = option.upper()
            if option_upper == "DM":
                option_normalized = "DM"
            elif option in {"60", "80", "100"}:
                option_normalized = option
            else:
                option_normalized = option
    
    map_key = option_normalized.upper() if option_normalized else None
    if map_key and map_key in SIZE_OPTION_MAP:
        return option_normalized, SIZE_OPTION_MAP[map_key]
    
    if size_value is not None:
        try:
            return option_normalized, float(size_value)
        except (TypeError, ValueError):
            return option_normalized, None
    
    return option_normalized, None


_SIZE_TO_SHIPPING_KEY = {
    30: "regular",
    60: "size60",
    80: "size80",
    100: "size100",
}


def _select_domestic_shipping_cost(
    product_size: Optional[float],
    shipping_costs: Optional[Dict[str, Any]],
    default_cost: float,
) -> float:
    """
    Determine the appropriate domestic shipping cost based on product size.
    Size mapping: 30 -> regular, 60 -> size60, 80 -> size80, 100 -> size100
    """
    try:
        base_cost = float(default_cost)
    except (TypeError, ValueError):
        base_cost = 0.0
    
    if not shipping_costs:
        logger.debug(f"💰 No shipping_costs dict provided, using default: {base_cost} JPY")
        return base_cost
    
    if product_size is None:
        logger.debug(f"💰 No product_size provided, using default: {base_cost} JPY")
        return base_cost
    
    try:
        size_value = float(product_size)
    except (TypeError, ValueError):
        logger.warning(f"💰 Invalid product_size value: {product_size}, using default: {base_cost} JPY")
        return base_cost
    
    size_key = _SIZE_TO_SHIPPING_KEY.get(int(round(size_value)))
    if not size_key:
        logger.debug(
            f"💰 Product size {size_value} doesn't match known sizes (30/60/80/100), "
            f"using default: {base_cost} JPY"
        )
        return base_cost
    
    selected = shipping_costs.get(size_key)
    if selected is None:
        logger.warning(
            f"💰 Shipping cost key '{size_key}' not found in shipping_costs dict, "
            f"using default: {base_cost} JPY"
        )
        return base_cost
    
    try:
        selected_cost = float(selected)
        logger.info(
            f"💰 Selected size-based shipping cost: {selected_cost} JPY "
            f"(size={size_value} -> key='{size_key}')"
        )
        return selected_cost
    except (TypeError, ValueError):
        logger.warning(
            f"💰 Invalid shipping cost value for key '{size_key}': {selected}, "
            f"using default: {base_cost} JPY"
        )
        return base_cost


def _select_unit_price_from_value_t(entries: Sequence[Any]) -> Optional[float]:
    """
    Pick the most relevant unit price from goodsInventory.valueT entries.
    Preference order:
      1. startQuantity of 1 (single-unit price)
      2. The smallest available startQuantity
      3. First valid price encountered
    """
    if not entries:
        return None
    
    selected_price: Optional[float] = None
    best_rank: Optional[int] = None
    
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        
        price_value = _to_numeric(entry.get("price"))
        if price_value is None:
            continue
        
        start_quantity_raw = entry.get("startQuantity")
        try:
            start_quantity = int(start_quantity_raw)
        except (TypeError, ValueError):
            start_quantity = None
        
        if start_quantity in (None, 0, 1):
            return float(price_value)
        
        if best_rank is None or (start_quantity is not None and start_quantity < best_rank):
            selected_price = float(price_value)
            best_rank = start_quantity
    
    return selected_price


def _update_products_numeric_column_by_category_ids(
    category_ids: list[str],
    column_name: str,
    value: Optional[float],
    *,
    dsn: Optional[str] = None,
) -> int:
    """
    Update a numeric column (weight/length/width/height/size) for products matching the given category IDs.
    """
    allowed_columns = {"weight", "length", "width", "height", "size"}
    if column_name not in allowed_columns:
        raise ValueError(f"Unsupported column '{column_name}' for products_origin update.")
    
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    _ensure_products_origin_numeric_column(column_name, dsn_final)
    
    if not category_ids:
        return 0
    
    clean_category_ids = [str(cid).strip() for cid in category_ids if cid and str(cid).strip()]
    if not clean_category_ids:
        return 0
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                if value is None:
                    cur.execute(
                        f"""
                        UPDATE products_origin
                           SET {column_name} = NULL
                         WHERE middle_category = ANY(%s::text[])
                            OR main_category = ANY(%s::text[])
                        """,
                        (clean_category_ids, clean_category_ids),
                    )
                else:
                    cur.execute(
                        f"""
                        UPDATE products_origin
                           SET {column_name} = %s
                         WHERE middle_category = ANY(%s::text[])
                            OR main_category = ANY(%s::text[])
                        """,
                        (float(value), clean_category_ids, clean_category_ids),
                    )
                
                updated_count = cur.rowcount
                conn.commit()
                logger.info(
                    f"Updated {column_name} for {updated_count} products in products_origin "
                    f"for category IDs: {clean_category_ids}, new value: {value}"
                )
                return updated_count
    except Exception as e:
        logger.error(f"Failed to update products {column_name} by category IDs: {e}")
        raise


def update_products_weight_by_category_ids(
    category_ids: list[str],
    weight: Optional[float],
    *,
    dsn: Optional[str] = None,
) -> int:
    """
    Update weight of products in products_origin table that match the given category IDs.
    Products are matched by middle_category or main_category.
    
    Args:
        category_ids: List of category IDs to match products against
        weight: New weight value (None to set weight to NULL)
        dsn: Database connection string
        
    Returns:
        Number of products updated
    """
    return _update_products_numeric_column_by_category_ids(
        category_ids,
        "weight",
        weight,
        dsn=dsn,
    )


SCHEMA_SQL = """
create table if not exists products_origin (
    id bigserial primary key,
    product_id text,
    main_category text,
    middle_category text,
    product_name text,
    product_description text,
    type text,
    monthly_sales int,
    wholesale_price numeric,
    weight numeric,
    length numeric,
    width numeric,
    height numeric,
    size numeric,
    creation_date timestamptz,
    repurchase_rate numeric,
    rating_score numeric,
    detail_json jsonb,
    registration_status int default 1,
    r_cat_id jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now()
);
"""


def _reset_products_origin_id_sequence(*, dsn: Optional[str] = None) -> None:
    """
    Ensure the products_origin.id sequence is aligned with existing rows.
    
    In some legacy databases the sequence can fall behind the max(id),
    which causes "duplicate key value violates unique constraint
    \"products_origin_pkey\"" errors when inserting.
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        return

    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                # Get the sequence name that backs products_origin.id
                cur.execute(
                    "SELECT pg_get_serial_sequence('products_origin', 'id');"
                )
                row = cur.fetchone()
                if not row or not row[0]:
                    logger.warning("Could not determine sequence for products_origin.id")
                    return
                seq_name = row[0]

                # Align sequence to current MAX(id)
                cur.execute("SELECT COALESCE(MAX(id), 0) FROM products_origin;")
                max_id = cur.fetchone()[0] or 0

                # If table is empty, setval to 1 so nextval starts at 1
                new_val = max_id if max_id > 0 else 1
                logger.info(
                    f"Resetting products_origin.id sequence '{seq_name}' to at least {new_val}"
                )
                cur.execute("SELECT setval(%s, %s, true);", (seq_name, new_val))
                conn.commit()
    except Exception as e:
        logger.error(f"Failed to reset products_origin.id sequence: {e}")


# Product Management table schema (matches Rakuten API structure from origin_rakuten.json)
SCHEMA_SQL_PRODUCT_MANAGEMENT = """
create table if not exists product_management (
    id bigserial primary key,
    -- Core identity
    item_number text unique,
    title text,
    tagline text,
    item_type text,

    -- Descriptions
    product_description jsonb,  -- { pc: text, sp: text }
    sales_description text,

    -- Media & classification
    images jsonb,               -- [{ type, location, alt }]
    product_image_code text,     -- 8-digit code for S3 image folder naming
    genre_id text,
    tags bigint[],

    -- Visibility & inventory
    hide_item boolean,
    unlimited_inventory_flag boolean,

    -- Features & payment
    features jsonb,             -- { searchVisibility, displayNormalCartButton, etc. }
    payment jsonb,              -- { taxIncluded, taxRate, cashOnDeliveryFeeIncluded }

    -- Display & layout
    layout jsonb,               -- { itemLayoutId, navigationId, etc. }

    -- Variations
    variant_selectors jsonb,    -- [{ key, displayName, values: [{displayValue}] }]
    variants jsonb,             -- { skuId: { selectorValues, standardPrice, articleNumber, images, attributes, shipping, features } }
    
    -- Inventory
    inventory jsonb,            -- { manage_number: str, variants: [{ variant_id: str, quantity: int, mode: str }] }
    
    -- Rakuten registration status
    rakuten_registration_status text,  -- "unregistered" (null/empty): not registered, "false": registration failed, "true": registration successful
    
    -- Registration status flags
    image_registration_status boolean default false,  -- true if images successfully registered to Rakuten
    inventory_registration_status boolean default false,  -- true if inventory successfully registered to Rakuten
    
    -- Source URL
    src_url text,  -- Source URL from detail_json.fromUrl (original product page URL)
    
    -- Rakuten registration timestamp
    rakuten_registered_at timestamptz,  -- Timestamp when product was successfully registered to Rakuten
    
    -- Category information from products_origin
    main_category text,  -- Main category from products_origin
    middle_category text,  -- Middle category from products_origin
    r_cat_id jsonb not null default '[]'::jsonb,  -- Rakuten category IDs from category_management
    
    -- Purchase price information
    actual_purchase_price numeric,  -- Calculated actual purchase price (sale price) in JPY
    
    -- Change status
    change_status text,  -- Change status for tracking modifications (e.g., "modified", "pending", "approved")
    
    -- Audit
    created_at timestamptz not null default now()
);
"""

SCHEMA_SQL_PRIMARY_CATEGORY_MANAGEMENT = """
create table if not exists primary_category_management (
    id bigserial primary key,
    category_name text not null unique,
    default_category_ids jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
"""

SCHEMA_SQL_CATEGORY_MANAGEMENT = """
create table if not exists category_management (
    id bigserial primary key,
    category_name text not null,
    category_ids jsonb not null default '[]'::jsonb,
    rakuten_category_ids jsonb not null default '[]'::jsonb,
    genre_id text,
    primary_category_id bigint references primary_category_management(id) on delete set null,
    weight numeric,
    length numeric,
    width numeric,
    height numeric,
    size_option text,
    size numeric,
    attributes jsonb not null default '[]'::jsonb,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);
"""

SCHEMA_SQL_SETTINGS = """
create table if not exists app_settings (
    id bigserial primary key,
    setting_key text not null unique,
    setting_value jsonb not null,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now()
);

-- Create index for faster lookups
create index if not exists idx_app_settings_key on app_settings(setting_key);
"""

SCHEMA_SQL_USERS = """
create table if not exists users (
    id bigserial primary key,
    email text not null unique,
    password_hash text not null,
    name text not null,
    is_active boolean not null default true,
    created_at timestamptz not null default now(),
    updated_at timestamptz not null default now(),
    last_login timestamptz
);

-- Create indexes for faster lookups
create index if not exists idx_users_email on users(email);
create index if not exists idx_users_active on users(is_active);
"""


def init_products_origin_table(*, dsn: Optional[str] = None) -> None:
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL)
            # Online schema upgrades to ensure columns exist in any order
            # This handles cases where table was created before schema changes
            cur.execute("alter table products_origin add column if not exists creation_date timestamptz;")
            cur.execute("alter table products_origin add column if not exists repurchase_rate numeric;")
            cur.execute("alter table products_origin add column if not exists rating_score numeric;")
            cur.execute("alter table products_origin add column if not exists detail_json jsonb;")
            cur.execute("alter table products_origin add column if not exists weight numeric;")
            cur.execute("alter table products_origin add column if not exists length numeric;")
            cur.execute("alter table products_origin add column if not exists width numeric;")
            cur.execute("alter table products_origin add column if not exists height numeric;")
            cur.execute("alter table products_origin add column if not exists size numeric;")
            # Add registration_status column if it doesn't exist (1=unregistered, 2=registered, 3=previously_registered)
            cur.execute("alter table products_origin add column if not exists registration_status int default 1;")
            # Ensure registration_status uses integer storage even if the column existed previously as text
            cur.execute(
                """
                SELECT data_type
                FROM information_schema.columns
                WHERE table_name = 'products_origin'
                  AND column_name = 'registration_status'
                """
            )
            reg_status_type = cur.fetchone()
            if reg_status_type and reg_status_type[0] != "integer":
                logger.info("Converting products_origin.registration_status column to integer type")
                # Remove incompatible default before conversion
                cur.execute(
                    """
                    ALTER TABLE products_origin
                    ALTER COLUMN registration_status DROP DEFAULT
                    """
                )
                # Normalise existing values to numeric strings before altering the column type
                cur.execute(
                    """
                    UPDATE products_origin
                    SET registration_status = CASE
                        WHEN registration_status IN ('registered', '2') THEN '2'
                        WHEN registration_status IN ('previously_registered', '3') THEN '3'
                        WHEN registration_status IN ('unregistered', '1') THEN '1'
                        WHEN registration_status IS NULL OR registration_status = '' THEN '1'
                        WHEN registration_status ~ '^[0-9]+$' THEN registration_status
                        ELSE '1'
                    END
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE products_origin
                    ALTER COLUMN registration_status TYPE integer
                    USING registration_status::integer
                    """
                )
                cur.execute(
                    """
                    ALTER TABLE products_origin
                    ALTER COLUMN registration_status SET DEFAULT 1
                    """
                )
                # Ensure registration_status reflects current product_management state
                cur.execute(
                    """
                    UPDATE products_origin po
                    SET registration_status = 2
                    FROM product_management pm
                    WHERE po.product_id = pm.item_number
                      AND po.registration_status <> 2
                    """
                )
                cur.execute(
                    """
                    UPDATE products_origin
                    SET registration_status = 1
                    WHERE registration_status IS NULL OR registration_status NOT IN (1,2,3)
                    """
                )
            # Ensure upsert works by having a UNIQUE index on product_id (NULLs allowed, treated distinct)
            cur.execute("create unique index if not exists uniq_products_origin_product_id on products_origin(product_id);")

    # Drop legacy columns (including image_* fields) if they still exist
    drop_removed_columns_from_products_origin(dsn=dsn_final)


def ensure_primary_category_table(*, dsn: Optional[str] = None) -> None:
    """
    Ensure the primary_category_management table exists.
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL_PRIMARY_CATEGORY_MANAGEMENT)
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_primary_category_name_lower
                ON primary_category_management(lower(category_name));
                """
            )
            # Add default_category_ids column if it doesn't exist (migration)
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'primary_category_management' 
                        AND column_name = 'default_category_ids'
                    ) THEN
                        ALTER TABLE primary_category_management 
                        ADD COLUMN default_category_ids jsonb NOT NULL DEFAULT '[]'::jsonb;
                    END IF;
                END $$;
            """)
        conn.commit()


def ensure_settings_table(*, dsn: Optional[str] = None) -> None:
    """
    Ensure the app_settings table exists and initialize with default pricing settings if empty.
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(SCHEMA_SQL_SETTINGS)
            
            # Check if pricing_settings already exists
            cur.execute(
                "SELECT COUNT(*) as count FROM app_settings WHERE setting_key = 'pricing_settings'"
            )
            result = cur.fetchone()
            if result and result.get('count', 0) == 0:
                # Initialize with default pricing settings
                default_settings = {
                    "exchange_rate": 22.0,
                    "profit_margin_percent": 30.0,
                    "sales_commission_percent": 3.0,
                    "currency": "JPY",
                    "domestic_shipping_cost": 330.0,
                    "domestic_shipping_costs": {
                        "regular": 330.0,
                        "size60": 430.0,
                        "size80": 530.0,
                        "size100": 630.0,
                    },
                    "international_shipping_rate": 18.7,
                    "customs_duty_rate": 100.0,
                }
                cur.execute(
                    """
                    INSERT INTO app_settings (setting_key, setting_value, updated_at)
                    VALUES ('pricing_settings', %s::jsonb, now())
                    """,
                    (json.dumps(default_settings),)
                )
                logger.info("✅ Initialized pricing_settings with default values")
            conn.commit()


def save_pricing_settings(
    exchange_rate: float,
    profit_margin_percent: float,
    sales_commission_percent: float,
    currency: str,
    domestic_shipping_cost: float,
    international_shipping_rate: float,
    customs_duty_rate: float,
    domestic_shipping_costs: Optional[Dict[str, Any]] = None,
    *,
    dsn: Optional[str] = None
) -> None:
    """
    Save pricing settings to the database.
    
    Args:
        exchange_rate: Exchange rate (CNY → JPY)
        profit_margin_percent: Profit margin percentage
        sales_commission_percent: Sales commission percentage
        currency: Currency code (e.g., "JPY")
        domestic_shipping_cost: Domestic shipping cost in JPY
        domestic_shipping_costs: Mapping of domestic shipping costs per size option
        international_shipping_rate: International shipping rate per kg in CNY
        customs_duty_rate: Customs duty in JPY
        dsn: Optional database connection string
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    ensure_settings_table(dsn=dsn_final)
    
    normalized_shipping_costs = _normalize_domestic_shipping_costs(domestic_shipping_costs, domestic_shipping_cost)
    
    settings = {
        "exchange_rate": float(exchange_rate),
        "profit_margin_percent": float(profit_margin_percent),
        "sales_commission_percent": float(sales_commission_percent),
        "currency": str(currency),
        "domestic_shipping_cost": float(normalized_shipping_costs["regular"]),
        "domestic_shipping_costs": normalized_shipping_costs,
        "international_shipping_rate": float(international_shipping_rate),
        "customs_duty_rate": float(customs_duty_rate),
    }
    
    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO app_settings (setting_key, setting_value, updated_at)
                VALUES ('pricing_settings', %s::jsonb, now())
                ON CONFLICT (setting_key)
                DO UPDATE SET
                    setting_value = EXCLUDED.setting_value,
                    updated_at = now()
                """,
                (json.dumps(settings),)
            )
            conn.commit()
            logger.info("✅ Saved pricing settings to database")


def update_product_hide_item(
    item_number: str,
    hide_item: bool,
    *,
    dsn: Optional[str] = None
) -> bool:
    """
    Update the hide_item field for a product in the product_management table.
    Only updates if rakuten_registration_status is NULL, empty, 'onsale', 'true', or 'false'.
    
    Args:
        item_number: The item_number (product ID) to update
        hide_item: The new hide_item value (True = hidden, False = visible)
        dsn: Optional database connection string
        
    Returns:
        True if update was successful, False otherwise
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE product_management
                    SET hide_item = %s
                    WHERE item_number = %s
                    AND (
                        rakuten_registration_status IS NULL
                        OR rakuten_registration_status = ''
                        OR rakuten_registration_status = 'onsale'
                        OR rakuten_registration_status = 'true'
                        OR rakuten_registration_status = 'false'
                    )
                    """,
                    (hide_item, item_number)
                )
                if cur.rowcount == 0:
                    logger.warning(f"⚠️  No product found with item_number: {item_number} or rakuten_registration_status is not NULL, '', 'onsale', 'true', or 'false'")
                    return False
                conn.commit()
                logger.info(f"✅ Updated hide_item for product {item_number} to {hide_item}")
                return True
    except Exception as e:
        logger.error(f"❌ Failed to update hide_item for product {item_number}: {e}")
        raise


def update_products_hide_item_batch(
    item_numbers: list[str],
    hide_item: bool,
    *,
    dsn: Optional[str] = None
) -> int:
    """
    Update the hide_item field for multiple products in the product_management table.
    Only updates products where rakuten_registration_status is NULL, empty, 'onsale', or 'true'.
    
    Args:
        item_numbers: List of item_numbers (product IDs) to update
        hide_item: The new hide_item value (True = hidden, False = visible)
        dsn: Optional database connection string
        
    Returns:
        Number of products updated
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    if not item_numbers:
        return 0
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE product_management
                    SET hide_item = %s
                    WHERE item_number = ANY(%s)
                    AND (
                        rakuten_registration_status IS NULL
                        OR rakuten_registration_status = ''
                        OR rakuten_registration_status = 'onsale'
                        OR rakuten_registration_status = 'true'
                        OR rakuten_registration_status = 'false'
                    )
                    """,
                    (hide_item, item_numbers)
                )
                updated_count = cur.rowcount
                conn.commit()
                logger.info(f"✅ Updated hide_item for {updated_count} products to {hide_item} (only products with rakuten_registration_status = NULL, '', 'onsale', 'true', or 'false')")
                return updated_count
    except Exception as e:
        logger.error(f"❌ Failed to update hide_item for products: {e}")
        raise


def delete_product_image(item_number: str, image_location: str) -> bool:
    """
    Delete an image from the images field in product_management table.
    
    Args:
        item_number: Product item_number
        image_location: Location of the image to delete (e.g., "/01306503/01306503_4.jpg")
        
    Returns:
        True if image was deleted successfully, False otherwise
    """
    _ensure_import()
    dsn_final = _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                # Get current images
                cur.execute(
                    """
                    SELECT images FROM product_management
                    WHERE item_number = %s
                    """,
                    (item_number,)
                )
                row = cur.fetchone()
                
                if not row or not row[0]:
                    logger.warning(f"Product {item_number} not found or has no images")
                    return False
                
                # Parse images JSON
                images = row[0]
                if isinstance(images, str):
                    try:
                        images = json.loads(images)
                    except json.JSONDecodeError:
                        logger.error(f"Invalid JSON in images field for product {item_number}")
                        return False
                
                if not isinstance(images, list):
                    logger.warning(f"Images field is not a list for product {item_number}")
                    return False
                
                # Normalize image_location for comparison (strip whitespace)
                image_location_normalized = image_location.strip() if image_location else ""
                
                # Filter out the image with matching location
                # Compare locations exactly as stored in the database
                original_count = len(images)
                filtered_images = []
                found_image = False
                
                for img in images:
                    if not isinstance(img, dict):
                        # Keep non-dict entries (shouldn't happen, but be safe)
                        filtered_images.append(img)
                        continue
                    
                    img_location = img.get('location', '')
                    if isinstance(img_location, str):
                        img_location_normalized = img_location.strip()
                    else:
                        img_location_normalized = str(img_location).strip()
                    
                    # Exact match (case-sensitive, as paths are case-sensitive)
                    if img_location_normalized == image_location_normalized:
                        found_image = True
                        logger.info(
                            f"Found image to delete: location='{img_location_normalized}' "
                            f"from product {item_number}"
                        )
                        # Skip this image (don't add to filtered_images)
                        continue
                    
                    # Keep this image
                    filtered_images.append(img)
                
                images = filtered_images
                new_count = len(images)
                
                # Check if any image was removed
                if not found_image:
                    logger.warning(
                        f"Image with location '{image_location_normalized}' not found in product {item_number}. "
                        f"Available locations: {[img.get('location', 'N/A') for img in images if isinstance(img, dict)]}"
                    )
                    return False
                
                if original_count == new_count:
                    logger.warning(f"Image count unchanged after deletion attempt for product {item_number}")
                    return False
                
                # Update images field
                images_json = json.dumps(images, ensure_ascii=False)
                cur.execute(
                    """
                    UPDATE product_management
                    SET images = %s::jsonb
                    WHERE item_number = %s
                    """,
                    (images_json, item_number)
                )
                
                if cur.rowcount > 0:
                    conn.commit()
                    logger.info(f"Deleted image {image_location} from product {item_number}")
                    return True
                else:
                    logger.warning(f"No rows updated when deleting image for product {item_number}")
                    return False
                    
    except Exception as e:
        logger.error(f"Error deleting image for product {item_number}: {e}", exc_info=True)
        return False


def update_product_management_settings(
    item_number: str,
    item_type: Optional[str] = None,
    genre_id: Optional[str] = None,
    tags: Optional[list[int]] = None,
    unlimited_inventory_flag: Optional[bool] = None,
    features: Optional[dict] = None,
    payment: Optional[dict] = None,
    title: Optional[str] = None,
    tagline: Optional[str] = None,
    product_description: Optional[dict] = None,
    sales_description: Optional[str] = None,
    src_url: Optional[str] = None,
    normal_delivery_date_id: Optional[int] = None,
    change_status: Optional[str] = None,
    *,
    dsn: Optional[str] = None
) -> bool:
    """
    Update product_management table settings for a specific product.
    
    Args:
        item_number: The item_number (product ID) to update
        item_type: Item type (NORMAL, PRE_ORDER, BUYING_CLUB)
        genre_id: Genre ID (100000-999999)
        tags: List of tag numbers (5000000-9999999)
        unlimited_inventory_flag: Unlimited inventory flag (True/False)
        features: Features dict with searchVisibility, inventoryDisplay, review
        payment: Payment dict with taxIncluded, taxRate, cashOnDeliveryFeeIncluded
        dsn: Optional database connection string
        
    Returns:
        True if update was successful, False otherwise
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                # Build update query dynamically based on provided fields
                updates = []
                params = []
                
                if item_type is not None:
                    updates.append("item_type = %s")
                    params.append(item_type)
                
                if genre_id is not None:
                    updates.append("genre_id = %s")
                    params.append(genre_id)
                elif genre_id is not None and genre_id == "":
                    updates.append("genre_id = NULL")
                
                if tags is not None:
                    updates.append("tags = %s")
                    params.append(tags)
                
                if unlimited_inventory_flag is not None:
                    updates.append("unlimited_inventory_flag = %s")
                    params.append(unlimited_inventory_flag)
                
                if features is not None:
                    # Get existing features and merge
                    cur.execute(
                        "SELECT features FROM product_management WHERE item_number = %s",
                        (item_number,)
                    )
                    existing_row = cur.fetchone()
                    existing_features = {}
                    if existing_row and existing_row[0]:
                        if isinstance(existing_row[0], dict):
                            existing_features = existing_row[0]
                        elif isinstance(existing_row[0], str):
                            try:
                                existing_features = json.loads(existing_row[0])
                            except Exception:
                                pass
                    
                    # Merge new features with existing
                    merged_features = {**existing_features, **features}
                    updates.append("features = %s::jsonb")
                    params.append(json.dumps(merged_features))
                
                if payment is not None:
                    # Get existing payment and merge
                    cur.execute(
                        "SELECT payment FROM product_management WHERE item_number = %s",
                        (item_number,)
                    )
                    existing_row = cur.fetchone()
                    existing_payment = {}
                    if existing_row and existing_row[0]:
                        if isinstance(existing_row[0], dict):
                            existing_payment = existing_row[0]
                        elif isinstance(existing_row[0], str):
                            try:
                                existing_payment = json.loads(existing_row[0])
                            except Exception:
                                pass
                    
                    # Merge new payment with existing
                    merged_payment = {**existing_payment, **payment}
                    updates.append("payment = %s::jsonb")
                    params.append(json.dumps(merged_payment))
                
                if title is not None:
                    updates.append("title = %s")
                    params.append(title)
                
                if tagline is not None:
                    updates.append("tagline = %s")
                    params.append(tagline)
                
                if product_description is not None:
                    updates.append("product_description = %s::jsonb")
                    params.append(json.dumps(product_description, ensure_ascii=False))
                
                if sales_description is not None:
                    updates.append("sales_description = %s")
                    params.append(sales_description)
                
                if src_url is not None:
                    updates.append("src_url = %s")
                    params.append(src_url)
                
                if change_status is not None:
                    updates.append("change_status = %s")
                    params.append(change_status)
                
                # Update variants with delivery date IDs if provided
                if normal_delivery_date_id is not None:
                    # Get current variants
                    cur.execute(
                        "SELECT variants FROM product_management WHERE item_number = %s",
                        (item_number,)
                    )
                    existing_row = cur.fetchone()
                    current_variants = {}
                    if existing_row and existing_row[0]:
                        if isinstance(existing_row[0], dict):
                            current_variants = existing_row[0]
                        elif isinstance(existing_row[0], str):
                            try:
                                current_variants = json.loads(existing_row[0])
                            except Exception:
                                pass
                    
                    # Update all variants with delivery date IDs
                    updated_variants = {}
                    for sku_id, variant in current_variants.items():
                        updated_variant = dict(variant) if isinstance(variant, dict) else {}
                        if normal_delivery_date_id is not None:
                            updated_variant["normalDeliveryDateId"] = normal_delivery_date_id
                        updated_variants[sku_id] = updated_variant
                    
                    if updated_variants:
                        updates.append("variants = %s::jsonb")
                        params.append(json.dumps(updated_variants, ensure_ascii=False))
                        logger.info(f"✅ Updated {len(updated_variants)} variants with delivery date IDs for product {item_number}")
                
                if not updates:
                    logger.warning(f"⚠️  No fields to update for product {item_number}")
                    return False
                
                # Add item_number to params for WHERE clause
                params.append(item_number)
                
                query = f"""
                    UPDATE product_management
                    SET {', '.join(updates)}
                    WHERE item_number = %s
                """
                
                cur.execute(query, params)
                
                if cur.rowcount == 0:
                    logger.warning(f"⚠️  No product found with item_number: {item_number}")
                    return False
                
                conn.commit()
                logger.info(f"✅ Updated settings for product {item_number}")
                return True
    except Exception as e:
        logger.error(f"❌ Failed to update settings for product {item_number}: {e}")
        raise


def update_all_products_hide_item(
    hide_item: bool,
    *,
    dsn: Optional[str] = None
) -> int:
    """
    Update the hide_item field for ALL products in the product_management table.
    Only updates products where rakuten_registration_status is NULL, empty, 'onsale', or 'true'.
    
    Args:
        hide_item: The new hide_item value (True = hidden, False = visible)
        dsn: Optional database connection string
        
    Returns:
        Number of products updated
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE product_management
                    SET hide_item = %s
                    WHERE (
                        rakuten_registration_status IS NULL
                        OR rakuten_registration_status = ''
                        OR rakuten_registration_status = 'onsale'
                        OR rakuten_registration_status = 'true'
                        OR rakuten_registration_status = 'false'
                    )
                    """,
                    (hide_item,)
                )
                updated_count = cur.rowcount
                conn.commit()
                logger.info(f"✅ Updated hide_item for {updated_count} products to {hide_item} (only products with rakuten_registration_status = NULL, '', 'onsale', 'true', or 'false')")
                return updated_count
    except Exception as e:
        logger.error(f"❌ Failed to update hide_item for all products: {e}")
        raise


def update_rakuten_registration_status(
    item_number: str,
    status: str,
    *,
    dsn: Optional[str] = None
) -> bool:
    """
    Update the Rakuten registration status for a product.
    
    Args:
        item_number: The item_number (product ID) to update
        status: Status value - "true" for success, "false" for failure, "unregistered" for not registered
        dsn: Optional database connection string
        
    Returns:
        True if update was successful, False otherwise
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    # Validate status value
    if status not in ["true", "false", "unregistered", "deleted", "onsale", "stop"]:
        logger.error(f"Invalid registration status value: {status}. Must be 'true', 'false', 'unregistered', 'deleted', 'onsale', or 'stop'")
        return False
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                # Update timestamp when registration is successful
                if status == "true":
                    cur.execute(
                        """
                        UPDATE product_management
                        SET rakuten_registration_status = %s,
                            rakuten_registered_at = now()
                        WHERE item_number = %s
                        """,
                        (status, item_number)
                    )
                elif status in ["onsale", "stop"]:
                    # For "onsale" and "stop", update status but keep existing timestamp
                    cur.execute(
                        """
                        UPDATE product_management
                        SET rakuten_registration_status = %s
                        WHERE item_number = %s
                        """,
                        (status, item_number)
                    )
                else:
                    # For "false", "unregistered", or "deleted", don't update timestamp (keep existing or set to NULL)
                    if status == "unregistered":
                        cur.execute(
                            """
                            UPDATE product_management
                            SET rakuten_registration_status = NULL,
                                rakuten_registered_at = NULL
                            WHERE item_number = %s
                            """,
                            (item_number,)
                        )
                    else:  # status == "false" or "deleted"
                        cur.execute(
                            """
                            UPDATE product_management
                            SET rakuten_registration_status = %s
                            WHERE item_number = %s
                            """,
                            (status, item_number)
                        )
                
                if cur.rowcount > 0:
                    conn.commit()
                    logger.info(f"Updated Rakuten registration status for product {item_number}: {status}")
                    return True
                else:
                    logger.warning(f"Product {item_number} not found when updating registration status")
                    return False
                    
    except Exception as e:
        logger.error(f"Error updating Rakuten registration status for product {item_number}: {e}", exc_info=True)
        return False


def get_pricing_settings(*, dsn: Optional[str] = None) -> dict:
    """
    Get pricing settings from the database.
    Falls back to settings.json if database settings don't exist.
    
    Args:
        dsn: Optional database connection string
        
    Returns:
        Dictionary with pricing settings:
        {
            "exchange_rate": float,
            "profit_margin_percent": float,
            "sales_commission_percent": float,
            "currency": str,
            "domestic_shipping_cost": float,
            "domestic_shipping_costs": dict,
            "international_shipping_rate": float,
            "customs_duty_rate": float,
        }
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    
    # Try to get from database first
    if dsn_final:
        try:
            ensure_settings_table(dsn=dsn_final)
            with get_db_connection_context(dsn=dsn_final) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        "SELECT setting_value FROM app_settings WHERE setting_key = 'pricing_settings'"
                    )
                    result = cur.fetchone()
                    if result and result.get('setting_value'):
                        settings = result['setting_value']
                        if isinstance(settings, dict):
                            shipping_costs = _normalize_domestic_shipping_costs(
                                settings.get("domestic_shipping_costs"),
                                settings.get("domestic_shipping_cost", 330.0),
                            )
                            settings["domestic_shipping_costs"] = shipping_costs
                            settings["domestic_shipping_cost"] = shipping_costs["regular"]
                            logger.info("✅ Loaded pricing settings from database")
                            return settings
        except Exception as e:
            logger.warning(f"⚠️  Failed to load pricing settings from database: {e}. Falling back to settings.json")
    
    # Fallback to settings.json
    try:
        settings_file = os.path.join(os.path.dirname(os.path.dirname(__file__)), "settings.json")
        if os.path.exists(settings_file):
            with open(settings_file, 'r', encoding='utf-8') as f:
                settings_data = json.load(f)
                shipping_costs = _normalize_domestic_shipping_costs(
                    settings_data.get("domestic_shipping_costs"),
                    settings_data.get("domestic_shipping_cost", 330.0),
                )
                return {
                    "exchange_rate": float(settings_data.get("exchange_rate", 22.0)),
                    "profit_margin_percent": float(settings_data.get("profit_margin_percent", 30.0)),
                    "sales_commission_percent": float(settings_data.get("sales_commission_percent", 3.0)),
                    "currency": str(settings_data.get("currency", "JPY")),
                    "domestic_shipping_cost": shipping_costs["regular"],
                    "domestic_shipping_costs": shipping_costs,
                    "international_shipping_rate": float(settings_data.get("international_shipping_rate", 18.7)),
                    "customs_duty_rate": float(settings_data.get("customs_duty_rate", 100.0)),
                }
    except Exception as e:
        logger.warning(f"⚠️  Failed to load pricing settings from settings.json: {e}. Using defaults")
    
    # Final fallback to defaults
    default_shipping_costs = _normalize_domestic_shipping_costs(None, 330.0)
    return {
        "exchange_rate": 22.0,
        "profit_margin_percent": 30.0,
        "sales_commission_percent": 3.0,
        "currency": "JPY",
        "domestic_shipping_cost": default_shipping_costs["regular"],
        "domestic_shipping_costs": default_shipping_costs,
        "international_shipping_rate": 18.7,
        "customs_duty_rate": 100.0,
    }


def ensure_category_management_table(*, dsn: Optional[str] = None) -> None:
    """
    Ensure the category_management table exists.
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    ensure_primary_category_table(dsn=dsn_final)
    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL_CATEGORY_MANAGEMENT)
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_category_management_name
                ON category_management (lower(category_name));
                """
            )
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'category_management'
                          AND column_name = 'primary_category_id'
                    ) THEN
                        ALTER TABLE category_management
                            ADD COLUMN primary_category_id bigint
                            REFERENCES primary_category_management(id)
                            ON DELETE SET NULL;
                    END IF;
                END $$;
                """
            )
            cur.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_category_management_primary_category
                ON category_management(primary_category_id);
                """
            )
            cur.execute("alter table category_management add column if not exists size_option text;")
            cur.execute("alter table category_management add column if not exists size numeric;")
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'category_management'
                          AND column_name = 'genre_id'
                    ) THEN
                        ALTER TABLE category_management
                            ADD COLUMN genre_id text;
                    END IF;
                END $$;
                """
            )
            # Add rakuten_category_ids column if it doesn't exist
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'category_management'
                          AND column_name = 'rakuten_category_ids'
                    ) THEN
                        ALTER TABLE category_management
                            ADD COLUMN rakuten_category_ids jsonb NOT NULL DEFAULT '[]'::jsonb;
                    END IF;
                END $$;
                """
            )
            # Ensure attributes column exists as jsonb[]
            cur.execute(
                """
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                         WHERE table_name = 'category_management'
                           AND column_name = 'attributes'
                    ) THEN
                        ALTER TABLE category_management
                            ADD COLUMN attributes jsonb NOT NULL DEFAULT '[]'::jsonb;
                    END IF;
                END $$;
                """
            )
        conn.commit()


def _normalise_dimension(value: Optional[float]) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _normalise_primary_category_id(value: object) -> Optional[int]:
    if value in (None, "", 0):
        return None
    try:
        candidate = int(value)
    except (TypeError, ValueError):
        return None
    return candidate if candidate > 0 else None


def _primary_category_exists(category_id: int, *, dsn: Optional[str] = None) -> bool:
    _ensure_import()
    ensure_primary_category_table(dsn=dsn)
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM primary_category_management WHERE id = %s", (category_id,))
            return cur.fetchone() is not None


def list_primary_categories(*, dsn: Optional[str] = None) -> list[dict]:
    """
    Return all primary categories ordered by most recent update.
    """
    _ensure_import()
    ensure_primary_category_table(dsn=dsn)
    dsn_final = dsn or _get_dsn()

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, category_name, default_category_ids, created_at, updated_at
                  FROM primary_category_management
              ORDER BY updated_at DESC, id DESC
                """
            )
            rows = cur.fetchall()
            
            # Convert JSONB default_category_ids to list of strings
            for row in rows:
                default_ids = row.get("default_category_ids")
                if default_ids is None:
                    row["default_category_ids"] = []
                elif isinstance(default_ids, str):
                    try:
                        row["default_category_ids"] = json.loads(default_ids)
                    except json.JSONDecodeError:
                        row["default_category_ids"] = []
                elif isinstance(default_ids, (list, tuple)):
                    row["default_category_ids"] = [str(id) for id in default_ids]
                else:
                    row["default_category_ids"] = []

    return rows


def create_primary_category(
    *,
    category_name: str,
    default_category_ids: Optional[List[str]] = None,
    dsn: Optional[str] = None,
) -> dict:
    """
    Create a primary category and return the inserted row.
    """
    _ensure_import()
    ensure_primary_category_table(dsn=dsn)
    dsn_final = dsn or _get_dsn()
    trimmed_name = category_name.strip()
    if not trimmed_name:
        raise ValueError("Category name is required.")
    
    # Normalize default_category_ids
    if default_category_ids is None:
        default_category_ids = []
    # Ensure all IDs are strings
    normalized_ids = [str(cid).strip() for cid in default_category_ids if cid and str(cid).strip()]
    default_category_ids_json = json.dumps(normalized_ids, ensure_ascii=False)

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id FROM primary_category_management WHERE category_name = %s
                """,
                (trimmed_name,),
            )
            row = cur.fetchone()
            if row:
                # Existing category reuse
                cur.execute(
                    """
                    UPDATE primary_category_management
                       SET default_category_ids = %s::jsonb,
                           updated_at = now()
                     WHERE id = %s
                    RETURNING id, category_name, default_category_ids, created_at, updated_at
                    """,
                    (default_category_ids_json, row["id"]),
                )
                result = cur.fetchone()
            else:
                cur.execute(
                    """
                    INSERT INTO primary_category_management (category_name, default_category_ids)
                    VALUES (%s, %s::jsonb)
                    RETURNING id, category_name, default_category_ids, created_at, updated_at
                    """,
                    (trimmed_name, default_category_ids_json),
                )
                result = cur.fetchone()
        conn.commit()
        
        # Convert JSONB default_category_ids to list of strings
        if result:
            default_ids = result.get("default_category_ids")
            if default_ids is None:
                result["default_category_ids"] = []
            elif isinstance(default_ids, str):
                try:
                    result["default_category_ids"] = json.loads(default_ids)
                except json.JSONDecodeError:
                    result["default_category_ids"] = []
            elif isinstance(default_ids, (list, tuple)):
                result["default_category_ids"] = [str(id) for id in default_ids]
            else:
                result["default_category_ids"] = []

    return result


def update_primary_category(
    category_id: int,
    *,
    category_name: Optional[str] = None,
    default_category_ids: Optional[List[str]] = None,
    dsn: Optional[str] = None,
) -> Optional[dict]:
    """
    Update a primary category name. Returns None if not found.
    """
    _ensure_import()
    ensure_primary_category_table(dsn=dsn)
    dsn_final = dsn or _get_dsn()
    
    # Build update assignments
    assignments = []
    params = []
    
    if category_name is not None:
        trimmed_name = category_name.strip()
        if not trimmed_name:
            raise ValueError("Category name cannot be empty.")
        assignments.append("category_name = %s")
        params.append(trimmed_name)
    
    if default_category_ids is not None:
        # Normalize default_category_ids
        normalized_ids = [str(cid).strip() for cid in default_category_ids if cid and str(cid).strip()]
        default_category_ids_json = json.dumps(normalized_ids, ensure_ascii=False)
        assignments.append("default_category_ids = %s::jsonb")
        params.append(default_category_ids_json)
    
    if not assignments:
        raise ValueError("At least one field must be provided for update.")
    
    assignments.append("updated_at = now()")
    params.append(category_id)

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                UPDATE primary_category_management
                   SET {', '.join(assignments)}
                 WHERE id = %s
                RETURNING id, category_name, default_category_ids, created_at, updated_at
                """,
                tuple(params),
            )
            row = cur.fetchone()
        conn.commit()
        
        # Convert JSONB default_category_ids to list of strings
        if row:
            default_ids = row.get("default_category_ids")
            if default_ids is None:
                row["default_category_ids"] = []
            elif isinstance(default_ids, str):
                try:
                    row["default_category_ids"] = json.loads(default_ids)
                except json.JSONDecodeError:
                    row["default_category_ids"] = []
            elif isinstance(default_ids, (list, tuple)):
                row["default_category_ids"] = [str(id) for id in default_ids]
            else:
                row["default_category_ids"] = []

    return row


def delete_primary_category(
    category_id: int,
    *,
    dsn: Optional[str] = None,
) -> bool:
    """
    Delete a primary category. Secondary categories referencing it will be set to NULL.
    """
    _ensure_import()
    ensure_primary_category_table(dsn=dsn)
    ensure_category_management_table(dsn=dsn)
    dsn_final = dsn or _get_dsn()

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE category_management SET primary_category_id = NULL WHERE primary_category_id = %s",
                (category_id,),
            )
            cur.execute(
                "DELETE FROM primary_category_management WHERE id = %s",
                (category_id,),
            )
            deleted = cur.rowcount > 0
        conn.commit()

    return deleted


def list_categories(*, dsn: Optional[str] = None) -> list[dict]:
    """
    Return all category management entries ordered by most recent update.
    """
    _ensure_import()
    ensure_category_management_table(dsn=dsn)
    dsn_final = dsn or _get_dsn()
    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT cm.id,
                       cm.category_name,
                       cm.category_ids,
                       cm.rakuten_category_ids,
                       cm.genre_id,
                       cm.primary_category_id,
                       pcm.category_name as primary_category_name,
                       cm.weight,
                       cm.length,
                       cm.width,
                       cm.height,
                       cm.size_option,
                       cm.size,
                       cm.attributes,
                       cm.created_at,
                       cm.updated_at
                  FROM category_management cm
             LEFT JOIN primary_category_management pcm
                    ON cm.primary_category_id = pcm.id
              ORDER BY cm.updated_at DESC, cm.id DESC
                """
            )
            rows = cur.fetchall()

    normalised_rows: list[dict] = []
    for row in rows:
        category_ids_value = row.get("category_ids") or []
        if isinstance(category_ids_value, str):
            try:
                category_ids_value = json.loads(category_ids_value)
            except json.JSONDecodeError:
                category_ids_value = []
        if not isinstance(category_ids_value, (list, tuple)):
            category_ids_value = []

        rakuten_ids_value = row.get("rakuten_category_ids") or []
        if isinstance(rakuten_ids_value, str):
            try:
                rakuten_ids_value = json.loads(rakuten_ids_value)
            except json.JSONDecodeError:
                rakuten_ids_value = []
        if not isinstance(rakuten_ids_value, (list, tuple)):
            rakuten_ids_value = []

        attributes_value = row.get("attributes") or []
        if isinstance(attributes_value, str):
            try:
                attributes_value = json.loads(attributes_value)
            except json.JSONDecodeError:
                attributes_value = []
        if not isinstance(attributes_value, (list, tuple)):
            attributes_value = []

        normalised_rows.append(
            {
                "id": row["id"],
                "category_name": row["category_name"],
                "category_ids": category_ids_value,
                "rakuten_category_ids": [str(rid).strip() for rid in rakuten_ids_value if rid and str(rid).strip()],
                "genre_id": row.get("genre_id"),
                "attributes": attributes_value,
                "primary_category_id": row.get("primary_category_id"),
                "primary_category_name": row.get("primary_category_name"),
                "weight": _normalise_dimension(row.get("weight")),
                "length": _normalise_dimension(row.get("length")),
                "width": _normalise_dimension(row.get("width")),
                "height": _normalise_dimension(row.get("height")),
                "size_option": row.get("size_option"),
                "size": _normalise_dimension(row.get("size")),
                "created_at": row["created_at"],
                "updated_at": row["updated_at"],
            }
        )

    return normalised_rows


def _sync_r_cat_id_for_category_ids(
    category_ids: list[str],
    rakuten_category_ids: Optional[list[str]],
    *,
    dsn: str,
) -> None:
    """
    Synchronise r_cat_id in products_origin and product_management for the given category IDs.

    If rakuten_category_ids is empty or None, r_cat_id will be set to an empty array for matching products.
    Otherwise, the full Rakuten category ID list is written to r_cat_id as a JSON array.
    """
    if not category_ids:
        return

    clean_category_ids = [str(cid).strip() for cid in category_ids if cid and str(cid).strip()]
    if not clean_category_ids:
        return

    clean_rakuten_ids: list[str] = []
    if rakuten_category_ids:
        clean_rakuten_ids = [str(rid).strip() for rid in rakuten_category_ids if rid and str(rid).strip()]

    # Represent r_cat_id as JSON array (text form for ::jsonb cast)
    r_cat_id_json = json.dumps(clean_rakuten_ids, ensure_ascii=False)

    try:
        with get_db_connection_context(dsn=dsn) as conn:
            with conn.cursor() as cur:
                # Set r_cat_id JSON array (may be empty) for matching products
                cur.execute(
                    """
                    UPDATE products_origin
                       SET r_cat_id = %s::jsonb
                     WHERE middle_category = ANY(%s::text[])
                        OR main_category = ANY(%s::text[])
                    """,
                    (r_cat_id_json, clean_category_ids, clean_category_ids),
                )
                cur.execute(
                    """
                    UPDATE product_management pm
                       SET r_cat_id = %s::jsonb
                      FROM products_origin po
                     WHERE pm.item_number = po.product_id
                       AND (po.main_category = ANY(%s::text[])
                            OR po.middle_category = ANY(%s::text[]))
                    """,
                    (r_cat_id_json, clean_category_ids, clean_category_ids),
                )
            conn.commit()
        logger.info(
            "✅ Synced r_cat_id for category IDs %s to %s",
            clean_category_ids,
            clean_rakuten_ids,
        )
    except Exception as exc:
        logger.warning(f"⚠️  Failed to sync r_cat_id for category IDs {clean_category_ids}: {exc}")


def create_category_entry(
    *,
    category_name: str,
    category_ids: list[str],
    rakuten_category_ids: Optional[list[str]] = None,
    genre_id: Optional[str] = None,
    primary_category_id: Optional[int] = None,
    weight: Optional[float] = None,
    length: Optional[float] = None,
    width: Optional[float] = None,
    height: Optional[float] = None,
    size_option: Optional[str] = None,
    size: Optional[float] = None,
    attributes: Optional[list[dict]] = None,
    dsn: Optional[str] = None,
) -> dict:
    """
    Insert a new category entry and return the created row.
    """
    _ensure_import()
    ensure_category_management_table(dsn=dsn)
    dsn_final = dsn or _get_dsn()

    clean_ids = [cid.strip() for cid in category_ids if cid and cid.strip()]
    if not clean_ids:
        raise ValueError("At least one category ID is required.")

    # Normalize rakuten_category_ids
    if rakuten_category_ids is None:
        rakuten_category_ids = []
    clean_rakuten_ids = [rid.strip() for rid in rakuten_category_ids if rid and str(rid).strip()]
    rakuten_category_ids_json = json.dumps(clean_rakuten_ids, ensure_ascii=False)

    # Normalize attributes (list of {name, values: [...]})
    if attributes is None:
        attributes = []
    clean_attributes: list[dict] = []
    for attr in attributes:
        if not isinstance(attr, dict):
            continue
        name = str(attr.get("name", "")).strip()
        raw_values = attr.get("values", [])
        if not name:
            continue
        if isinstance(raw_values, str):
            values_list = [v.strip() for v in raw_values.split(",") if v.strip()]
        elif isinstance(raw_values, (list, tuple)):
            values_list = [str(v).strip() for v in raw_values if v and str(v).strip()]
        else:
            values_list = []
        clean_attributes.append({"name": name, "values": values_list})
    attributes_json = json.dumps(clean_attributes, ensure_ascii=False)

    db_primary_category_id = _normalise_primary_category_id(primary_category_id)
    if db_primary_category_id is not None and not _primary_category_exists(db_primary_category_id, dsn=dsn):
        raise ValueError(f"Primary category ID {db_primary_category_id} does not exist.")

    normalized_size_option, normalized_size_value = _normalize_size_selection(
        size_option,
        size,
    )

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO category_management (
                    category_name,
                    category_ids,
                    rakuten_category_ids,
                    genre_id,
                    primary_category_id,
                    weight,
                    length,
                    width,
                    height,
                    size_option,
                    size,
                    attributes
                )
                VALUES (%s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                RETURNING id, category_name, category_ids, rakuten_category_ids, genre_id, primary_category_id, weight, length, width, height, size_option, size, attributes, created_at, updated_at
                """,
                (
                    category_name,
                    json.dumps(clean_ids),
                    rakuten_category_ids_json,
                    genre_id.strip() if genre_id is not None and str(genre_id).strip() else None,
                    db_primary_category_id,
                    _normalise_dimension(weight),
                    _normalise_dimension(length),
                    _normalise_dimension(width),
                    _normalise_dimension(height),
                    normalized_size_option,
                    _normalise_dimension(normalized_size_value),
                    attributes_json,
                ),
            )
            row = cur.fetchone()
            if row and row.get("primary_category_id"):
                cur.execute(
                    "SELECT category_name FROM primary_category_management WHERE id = %s",
                    (row["primary_category_id"],),
                )
                name_row = cur.fetchone()
                if name_row:
                    row["primary_category_name"] = name_row["category_name"]
            
            # Update products_origin measurements if provided
            if row:
                final_category_ids = clean_ids  # Use the clean_ids that were inserted
                
                if final_category_ids:
                    # Weight update
                    if weight is not None:
                        try:
                            updated_count = update_products_weight_by_category_ids(
                                final_category_ids,
                                weight,
                                dsn=dsn_final,
                            )
                            logger.info(
                                f"Updated {updated_count} products in products_origin "
                                f"with weight {weight} for category IDs: {final_category_ids}"
                            )
                        except Exception as e:
                            logger.error(f"Failed to update product weights after category creation: {e}")
                    # Dimensions update (length/width/height)
                    for column_name, column_value in (
                        ("length", length),
                        ("width", width),
                        ("height", height),
                        ("size", normalized_size_value),
                    ):
                        if column_value is None:
                            continue
                        try:
                            _update_products_numeric_column_by_category_ids(
                                final_category_ids,
                                column_name,
                                _normalise_dimension(column_value),
                                dsn=dsn_final,
                            )
                        except Exception as e:
                            logger.error(
                                f"Failed to update product {column_name} after category creation: {e}"
                            )
        conn.commit()

    # Sync r_cat_id for this category (if any Rakuten IDs provided)
    try:
        _sync_r_cat_id_for_category_ids(clean_ids, clean_rakuten_ids, dsn=dsn_final)
    except Exception as exc:
        logger.warning(f"⚠️  Failed to sync r_cat_id after category creation: {exc}")

    return {
        "id": row["id"],
        "category_name": row["category_name"],
        "category_ids": row["category_ids"] or [],
        "rakuten_category_ids": clean_rakuten_ids,
        "attributes": clean_attributes,
        "primary_category_id": row.get("primary_category_id"),
        "primary_category_name": row.get("primary_category_name"),
        "weight": _normalise_dimension(row.get("weight")),
        "length": _normalise_dimension(row.get("length")),
        "width": _normalise_dimension(row.get("width")),
        "height": _normalise_dimension(row.get("height")),
        "size_option": row.get("size_option"),
        "size": _normalise_dimension(row.get("size")),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def update_category_entry(
    category_id: int,
    *,
    category_name: Optional[str] = None,
    category_ids: Optional[list[str]] = None,
    rakuten_category_ids: Optional[list[str]] = None,
    genre_id: object = _UNSET,
    primary_category_id: object = _UNSET,
    weight: object = _UNSET,
    length: object = _UNSET,
    width: object = _UNSET,
    height: object = _UNSET,
    size_option: object = _UNSET,
    size: object = _UNSET,
    attributes: object = _UNSET,
    dsn: Optional[str] = None,
) -> Optional[dict]:
    """
    Update an existing category entry and return the updated row.
    Returns None if the entry does not exist.
    """
    _ensure_import()
    ensure_category_management_table(dsn=dsn)
    dsn_final = dsn or _get_dsn()

    assignments: list[str] = []
    values: list = []

    if category_name is not None:
        assignments.append("category_name = %s")
        values.append(category_name)

    if category_ids is not None:
        clean_ids = [cid.strip() for cid in category_ids if cid and cid.strip()]
        if not clean_ids:
            raise ValueError("At least one category ID is required.")
        assignments.append("category_ids = %s::jsonb")
        values.append(json.dumps(clean_ids))

    if rakuten_category_ids is not None:
        clean_rakuten_ids = [rid.strip() for rid in rakuten_category_ids if rid and str(rid).strip()]
        assignments.append("rakuten_category_ids = %s::jsonb")
        values.append(json.dumps(clean_rakuten_ids, ensure_ascii=False))

    if genre_id is not _UNSET:
        if genre_id is None:
            assignments.append("genre_id = NULL")
        else:
            genre_value = str(genre_id).strip()
            assignments.append("genre_id = %s")
            values.append(genre_value if genre_value else None)

    if primary_category_id is not _UNSET:
        normalised_primary_id = _normalise_primary_category_id(primary_category_id)
        if normalised_primary_id is not None and not _primary_category_exists(normalised_primary_id, dsn=dsn):
            raise ValueError(f"Primary category ID {normalised_primary_id} does not exist.")
        if normalised_primary_id is None:
            assignments.append("primary_category_id = NULL")
        else:
            assignments.append("primary_category_id = %s")
            values.append(normalised_primary_id)

    numeric_updates: dict[str, Optional[float]] = {}
    for column_name, column_value in (
        ("weight", weight),
        ("length", length),
        ("width", width),
        ("height", height),
    ):
        if column_value is _UNSET:
            continue
        if column_value is None:
            assignments.append(f"{column_name} = NULL")
        else:
            normalised_value = _normalise_dimension(column_value)
            assignments.append(f"{column_name} = %s")
            values.append(normalised_value)
            numeric_updates[column_name] = normalised_value
            continue
        numeric_updates[column_name] = None

    size_option_input = None if size_option is _UNSET else (size_option if size_option is None else str(size_option))
    size_value_input = None if size is _UNSET else _normalise_dimension(size)
    if size_option is not _UNSET or size is not _UNSET:
        normalized_size_option, normalized_size_value = _normalize_size_selection(
            size_option_input,
            size_value_input,
        )
        if size_option is not _UNSET:
            if normalized_size_option is None:
                assignments.append("size_option = NULL")
            else:
                assignments.append("size_option = %s")
                values.append(normalized_size_option)
        if size_option is not _UNSET or size is not _UNSET:
            if normalized_size_value is None:
                assignments.append("size = NULL")
                numeric_updates["size"] = None
            else:
                assignments.append("size = %s")
                values.append(normalized_size_value)
                numeric_updates["size"] = normalized_size_value

    # Attributes update (list of {name, values})
    if attributes is not _UNSET:
        clean_attributes: list[dict] = []
        source = attributes
        if isinstance(source, str):
            try:
                loaded = json.loads(source)
            except Exception:
                loaded = []
            source = loaded
        if isinstance(source, (list, tuple)):
            for attr in source:
                if not isinstance(attr, dict):
                    continue
                name = str(attr.get("name", "")).strip()
                raw_values = attr.get("values", [])
                if not name:
                    continue
                if isinstance(raw_values, str):
                    values_list = [v.strip() for v in raw_values.split(",") if v.strip()]
                elif isinstance(raw_values, (list, tuple)):
                    values_list = [str(v).strip() for v in raw_values if v and str(v).strip()]
                else:
                    values_list = []
                clean_attributes.append({"name": name, "values": values_list})
        assignments.append("attributes = %s::jsonb")
        values.append(json.dumps(clean_attributes, ensure_ascii=False))

    if not assignments:
        return None

    assignments.append("updated_at = now()")
    values.append(category_id)

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                f"""
                UPDATE category_management
                   SET {', '.join(assignments)}
                 WHERE id = %s
                RETURNING id, category_name, category_ids, rakuten_category_ids, genre_id, primary_category_id, weight, length, width, height, size_option, size, created_at, updated_at
                """,
                values,
            )
            row = cur.fetchone()
            if row and row.get("primary_category_id"):
                cur.execute(
                    "SELECT category_name FROM primary_category_management WHERE id = %s",
                    (row["primary_category_id"],),
                )
                name_row = cur.fetchone()
                if name_row:
                    row["primary_category_name"] = name_row["category_name"]
            
        conn.commit()

    if not row:
        return None

    # Update r_cat_id in products_origin and product_management if rakuten_category_ids was updated
    if rakuten_category_ids is not None:
        try:
            # Get the updated category_ids for this category
            final_category_ids = row.get("category_ids") or []
            if isinstance(final_category_ids, str):
                try:
                    final_category_ids = json.loads(final_category_ids)
                except json.JSONDecodeError:
                    final_category_ids = []
            elif not isinstance(final_category_ids, (list, tuple)):
                final_category_ids = []

            final_rakuten_ids = row.get("rakuten_category_ids") or []
            if isinstance(final_rakuten_ids, str):
                try:
                    final_rakuten_ids = json.loads(final_rakuten_ids)
                except json.JSONDecodeError:
                    final_rakuten_ids = []
            elif not isinstance(final_rakuten_ids, (list, tuple)):
                final_rakuten_ids = []

            _sync_r_cat_id_for_category_ids(
                [str(cid).strip() for cid in final_category_ids if cid and str(cid).strip()],
                [str(rid).strip() for rid in final_rakuten_ids if rid and str(rid).strip()],
                dsn=dsn_final,
            )
        except Exception as e:
            logger.warning(f"⚠️  Failed to update r_cat_id for related products: {e}")

    # Update products_origin measurements outside the transaction
    if row and numeric_updates:
        final_category_ids = row.get("category_ids") or []
        if isinstance(final_category_ids, str):
                    try:
                        final_category_ids = json.loads(final_category_ids)
                    except json.JSONDecodeError:
                        final_category_ids = []
        elif not isinstance(final_category_ids, (list, tuple)):
            final_category_ids = []
        
        clean_category_ids = [str(cid).strip() for cid in final_category_ids if cid and str(cid).strip()]
        
        if clean_category_ids:
            # Weight
            if "weight" in numeric_updates:
                try:
                    update_products_weight_by_category_ids(
                        clean_category_ids,
                        numeric_updates["weight"],
                        dsn=dsn_final,
                    )
                except Exception as e:
                    logger.error(f"Failed to update product weights after category update: {e}", exc_info=True)
            # Dimensions
            for column_name in ("length", "width", "height", "size"):
                if column_name not in numeric_updates:
                    continue
                try:
                    _update_products_numeric_column_by_category_ids(
                        clean_category_ids,
                        column_name,
                        numeric_updates[column_name],
                        dsn=dsn_final,
                    )
                except Exception as e:
                    logger.error(
                        f"Failed to update product {column_name} after category update: {e}",
                        exc_info=True,
                    )
        else:
            logger.warning(f"Category {category_id} has no valid category_ids for dimension propagation")

    # Convert JSONB rakuten_category_ids to list of strings
    rakuten_ids = row.get("rakuten_category_ids")
    if rakuten_ids is None:
        rakuten_ids_list = []
    elif isinstance(rakuten_ids, str):
        try:
            rakuten_ids_list = json.loads(rakuten_ids)
        except json.JSONDecodeError:
            rakuten_ids_list = []
    elif isinstance(rakuten_ids, (list, tuple)):
        rakuten_ids_list = [str(rid) for rid in rakuten_ids]
    else:
        rakuten_ids_list = []
    
    return {
        "id": row["id"],
        "category_name": row["category_name"],
        "category_ids": row["category_ids"] or [],
        "rakuten_category_ids": rakuten_ids_list,
        "primary_category_id": row.get("primary_category_id"),
        "primary_category_name": row.get("primary_category_name"),
        "weight": _normalise_dimension(row.get("weight")),
        "length": _normalise_dimension(row.get("length")),
        "width": _normalise_dimension(row.get("width")),
        "height": _normalise_dimension(row.get("height")),
        "size_option": row.get("size_option"),
        "size": _normalise_dimension(row.get("size")),
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def delete_category_entry(category_id: int, *, dsn: Optional[str] = None) -> bool:
    """
    Delete a category entry by ID. Returns True if a row was deleted.
    """
    _ensure_import()
    ensure_category_management_table(dsn=dsn)
    dsn_final = dsn or _get_dsn()

    # Load category_ids (and Rakuten IDs, if needed) before deletion so we can clear r_cat_id mappings
    category_ids_value: list[str] = []
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT category_ids FROM category_management WHERE id = %s",
                    (category_id,),
                )
                row = cur.fetchone()
                if row:
                    category_ids_raw = row[0]
                    if isinstance(category_ids_raw, str):
                        try:
                            category_ids_value = json.loads(category_ids_raw)
                        except json.JSONDecodeError:
                            category_ids_value = []
                    elif isinstance(category_ids_raw, (list, tuple)):
                        category_ids_value = list(category_ids_raw)
    except Exception as exc:
        logger.warning(f"⚠️  Failed to load category_ids before delete for category_id={category_id}: {exc}")

    # Clear r_cat_id for products linked to this category (if we successfully loaded IDs)
    if category_ids_value:
        try:
            _sync_r_cat_id_for_category_ids(
                [str(cid).strip() for cid in category_ids_value if cid and str(cid).strip()],
                [],  # empty Rakuten list → clear r_cat_id
                dsn=dsn_final,
            )
        except Exception as exc:
            logger.warning(f"⚠️  Failed to clear r_cat_id when deleting category_id={category_id}: {exc}")

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM category_management WHERE id = %s", (category_id,))
            deleted = cur.rowcount > 0
        conn.commit()

    return deleted


def fix_products_origin_schema(*, dsn: Optional[str] = None) -> None:
    """Fix the products_origin table schema by ensuring all required columns exist."""
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            # Check if table exists
            cur.execute("""
                SELECT EXISTS (
                    SELECT FROM information_schema.tables 
                    WHERE table_name = 'products_origin'
                );
            """)
            table_exists = cur.fetchone()[0]
            
            if not table_exists:
                # Create table with full schema
                cur.execute(SCHEMA_SQL)
            else:
                # Add missing columns - ensure all required columns exist
                cur.execute("alter table products_origin add column if not exists creation_date timestamptz;")
                cur.execute("alter table products_origin add column if not exists repurchase_rate numeric;")
                cur.execute("alter table products_origin add column if not exists rating_score numeric;")
                cur.execute("alter table products_origin add column if not exists detail_json jsonb;")
                cur.execute("alter table products_origin add column if not exists weight numeric;")
                cur.execute("alter table products_origin add column if not exists length numeric;")
                cur.execute("alter table products_origin add column if not exists width numeric;")
                cur.execute("alter table products_origin add column if not exists height numeric;")
                cur.execute("alter table products_origin add column if not exists size numeric;")
                # Add registration_status column if it doesn't exist (1=unregistered, 2=registered, 3=previously_registered)
                cur.execute("alter table products_origin add column if not exists registration_status int default 1;")
                # Ensure r_cat_id column exists as jsonb[] (Rakuten category IDs)
                cur.execute("""
                    DO $$
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM information_schema.columns
                             WHERE table_name = 'products_origin'
                               AND column_name = 'r_cat_id'
                        ) THEN
                            ALTER TABLE products_origin
                                ADD COLUMN r_cat_id jsonb NOT NULL DEFAULT '[]'::jsonb;
                        ELSIF EXISTS (
                            SELECT 1 FROM information_schema.columns
                             WHERE table_name = 'products_origin'
                               AND column_name = 'r_cat_id'
                               AND data_type <> 'jsonb'
                        ) THEN
                            -- Migrate old scalar/text values to jsonb array
                            ALTER TABLE products_origin
                                ALTER COLUMN r_cat_id TYPE jsonb
                                USING
                                    CASE
                                        WHEN r_cat_id IS NULL OR r_cat_id::text = '' THEN '[]'::jsonb
                                        WHEN r_cat_id::text LIKE '[%' THEN r_cat_id::jsonb
                                        ELSE jsonb_build_array(r_cat_id::text)
                                    END;
                            ALTER TABLE products_origin
                                ALTER COLUMN r_cat_id SET DEFAULT '[]'::jsonb;
                            UPDATE products_origin
                               SET r_cat_id = '[]'::jsonb
                             WHERE r_cat_id IS NULL;
                        END IF;
                    END $$;
                """)
                # Ensure registration_status uses integer storage even if the column existed previously as text
                cur.execute(
                    """
                    SELECT data_type
                    FROM information_schema.columns
                    WHERE table_name = 'products_origin'
                      AND column_name = 'registration_status'
                    """
                )
                reg_status_type = cur.fetchone()
                if reg_status_type and reg_status_type[0] != "integer":
                    logger.info("Converting products_origin.registration_status column to integer type")
                    cur.execute(
                        """
                        ALTER TABLE products_origin
                        ALTER COLUMN registration_status DROP DEFAULT
                        """
                    )
                    cur.execute(
                        """
                        UPDATE products_origin
                        SET registration_status = CASE
                            WHEN registration_status IN ('registered', '2') THEN '2'
                            WHEN registration_status IN ('previously_registered', '3') THEN '3'
                            WHEN registration_status IN ('unregistered', '1') THEN '1'
                            WHEN registration_status IS NULL OR registration_status = '' THEN '1'
                            WHEN registration_status ~ '^[0-9]+$' THEN registration_status
                            ELSE '1'
                        END
                        """
                    )
                    cur.execute(
                        """
                        ALTER TABLE products_origin
                        ALTER COLUMN registration_status TYPE integer
                        USING registration_status::integer
                        """
                    )
                    cur.execute(
                        """
                        ALTER TABLE products_origin
                        ALTER COLUMN registration_status SET DEFAULT 1
                        """
                    )
                    cur.execute(
                        """
                        UPDATE products_origin po
                        SET registration_status = 2
                        FROM product_management pm
                        WHERE po.product_id = pm.item_number
                          AND po.registration_status <> 2
                        """
                    )
                    cur.execute(
                        """
                        UPDATE products_origin
                        SET registration_status = 1
                        WHERE registration_status IS NULL OR registration_status NOT IN (1,2,3)
                        """
                    )
                # Deduplicate existing duplicate product_id rows, keep latest created_at
                cur.execute("""
                    delete from products_origin a
                    using products_origin b
                    where a.product_id is not null
                      and a.product_id = b.product_id
                      and a.ctid < b.ctid;
                """)
                # Ensure UNIQUE index for upsert by product_id
                try:
                    cur.execute("create unique index if not exists uniq_products_origin_product_id on products_origin(product_id);")
                except Exception as e:
                    logger.warning(f"Could not create unique index on product_id: {e}")
            
            conn.commit()
            
            # Make sure the products_origin.id sequence is not behind MAX(id)
            _reset_products_origin_id_sequence(dsn=dsn_final)
            
            # Clean up any existing empty records
            cleanup_empty_records(dsn_final)

    # Ensure legacy columns are removed
    drop_removed_columns_from_products_origin(dsn=dsn_final)


def drop_removed_columns_from_products_origin(*, dsn: Optional[str] = None) -> None:
    """
    Migration function to drop removed columns from products_origin table.
    Drops: manufacturer_name, brand, sub_category, catch_copy, color, size, shape,
    features, material_specifications, packaging_size, selling_unit, total_weight_per_unit,
    product_image, image_1, image_2, image_3, image_4, image_5, image_6, image_7, image_8,
    minimum_order_quantity, in_stock_quantity, product_reviews,
    jpy_price, wholesale_margin, shipping_cost, shipping_type, delivery_time, country_of_origin,
    image_processing_status, processed_images, image_processing_date, detail_images
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    columns_to_drop = [
        "manufacturer_name",
        "brand",
        "sub_category",
        "catch_copy",
        "color",
    
        "shape",
        "features",
        "material_specifications",
        "packaging_size",
        "selling_unit",
        "total_weight_per_unit",
        "product_image",
        "image_1",
        "image_2",
        "image_3",
        "image_4",
        "image_5",
        "image_6",
        "image_7",
        "image_8",
        "minimum_order_quantity",
        "in_stock_quantity",
        "product_reviews",
        "jpy_price",
        "wholesale_margin",
        "shipping_cost",
        "shipping_type",
        "delivery_time",
        "country_of_origin",
        "image_processing_status",
        "processed_images",
        "image_processing_date",
        "detail_images",
    ]
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                # Check if table exists
                cur.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'products_origin'
                    );
                """)
                table_exists = cur.fetchone()[0]
                
                if not table_exists:
                    logger.info("products_origin table does not exist, skipping column drop")
                    return
                
                # Drop each column if it exists
                for column in columns_to_drop:
                    try:
                        cur.execute(f"ALTER TABLE products_origin DROP COLUMN IF EXISTS {column} CASCADE;")
                        logger.info(f"Dropped column {column} from products_origin table")
                    except Exception as e:
                        logger.warning(f"Could not drop column {column}: {e}")
                
                conn.commit()
                logger.info("Successfully dropped removed columns from products_origin table")
                
    except Exception as e:
        logger.error(f"Error dropping columns from products_origin table: {e}")
        raise


def reset_products_origin_table(*, dsn: Optional[str] = None) -> None:
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            cur.execute("drop table if exists products_origin cascade;")
            cur.execute(SCHEMA_SQL)


def save_products_origin_to_db(
    products: Iterable[dict],
    *,
    dsn: Optional[str] = None,
    create_table_if_missing: bool = True,
) -> int:
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    rows = []
    now = dt.datetime.utcnow()
    # Exchange rate RMB->JPY, default 20.0 if not provided
    # Read exchange rate from database (with fallback to settings.json, then env)
    def _get_exchange_rate() -> float:
        try:
            pricing_settings = get_pricing_settings(dsn=dsn)
            fx = float(pricing_settings.get('exchange_rate', 20.0))
            return fx if fx > 0 else 20.0
        except Exception as e:
            logger.warning(f"⚠️  Failed to get exchange rate from database: {e}. Using fallback.")
            try:
                import json as _json
                settings_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'settings.json')
                if os.path.exists(settings_path):
                    with open(settings_path, 'r', encoding='utf-8') as f:
                        s = _json.load(f)
                        fx = float(s.get('exchange_rate', 20.0))
                        return fx if fx > 0 else 20.0
            except Exception:
                pass
            try:
                return float(os.getenv("EXCHANGE_RATE", "20.0"))
            except Exception:
                return 20.0

    default_fx = _get_exchange_rate()
    
    # Filter out products with missing essential data
    valid_products = []
    for p in products:
        # Check if product has essential fields
        goods_id = p.get("goodsId")
        title_c = p.get("titleC")
        title_t = p.get("titleT")
        
        # Must have a valid goods ID
        if not goods_id or str(goods_id).strip() == "":
            logger.warning(f"Skipping product with missing/invalid goodsId: {goods_id}")
            continue
            
        # Must have at least one valid title
        if (not title_c or str(title_c).strip() == "") and (not title_t or str(title_t).strip() == ""):
            logger.warning(f"Skipping product with missing titles: goodsId={goods_id}, titleC={title_c}, titleT={title_t}")
            continue
            
        valid_products.append(p)
    
    logger.info(f"Filtered {len(products)} products to {len(valid_products)} valid products")
    
    if len(valid_products) == 0:
        logger.warning("No valid products after filtering - all products were missing essential fields")
        return 0
    
    category_weight_map = _load_category_weight_map(dsn_final)
    category_size_map = _load_category_size_map(dsn_final)
    category_rakuten_map = _load_category_rakuten_map(dsn_final)

    for p in valid_products:
        title_c = p.get("titleC")
        title_t = p.get("titleT")
        
        # Ensure product_id is not empty (validate early for logging)
        product_id = str(p.get("goodsId")) if p.get("goodsId") is not None else None
        if not product_id or product_id.strip() == "":
            logger.warning(f"Skipping product with empty ID: {p.get('goodsId', 'unknown')}")
            continue
        
        # detail info from enrich step (if any)
        detail_payload_raw = p.get("detailNormalized") if isinstance(p.get("detailNormalized"), dict) else None
        # Filter to only keep fields ending with 'T' and exclude specific fields
        # Removes: fields ending with 'C' (titleC, keyC, valueC) and excluded fields (video, description, fromPlatform_logo, picUrl)
        # Preserves: address, fromUrl, fromPlatform, shopName (these are included in detail_json)
        # Note: Translation is NOT applied - detail_json is stored as-is (may contain Chinese characters)
        detail_payload = _filter_detail_json_t_only(detail_payload_raw) if detail_payload_raw else None
        if detail_payload_raw and detail_payload:
            logger.debug(f"Filtered detail_json for product {product_id}: removed 'C' fields and excluded fields, kept 'T' fields")
        
        # If detail_payload is empty, create a minimal detail_json from available product data
        # This allows rakumart products without detailNormalized to be saved
        if not detail_payload:
            # Try to create minimal detail_json from product data
            minimal_detail = {}
            
            # Try to get fromUrl from various possible locations
            from_url = p.get("fromUrl") or p.get("from_url") or p.get("url")
            if from_url:
                minimal_detail["fromUrl"] = from_url
            
            # Try to get fromPlatform from various possible locations
            from_platform = p.get("fromPlatform") or p.get("from_platform") or p.get("shopType") or p.get("shop_type")
            if from_platform:
                minimal_detail["fromPlatform"] = from_platform
            
            # Try to get shopName from various possible locations
            shop_name = p.get("shopName") or p.get("shop_name")
            if not shop_name and p.get("shopInfo") and isinstance(p.get("shopInfo"), dict):
                shop_info = p.get("shopInfo")
                shop_name = shop_info.get("shopName") or shop_info.get("shop_name")
            if shop_name:
                minimal_detail["shopName"] = shop_name
            
            # Always create at least a minimal detail_json with shopType if available
            # This ensures products can be saved even without other detail fields
            if not minimal_detail and p.get("shopType"):
                minimal_detail["fromPlatform"] = p.get("shopType")
                logger.debug(f"Created minimal detail_json for product {product_id} with only shopType")
            
            # Only skip if we can't create even a minimal detail_json
            if not minimal_detail:
                logger.warning(f"Skipping product {product_id}: detail_json が空のため登録しません (available fields: {list(p.keys())[:10]})")
                continue
            
            detail_payload = minimal_detail
            logger.debug(f"Created minimal detail_json for product {product_id} from product data: {list(minimal_detail.keys())}")
        
        # Use existing title as name; OpenAI removed
        gen_name = title_t or title_c or None
        
        # Additional validation for critical fields
        if not gen_name or gen_name.strip() == "":
            logger.warning(f"Skipping product with empty name: {product_id}")
            continue

        # Prices
        rmb_price = _to_numeric(p.get("goodsPrice"))

        main_category = str(p.get("topCategoryId")) if p.get("topCategoryId") is not None else None
        middle_category = str(p.get("secondCategoryId")) if p.get("secondCategoryId") is not None else None
        category_weight = None
        category_size = None
        r_cat_id_list: list[str] = []
        if middle_category:
            category_weight = category_weight_map.get(middle_category)
            category_size = category_size_map.get(middle_category)
            r_cat_id_list = category_rakuten_map.get(middle_category, []) or []
        if main_category:
            if category_weight is None:
                category_weight = category_weight_map.get(main_category)
            if category_size is None:
                category_size = category_size_map.get(main_category)
            if not r_cat_id_list:
                r_cat_id_list = category_rakuten_map.get(main_category, []) or []

        # Normalise r_cat_id as JSON-serialisable list
        clean_r_cat_ids = [
            str(cid).strip() for cid in r_cat_id_list if cid and str(cid).strip()
        ]
        r_cat_id_json = json.dumps(clean_r_cat_ids, ensure_ascii=False)

        rows.append((
            product_id,                            # product_id (validated)
            main_category,                         # main_category
            middle_category,                       # middle_category
            gen_name,                              # product_name (generated)
            p.get("detailDescription"),            # product_description
            p.get("shopType"),                     # type
            int(p.get("monthSold")) if str(p.get("monthSold")).isdigit() else None,  # monthly_sales
            rmb_price,                             # wholesale_price (RMB)
            category_weight,                       # weight (derived)
            None,                                  # length (category-driven)
            None,                                  # width (category-driven)
            None,                                  # height (category-driven)
            category_size,                         # size (derived)
            # Rakumart product create date (createDate) if available
            (dt.datetime.fromisoformat(p.get("createDate")[:19]) if isinstance(p.get("createDate"), str) and len(p.get("createDate")) >= 19 else None),
            # repurchase rate
            _to_numeric(p.get("repurchaseRate")),
            # rating score (tradeScore or rating)
            _to_numeric(p.get("tradeScore") or p.get("rating")),
            # detail payload (json)
            json.dumps(detail_payload, ensure_ascii=False) if detail_payload else None,
            r_cat_id_json,                         # r_cat_id (JSON array derived from category_management.rakuten_category_ids)
            now,
        ))

    if not rows:
        logger.warning("No valid products to save to database")
        return 0
    
    # Final validation: filter out any rows with empty essential data
    validated_rows = []
    for i, row in enumerate(rows):
        if _is_valid_product_data(row):
            validated_rows.append(row)
        else:
            logger.warning(f"Skipping row {i} with invalid data: product_id={row[0] if len(row) > 0 else 'N/A'}, product_name={row[3] if len(row) > 3 else 'N/A'}")
    
    if not validated_rows:
        logger.warning("No products passed final validation - all had empty essential data")
        logger.warning(f"Total products received: {len(products)}, Valid products after filtering: {len(valid_products)}, Rows prepared: {len(rows)}, Validated rows: {len(validated_rows)}")
        return 0
    
    logger.info(f"Final validation: {len(rows)} -> {len(validated_rows)} products to save")
    logger.info(f"Total products received: {len(products)}, Valid products after filtering: {len(valid_products)}, Rows prepared: {len(rows)}, Validated rows: {len(validated_rows)}")

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            if create_table_if_missing:
                cur.execute(SCHEMA_SQL)

            base_insert = """
                insert into products_origin (
                    product_id, main_category, middle_category,
                    product_name, product_description, type,
                    monthly_sales, wholesale_price, weight,
                    length, width, height, size,
                    creation_date, repurchase_rate, rating_score, detail_json,
                    r_cat_id, created_at
                ) values {values_clause}
                on conflict (product_id) do update set
                    main_category = excluded.main_category,
                    middle_category = excluded.middle_category,
                    product_name = excluded.product_name,
                    product_description = excluded.product_description,
                    type = excluded.type,
                    monthly_sales = excluded.monthly_sales,
                    wholesale_price = excluded.wholesale_price,
                    weight = excluded.weight,
                    length = excluded.length,
                    width = excluded.width,
                    height = excluded.height,
                    size = excluded.size,
                    creation_date = excluded.creation_date,
                    repurchase_rate = excluded.repurchase_rate,
                    rating_score = excluded.rating_score,
                    detail_json = excluded.detail_json,
                    r_cat_id = excluded.r_cat_id,
                    registration_status = coalesce(products_origin.registration_status, 1),
                    created_at = least(products_origin.created_at, excluded.created_at)
            """

            execute_values_fn = getattr(psycopg2.extras, "execute_values", None)
            if execute_values_fn:
                execute_values_fn(
                    cur,
                    base_insert.format(values_clause="%s"),
                    validated_rows,
                    template="(" + ", ".join(["%s"] * 19) + ")",
                    page_size=100,
                )
            else:
                # Fall back to executemany for environments without execute_values (older psycopg2 versions)
                cur.executemany(
                    base_insert.format(values_clause="(" + ", ".join(["%s"] * 19) + ")"),
                    validated_rows,
                )
            conn.commit()
            logger.info(f"Successfully saved {len(validated_rows)} products to products_origin table")
    return len(validated_rows)


def update_image_processing_status(
    product_id: str,
    status: str,
    processed_images: Optional[dict] = None,
    *,
    dsn: Optional[str] = None,
) -> bool:
    """
    Update image processing status for a product.
    NOTE: This function is deprecated as image_processing_status, image_processing_date, 
    and processed_images columns have been removed from the products_origin table.
    
    Args:
        product_id: Product ID to update
        status: Processing status ('pending', 'processing', 'completed', 'failed')
        processed_images: Optional processed images data (ignored)
        dsn: Database connection string
        
    Returns:
        False (always returns False as the columns no longer exist)
    """
    logger.warning("update_image_processing_status is deprecated - image processing columns have been removed")
    return False


# Backwards-compat for existing CLI imports
def init_products_table(*, dsn: Optional[str] = None) -> None:
    """Legacy name kept for CLI; initializes the clean table."""
    init_products_origin_table(dsn=dsn)


def get_products_from_db(
    limit: int = 50,
    offset: int = 0,
    keyword: Optional[str] = None,
    dsn: Optional[str] = None,
) -> list[dict]:
    """
    Get products from the database
    
    Args:
        limit: Maximum number of products to return
        offset: Number of products to skip
        keyword: Filter by keyword (optional)
        dsn: Database connection string
        
    Returns:
        List of product dictionaries
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Build query - include registration_status check
                # Status values: 1=unregistered, 2=registered, 3=previously_registered
                # Logic:
                # 1. If product exists in product_management → status = 2 (registered)
                # 2. If product does NOT exist in product_management AND stored status = 3 → status = 3 (previously_registered)
                # 3. Otherwise → status = 1 (unregistered)
                query = """
                    SELECT po.*,
                           CASE 
                               WHEN pm.item_number IS NOT NULL THEN 2
                               WHEN pm.item_number IS NULL AND COALESCE(
                                   CASE
                                       WHEN po.registration_status IS NULL THEN 1
                                       WHEN po.registration_status::text IN ('registered', '2') THEN 2
                                       WHEN po.registration_status::text IN ('previously_registered', '3') THEN 3
                                       WHEN po.registration_status::text IN ('unregistered', '1') THEN 1
                                       ELSE CASE
                                               WHEN po.registration_status::text ~ '^[0-9]+$'
                                                   THEN po.registration_status::text::integer
                                               ELSE 1
                                            END
                                   END, 1
                               ) = 3 THEN 3
                               ELSE 1
                           END as computed_registration_status
                    FROM products_origin po
                    LEFT JOIN product_management pm ON po.product_id = pm.item_number
                """
                params = []
                
                if keyword:
                    query += " WHERE po.product_name ILIKE %s OR po.product_id ILIKE %s"
                    params.extend([f"%{keyword}%", f"%{keyword}%"])
                
                query += " ORDER BY po.created_at DESC LIMIT %s OFFSET %s"
                params.extend([limit, offset])
                
                cur.execute(query, params)
                rows = cur.fetchall()
                
                # Convert to list of dictionaries
                products = []
                for row in rows:
                    product = dict(row)
                    # Convert any JSON fields back to Python objects
                    for key, value in product.items():
                        if isinstance(value, str) and key in ['specifications', 'detail_json']:
                            try:
                                product[key] = json.loads(value) if value else None
                            except (json.JSONDecodeError, TypeError):
                                product[key] = value
                    # Use computed registration status (1=unregistered, 2=registered, 3=previously_registered)
                    computed_status = product.pop('computed_registration_status', None)
                    if computed_status is None:
                        computed_status = 1
                    try:
                        computed_status_int = int(computed_status)
                    except (TypeError, ValueError):
                        computed_status_map = {
                            'unregistered': 1,
                            'registered': 2,
                            'previously_registered': 3
                        }
                        computed_status_int = computed_status_map.get(str(computed_status).lower(), 1)
                    product['registration_status'] = computed_status_int
                    products.append(product)
                
                return products
                
    except Exception as e:
        logger.error(f"Error getting products from database: {e}")
        return []


def save_products_to_db(
    products: Iterable[dict],
    *,
    keyword: Optional[str] = None,  # kept for signature compatibility; ignored
    dsn: Optional[str] = None,
    create_table_if_missing: bool = True,
) -> int:
    """Legacy API used by CLI. Writes into products_origin."""
    return save_products_origin_to_db(
        products,
        dsn=dsn,
        create_table_if_missing=create_table_if_missing,
    )


def fix_product_management_schema(*, dsn: Optional[str] = None) -> None:
    """Ensure the product_management table exists with the latest schema."""
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            cur.execute(SCHEMA_SQL_PRODUCT_MANAGEMENT)
            # Add product_image_code column if it doesn't exist (for existing tables)
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='product_image_code'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN product_image_code text;
                    END IF;
                END $$;
            """)
            # Add inventory column if it doesn't exist (for existing tables)
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='inventory'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN inventory jsonb;
                    END IF;
                END $$;
            """)
            # Add rakuten_registration_status column if it doesn't exist (for existing tables)
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='rakuten_registration_status'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN rakuten_registration_status text;
                    END IF;
                END $$;
            """)
            # Add rakuten_registered_at column if it doesn't exist (for existing tables)
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='rakuten_registered_at'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN rakuten_registered_at timestamptz;
                    END IF;
                END $$;
            """)
            # Remove old rakuten_registered boolean column if it exists (migration from old schema)
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='rakuten_registered'
                    ) THEN
                        -- Migrate data from old boolean field to new text field
                        UPDATE product_management
                        SET rakuten_registration_status = CASE 
                            WHEN rakuten_registered = true THEN 'true'
                            WHEN rakuten_registered = false THEN 'false'
                            ELSE NULL
                        END
                        WHERE rakuten_registration_status IS NULL;
                        -- Drop old column
                        ALTER TABLE product_management DROP COLUMN rakuten_registered;
                    END IF;
                END $$;
            """)
            # Add image_registration_status column if it doesn't exist
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='image_registration_status'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN image_registration_status boolean default false;
                    END IF;
                END $$;
            """)
            # Add inventory_registration_status column if it doesn't exist
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='inventory_registration_status'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN inventory_registration_status boolean default false;
                    END IF;
                END $$;
            """)
            # Add main_category column if it doesn't exist
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='main_category'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN main_category text;
                    END IF;
                END $$;
            """)
            # Add middle_category column if it doesn't exist
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='middle_category'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN middle_category text;
                    END IF;
                END $$;
            """)
            # Add src_url column if it doesn't exist
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='src_url'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN src_url text;
                    END IF;
                END $$;
            """)
            # Ensure r_cat_id column exists as jsonb[] (Rakuten category IDs)
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1
                          FROM information_schema.columns
                         WHERE table_name = 'product_management'
                           AND column_name = 'r_cat_id'
                    ) THEN
                        ALTER TABLE product_management
                            ADD COLUMN r_cat_id jsonb NOT NULL DEFAULT '[]'::jsonb;
                    ELSIF EXISTS (
                        SELECT 1
                          FROM information_schema.columns
                         WHERE table_name = 'product_management'
                           AND column_name = 'r_cat_id'
                           AND data_type <> 'jsonb'
                    ) THEN
                        -- Migrate old scalar/text values to jsonb array
                        ALTER TABLE product_management
                            ALTER COLUMN r_cat_id TYPE jsonb
                            USING
                                CASE
                                    WHEN r_cat_id IS NULL OR r_cat_id::text = '' THEN '[]'::jsonb
                                    WHEN r_cat_id::text LIKE '[%' THEN r_cat_id::jsonb
                                    ELSE jsonb_build_array(r_cat_id::text)
                                END;
                        ALTER TABLE product_management
                            ALTER COLUMN r_cat_id SET DEFAULT '[]'::jsonb;
                        UPDATE product_management
                           SET r_cat_id = '[]'::jsonb
                         WHERE r_cat_id IS NULL;
                    END IF;
                END $$;
            """)
            # Drop legacy rakuten_category_ids column if it exists (moved to category_management)
            cur.execute("""
                DO $$
                BEGIN
                    IF EXISTS (
                        SELECT 1
                          FROM information_schema.columns
                         WHERE table_name = 'product_management'
                           AND column_name = 'rakuten_category_ids'
                    ) THEN
                        ALTER TABLE product_management DROP COLUMN rakuten_category_ids;
                    END IF;
                END $$;
            """)
            # Add change_status column if it doesn't exist
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='change_status'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN change_status text;
                    END IF;
                END $$;
            """)
            # Ensure useful indexes
            cur.execute("create unique index if not exists uniq_product_management_item_number on product_management(item_number);")
        conn.commit()


def reset_product_management_table(*, dsn: Optional[str] = None) -> None:
    """Drop and recreate the product_management table with the latest schema matching Rakuten API structure."""
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            logger.info("Dropping existing product_management table...")
            cur.execute("drop table if exists product_management cascade;")
            logger.info("Creating new product_management table with updated schema...")
            cur.execute(SCHEMA_SQL_PRODUCT_MANAGEMENT)
            cur.execute("create unique index if not exists uniq_product_management_item_number on product_management(item_number);")
            conn.commit()
            logger.info("Product management table reset successfully")


def _transform_specification_to_variant_selectors(
    specification: list[dict],
) -> Optional[list[dict]]:
    """
    Transform specification from detail_json into variant_selectors format.
    Uses DeepL API to translate with automatic source language detection:
    
    Translation rules:
    - key: Always translated to English (normalized key format) if source is Japanese, Chinese, or English
    - displayName: Translated to Japanese only if source is Chinese; if Japanese, returns as-is
    - displayValue: Translated to Japanese only if source is Chinese; if English or Japanese, returns as-is
    
    The source text language is automatically detected (Chinese, Japanese, English, etc.).
    
    Args:
        specification: List of specification objects with keyT and valueT fields
        
    Returns:
        List of variant selector objects, or None if specification is invalid/empty
    """
    if not specification or not isinstance(specification, list):
        logger.info(f"❌ Specification is invalid or not a list: {type(specification)}")
        return None
    
    # Import DeepL translation module
    try:
        from .deepl_trans import translate_key_to_english, translate_text_to_japanese, detect_source_language
    except ImportError:
        logger.error("Failed to import deepl_trans module. Translations will not be available.")
        translate_key_to_english = None
        translate_text_to_japanese = None
        detect_source_language = None
    
    logger.info(f"🔄 Processing {len(specification)} specification items")
    variant_selectors = []
    
    for idx, spec_item in enumerate(specification):
        if not isinstance(spec_item, dict):
            logger.warning(f"⚠️  Spec item {idx} is not a dict: {type(spec_item)}")
            continue
        
        key_t = spec_item.get("keyT")
        value_t = spec_item.get("valueT")
        
        logger.info(f"📝 Spec item {idx}: keyT={key_t}, valueT type={type(value_t)}, valueT length={len(value_t) if isinstance(value_t, list) else 'N/A'}")
        
        if not key_t or not value_t or not isinstance(value_t, list):
            logger.warning(f"⚠️  Skipping spec item {idx}: missing keyT ({key_t}) or valueT ({value_t}), or valueT is not a list")
            continue
        
        # Translate keyT to English and normalize as key (auto-detect source language)
        key_t_str = str(key_t)
        if translate_key_to_english:
            # Auto-detect source language - don't hardcode "JA"
            key_en = translate_key_to_english(key_t_str, source_lang=None)  # None triggers auto-detection
            if not key_en:
                logger.warning(f"⚠️  Translation failed for keyT '{key_t}'. Using fallback normalization.")
                # Fallback to normalization without translation
                key_en_normalized = key_t_str.lower().strip().replace(" ", "_").replace("-", "_")
                key_en_normalized = re.sub(r"[^a-z0-9_]", "", key_en_normalized)
                if not key_en_normalized:
                    fallback = re.sub(r"[^a-zA-Z0-9\s]", "", key_t_str)
                    fallback = fallback.lower().strip().replace(" ", "_")
                    words = fallback.split("_")[:3]
                    key_en = "_".join(words) if words else "key_" + str(idx)
                else:
                    key_en = key_en_normalized
        else:
            # Fallback if translation module not available
            logger.warning(f"⚠️  DeepL translation not available. Using fallback normalization for keyT '{key_t}'.")
            key_en_normalized = key_t_str.lower().strip().replace(" ", "_").replace("-", "_")
            key_en_normalized = re.sub(r"[^a-z0-9_]", "", key_en_normalized)
            if not key_en_normalized:
                fallback = re.sub(r"[^a-zA-Z0-9\s]", "", key_t_str)
                fallback = fallback.lower().strip().replace(" ", "_")
                words = fallback.split("_")[:3]
                key_en = "_".join(words) if words else "key_" + str(idx)
            else:
                key_en = key_en_normalized
        
        logger.info(f"🔤 Translated keyT '{key_t}' to key: '{key_en}'")
        
        # Translate keyT to Japanese for displayName (only if source is Chinese)
        # If source is Japanese, return as-is
        if detect_source_language and translate_text_to_japanese:
            detected_lang = detect_source_language(key_t_str)
            logger.info(f"🔍 Detected source language for keyT '{key_t}': {detected_lang}")
            
            if detected_lang == "ZH":
                # Translate Chinese to Japanese
                display_name_ja = translate_text_to_japanese(key_t_str, source_lang="ZH")
                if not display_name_ja:
                    logger.warning(f"⚠️  Translation failed for keyT '{key_t}'. Using original text.")
                    display_name_ja = key_t_str
            elif detected_lang == "JA":
                # If already Japanese, return as-is
                display_name_ja = key_t_str
                logger.info(f"✅ keyT is already Japanese, using as-is: '{display_name_ja}'")
            else:
                # For other languages (like English), return as-is
                display_name_ja = key_t_str
                logger.info(f"✅ keyT is '{detected_lang}', using as-is: '{display_name_ja}'")
        else:
            # Fallback if translation module not available
            logger.warning(f"⚠️  DeepL translation not available. Using original keyT '{key_t}'.")
            display_name_ja = key_t_str
        
        logger.info(f"🔤 Using displayName: '{display_name_ja}'")
        
        # Process values - translate name to Japanese for displayValue (only if source is Chinese)
        # If source is English or Japanese, return as-is
        values = []
        logger.info(f"🔄 Processing {len(value_t)} value items for spec item {idx}")
        for value_idx, value_item in enumerate(value_t):
            if not isinstance(value_item, dict):
                logger.warning(f"⚠️  Value item {value_idx} in spec item {idx} is not a dict: {type(value_item)}")
                continue
            
            name = value_item.get("name")
            logger.info(f"📝 Value item {value_idx}: name={name}")
            if not name:
                logger.warning(f"⚠️  Value item {value_idx} in spec item {idx} has no 'name' field")
                continue
            
            # Translate value name to Japanese using DeepL translation
            # This ensures accurate translation and 32-byte limit compliance
            name_str = str(name)
            display_value_ja = None
            
            # Step 1: Use DeepL translation via _translate_variant_value_with_context
            try:
                from .deepl_trans import _translate_variant_value_with_context
                display_value_ja = _translate_variant_value_with_context(
                    value=name_str,
                    key=key_en,  # Use the translated key for context
                    context=None,  # Can be enhanced to pass product category context
                    max_bytes=32
                )
                if display_value_ja:
                    logger.info(f"✅ DeepL translation: '{name_str}' -> '{display_value_ja}'")
            except Exception as e:
                logger.debug(f"DeepL translation failed for '{name_str}': {e}, trying DeepL directly...")
                display_value_ja = None
            
            # Step 2: Fallback to DeepL if enhanced translation not available
            if not display_value_ja and detect_source_language and translate_text_to_japanese:
                detected_lang = detect_source_language(name_str)
                logger.info(f"🔍 Detected source language for value '{name}': {detected_lang}")
                
                # Use translate_text_to_japanese which handles all cases:
                # - Japanese -> returns as-is
                # - Chinese -> translates to Japanese
                # - Other -> translates to Japanese
                display_value_ja = translate_text_to_japanese(name_str, source_lang=detected_lang)
                if not display_value_ja:
                    logger.warning(f"⚠️  Translation failed for value name '{name}'. Using original text.")
                    display_value_ja = name_str
                else:
                    if detected_lang == "JA":
                        logger.info(f"✅ Value is Japanese, using as-is: '{display_value_ja}'")
                    elif detected_lang == "ZH":
                        logger.info(f"✅ Value is Chinese, translated to Japanese: '{name_str}' -> '{display_value_ja}'")
                    else:
                        logger.info(f"✅ Value is '{detected_lang}', translated to Japanese: '{name_str}' -> '{display_value_ja}'")
            elif not display_value_ja:
                # Fallback if translation module not available
                logger.warning(f"⚠️  Translation not available. Using original value name '{name}'.")
                display_value_ja = name_str
            
            # Step 3: Apply comprehensive cleaning to ensure 32-byte limit and remove Chinese characters
            if display_value_ja:
                try:
                    from .deepl_trans import clean_chinese_color_for_rakuten
                    display_value_ja = clean_chinese_color_for_rakuten(
                        display_value_ja,
                        original_value=name_str,
                        max_bytes=32
                    )
                except Exception:
                    # Fallback to basic cleaning
                    from .rakuten_product import clean_text_for_rakuten
                    display_value_ja = clean_text_for_rakuten(display_value_ja, strict=True)
                    # Enforce 32-byte limit
                    byte_length = len(display_value_ja.encode('utf-8'))
                    if byte_length > 32:
                        # Truncate character by character
                        truncated = display_value_ja
                        while len(truncated.encode('utf-8')) > 32 and len(truncated) > 0:
                            truncated = truncated[:-1]
                        display_value_ja = truncated.strip()
            
            if not display_value_ja or display_value_ja.strip() == '':
                logger.warning(f"⚠️  Empty displayValue after cleaning for '{name}'. Skipping.")
                continue
            
            logger.info(f"✅ Using displayValue: '{display_value_ja}' (bytes: {len(display_value_ja.encode('utf-8'))})")
            
            values.append({
                "displayValue": display_value_ja
            })
        
        if values:
            variant_selectors.append({
                "key": key_en,
                "displayName": display_name_ja,
                "values": values
            })
            logger.info(f"✅ Added variant selector: key={key_en}, displayName={display_name_ja}, {len(values)} values")
        else:
            logger.warning(f"⚠️  No values extracted for spec item {idx} (keyT={key_t})")
    
    logger.info(f"🎯 Transformed {len(specification)} specification items into {len(variant_selectors)} variant selectors")
    return variant_selectors if variant_selectors else None


def _calculate_purchase_price(
    product_cost_cny: float,
    product_weight_kg: Optional[float],
    exchange_rate: float = 22.0,
    domestic_shipping_cost: float = 326.0,
    international_shipping_rate: float = 19.2,
    profit_margin_percent: float = 1.5,
    sales_commission_percent: float = 10.0,
) -> Optional[str]:
    """
    Calculate purchase price in JPY using the same logic as purchase-price-utils.ts
    
    Args:
        product_cost_cny: Wholesale price in CNY (from wholesale_price field)
        product_weight_kg: Weight in kilograms (from weight field)
        exchange_rate: Exchange rate (default 22.0)
        domestic_shipping_cost: Domestic shipping cost in JPY (default 326.0)
        international_shipping_rate: International shipping rate per kg in CNY (default 19.2)
        profit_margin_percent: Profit margin percentage (default 1.5)
        sales_commission_percent: Sales commission percentage (default 10.0)
        
    Returns:
        Purchase price as string in JPY, or None if weight is missing
    """
    if product_weight_kg is None or product_weight_kg <= 0:
        logger.warning("⚠️  Weight is required for purchase price calculation")
        return None
    
    normalized_cost = max(0, float(product_cost_cny) if product_cost_cny else 0)
    weight_kg = max(0, float(product_weight_kg))
    
    if weight_kg <= 0 or not (weight_kg > 0 and weight_kg < float('inf')):
        logger.warning("⚠️  Invalid weight for purchase price calculation")
        return None
    
    base_cost = normalized_cost * exchange_rate * 1.05
    international_shipping = international_shipping_rate * weight_kg * exchange_rate
    numerator = base_cost + international_shipping + domestic_shipping_cost
    
    denominator = 100 - (profit_margin_percent + sales_commission_percent)
    safe_denominator = 1 if abs(denominator) < 0.0001 else denominator
    
    actual_price = (numerator * 100) / safe_denominator
    # Round to nearest 10 (set ones digit to 0)
    rounded_price = int(round(actual_price / 10)) * 10
    return str(rounded_price)


def _parse_keyt_to_variant_values(key_t: str, variant_selectors: list[dict]) -> dict[str, str]:
    """
    Parse keyT string (e.g., "白色㊖㊎S" or "灰色㊖㊎M") to extract variant values and match them with variantSelectors.
    Translates Chinese text to Japanese before matching.
    Ensures ALL variant selectors are matched.
    
    Args:
        key_t: The keyT string from goodsInventory (e.g., "白色㊖㊎S", "灰色㊖㊎M")
        variant_selectors: List of variant selector objects with keys and displayValues
        
    Returns:
        Dictionary mapping variant selector keys to their displayValues (must include all selectors)
    """
    if not key_t or not variant_selectors:
        return {}
    
    result = {}
    
    # Try to translate Chinese parts to Japanese for better matching
    # Import translation function if available
    try:
        from .deepl_trans import translate_text_to_japanese, detect_source_language
        translation_available = True
    except ImportError:
        translation_available = False
        translate_text_to_japanese = None
        detect_source_language = None
    
    # Split keyT by common separators to get individual parts
    parts = re.split(r'[㊖㊎\s\u3000]+', key_t)  # \u3000 is full-width space
    parts = [p.strip() for p in parts if p.strip()]
    
    # Translate Chinese parts to Japanese for matching
    translated_parts = []
    for part in parts:
        if translation_available and translate_text_to_japanese:
            # Detect if part is Chinese
            detected_lang = detect_source_language(part) if detect_source_language else None
            if detected_lang == "ZH":
                # Translate Chinese to Japanese
                translated = translate_text_to_japanese(part, source_lang="ZH")
                if translated and translated != part:
                    translated_parts.append((part, translated))
                    logger.debug(f"🔤 Translated part '{part}' (ZH) -> '{translated}' (JA)")
                    continue
        # If not Chinese or translation failed, use original
        translated_parts.append((part, part))
    
    # Create a mapping of original parts to translated parts for matching
    part_translation_map = {orig: trans for orig, trans in translated_parts}
    all_parts_for_matching = [trans for _, trans in translated_parts]  # Use translated parts for matching
    
    logger.debug(f"🔍 Parsing keyT '{key_t}' into parts: {parts}, translated: {all_parts_for_matching}")
    
    # Strategy 1: Try exact substring matches first (most reliable)
    for selector in variant_selectors:
        selector_key = selector.get("key")
        if not selector_key:
            continue
        
        values = selector.get("values", [])
        if not values:
            continue
        
        # Try to find a matching displayValue in the keyT string
        best_match = None
        best_match_length = 0
        
        for value_obj in values:
            display_value = value_obj.get("displayValue", "")
            if not display_value:
                continue
            
            # Check if displayValue appears in keyT (exact substring match)
            if display_value in key_t:
                # Prefer longer matches
                if len(display_value) > best_match_length:
                    best_match = display_value
                    best_match_length = len(display_value)
        
        if best_match:
            result[selector_key] = best_match
            logger.debug(f"✅ Matched {selector_key} = {best_match} (exact substring)")
    
    # Strategy 2: Match parts with variant selectors (try each part against each selector)
    unmatched_selectors = [s for s in variant_selectors if s.get("key") not in result]
    used_parts = set()
    
    # Try to match each unmatched selector with any remaining part
    for selector in unmatched_selectors:
        selector_key = selector.get("key")
        if selector_key in result:
            continue
        
        values = selector.get("values", [])
        if not values:
            continue
        
        # Try matching with each part (skip already used parts)
        # Use translated parts for matching
        for part_idx, (original_part, translated_part) in enumerate(translated_parts):
            if not original_part or original_part in used_parts:
                continue
            
            # Try exact match first (using translated part)
            matched = False
            for value_obj in values:
                display_value = value_obj.get("displayValue", "")
                if not display_value:
                    continue
                
                # Exact match (case-insensitive, space-insensitive) - try both original and translated
                if (translated_part.replace(" ", "").lower() == display_value.replace(" ", "").lower() or
                    original_part.replace(" ", "").lower() == display_value.replace(" ", "").lower()):
                    result[selector_key] = display_value
                    used_parts.add(original_part)
                    matched = True
                    logger.debug(f"✅ Matched {selector_key} = {display_value} (exact match with part '{original_part}' -> '{translated_part}')")
                    break
            
            if matched:
                break
            
            # Try substring match (using translated part)
            if not matched:
                for value_obj in values:
                    display_value = value_obj.get("displayValue", "")
                    if not display_value:
                        continue
                    
                    # Check if translated part or original part is in displayValue or vice versa
                    if (translated_part in display_value or display_value in translated_part or
                        original_part in display_value or display_value in original_part):
                        result[selector_key] = display_value
                        used_parts.add(original_part)
                        matched = True
                        logger.debug(f"✅ Matched {selector_key} = {display_value} (substring match with part '{original_part}' -> '{translated_part}')")
                        break
            
            if matched:
                break
    
    # Strategy 3: If still not all matched, try matching any remaining parts with any remaining selectors
    # This handles cases where the order might be different
    unmatched_selectors = [s for s in variant_selectors if s.get("key") not in result]
    remaining_parts = [(orig, trans) for orig, trans in translated_parts if orig not in used_parts]
    
    for original_part, translated_part in remaining_parts:
        for selector in unmatched_selectors:
            selector_key = selector.get("key")
            if selector_key in result:
                continue
            
            values = selector.get("values", [])
            if not values:
                continue
            
            for value_obj in values:
                display_value = value_obj.get("displayValue", "")
                if not display_value:
                    continue
                
                # Try various matching strategies (using both original and translated parts)
                if (translated_part in display_value or 
                    display_value in translated_part or 
                    original_part in display_value or 
                    display_value in original_part or
                    translated_part.replace(" ", "").lower() == display_value.replace(" ", "").lower() or
                    original_part.replace(" ", "").lower() == display_value.replace(" ", "").lower()):
                    result[selector_key] = display_value
                    used_parts.add(original_part)
                    logger.debug(f"✅ Matched {selector_key} = {display_value} (fallback match with part '{original_part}' -> '{translated_part}')")
                    break
            
            if selector_key in result:
                break
    
    # Strategy 4: If we still have unmatched selectors, try matching with the entire keyT
    # This handles cases where the variant value might be embedded in the keyT differently
    unmatched_selectors = [s for s in variant_selectors if s.get("key") not in result]
    
    for selector in unmatched_selectors:
        selector_key = selector.get("key")
        if selector_key in result:
            continue
        
        values = selector.get("values", [])
        if not values:
            continue
        
        # Try matching each displayValue with the entire keyT
        best_match = None
        best_match_score = 0
        
        for value_obj in values:
            display_value = value_obj.get("displayValue", "")
            if not display_value:
                continue
            
            # Calculate a match score based on how much of the displayValue appears in keyT
            if display_value in key_t:
                score = len(display_value)
                if score > best_match_score:
                    best_match = display_value
                    best_match_score = score
        
        if best_match:
            result[selector_key] = best_match
            logger.debug(f"✅ Matched {selector_key} = {best_match} (full keyT match)")
    
    # Final check: Ensure we have matches for ALL variant selectors
    if len(result) < len(variant_selectors):
        missing_keys = [s.get("key") for s in variant_selectors if s.get("key") not in result]
        logger.warning(f"⚠️  Could not match all variant selectors. Missing: {missing_keys} for keyT: {key_t}")
        # For missing selectors, try to use the first available value as a fallback
        for selector in variant_selectors:
            selector_key = selector.get("key")
            if selector_key not in result:
                values = selector.get("values", [])
                if values and len(values) > 0:
                    # Use the first value as a fallback (not ideal, but better than missing)
                    fallback_value = values[0].get("displayValue", "")
                    if fallback_value:
                        result[selector_key] = fallback_value
                        logger.warning(f"⚠️  Using fallback value for {selector_key}: {fallback_value}")
    
    logger.debug(f"🎯 Final selectorValues: {result}")
    return result


def _generate_all_variant_combinations(variant_selectors: list[dict]) -> list[dict]:
    """
    Generate all possible combinations of variant selector values.
    
    Args:
        variant_selectors: List of variant selector objects
        
    Returns:
        List of dictionaries, each containing a combination of selector values
        Example: [{"color": "ブラック", "size": "M"}, {"color": "ブラック", "size": "L"}, ...]
    """
    if not variant_selectors:
        return []
    
    # Extract all selector keys and their values
    selectors_data = []
    for selector in variant_selectors:
        selector_key = selector.get("key")
        if not selector_key:
            continue
        
        values = selector.get("values", [])
        display_values = [v.get("displayValue", "") for v in values if v.get("displayValue")]
        
        if display_values:
            selectors_data.append({
                "key": selector_key,
                "values": display_values
            })
    
    if not selectors_data:
        return []
    
    # Generate all combinations using itertools.product
    import itertools
    
    # Get all value lists
    value_lists = [sel["values"] for sel in selectors_data]
    keys = [sel["key"] for sel in selectors_data]
    
    # Generate cartesian product
    combinations = []
    for combo in itertools.product(*value_lists):
        combo_dict = {keys[i]: combo[i] for i in range(len(keys))}
        combinations.append(combo_dict)
    
    return combinations


def _extract_inventory_from_goods_inventory(
    goods_inventory: list,
    item_number: str,
) -> Optional[dict]:
    """
    Extract inventory data from goodsInventory for product_management table.
    
    Args:
        goods_inventory: List of goodsInventory items with keyT and valueT fields
        item_number: Product item_number (used as manage_number)
        
    Returns:
        Dictionary with manage_number and variants list, or None if invalid input
    """
    if not goods_inventory or not isinstance(goods_inventory, list):
        return None
    
    variants_list = []
    
    for inv_item in goods_inventory:
        if not isinstance(inv_item, dict):
            continue
        
        value_t = inv_item.get("valueT")
        if not value_t:
            continue
        
        # valueT should be a list of objects with skuId and amountOnSale
        if not isinstance(value_t, list):
            value_t = [value_t] if value_t else []
        
        # Process each valueT entry
        for entry in value_t:
            if not isinstance(entry, dict):
                continue
            
            sku_id = entry.get("skuId")
            amount_on_sale = entry.get("amountOnSale")
            
            # Only add if we have both skuId and amountOnSale
            if sku_id is not None and amount_on_sale is not None:
                # Apply quantity rules based on amountOnSale:
                # - >= 1000: quantity = 100
                # - 500 to 999: quantity = 50
                # - 50 to 499: quantity = 5
                # - < 50: quantity = 1
                amount_on_sale_int = int(amount_on_sale)
                if amount_on_sale_int >= 1000:
                    quantity = 100
                elif amount_on_sale_int >= 500:
                    quantity = 100
                elif amount_on_sale_int >= 50:
                    quantity = 0
                else:
                    quantity = 0
                
                variants_list.append({
                    "variant_id": str(sku_id),
                    "quantity": quantity,
                    "mode": "ABSOLUTE",
                    "operationLeadTime": {
                        "normalDeliveryTimeId": 225554
                    }
                })
                logger.debug(f"📦 Extracted inventory: variant_id={sku_id}, amountOnSale={amount_on_sale_int} -> quantity={quantity}")
    
    if not variants_list:
        return None
    
    return {
        "manage_number": item_number,
        "variants": variants_list
    }


def _transform_goods_inventory_to_variants(
    goods_inventory: list[dict],
    variant_selectors: Optional[list[dict]],
    product_weight_kg: Optional[float],
    wholesale_price_cny: Optional[float],
    product_size: Optional[float] = None,
    *,
    exchange_rate: float = 22.0,
    domestic_shipping_cost: float = 326.0,
    international_shipping_rate: float = 19.2,
    profit_margin_percent: float = 1.5,
    sales_commission_percent: float = 10.0,
    domestic_shipping_costs: Optional[Dict[str, Any]] = None,
    category_attributes: Optional[list[dict]] = None,
) -> Optional[dict]:
    """
    Transform goodsInventory from detail_json.goodsInfo into variants format for product_management table.
    Generates ALL combinations from variantSelectors, then matches them with goodsInventory items.
    
    Args:
        goods_inventory: List of goodsInventory items with keyT and valueT fields
        variant_selectors: List of variant selector objects from product_management table
        product_weight_kg: Product weight in kilograms (from products_origin.weight)
        wholesale_price_cny: Wholesale price in CNY (from products_origin.wholesale_price)
        product_size: Size value (from products_origin.size) used to select shipping cost tier
        exchange_rate: Exchange rate for price calculation
        domestic_shipping_cost: Base domestic shipping cost in JPY
        domestic_shipping_costs: Mapping of size-specific domestic shipping costs
        international_shipping_rate: International shipping rate per kg in CNY
        profit_margin_percent: Profit margin percentage
        sales_commission_percent: Sales commission percentage
        
    Returns:
        Dictionary of variants keyed by skuId, or None if invalid input
    """
    if not variant_selectors or not isinstance(variant_selectors, list):
        logger.warning("⚠️  variant_selectors is invalid or not a list")
        return None
    
    if not variant_selectors:
        logger.warning("⚠️  variant_selectors is empty")
        return None
    
    # Select domestic shipping cost based on product size
    # Size 30 -> regular, 60 -> size60, 80 -> size80, 100 -> size100
    # Logging is handled inside _select_domestic_shipping_cost()
    effective_domestic_cost = _select_domestic_shipping_cost(
        product_size,
        domestic_shipping_costs,
        domestic_shipping_cost,
    )
    
    # Generate ALL combinations from variantSelectors
    all_combinations = _generate_all_variant_combinations(variant_selectors)
    logger.info(f"🔄 Generated {len(all_combinations)} variant combinations from {len(variant_selectors)} selectors")
    
    if not all_combinations:
        logger.warning("⚠️  No combinations generated from variantSelectors")
        return None
    
    # Build a lookup map from goodsInventory: keyT -> (skuId, price)
    # This allows us to match combinations with inventory items
    inventory_lookup = {}
    if goods_inventory and isinstance(goods_inventory, list):
        logger.info(f"🔄 Processing {len(goods_inventory)} goodsInventory items for matching")
        for inv_item in goods_inventory:
            if not isinstance(inv_item, dict):
                continue
            
            key_t = inv_item.get("keyT")
            value_t = inv_item.get("valueT")
            
            if not key_t or not value_t:
                continue
            
            # valueT should be a list of objects with price, skuId, etc.
            if not isinstance(value_t, list):
                value_t = [value_t] if value_t else []
            
            # Parse keyT to extract variant values for matching
            # This function now translates Chinese to Japanese automatically
            parsed_values = _parse_keyt_to_variant_values(str(key_t), variant_selectors)
            
            # Ensure we have all required selector keys before creating lookup
            if not parsed_values or len(parsed_values) < len(variant_selectors):
                logger.warning(f"⚠️  Incomplete parsing for keyT '{key_t}': got {parsed_values}, expected {len(variant_selectors)} selectors")
                # Try to fill in missing selectors with fallbacks
                for selector in variant_selectors:
                    selector_key = selector.get("key")
                    if selector_key and selector_key not in parsed_values:
                        values = selector.get("values", [])
                        if values and len(values) > 0:
                            parsed_values[selector_key] = values[0].get("displayValue", "")
                            logger.debug(f"⚠️  Added fallback for {selector_key}: {parsed_values[selector_key]}")
            
            # Store the first valueT entry (usually there's only one)
            sku_id = None
            if value_t and len(value_t) > 0:
                first_entry = value_t[0]
                if isinstance(first_entry, dict):
                    sku_id = first_entry.get("skuId")
            
            # Extract unit price from valueT (goodsInventory entry)
            # This price comes from detail_json.goodsInfo.goodsInventory[].valueT[].price
            unit_price = _select_unit_price_from_value_t(value_t)
            
            if sku_id and parsed_values:
                # Create a normalized key for lookup using translated/parsed values
                # Sort selector keys to ensure consistent matching
                sorted_keys = sorted(parsed_values.keys())
                lookup_key = tuple((k, parsed_values[k]) for k in sorted_keys)
                inventory_lookup[lookup_key] = {
                    "skuId": sku_id,
                    "price": unit_price,  # Price from valueT in goodsInventory
                }
                logger.info(
                    f"📦 Mapped inventory: keyT='{key_t}' -> parsed={parsed_values} -> skuId={sku_id}, "
                    f"price={unit_price} (from valueT)"
                )
            else:
                logger.warning(f"⚠️  Skipping inventory item: missing skuId or parsed_values for keyT '{key_t}'")
    
    variants = {}
    
    # Generate variants for each combination
    for combo_idx, combination in enumerate(all_combinations):
        # Create lookup key for this combination
        sorted_keys = sorted(combination.keys())
        lookup_key = tuple((k, combination[k]) for k in sorted_keys)
        
        # Try to find matching inventory item
        inventory_match = inventory_lookup.get(lookup_key)
        
        # Get skuId and price from inventory, or use fallback
        sku_id = None
        price = None
        
        if inventory_match:
            sku_id = inventory_match.get("skuId")
            price = inventory_match.get("price")
            logger.debug(f"✅ Found inventory match for combination {combo_idx + 1}: skuId={sku_id}")
        else:
            # No exact match found - try to find a partial match or use sequential skuId
            # For now, we'll generate a sequential ID if no match
            # In a real scenario, you might want to handle this differently
            logger.warning(f"⚠️  No inventory match for combination {combo_idx + 1}: {combination}")
            # Try to find any inventory item with matching first selector value
            for inv_key, inv_data in inventory_lookup.items():
                # Check if first selector matches
                if inv_key and len(inv_key) > 0:
                    first_selector_key, first_selector_value = inv_key[0]
                    if first_selector_key in combination and combination[first_selector_key] == first_selector_value:
                        sku_id = inv_data.get("skuId")
                        price = inv_data.get("price")
                        logger.debug(f"⚠️  Using partial match for combination {combo_idx + 1}: skuId={sku_id}")
                        break
        
        # If still no skuId, we can't create a variant
        if not sku_id:
            logger.warning(f"⚠️  Skipping combination {combo_idx + 1}: no skuId available for {combination}")
            continue
        
        # Convert skuId to string for use as key
        sku_id_str = str(sku_id)
        
        # Calculate standard price using price from valueT (goodsInventory)
        # This price comes from detail_json.goodsInfo.goodsInventory[].valueT[].price
        purchase_price = None
        if price is not None:
            try:
                price_cny = float(price)  # Price from valueT in goodsInventory
                logger.debug(
                    f"💵 Using price from valueT: {price_cny} CNY for skuId {sku_id}"
                )
            except (ValueError, TypeError):
                price_cny = None
            
            if price_cny is None and wholesale_price_cny is not None:
                try:
                    price_cny = float(wholesale_price_cny)
                    logger.debug(
                        f"💵 Falling back to wholesale_price: {price_cny} CNY for skuId {sku_id}"
                    )
                except (ValueError, TypeError):
                    price_cny = None
            
            if price_cny is not None:
                try:
                    purchase_price = _calculate_purchase_price(
                        product_cost_cny=price_cny,  # Price from valueT (or wholesale_price fallback)
                    product_weight_kg=product_weight_kg,
                        exchange_rate=exchange_rate,  # From app_setting.setting_values
                        domestic_shipping_cost=effective_domestic_cost,  # Size-based from app_setting
                        international_shipping_rate=international_shipping_rate,  # From app_setting
                        profit_margin_percent=profit_margin_percent,  # From app_setting
                        sales_commission_percent=sales_commission_percent,  # From app_setting
                    )
                    logger.info(
                        f"✅ Calculated standardPrice: {purchase_price} JPY for skuId {sku_id} "
                        f"(price={price_cny} CNY, size={product_size}, shipping={effective_domestic_cost} JPY)"
                    )
                except (ValueError, TypeError) as e:
                    logger.warning(f"⚠️  Failed to calculate purchase price for skuId {sku_id}: {e}")
        
        # Use wholesale_price as fallback if price calculation failed
        if not purchase_price and wholesale_price_cny is not None:
            try:
                purchase_price = _calculate_purchase_price(
                    product_cost_cny=wholesale_price_cny,
                    product_weight_kg=product_weight_kg,
                    exchange_rate=exchange_rate,
                    domestic_shipping_cost=effective_domestic_cost,
                    international_shipping_rate=international_shipping_rate,
                    profit_margin_percent=profit_margin_percent,
                    sales_commission_percent=sales_commission_percent,
                )
            except (ValueError, TypeError) as e:
                logger.warning(f"⚠️  Failed to calculate purchase price using wholesale_price: {e}")
        
        # Default purchase price if calculation failed
        if not purchase_price:
            purchase_price = "0"
            logger.warning(f"⚠️  Using default purchase price 0 for skuId {sku_id}")
        
        # Use category_attributes from category_management if available, otherwise use default
        variant_attributes = None
        if category_attributes and isinstance(category_attributes, list) and len(category_attributes) > 0:
            # Convert category_attributes format to variant attributes format
            variant_attributes = []
            for attr in category_attributes:
                if isinstance(attr, dict):
                    attr_name = attr.get("name", "").strip()
                    attr_values = attr.get("values", [])
                    if attr_name:
                        # Ensure values is a list
                        if isinstance(attr_values, str):
                            attr_values = [v.strip() for v in attr_values.split(",") if v.strip()]
                        elif isinstance(attr_values, (list, tuple)):
                            attr_values = [str(v).strip() for v in attr_values if v and str(v).strip()]
                        else:
                            attr_values = []
                        variant_attributes.append({
                            "name": attr_name,
                            "values": attr_values if attr_values else ["-"]
                        })
            logger.info(f"✅ Using {len(variant_attributes)} attributes from category_management for variant {sku_id_str}")
        else:
            # Use default attributes if category_attributes not available
            variant_attributes = [
                {
                    "name": "ブランド名",
                    "values": ["LICEL"]
                },
                {
                    "name": "シリーズ名",
                    "values": ["-"]
                },
                {
                    "name": "原産国／製造国",
                    "values": ["中国"]
                },
                {
                    "name": "総個数",
                    "values": ["1"]
                }
            ]
            logger.debug(f"ℹ️  Using default attributes for variant {sku_id_str} (no category_management attributes)")
        
        # Build variant object with the combination as selectorValues
        variant = {
            "selectorValues": combination,  # Use the generated combination
            "standardPrice": purchase_price,
            "articleNumber": {
                "exemptionReason": 5
            },
            "attributes": variant_attributes,
            "shipping": {
                "postageIncluded": True
            },
            "features": {
                "restockNotification": False,
                "noshi": False
            },
            "normalDeliveryDateId": 1
        }
        
        variants[sku_id_str] = variant
        logger.info(f"✅ Created variant {combo_idx + 1}/{len(all_combinations)}: skuId={sku_id_str}, selectorValues={combination}, standardPrice={purchase_price}")
    
    logger.info(f"🎯 Generated {len(variants)} variants from {len(all_combinations)} combinations")
    return variants if variants else None


def upsert_product_management_from_origin_ids(
    product_ids: Iterable[str],
    *,
    dsn: Optional[str] = None,
) -> int:
    """Insert or update product_management rows using data from products_origin by product_id."""
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    ids = [str(pid) for pid in product_ids if str(pid).strip()]
    if not ids:
        return 0

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Ensure schema exists and has all required columns
            cur.execute(SCHEMA_SQL_PRODUCT_MANAGEMENT)
            # Add src_url column if it doesn't exist (for existing tables)
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='src_url'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN src_url text;
                    END IF;
                END $$;
            """)
            # Add main_category column if it doesn't exist (for existing tables)
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='main_category'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN main_category text;
                    END IF;
                END $$;
            """)
            # Add middle_category column if it doesn't exist (for existing tables)
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='middle_category'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN middle_category text;
                    END IF;
                END $$;
            """)
            # Add r_cat_id column if it doesn't exist (for existing tables)
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='r_cat_id'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN r_cat_id text;
                    END IF;
                END $$;
            """)
            # Add change_status column if it doesn't exist (for existing tables)
            cur.execute("""
                DO $$ 
                BEGIN 
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name='product_management' AND column_name='change_status'
                    ) THEN
                        ALTER TABLE product_management ADD COLUMN change_status text;
                    END IF;
                END $$;
            """)

            # Fetch source rows (include weight, size, wholesale_price, and r_cat_id for variants & category mapping)
            cur.execute(
                """
                SELECT product_id, product_name, product_description, type, detail_json,
                       main_category, middle_category, weight, size, wholesale_price, r_cat_id
                FROM products_origin
                WHERE product_id = ANY(%s)
                """,
                (ids,),
            )
            rows = cur.fetchall()
            if not rows:
                return 0

            # Load category mapping: category_ids -> rakuten_category_ids
            category_rakuten_map: dict[str, list[str]] = {}
            cur.execute(
                """
                SELECT category_ids, rakuten_category_ids
                FROM category_management
                WHERE rakuten_category_ids IS NOT NULL AND jsonb_array_length(rakuten_category_ids) > 0
                """
            )
            for cat_ids_row, rakuten_ids_row in cur.fetchall():
                if not cat_ids_row or not rakuten_ids_row:
                    continue
                # Parse category_ids (JSONB array)
                if isinstance(cat_ids_row, str):
                    try:
                        cat_ids = json.loads(cat_ids_row)
                    except json.JSONDecodeError:
                        continue
                elif isinstance(cat_ids_row, (list, tuple)):
                    cat_ids = list(cat_ids_row)
                else:
                    continue
                
                # Parse rakuten_category_ids (JSONB array)
                if isinstance(rakuten_ids_row, str):
                    try:
                        rakuten_ids = json.loads(rakuten_ids_row)
                    except json.JSONDecodeError:
                        continue
                elif isinstance(rakuten_ids_row, (list, tuple)):
                    rakuten_ids = list(rakuten_ids_row)
                else:
                    continue
                
                # Map each category_id to rakuten_category_ids
                for cat_id in cat_ids:
                    if cat_id:
                        category_rakuten_map[str(cat_id)] = [str(rid) for rid in rakuten_ids if rid]

            # Load category_management mapping: category_ids -> (genre_id, attributes)
            category_genre_attrs_map: dict[str, tuple[Optional[str], Optional[list[dict]]]] = {}
            cur.execute(
                """
                SELECT category_ids, genre_id, attributes
                FROM category_management
                WHERE category_ids IS NOT NULL AND jsonb_array_length(category_ids) > 0
                """
            )
            category_management_rows = cur.fetchall()
            logger.info(f"📋 Loaded {len(category_management_rows)} category_management rows for genre_id/attributes mapping")
            
            # Debug: Check first row structure
            if category_management_rows:
                first_row = category_management_rows[0]
                logger.debug(f"🔍 First row type: {type(first_row)}, value: {first_row}")
                if isinstance(first_row, (list, tuple)):
                    logger.debug(f"🔍 First row length: {len(first_row)}, items: {[type(x).__name__ for x in first_row]}")
                elif isinstance(first_row, dict):
                    logger.debug(f"🔍 First row keys: {list(first_row.keys())}")
            
            for row in category_management_rows:
                # Standard cursor returns tuple, RealDictCursor returns dict
                if isinstance(row, dict):
                    cat_ids_row = row.get("category_ids")
                    genre_id_row = row.get("genre_id")
                    attrs_row = row.get("attributes")
                elif isinstance(row, (list, tuple)):
                    cat_ids_row = row[0] if len(row) > 0 else None
                    genre_id_row = row[1] if len(row) > 1 else None
                    attrs_row = row[2] if len(row) > 2 else None
                else:
                    logger.warning(f"⚠️  Unexpected row type: {type(row)}, skipping")
                    continue
                
                logger.debug(f"🔍 Processing row: cat_ids_row type={type(cat_ids_row).__name__}, genre_id={genre_id_row}, attrs_row type={type(attrs_row).__name__ if attrs_row else None}")
                
                if not cat_ids_row:
                    logger.debug(f"⚠️  Skipping row with empty category_ids")
                    continue
                
                # Parse category_ids (JSONB array)
                # PostgreSQL JSONB is automatically parsed by psycopg2, so it should be a list
                cat_ids = None
                if isinstance(cat_ids_row, (list, tuple)):
                    cat_ids = list(cat_ids_row)
                elif isinstance(cat_ids_row, str):
                    # If it's a string, check if it's a column name (error case) or JSON string
                    if cat_ids_row == "category_ids":
                        logger.error(f"❌ Got column name 'category_ids' instead of data! Row structure may be incorrect.")
                        continue
                    # Check if it's a JSON string
                    if cat_ids_row.strip().startswith('[') or cat_ids_row.strip().startswith('{'):
                        try:
                            cat_ids = json.loads(cat_ids_row)
                        except json.JSONDecodeError:
                            logger.warning(f"⚠️  Failed to parse category_ids as JSON: {cat_ids_row[:100]}")
                            continue
                    else:
                        # Single category ID string
                        cat_ids = [cat_ids_row.strip()]
                else:
                    logger.warning(f"⚠️  Unexpected category_ids type: {type(cat_ids_row)}, value: {cat_ids_row}")
                    continue
                
                if not cat_ids:
                    logger.debug(f"⚠️  Skipping row with empty parsed category_ids")
                    continue
                
                # Parse attributes (JSONB array)
                # PostgreSQL JSONB is automatically parsed by psycopg2, so it should be a list
                attributes = None
                if attrs_row:
                    if isinstance(attrs_row, (list, tuple)):
                        attributes = list(attrs_row)
                    elif isinstance(attrs_row, str):
                        # If it's a string, try to parse as JSON
                        try:
                            attributes = json.loads(attrs_row)
                        except json.JSONDecodeError:
                            logger.warning(f"⚠️  Failed to parse attributes as JSON: {attrs_row[:100] if len(str(attrs_row)) > 100 else attrs_row}")
                            attributes = None
                    elif isinstance(attrs_row, dict):
                        # If it's a dict, convert to list format
                        attributes = [attrs_row] if attrs_row else None
                
                # Map each category_id to (genre_id, attributes)
                for cat_id in cat_ids:
                    if cat_id:
                        cat_id_str = str(cat_id).strip()
                        if cat_id_str:
                            category_genre_attrs_map[cat_id_str] = (genre_id_row, attributes)
                            logger.debug(
                                f"📝 Mapped category_id={cat_id_str} -> genre_id={genre_id_row}, "
                                f"attributes={len(attributes) if attributes else 0} items"
                            )
            
            logger.info(f"✅ Built category_genre_attrs_map with {len(category_genre_attrs_map)} entries")
            if len(category_genre_attrs_map) > 0:
                sample_keys = list(category_genre_attrs_map.keys())[:5]
                logger.info(f"📋 Sample category_ids in map: {sample_keys}")

            # Load pricing settings for actualPurchasePrice calculation
            pricing_settings = get_pricing_settings(dsn=dsn_final)
            exchange_rate = pricing_settings.get("exchange_rate", 22.0)
            profit_margin_percent = pricing_settings.get("profit_margin_percent", 1.5)
            sales_commission_percent = pricing_settings.get("sales_commission_percent", 10.0)
            international_shipping_rate = pricing_settings.get("international_shipping_rate", 19.2)
            domestic_shipping_costs = pricing_settings.get("domestic_shipping_costs", {})
            default_domestic_shipping = pricing_settings.get("domestic_shipping_cost", 326.0)

            # Process and save each product individually to ensure immediate database persistence
            saved_count = 0
            for r in rows:
                product_id = r.get("product_id")
                product_name = r.get("product_name") or ""
                main_category = r.get("main_category")
                middle_category = r.get("middle_category")
                product_type = r.get("type")  # This is the category ID from products_origin
                wholesale_price = r.get("wholesale_price")
                weight = r.get("weight")
                size = r.get("size")
                
                # Get genre_id and attributes from category_management based on middle_category
                category_genre_id = None
                category_attributes = None
                if middle_category:
                    middle_category_str = str(middle_category).strip()
                    logger.debug(
                        f"🔍 Looking up category_management for middle_category='{middle_category_str}' "
                        f"(product_id={product_id})"
                    )
                    logger.debug(
                        f"📋 Available category_ids in map: {list(category_genre_attrs_map.keys())[:10]}..."
                        if len(category_genre_attrs_map) > 10
                        else f"📋 Available category_ids in map: {list(category_genre_attrs_map.keys())}"
                    )
                    
                    if middle_category_str in category_genre_attrs_map:
                        category_genre_id, category_attributes = category_genre_attrs_map[middle_category_str]
                        logger.info(
                            f"✅ Found category_management match for middle_category='{middle_category_str}': "
                            f"genre_id={category_genre_id}, attributes={len(category_attributes) if category_attributes else 0} items"
                        )
                    else:
                        logger.warning(
                            f"⚠️  No category_management match found for middle_category='{middle_category_str}' "
                            f"(product_id={product_id}). Available keys: {list(category_genre_attrs_map.keys())[:5]}..."
                        )
                else:
                    logger.debug(f"ℹ️  No middle_category for product {product_id}, using defaults")

                # Determine Rakuten category IDs (r_cat_id) for this product.
                # 1. Prefer explicit r_cat_id already stored on products_origin.
                # 2. If missing/empty, fall back to mapping from category_management.rakuten_category_ids.
                r_cat_ids_list: list[str] = []
                raw_r_cat = r.get("r_cat_id")
                if isinstance(raw_r_cat, (list, tuple)):
                    r_cat_ids_list = [
                        str(cid).strip()
                        for cid in raw_r_cat
                        if cid is not None and str(cid).strip()
                    ]
                elif isinstance(raw_r_cat, str):
                    raw_r_cat_str = raw_r_cat.strip()
                    if raw_r_cat_str:
                        parsed: list[str] = []
                        try:
                            loaded = json.loads(raw_r_cat_str)
                            if isinstance(loaded, list):
                                parsed = [
                                    str(cid).strip()
                                    for cid in loaded
                                    if cid is not None and str(cid).strip()
                                ]
                        except Exception:
                            parsed = []
                        if parsed:
                            r_cat_ids_list = parsed
                        else:
                            # Treat as single ID string
                            r_cat_ids_list = [raw_r_cat_str]
                elif raw_r_cat is not None:
                    cid_str = str(raw_r_cat).strip()
                    if cid_str:
                        r_cat_ids_list = [cid_str]

                # Fallback: infer from category_management if no r_cat_id on products_origin
                if not r_cat_ids_list:
                    # Try to match by type (category ID)
                    if product_type and str(product_type) in category_rakuten_map:
                        rakuten_ids = category_rakuten_map[str(product_type)]
                        r_cat_ids_list = [
                            str(rid).strip()
                            for rid in rakuten_ids
                            if rid is not None and str(rid).strip()
                        ]
                        if r_cat_ids_list:
                            logger.info(
                                f"✅ Inferred r_cat_id for product {product_id} from type={product_type}: {r_cat_ids_list}"
                            )
                    # Try to match by main_category
                    if not r_cat_ids_list and main_category and str(main_category) in category_rakuten_map:
                        rakuten_ids = category_rakuten_map[str(main_category)]
                        r_cat_ids_list = [
                            str(rid).strip()
                            for rid in rakuten_ids
                            if rid is not None and str(rid).strip()
                        ]
                        if r_cat_ids_list:
                            logger.info(
                                f"✅ Inferred r_cat_id for product {product_id} from main_category={main_category}: {r_cat_ids_list}"
                            )
                    # Try to match by middle_category
                    if not r_cat_ids_list and middle_category and str(middle_category) in category_rakuten_map:
                        rakuten_ids = category_rakuten_map[str(middle_category)]
                        r_cat_ids_list = [
                            str(rid).strip()
                            for rid in rakuten_ids
                            if rid is not None and str(rid).strip()
                        ]
                        if r_cat_ids_list:
                            logger.info(
                                f"✅ Inferred r_cat_id for product {product_id} from middle_category={middle_category}: {r_cat_ids_list}"
                            )

                if not r_cat_ids_list:
                    logger.debug(
                        f"ℹ️  No r_cat_id determined for product {product_id} "
                        f"(type={product_type}, main={main_category}, middle={middle_category})"
                    )

                # Store as JSON array string for product_management.r_cat_id (jsonb-compatible)
                r_cat_id_json = json.dumps(r_cat_ids_list, ensure_ascii=False)

                # Parse detail_json payload (already filtered to 'T' fields)
                detail_payload: Optional[dict] = None
                detail_raw = r.get("detail_json")
                if detail_raw:
                    if isinstance(detail_raw, dict):
                        detail_payload = detail_raw
                    elif isinstance(detail_raw, str):
                        try:
                            detail_payload = json.loads(detail_raw)
                        except json.JSONDecodeError:
                            detail_payload = None
                
                # Log detail_json structure for debugging
                if detail_payload and isinstance(detail_payload, dict):
                    logger.info(f"📦 detail_json keys for product {product_id}: {list(detail_payload.keys())}")
                    
                    # Check for specification at root level
                    if "specification" in detail_payload:
                        logger.info(f"✅ Found specification field at root level in detail_json for product {product_id}")
                    else:
                        logger.info(f"❌ No specification field at root level in detail_json for product {product_id}")
                    
                    # Check for goodsinfo and specification inside it (try different possible names)
                    goodsinfo = (
                        detail_payload.get("goodsinfo") or
                        detail_payload.get("goodsInfo") or
                        detail_payload.get("goods_info") or
                        detail_payload.get("productInfo") or
                        detail_payload.get("product_info")
                    )
                    if goodsinfo and isinstance(goodsinfo, dict):
                        logger.info(f"✅ Found goodsinfo object for product {product_id}")
                        logger.info(f"📦 goodsinfo keys: {list(goodsinfo.keys())}")
                        if "specification" in goodsinfo:
                            logger.info(f"✅ Found specification field inside goodsinfo for product {product_id}")
                            # Log a sample of the specification structure
                            spec_sample = goodsinfo.get("specification")
                            if isinstance(spec_sample, list) and len(spec_sample) > 0:
                                logger.info(f"📋 Specification sample (first item): {json.dumps(spec_sample[0], ensure_ascii=False)[:300]}")
                        else:
                            logger.info(f"❌ No specification field inside goodsinfo for product {product_id}")
                    else:
                        logger.info(f"❌ No goodsinfo object found in detail_json for product {product_id}")
                        # Log a sample of detail_json structure to help debug
                        detail_sample = json.dumps(detail_payload, ensure_ascii=False)[:500]
                        logger.info(f"📋 detail_json sample (first 500 chars): {detail_sample}")
                elif detail_payload is None:
                    logger.info(f"⚠️  No detail_json found for product {product_id}")
                else:
                    logger.info(f"⚠️  detail_json is not a dict for product {product_id}: {type(detail_payload)}")

                # Extract fromUrl from detail_json
                src_url: Optional[str] = None
                if detail_payload and isinstance(detail_payload, dict):
                    # Try to get fromUrl from root level first
                    src_url = detail_payload.get("fromUrl") or detail_payload.get("from_url") or detail_payload.get("fromURL")
                    
                    # If not found at root, try inside goodsInfo
                    if not src_url:
                        goodsinfo = (
                            detail_payload.get("goodsinfo") or
                            detail_payload.get("goodsInfo") or
                            detail_payload.get("goods_info") or
                            detail_payload.get("productInfo") or
                            detail_payload.get("product_info")
                        )
                        if goodsinfo and isinstance(goodsinfo, dict):
                            src_url = goodsinfo.get("fromUrl") or goodsinfo.get("from_url") or goodsinfo.get("fromURL")
                    
                    # Ensure it's a string
                    if src_url and not isinstance(src_url, str):
                        src_url = str(src_url) if src_url else None
                    
                    if src_url:
                        logger.info(f"🔗 Extracted src_url for product {product_id}: {src_url[:100] if len(src_url) > 100 else src_url}")
                    else:
                        logger.debug(f"ℹ️  No fromUrl found in detail_json for product {product_id}")

                # Check if this product already exists in product_management
                cur.execute(
                    """
                    SELECT images, product_image_code
                    FROM product_management
                    WHERE item_number = %s
                    """,
                    (product_id,),
                )
                existing_pm = cur.fetchone()
                existing_images = None
                existing_product_image_code = None
                if existing_pm:
                    existing_images = existing_pm.get("images")
                    existing_product_image_code = existing_pm.get("product_image_code")
                    logger.info(f"🔁 product_management row already exists for {product_id} - skipping image re-processing")

                # Image code and images JSON to be stored
                images_json: Optional[str] = None

                if existing_pm:
                    # Reuse existing images and product_image_code, and skip heavy image processing
                    if existing_images is not None:
                        images_json = json.dumps(existing_images, ensure_ascii=False)
                    product_image_code = existing_product_image_code
                else:
                    # Generate product_image_code for this product (always generate, even if no images)
                    from .image_pro import generate_product_image_code
                    product_image_code = generate_product_image_code(product_id)
                    
                    # Extract images from detail_json and process them with Gemini
                    processed_image_urls = []
                    image_urls: list[str] = []
                    try:
                        # Import image processing function (lazy import to avoid circular dependencies)
                        from .image_pro import process_and_upload_images_from_urls
                        
                        # Extract image URLs from detail_json
                        if detail_payload and isinstance(detail_payload, dict):
                            detail_images = detail_payload.get("images", [])
                            if isinstance(detail_images, list):
                                image_urls = [url for url in detail_images if isinstance(url, str) and url.strip()]
                        
                        # Process images if we have any
                        if image_urls:
                            # Check if image processing is enabled
                            from .image_pro import IMAGE_PROCESSING_ENABLED
                            if not IMAGE_PROCESSING_ENABLED:
                                logger.info(f"⚠️  Image processing is temporarily disabled. Skipping {len(image_urls)} image(s) for product {product_id}")
                            else:
                                logger.info(f"Processing {len(image_urls)} image(s) for product {product_id} using Gemini")
                                logger.info(f"Product image code: {product_image_code}")
                            
                            # Get S3 configuration from environment variables
                            bucket_name = os.getenv("S3_BUCKET", "licel-product-image")
                            folder_name = os.getenv("S3_FOLDER", "products")
                            
                            # Get Gemini prompt for image processing from image_pro module
                            from .image_pro import get_default_prompt_tags
                            prompt_tags = get_default_prompt_tags()
                            
                            # Process images: download → process with Gemini → upload to S3
                            # Only upload processed images (not originals) to save storage
                            processed_urls, failed_urls, image_results = process_and_upload_images_from_urls(
                                image_urls=image_urls,
                                product_id=product_id,
                                bucket_name=bucket_name,
                                folder_name=folder_name,
                                prompt_tags=prompt_tags,
                                upload_original=False,  # Upload originals so we can keep failed images too
                                max_retries=2,
                                retry_delay=3,
                            )
                            
                            # Build images array in the format expected by product_management table
                            # Format: [{"type": "CABINET", "location": relative_path, "alt": product_name}]
                            # Store only relative path (e.g., "/01306503/01306503_4.jpg") instead of full URL
                            # Use processed image if available; otherwise fall back to uploaded original for failed items
                            added_count = 0
                            for img_url, result in (image_results or {}).items():
                                chosen_url = result.get("processed_url") or result.get("original_url")
                                if not chosen_url:
                                    continue
                                relative_path = _extract_relative_image_path(chosen_url)
                                processed_image_urls.append({
                                    "type": "CABINET",
                                    "location": relative_path,
                                    "alt": product_name
                                })
                                added_count += 1
                            
                            if processed_urls:
                                logger.info(f"Successfully processed {len(processed_urls)}/{len(image_urls)} image(s) for product {product_id}")
                            if failed_urls:
                                logger.warning(f"Failed to process {len(failed_urls)} image(s) for product {product_id} (kept originals where available)")
                            logger.info(f"Images stored to DB (processed or original fallback): {added_count}")
                        else:
                            logger.info(f"No images found in detail_json for product {product_id}")
                            
                    except Exception as e:
                        logger.error(f"Error processing images for product {product_id}: {e}")
                        # Continue with registration even if image processing fails
                        # Fall back to original image URLs if available
                    
                    # If no processed images but original URLs exist, use them directly
                    # Store only relative path instead of full URL
                    if not processed_image_urls and image_urls:
                        for url in image_urls:
                            relative_path = _extract_relative_image_path(url)
                            processed_image_urls.append({"type": "CABINET", "location": relative_path, "alt": product_name})

                    images_json = json.dumps(processed_image_urls, ensure_ascii=False) if processed_image_urls else None

                # Existing description from products_origin
                existing_description = r.get("product_description") or ""

                # Generate complete Rakuten product content using new professional system
                try:
                    from .openai_utils import generate_rakuten_product_content
                    rakuten_content = generate_rakuten_product_content(
                        product_name=product_name,
                        detail_payload=detail_payload,
                        existing_description=existing_description,
                    )
                    
                    # Use the generated title (already formatted with half-width spaces)
                    refined_title = rakuten_content.title
                    
                    # Validate title length (ensure it's not empty and within reasonable bounds)
                    if not refined_title or len(refined_title.strip()) == 0:
                        raise ValueError("Generated title is empty")
                    
                    # Map to legacy format for backward compatibility
                    # Use generated sales_description if available, otherwise use description
                    tagline = rakuten_content.catchphrase[:80] if rakuten_content.catchphrase else ""
                    sales_description = rakuten_content.sales_description if rakuten_content.sales_description else (rakuten_content.description[:600] if rakuten_content.description else "")
                    
                    # Split description into PC and SP versions
                    # Note: description should NOT include delivery message (it's only in sales_description)
                    base_description = rakuten_content.description if rakuten_content.description else ""
                    
                    # Remove delivery message from base description if it exists (safety check)
                    from .openai_utils import DELIVERY_PATTERNS, DELIVERY_MESSAGE
                    cleaned_base = base_description
                    for pattern in DELIVERY_PATTERNS:
                        cleaned_base = pattern.sub('', cleaned_base)
                    # Also remove the exact delivery message
                    cleaned_base = cleaned_base.replace(DELIVERY_MESSAGE, '').rstrip('<br>').rstrip()
                    
                    # Create PC and SP descriptions WITHOUT delivery message
                    description_pc = cleaned_base[:800] if cleaned_base else ""
                    description_sp = cleaned_base[:400] if cleaned_base else ""
                    
                    # Add delivery message to sales_description (not to PC/SP descriptions)
                    if rakuten_content.sales_description:
                        # Remove any existing delivery messages first
                        sales_desc_cleaned = rakuten_content.sales_description
                        for pattern in DELIVERY_PATTERNS:
                            sales_desc_cleaned = pattern.sub('', sales_desc_cleaned)
                        sales_desc_cleaned = sales_desc_cleaned.replace(DELIVERY_MESSAGE, '').rstrip('<br>').rstrip()
                        # Add delivery message
                        from .openai_utils import add_delivery_message
                        sales_description = add_delivery_message(sales_desc_cleaned) if sales_desc_cleaned else DELIVERY_MESSAGE.lstrip('<br>')
                    else:
                        # If no sales_description, create one with delivery message
                        sales_description = DELIVERY_MESSAGE.lstrip('<br>')
                except Exception as e:
                    # Fallback if OpenAI generation fails
                    logger.warning(f"⚠️  Failed to generate Rakuten content with OpenAI for product {product_id}: {e}. Using fallback.")
                    
                    # Extract product details for fallback title
                    try:
                        from .openai_utils import extract_product_details_from_detail_json
                        product_details = extract_product_details_from_detail_json(detail_payload)
                    except Exception:
                        product_details = {}
                    
                    # Use product_name as fallback title (ensure it's within 100-110 chars)
                    refined_title = product_name[:110] if product_name else "商品名未設定"
                    tagline = product_name[:80] if product_name else ""
                    
                    # Create PC and SP descriptions WITHOUT delivery message in fallback
                    description_pc = existing_description[:350] if existing_description else ""
                    description_sp = existing_description[:220] if existing_description else ""
                    
                    # Add delivery message to sales_description in fallback (not to PC/SP descriptions)
                    from .openai_utils import DELIVERY_MESSAGE, add_delivery_message
                    sales_description = add_delivery_message(existing_description[:220] if existing_description else "") if existing_description else DELIVERY_MESSAGE.lstrip('<br>')
                    
                    # Ensure title is at least 100 characters if possible
                    if len(refined_title) < 100 and product_details:
                        detail_values = [str(v) for k, v in product_details.items() if v]
                        if detail_values:
                            additional = ' '.join(detail_values[:5])
                            refined_title = f"{refined_title} {additional}"[:110]
                    
                    # Ensure title is not empty
                    if not refined_title or len(refined_title.strip()) == 0:
                        refined_title = product_name or "商品名未設定"
                
                # Store full description with HTML formatting
                product_description_payload = {
                    "pc": description_pc,
                    "sp": description_sp,
                }
                product_description_json = json.dumps(product_description_payload, ensure_ascii=False)

                # Extract other fields with defaults
                item_type = "NORMAL"  # Always set to NORMAL
                # Use genre_id from category_management if found, otherwise default to 201198
                # Convert genre_id to string if it's not None (product_management.genre_id is text type)
                if category_genre_id:
                    try:
                        # Ensure genre_id is a string (product_management.genre_id is text type)
                        genre_id = str(category_genre_id).strip() if category_genre_id else "201198"
                        logger.info(f"✅ Using genre_id={genre_id} from category_management for product {product_id}")
                    except Exception as e:
                        logger.warning(f"⚠️  Failed to convert genre_id to string: {e}, using default")
                        genre_id = "201198"
                else:
                    genre_id = "201198"
                    logger.debug(f"ℹ️  Using default genre_id=201198 for product {product_id} (no category_management match)")

                # Default flags
                hide_item = True
                unlimited_inventory_flag = False

                # Default features JSON
                features_payload = {
                    "searchVisibility": "ALWAYS_VISIBLE",
                    "displayNormalCartButton": True,
                    "displaySubscriptionCartButton": False,
                    "inventoryDisplay": "DISPLAY_ABSOLUTE_STOCK_COUNT",
                    "shopContact": True,
                    "review": "SHOP_SETTING",
                    "displayManufacturerContents": False,
                }
                features_json = json.dumps(features_payload, ensure_ascii=False)

                # Default payment JSON
                payment_payload = {
                    "taxIncluded": True,
                    "taxRate": "0.1",
                    "cashOnDeliveryFeeIncluded": False,
                }
                payment_json = json.dumps(payment_payload, ensure_ascii=False)

                # Default layout JSON
                layout_payload = {
                    "itemLayoutId": 1,
                    "navigationId": 0,
                    "layoutSequenceId": 0,
                    "smallDescriptionId": 0,
                    "largeDescriptionId": 0,
                    "showcaseId": 0,
                }
                layout_json = json.dumps(layout_payload, ensure_ascii=False)

                # Extract and transform specification to variant_selectors
                logger.info(f"🔍 Starting variant_selectors extraction for product {product_id}")
                variant_selectors_json = None
                if detail_payload and isinstance(detail_payload, dict):
                    logger.info(f"📋 detail_payload is a dict for product {product_id}, checking for specification...")
                    logger.info(f"📦 detail_payload top-level keys: {list(detail_payload.keys())}")
                    
                    # First, check inside goodsinfo/goodsInfo object (most common location)
                    # Try different possible names for goodsinfo (case-insensitive matching)
                    goodsinfo = None
                    # Try exact matches first (most common cases)
                    for key in ["goodsInfo", "goodsinfo", "goods_info", "productInfo", "product_info"]:
                        if key in detail_payload:
                            goodsinfo = detail_payload.get(key)
                            logger.info(f"✅ Found goodsInfo object with key '{key}' for product {product_id}")
                            break
                    
                    # If not found with exact keys, try case-insensitive search
                    if not goodsinfo:
                        for key, value in detail_payload.items():
                            if isinstance(value, dict) and key.lower() in ["goodsinfo", "productinfo"]:
                                goodsinfo = value
                                logger.info(f"✅ Found goodsInfo object with key '{key}' (case-insensitive) for product {product_id}")
                                break
                    
                    if goodsinfo and isinstance(goodsinfo, dict):
                        logger.info(f"📦 goodsinfo keys: {list(goodsinfo.keys())}")
                        specification = (
                            goodsinfo.get("specification") or
                            goodsinfo.get("specifications") or
                            goodsinfo.get("spec")
                        )
                        if specification:
                            logger.info(f"✅ Found specification inside goodsinfo for product {product_id}")
                        else:
                            logger.warning(f"⚠️  No specification found inside goodsinfo. goodsinfo keys: {list(goodsinfo.keys())}")
                    else:
                        logger.info(f"❌ No goodsinfo object found. Checking all keys for nested objects...")
                        specification = None
                        # Check if any key contains a dict that might have specification
                        for key, value in detail_payload.items():
                            if isinstance(value, dict):
                                logger.info(f"📋 Found nested dict '{key}' with keys: {list(value.keys())}")
                                if "specification" in value or "specifications" in value or "spec" in value:
                                    logger.info(f"✅ Found specification in nested dict '{key}'")
                                    spec_candidate = (
                                        value.get("specification") or
                                        value.get("specifications") or
                                        value.get("spec")
                                    )
                                    if spec_candidate:
                                        specification = spec_candidate
                                        break
                    
                    # If not found in goodsinfo or nested dicts, try root level
                    if not specification:
                        logger.info(f"📋 Checking root level for specification...")
                        specification = (
                            detail_payload.get("specification") or
                            detail_payload.get("specifications") or
                            detail_payload.get("spec")
                        )
                    
                    if specification:
                        logger.info(f"✅ Found specification for product {product_id}: type={type(specification)}, length={len(specification) if isinstance(specification, (list, dict)) else 'N/A'}")
                        
                        # If specification is a dict, try to get a list from it
                        if isinstance(specification, dict):
                            # Try common keys that might contain the list
                            specification = (
                                specification.get("items") or
                                specification.get("list") or
                                specification.get("data") or
                                list(specification.values())[0] if specification else None
                            )
                            if specification and not isinstance(specification, list):
                                specification = None
                        
                        if specification and isinstance(specification, list):
                            # Log the first specification item structure for debugging
                            if len(specification) > 0:
                                logger.info(f"📋 First specification item structure for product {product_id}: {json.dumps(specification[0], ensure_ascii=False, indent=2) if isinstance(specification[0], dict) else str(specification[0])}")
                            
                            try:
                                variant_selectors = _transform_specification_to_variant_selectors(specification)
                                if variant_selectors:
                                    variant_selectors_json = json.dumps(variant_selectors, ensure_ascii=False)
                                    logger.info(f"✅ Generated variant_selectors for product {product_id}: {len(variant_selectors)} selectors")
                                    logger.debug(f"📝 variant_selectors JSON for product {product_id}: {variant_selectors_json[:500]}...")  # Log first 500 chars
                                else:
                                    logger.warning(f"⚠️  No variant_selectors generated for product {product_id} (transformation returned None)")
                            except Exception as e:
                                logger.error(f"❌ Error transforming specification to variant_selectors for product {product_id}: {e}", exc_info=True)
                        else:
                            logger.warning(f"⚠️  Specification for product {product_id} is not a list: {type(specification)}")
                    else:
                        logger.info(f"ℹ️  No specification found in detail_json for product {product_id}")
                else:
                    logger.info(f"ℹ️  No detail_payload or detail_payload is not a dict for product {product_id} (type: {type(detail_payload)})")

                # Log variant_selectors_json before saving
                if variant_selectors_json:
                    logger.info(f"💾 Saving variant_selectors for product {product_id}: {len(variant_selectors_json)} characters")
                else:
                    logger.debug(f"💾 No variant_selectors to save for product {product_id}")
                
                # Extract and transform goodsInventory to variants
                logger.info(f"🔍 Starting variants extraction for product {product_id}")
                variants_json = None
                
                # Get product weight, size, and wholesale_price for purchase price calculation
                product_weight_kg = r.get("weight")
                product_size_value = r.get("size")
                wholesale_price_cny = r.get("wholesale_price")
                
                # Load purchase price settings from database (with fallback to settings.json)
                purchase_price_settings = get_pricing_settings(dsn=dsn)
                
                if detail_payload and isinstance(detail_payload, dict):
                    # Get goodsInventory from goodsInfo
                    goodsinfo = (
                        detail_payload.get("goodsinfo") or
                        detail_payload.get("goodsInfo") or
                        detail_payload.get("goods_info") or
                        detail_payload.get("productInfo") or
                        detail_payload.get("product_info")
                    )
                    
                    if goodsinfo and isinstance(goodsinfo, dict):
                        goods_inventory = goodsinfo.get("goodsInventory") or goodsinfo.get("goods_inventory")
                        
                        if goods_inventory and isinstance(goods_inventory, list):
                            logger.info(f"✅ Found goodsInventory for product {product_id}: {len(goods_inventory)} items")
                            
                            # Get variant_selectors (either from the transformation above or from existing product_management)
                            variant_selectors_list = None
                            if variant_selectors_json:
                                try:
                                    variant_selectors_list = json.loads(variant_selectors_json)
                                except (json.JSONDecodeError, TypeError):
                                    pass
                            
                            # If variant_selectors weren't generated, try to fetch from existing product_management
                            if not variant_selectors_list:
                                try:
                                    cur.execute(
                                        """
                                        SELECT variant_selectors FROM product_management
                                        WHERE item_number = %s
                                        """,
                                        (product_id,),
                                    )
                                    existing_row = cur.fetchone()
                                    if existing_row and existing_row.get("variant_selectors"):
                                        existing_variant_selectors = existing_row.get("variant_selectors")
                                        if isinstance(existing_variant_selectors, str):
                                            variant_selectors_list = json.loads(existing_variant_selectors)
                                        elif isinstance(existing_variant_selectors, (list, dict)):
                                            variant_selectors_list = existing_variant_selectors
                                except Exception as e:
                                    logger.warning(f"⚠️  Failed to fetch existing variant_selectors for product {product_id}: {e}")
                            
                            if variant_selectors_list and isinstance(variant_selectors_list, list):
                                try:
                                    variants = _transform_goods_inventory_to_variants(
                                        goods_inventory=goods_inventory,
                                        variant_selectors=variant_selectors_list,
                                        product_weight_kg=float(product_weight_kg) if product_weight_kg else None,
                                        wholesale_price_cny=float(wholesale_price_cny) if wholesale_price_cny else None,
                                        product_size=float(product_size_value) if product_size_value is not None else None,
                                        exchange_rate=purchase_price_settings["exchange_rate"],
                                        domestic_shipping_cost=purchase_price_settings["domestic_shipping_cost"],
                                        international_shipping_rate=purchase_price_settings["international_shipping_rate"],
                                        profit_margin_percent=purchase_price_settings["profit_margin_percent"],
                                        sales_commission_percent=purchase_price_settings["sales_commission_percent"],
                                        domestic_shipping_costs=purchase_price_settings.get("domestic_shipping_costs"),
                                        category_attributes=category_attributes,  # Pass attributes from category_management
                                    )
                                    if variants:
                                        variants_json = json.dumps(variants, ensure_ascii=False)
                                        logger.info(f"✅ Generated variants for product {product_id}: {len(variants)} variants")
                                    else:
                                        logger.warning(f"⚠️  No variants generated for product {product_id}")
                                except Exception as e:
                                    logger.error(f"❌ Error transforming goodsInventory to variants for product {product_id}: {e}", exc_info=True)
                            else:
                                logger.warning(f"⚠️  No variant_selectors available for product {product_id}, cannot generate variants")
                        else:
                            logger.info(f"ℹ️  No goodsInventory found in goodsInfo for product {product_id}")
                    else:
                        logger.info(f"ℹ️  No goodsInfo found in detail_json for product {product_id}")
                else:
                    logger.info(f"ℹ️  No detail_payload or detail_payload is not a dict for product {product_id} (type: {type(detail_payload)})")
                
                # If variants_json is None but category_attributes exists, try to update existing variants
                if not variants_json and category_attributes:
                    try:
                        cur.execute(
                            """
                            SELECT variants FROM product_management
                            WHERE item_number = %s
                            """,
                            (product_id,),
                        )
                        existing_variants_row = cur.fetchone()
                        if existing_variants_row and existing_variants_row.get("variants"):
                            existing_variants = existing_variants_row.get("variants")
                            if isinstance(existing_variants, str):
                                try:
                                    existing_variants = json.loads(existing_variants)
                                except json.JSONDecodeError:
                                    existing_variants = None
                            
                            if existing_variants and isinstance(existing_variants, dict):
                                # Update attributes for all existing variants
                                updated_variants = {}
                                for sku_id, variant_data in existing_variants.items():
                                    if isinstance(variant_data, dict):
                                        # Convert category_attributes to variant attributes format
                                        variant_attrs = []
                                        for attr in category_attributes:
                                            if isinstance(attr, dict):
                                                attr_name = attr.get("name", "").strip()
                                                attr_values = attr.get("values", [])
                                                if attr_name:
                                                    if isinstance(attr_values, str):
                                                        attr_values = [v.strip() for v in attr_values.split(",") if v.strip()]
                                                    elif isinstance(attr_values, (list, tuple)):
                                                        attr_values = [str(v).strip() for v in attr_values if v and str(v).strip()]
                                                    else:
                                                        attr_values = []
                                                    variant_attrs.append({
                                                        "name": attr_name,
                                                        "values": attr_values if attr_values else ["-"]
                                                    })
                                        
                                        updated_variant = {**variant_data, "attributes": variant_attrs}
                                        updated_variants[sku_id] = updated_variant
                                
                                if updated_variants:
                                    variants_json = json.dumps(updated_variants, ensure_ascii=False)
                                    logger.info(
                                        f"✅ Updated existing variants with category_attributes for product {product_id}: "
                                        f"{len(updated_variants)} variants updated"
                                    )
                    except Exception as e:
                        logger.warning(f"⚠️  Failed to update existing variants with category_attributes: {e}")
                
                # Log variants_json before saving
                if variants_json:
                    logger.info(f"💾 Saving variants for product {product_id}: {len(variants_json)} characters")
                else:
                    logger.debug(f"💾 No variants to save for product {product_id}")
                
                # Extract inventory from goodsInventory
                inventory_json = None
                if goods_inventory and isinstance(goods_inventory, list):
                    try:
                        inventory_data = _extract_inventory_from_goods_inventory(
                            goods_inventory=goods_inventory,
                            item_number=product_id,
                        )
                        if inventory_data:
                            inventory_json = json.dumps(inventory_data, ensure_ascii=False)
                            logger.info(f"✅ Extracted inventory for product {product_id}: {len(inventory_data.get('variants', []))} variants")
                        else:
                            logger.debug(f"💾 No inventory data extracted for product {product_id}")
                    except Exception as e:
                        logger.error(f"❌ Error extracting inventory for product {product_id}: {e}", exc_info=True)
                
                # Calculate actualPurchasePrice (sale price) from wholesale_price and weight
                actual_purchase_price = None
                if wholesale_price is not None and weight is not None:
                    try:
                        # Determine domestic shipping cost based on size
                        effective_domestic_cost = default_domestic_shipping
                        if size is not None:
                            if size <= 30:
                                effective_domestic_cost = domestic_shipping_costs.get("regular", default_domestic_shipping)
                            elif size <= 60:
                                effective_domestic_cost = domestic_shipping_costs.get("size60", default_domestic_shipping)
                            elif size <= 80:
                                effective_domestic_cost = domestic_shipping_costs.get("size80", default_domestic_shipping)
                            elif size <= 100:
                                effective_domestic_cost = domestic_shipping_costs.get("size100", default_domestic_shipping)
                        
                        purchase_price_str = _calculate_purchase_price(
                            product_cost_cny=float(wholesale_price),
                            product_weight_kg=float(weight),
                            exchange_rate=exchange_rate,
                            domestic_shipping_cost=effective_domestic_cost,
                            international_shipping_rate=international_shipping_rate,
                            profit_margin_percent=profit_margin_percent,
                            sales_commission_percent=sales_commission_percent,
                        )
                        if purchase_price_str:
                            actual_purchase_price = float(purchase_price_str)
                            logger.info(f"✅ Calculated actualPurchasePrice for product {product_id}: {actual_purchase_price} JPY")
                        else:
                            logger.warning(f"⚠️  Failed to calculate actualPurchasePrice for product {product_id} (weight or price missing)")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"⚠️  Failed to calculate actualPurchasePrice for product {product_id}: {e}")
                
                # Insert/update this product immediately (save as we process)
                try:
                    cur.execute(
                        """
                        INSERT INTO product_management (
                            item_number, title, product_description, images, product_image_code,
                            item_type, tagline, sales_description, genre_id, tags,
                            hide_item, unlimited_inventory_flag, features, payment, layout,
                            variant_selectors, variants, inventory, src_url, main_category, middle_category, r_cat_id, actual_purchase_price
                        )
                        VALUES (
                            %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s::jsonb, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (item_number) DO UPDATE SET
                            title = excluded.title,
                            product_description = excluded.product_description,
                            item_type = excluded.item_type,
                            tagline = excluded.tagline,
                            sales_description = excluded.sales_description,
                            genre_id = excluded.genre_id,
                            tags = excluded.tags,
                            hide_item = excluded.hide_item,
                            unlimited_inventory_flag = excluded.unlimited_inventory_flag,
                            features = excluded.features,
                            payment = excluded.payment,
                            layout = excluded.layout,
                            variant_selectors = excluded.variant_selectors,
                            variants = excluded.variants,
                            inventory = excluded.inventory,
                            src_url = excluded.src_url,
                            main_category = excluded.main_category,
                            middle_category = excluded.middle_category,
                            r_cat_id = excluded.r_cat_id,
                            actual_purchase_price = excluded.actual_purchase_price
                        """,
                        (
                            product_id,  # item_number
                            refined_title,  # title (refined using OpenAI)
                            product_description_json,  # product_description (JSONB)
                            images_json,  # images (JSONB)
                            product_image_code,  # product_image_code (8-digit code for S3)
                            item_type,  # item_type
                            tagline,  # tagline generated via OpenAI
                            sales_description,  # sales_description generated via OpenAI
                            genre_id,  # genre_id
                            None,  # tags (not available from products_origin)
                            hide_item,  # hide_item default false
                            unlimited_inventory_flag,  # unlimited_inventory_flag default false
                            features_json,  # features JSONB
                            payment_json,  # payment JSONB
                            layout_json,  # layout JSONB
                            variant_selectors_json,  # variant_selectors (transformed from specification)
                            variants_json,  # variants (transformed from goodsInventory)
                            inventory_json,  # inventory (extracted from goodsInventory)
                            src_url,  # src_url (from detail_json.fromUrl)
                            main_category,  # main_category (from products_origin)
                            middle_category,  # middle_category (from products_origin)
                            r_cat_id_json,  # r_cat_id (Rakuten category IDs as JSON array)
                            actual_purchase_price  # actual_purchase_price (calculated sale price)
                        ),
                    )
                    
                    # Update registration_status to 2 (registered) for this product immediately
                    cur.execute(
                        """
                        UPDATE products_origin
                        SET registration_status = 2
                        WHERE product_id = %s
                        """,
                        (product_id,),
                    )
                    
                    # Commit after each product to ensure immediate persistence
                    conn.commit()
                    saved_count += 1
                    logger.info(f"✅ Saved product {product_id} to product_management (total: {saved_count}/{len(rows)})")
                    
                except Exception as e:
                    # Log error but continue with other products
                    logger.error(f"❌ Failed to save product {product_id} to product_management: {e}", exc_info=True)
                    conn.rollback()  # Rollback this product's transaction
                    # Continue processing other products
            
        return saved_count


def get_product_management(
    *, limit: int = 50, offset: int = 0, sort_by: Optional[str] = None, sort_order: Optional[str] = None, dsn: Optional[str] = None
) -> list[dict]:
    """
    Read rows from product_management table only.
    Only products that were registered from the Product Research page (exist in product_management) are returned.
    
    Args:
        limit: Maximum number of products to return
        offset: Number of products to skip
        sort_by: Field to sort by ('created_at' or 'rakuten_registered_at')
        sort_order: Sort order ('asc' or 'desc')
        dsn: Optional database connection string
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    # Validate sort_by parameter
    valid_sort_fields = ['created_at', 'rakuten_registered_at']
    if sort_by and sort_by not in valid_sort_fields:
        raise ValueError(f"Invalid sort_by parameter: {sort_by}. Must be one of {valid_sort_fields}")
    
    # Validate sort_order parameter
    if sort_order and sort_order not in ['asc', 'desc']:
        raise ValueError(f"Invalid sort_order parameter: {sort_order}. Must be 'asc' or 'desc'")
    
    # Default to created_at DESC if not specified
    order_by_field = sort_by if sort_by else 'created_at'
    order_by_direction = sort_order.upper() if sort_order else 'DESC'
    
    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Only select from product_management table (products registered from Product Research page)
            # IMPORTANT: Do NOT filter by rakuten_registration_status - return all products regardless of status
            # This allows category filtering to show all products (unregistered, failed, registered)
            cur.execute(
                f"""
                SELECT id, item_number, title, tagline, product_description, sales_description,
                       genre_id, tags, images, hide_item, variant_selectors, variants, 
                       created_at, rakuten_registered_at, rakuten_registration_status, image_registration_status, 
                       inventory_registration_status, src_url, main_category, middle_category
                FROM product_management
                ORDER BY {order_by_field} {order_by_direction} NULLS LAST
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            )
            rows = cur.fetchall()
            # Parse json fields
            for r in rows:
                for key in ("product_description", "images", "variant_selectors", "variants", "tags"):
                    val = r.get(key)
                    if isinstance(val, str):
                        try:
                            r[key] = json.loads(val)
                        except Exception:
                            pass
                    # If it's already a dict/list (from jsonb), keep it as is
                
                # Ensure rakuten_registration_status is properly handled
                # Convert None/NULL to null or ensure it's a string value
                status = r.get("rakuten_registration_status")
                if status is None:
                    # Keep as None (will be serialized as null in JSON)
                    # Frontend will treat null/undefined as "unregistered"
                    pass
                elif isinstance(status, bool):
                    # Handle boolean if somehow present (migration edge case)
                    r["rakuten_registration_status"] = "true" if status else "false"
            
            return [dict(r) for r in rows]


def get_product_management_by_item_number(
    item_number: str, *, dsn: Optional[str] = None
) -> Optional[dict]:
    """Get a single product from product_management by item_number."""
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, item_number, title, tagline, item_type, product_description, 
                       sales_description, genre_id, tags, hide_item, unlimited_inventory_flag,
                       images, features, payment, layout, variant_selectors, variants, inventory,
                       rakuten_registration_status, image_registration_status, inventory_registration_status,
                       src_url, main_category, middle_category, r_cat_id
                FROM product_management
                WHERE item_number = %s
                """,
                (item_number,),
            )
            row = cur.fetchone()
            if not row:
                return None
            
            product = dict(row)
            # Parse jsonb fields
            jsonb_fields = [
                "product_description",
                "images",
                "features",
                "payment",
                "layout",
                "variant_selectors",
                "variants",
                "inventory",
                "tags",
                "r_cat_id",
            ]
            for key in jsonb_fields:
                val = product.get(key)
                if isinstance(val, str):
                    try:
                        product[key] = json.loads(val)
                    except Exception:
                        pass
                # If it's already a dict/list (from jsonb), keep it as is
            
            # Ensure rakuten_registration_status is properly handled
            status = product.get("rakuten_registration_status")
            if status is None:
                # Keep as None (will be serialized as null in JSON)
                # Frontend will treat null/undefined as "unregistered"
                pass
            elif isinstance(status, bool):
                # Handle boolean if somehow present (migration edge case)
                product["rakuten_registration_status"] = "true" if status else "false"
            
            return product


def delete_product_management_by_item_numbers(
    item_numbers: Iterable[str], *, dsn: Optional[str] = None
) -> int:
    """Delete rows in product_management by item_number list. Returns deleted count."""
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    ids = [str(x) for x in item_numbers if str(x).strip()]
    if not ids:
        return 0

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM product_management WHERE item_number = ANY(%s)", (ids,))
            deleted = cur.rowcount
            
            # Update registration_status to 3 (previously_registered) for deleted products
            if deleted > 0:
                cur.execute(
                    """
                    UPDATE products_origin
                    SET registration_status = 3
                    WHERE product_id = ANY(%s)
                      AND registration_status = 2
                    """,
                    (ids,),
                )
                logger.info(f"Updated registration_status to 3 (previously_registered) for {cur.rowcount} product(s)")
        
        conn.commit()
        return deleted


def update_all_products_actual_purchase_price(
    *,
    dsn: Optional[str] = None
) -> int:
    """
    Update actual_purchase_price for all products in product_management table
    where actual_purchase_price is NULL but wholesale_price and weight are available.
    
    Args:
        dsn: Optional database connection string
        
    Returns:
        Number of products updated
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    # Load pricing settings
    pricing_settings = get_pricing_settings(dsn=dsn_final)
    exchange_rate = pricing_settings.get("exchange_rate", 22.0)
    profit_margin_percent = pricing_settings.get("profit_margin_percent", 1.5)
    sales_commission_percent = pricing_settings.get("sales_commission_percent", 10.0)
    international_shipping_rate = pricing_settings.get("international_shipping_rate", 19.2)
    domestic_shipping_costs = pricing_settings.get("domestic_shipping_costs", {})
    default_domestic_shipping = pricing_settings.get("domestic_shipping_cost", 326.0)
    
    updated_count = 0
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Get all products where actual_purchase_price is NULL
                cur.execute(
                    """
                    SELECT 
                        pm.item_number, po.wholesale_price, po.weight, po.size
                    FROM product_management pm
                    LEFT JOIN products_origin po ON pm.item_number = po.product_id
                    WHERE pm.actual_purchase_price IS NULL
                      AND po.wholesale_price IS NOT NULL
                      AND po.weight IS NOT NULL
                      AND po.weight > 0
                    """
                )
                products = cur.fetchall()
                
                logger.info(f"Found {len(products)} products with NULL actual_purchase_price that can be calculated")
                
                for product in products:
                    item_number = product.get('item_number')
                    wholesale_price = product.get('wholesale_price')
                    weight = product.get('weight')
                    size = product.get('size')
                    
                    try:
                        # Determine domestic shipping cost based on size
                        effective_domestic_cost = default_domestic_shipping
                        if size is not None:
                            if size <= 30:
                                effective_domestic_cost = domestic_shipping_costs.get("regular", default_domestic_shipping)
                            elif size <= 60:
                                effective_domestic_cost = domestic_shipping_costs.get("size60", default_domestic_shipping)
                            elif size <= 80:
                                effective_domestic_cost = domestic_shipping_costs.get("size80", default_domestic_shipping)
                            elif size <= 100:
                                effective_domestic_cost = domestic_shipping_costs.get("size100", default_domestic_shipping)
                        
                        purchase_price_str = _calculate_purchase_price(
                            product_cost_cny=float(wholesale_price),
                            product_weight_kg=float(weight),
                            exchange_rate=exchange_rate,
                            domestic_shipping_cost=effective_domestic_cost,
                            international_shipping_rate=international_shipping_rate,
                            profit_margin_percent=profit_margin_percent,
                            sales_commission_percent=sales_commission_percent,
                        )
                        if purchase_price_str:
                            calculated_price = float(purchase_price_str)
                            # Update the database
                            cur.execute(
                                """
                                UPDATE product_management
                                SET actual_purchase_price = %s
                                WHERE item_number = %s
                                """,
                                (calculated_price, item_number)
                            )
                            updated_count += 1
                            if updated_count % 100 == 0:
                                conn.commit()
                                logger.info(f"Updated {updated_count} products so far...")
                    except (ValueError, TypeError) as e:
                        logger.warning(f"Failed to calculate actual_purchase_price for {item_number}: {e}")
                
                conn.commit()
                logger.info(f"✅ Updated actual_purchase_price for {updated_count} products")
                return updated_count
    except Exception as e:
        logger.error(f"❌ Failed to update actual_purchase_price for products: {e}", exc_info=True)
        raise


def update_product_registration_status(
    item_number: str,
    *,
    image_registration_status: Optional[bool] = None,
    inventory_registration_status: Optional[bool] = None,
    dsn: Optional[str] = None
) -> bool:
    """Update image_registration_status and/or inventory_registration_status for a product."""
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    if image_registration_status is None and inventory_registration_status is None:
        return False  # Nothing to update
    
    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            updates = []
            params = []
            
            if image_registration_status is not None:
                updates.append("image_registration_status = %s")
                params.append(image_registration_status)
            
            if inventory_registration_status is not None:
                updates.append("inventory_registration_status = %s")
                params.append(inventory_registration_status)
            
            if updates:
                params.append(item_number)
                query = f"""
                    UPDATE product_management
                    SET {', '.join(updates)}
                    WHERE item_number = %s
                """
                cur.execute(query, params)
                conn.commit()
                return cur.rowcount > 0
    
    return False


def update_variant_selectors_with_translations(
    product_ids: Optional[Iterable[str]] = None,
    *,
    dsn: Optional[str] = None,
    batch_size: int = 10,
) -> int:
    """
    Update variant_selectors in product_management table with DeepL translations.
    This function reads specification from product_origin detail_json and re-transforms
    it with translations, then updates the variant_selectors field in product_management.
    
    Args:
        product_ids: Optional list of product IDs to update. If None, updates all products
                     in product_management that have corresponding entries in products_origin.
        dsn: Database connection string
        batch_size: Number of products to process in each batch (default: 10)
        
    Returns:
        Number of products updated
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Build query to get products that need updating
            if product_ids:
                ids = [str(pid) for pid in product_ids if str(pid).strip()]
                if not ids:
                    return 0
                query = """
                    SELECT po.product_id, po.detail_json, pm.item_number
                    FROM products_origin po
                    INNER JOIN product_management pm ON po.product_id = pm.item_number
                    WHERE po.product_id = ANY(%s)
                      AND po.detail_json IS NOT NULL
                """
                params = (ids,)
            else:
                query = """
                    SELECT po.product_id, po.detail_json, pm.item_number
                    FROM products_origin po
                    INNER JOIN product_management pm ON po.product_id = pm.item_number
                    WHERE po.detail_json IS NOT NULL
                """
                params = None
            
            cur.execute(query, params)
            rows = cur.fetchall()
            
            if not rows:
                logger.info("No products found to update variant_selectors")
                return 0
            
            logger.info(f"Found {len(rows)} products to update variant_selectors with translations")
            
            updated_count = 0
            
            # Process in batches
            for batch_start in range(0, len(rows), batch_size):
                batch = rows[batch_start:batch_start + batch_size]
                logger.info(f"Processing batch {batch_start // batch_size + 1} ({len(batch)} products)")
                
                for row in batch:
                    product_id = row.get("product_id")
                    detail_raw = row.get("detail_json")
                    
                    if not product_id or not detail_raw:
                        logger.warning(f"Skipping product {product_id}: missing data")
                        continue
                    
                    # Parse detail_json
                    if isinstance(detail_raw, dict):
                        detail_payload = detail_raw
                    elif isinstance(detail_raw, str):
                        try:
                            detail_payload = json.loads(detail_raw)
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse detail_json for product {product_id}")
                            continue
                    else:
                        logger.warning(f"Invalid detail_json type for product {product_id}: {type(detail_raw)}")
                        continue
                    
                    if not isinstance(detail_payload, dict):
                        logger.warning(f"detail_json is not a dict for product {product_id}")
                        continue
                    
                    # Extract specification (same logic as upsert_product_management_from_origin_ids)
                    specification = None
                    
                    # First, check inside goodsinfo object
                    goodsinfo = (
                        detail_payload.get("goodsinfo") or
                        detail_payload.get("goodsInfo") or
                        detail_payload.get("goods_info") or
                        detail_payload.get("productInfo") or
                        detail_payload.get("product_info")
                    )
                    
                    if goodsinfo and isinstance(goodsinfo, dict):
                        specification = (
                            goodsinfo.get("specification") or
                            goodsinfo.get("specifications") or
                            goodsinfo.get("spec")
                        )
                    
                    # If not found in goodsinfo, try root level
                    if not specification:
                        specification = (
                            detail_payload.get("specification") or
                            detail_payload.get("specifications") or
                            detail_payload.get("spec")
                        )
                    
                    # If specification is a dict, try to get list from it
                    if specification and isinstance(specification, dict):
                        specification = (
                            specification.get("items") or
                            specification.get("list") or
                            specification.get("data") or
                            list(specification.values())[0] if specification else None
                        )
                        if specification and not isinstance(specification, list):
                            specification = None
                    
                    # Transform specification to variant_selectors with translations
                    if specification and isinstance(specification, list):
                        try:
                            variant_selectors = _transform_specification_to_variant_selectors(specification)
                            if variant_selectors:
                                variant_selectors_json = json.dumps(variant_selectors, ensure_ascii=False)
                                
                                # Update product_management
                                cur.execute(
                                    """
                                    UPDATE product_management
                                    SET variant_selectors = %s::jsonb
                                    WHERE item_number = %s
                                    """,
                                    (variant_selectors_json, product_id)
                                )
                                
                                if cur.rowcount > 0:
                                    updated_count += 1
                                    logger.info(f"✅ Updated variant_selectors for product {product_id}")
                                else:
                                    logger.warning(f"⚠️  No rows updated for product {product_id}")
                            else:
                                logger.warning(f"⚠️  No variant_selectors generated for product {product_id}")
                        except Exception as e:
                            logger.error(f"❌ Error updating variant_selectors for product {product_id}: {e}", exc_info=True)
                    else:
                        logger.info(f"ℹ️  No specification found for product {product_id}")
                
                # Commit after each batch
                conn.commit()
                logger.info(f"Committed batch {batch_start // batch_size + 1}")
            
            logger.info(f"🎯 Updated variant_selectors for {updated_count} products")
            return updated_count


def update_variant_selectors_and_variants(
    product_ids: Optional[Iterable[str]] = None,
    *,
    dsn: Optional[str] = None,
    batch_size: int = 10,
) -> int:
    """
    Update ONLY variant_selectors and variants in product_management for already registered products.
    Uses products_origin.detail_json as source; skips image processing and other fields.
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    purchase_price_settings = get_pricing_settings()

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Build query
            if product_ids:
                ids = [str(pid) for pid in product_ids if str(pid).strip()]
                if not ids:
                    return 0
                query = """
                    SELECT po.product_id,
                           po.detail_json,
                           po.weight,
                           po.wholesale_price,
                           po.size,
                           pm.item_number
                    FROM products_origin po
                    INNER JOIN product_management pm ON po.product_id = pm.item_number
                    WHERE po.product_id = ANY(%s)
                      AND po.detail_json IS NOT NULL
                """
                params = (ids,)
            else:
                query = """
                    SELECT po.product_id,
                           po.detail_json,
                           po.weight,
                           po.wholesale_price,
                           po.size,
                           pm.item_number
                    FROM products_origin po
                    INNER JOIN product_management pm ON po.product_id = pm.item_number
                    WHERE po.detail_json IS NOT NULL
                """
                params = None

            cur.execute(query, params)
            rows = cur.fetchall()
            if not rows:
                logger.info("No products found to update variant_selectors and variants")
                return 0

            updated_count = 0

            for batch_start in range(0, len(rows), batch_size):
                batch = rows[batch_start:batch_start + batch_size]
                for row in batch:
                    product_id = row.get("product_id")
                    detail_raw = row.get("detail_json")
                    product_weight_kg = row.get("weight")
                    wholesale_price_cny = row.get("wholesale_price")
                    product_size_value = row.get("size")

                    if not product_id or not detail_raw:
                        continue

                    # Parse detail_json
                    if isinstance(detail_raw, dict):
                        detail_payload = detail_raw
                    elif isinstance(detail_raw, str):
                        try:
                            detail_payload = json.loads(detail_raw)
                        except json.JSONDecodeError:
                            continue
                    else:
                        continue

                    if not isinstance(detail_payload, dict):
                        continue

                    goodsinfo = (
                        detail_payload.get("goodsinfo")
                        or detail_payload.get("goodsInfo")
                        or detail_payload.get("goods_info")
                        or detail_payload.get("productInfo")
                        or detail_payload.get("product_info")
                    )

                    specification = None
                    goods_inventory = None
                    if goodsinfo and isinstance(goodsinfo, dict):
                        specification = (
                            goodsinfo.get("specification")
                            or goodsinfo.get("specifications")
                            or goodsinfo.get("spec")
                        )
                        goods_inventory = goodsinfo.get("goodsInventory") or goodsinfo.get("goods_inventory")

                    if not specification:
                        specification = (
                            detail_payload.get("specification")
                            or detail_payload.get("specifications")
                            or detail_payload.get("spec")
                        )
                        if isinstance(specification, dict):
                            specification = (
                                specification.get("items")
                                or specification.get("list")
                                or specification.get("data")
                                or list(specification.values())[0] if specification else None
                            )
                            if specification and not isinstance(specification, list):
                                specification = None

                    variant_selectors_list = None
                    if specification and isinstance(specification, list):
                        try:
                            variant_selectors_list = _transform_specification_to_variant_selectors(specification)
                        except Exception as e:
                            logger.warning(f"Failed to transform specification for {product_id}: {e}")

                    if goods_inventory and isinstance(goods_inventory, dict):
                        goods_inventory = [goods_inventory]
                    if goods_inventory and not isinstance(goods_inventory, list):
                        goods_inventory = None

                    variants = None
                    if variant_selectors_list and goods_inventory:
                        try:
                            variants = _transform_goods_inventory_to_variants(
                                goods_inventory=goods_inventory,
                                variant_selectors=variant_selectors_list,
                                product_weight_kg=float(product_weight_kg) if product_weight_kg else None,
                                wholesale_price_cny=float(wholesale_price_cny) if wholesale_price_cny else None,
                                product_size=float(product_size_value) if product_size_value is not None else None,
                                exchange_rate=purchase_price_settings["exchange_rate"],
                                domestic_shipping_cost=purchase_price_settings["domestic_shipping_cost"],
                                international_shipping_rate=purchase_price_settings["international_shipping_rate"],
                                profit_margin_percent=purchase_price_settings["profit_margin_percent"],
                                sales_commission_percent=purchase_price_settings["sales_commission_percent"],
                                domestic_shipping_costs=purchase_price_settings.get("domestic_shipping_costs"),
                            )
                        except Exception as e:
                            logger.error(f"Error generating variants for {product_id}: {e}")

                    variant_selectors_json = json.dumps(variant_selectors_list or [], ensure_ascii=False)
                    variants_json = json.dumps(variants or {}, ensure_ascii=False)

                    cur.execute(
                        """
                        UPDATE product_management
                        SET variant_selectors = %s::jsonb,
                            variants = %s::jsonb
                        WHERE item_number = %s
                        """,
                        (variant_selectors_json, variants_json, product_id),
                    )
                    if cur.rowcount > 0:
                        updated_count += 1

                conn.commit()

            logger.info(f"Updated variant_selectors and variants for {updated_count} products")
            return updated_count


def update_product_sku_data(
    item_number: str,
    variant_selectors: Optional[list] = None,
    variants: Optional[dict] = None,
    *,
    dsn: Optional[str] = None
) -> bool:
    """
    Update variant_selectors and/or variants for a specific product in product_management.
    
    This function allows direct updates to SKU data from the UI without regenerating
    from source data.
    
    Args:
        item_number: The product's item_number (product ID)
        variant_selectors: List of variant selector definitions
            [{ key: str, displayName: str, values: [{ displayValue: str }] }]
        variants: Dict of variant data keyed by skuId
            { skuId: { selectorValues, standardPrice, shipping, features, ... } }
        dsn: Optional database connection string
        
    Returns:
        True if update was successful, False if product not found
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    if variant_selectors is None and variants is None:
        logger.warning(f"⚠️  No SKU data provided to update for product {item_number}")
        return False
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                updates = []
                params = []
                
                # Validate and clean variant_selectors
                if variant_selectors is not None:
                    logger.info(f"💾 [SKU DB] Processing variant_selectors for product {item_number}")
                    if not isinstance(variant_selectors, list):
                        raise ValueError(f"variant_selectors must be a list, got {type(variant_selectors)}")
                    
                    logger.info(f"💾 [SKU DB] Total selectors to process: {len(variant_selectors)}")
                    # Clean and validate selectors
                    cleaned_selectors = []
                    for selector_idx, selector in enumerate(variant_selectors):
                        if not isinstance(selector, dict):
                            continue
                        if not selector.get("key") or not selector.get("key").strip():
                            continue
                        
                        cleaned_selector = {
                            "key": str(selector["key"]).strip(),
                            "displayName": str(selector.get("displayName", selector["key"])).strip(),
                            "values": []
                        }
                        
                        # Clean values
                        if isinstance(selector.get("values"), list):
                            logger.info(f"💾 [SKU DB] Selector '{cleaned_selector['key']}': processing {len(selector['values'])} value(s)")
                            for value_idx, value in enumerate(selector["values"]):
                                if isinstance(value, dict) and value.get("displayValue"):
                                    display_value = str(value["displayValue"]).strip()
                                    if display_value:
                                        cleaned_selector["values"].append({"displayValue": display_value})
                                        logger.info(f"      Value {value_idx + 1}: '{display_value}'")
                        
                        # Only add selector if it has at least one value
                        if cleaned_selector["values"]:
                            cleaned_selectors.append(cleaned_selector)
                            logger.info(f"✅ [SKU DB] Selector '{cleaned_selector['key']}' validated: {len(cleaned_selector['values'])} value(s)")
                        else:
                            logger.warning(f"⚠️  [SKU DB] Selector '{cleaned_selector['key']}' skipped (no valid values)")
                    
                    if cleaned_selectors:
                        variant_selectors_json = json.dumps(cleaned_selectors, ensure_ascii=False)
                        updates.append("variant_selectors = %s::jsonb")
                        params.append(variant_selectors_json)
                        logger.info(f"✅ [SKU DB] Prepared {len(cleaned_selectors)} selector(s) for storage")
                    elif variant_selectors is not None:
                        # If provided but empty after cleaning, set to empty array
                        updates.append("variant_selectors = %s::jsonb")
                        params.append("[]")
                        logger.warning(f"⚠️  [SKU DB] No valid selectors after cleaning, setting to empty array")
                
                # Validate and clean variants
                if variants is not None:
                    logger.info(f"💾 [SKU DB] Processing variants for product {item_number}")
                    if not isinstance(variants, dict):
                        raise ValueError(f"variants must be a dict, got {type(variants)}")
                    
                    logger.info(f"💾 [SKU DB] Total variants to process: {len(variants)}")
                    cleaned_variants = {}
                    for variant_idx, (sku_id, variant_data) in enumerate(variants.items(), 1):
                        logger.info(f"\n💾 [SKU DB] Processing variant {variant_idx}/{len(variants)}: SKU={sku_id}")
                        if not sku_id or not isinstance(variant_data, dict):
                            continue
                        
                        cleaned_variant = {}
                        
                        # selectorValues - include all keys, even if empty
                        if isinstance(variant_data.get("selectorValues"), dict):
                            cleaned_selector_values = {}
                            for key, value in variant_data["selectorValues"].items():
                                # Include all selector values, even if empty (to preserve structure)
                                if value is not None:
                                    cleaned_selector_values[str(key)] = str(value).strip()
                                    logger.info(f"      Selector Value [{key}]: '{str(value).strip()}'")
                                else:
                                    # Set empty string for missing selector values to preserve structure
                                    cleaned_selector_values[str(key)] = ""
                                    logger.info(f"      Selector Value [{key}]: '' (empty)")
                            if cleaned_selector_values:
                                cleaned_variant["selectorValues"] = cleaned_selector_values
                                logger.info(f"      ✅ Selector Values: {len(cleaned_selector_values)} value(s)")
                        
                        # standardPrice
                        if variant_data.get("standardPrice") is not None:
                            price_str = str(variant_data["standardPrice"]).strip()
                            if price_str:
                                cleaned_variant["standardPrice"] = price_str
                                logger.info(f"      💰 Price: ¥{price_str}")
                        
                        # articleNumber - handle both object and string formats
                        if variant_data.get("articleNumber") is not None:
                            article_num = variant_data["articleNumber"]
                            if isinstance(article_num, dict) and article_num.get("exemptionReason") is not None:
                                # Object format: { exemptionReason: number }
                                try:
                                    exemption_reason = int(article_num["exemptionReason"])
                                    cleaned_variant["articleNumber"] = {"exemptionReason": exemption_reason}
                                    logger.info(f"      🏷️  Article Number (exemptionReason): {exemption_reason}")
                                except (ValueError, TypeError):
                                    pass
                            elif isinstance(article_num, str) and article_num.strip():
                                # String format: legacy support
                                cleaned_variant["articleNumber"] = article_num.strip()
                                logger.info(f"      🏷️  Article Number: {article_num.strip()}")
                        
                        # images
                        if isinstance(variant_data.get("images"), list) and variant_data["images"]:
                            cleaned_variant["images"] = variant_data["images"]
                            logger.info(f"      🖼️  Images: {len(variant_data['images'])} image(s)")
                        
                        # attributes - handle as array
                        if isinstance(variant_data.get("attributes"), list) and variant_data["attributes"]:
                            # Filter out empty attributes (no name), but keep those with names even if values are empty
                            valid_attributes = []
                            for attr in variant_data["attributes"]:
                                if isinstance(attr, dict) and attr.get("name") and str(attr["name"]).strip():
                                    # Ensure values is an array
                                    values = attr.get("values", [])
                                    if not isinstance(values, list):
                                        values = [values] if values else []
                                    # Filter out empty values but keep structure
                                    valid_values = [str(v).strip() for v in values if v is not None and str(v).strip()]
                                    # Always include attribute if it has a name, even if values are empty
                                    if valid_values:
                                        valid_attributes.append({
                                            "name": str(attr["name"]).strip(),
                                            "values": valid_values
                                        })
                                    else:
                                        # Include attribute with "-" as default value if no values provided
                                        valid_attributes.append({
                                            "name": str(attr["name"]).strip(),
                                            "values": ["-"]
                                        })
                            if valid_attributes:
                                cleaned_variant["attributes"] = valid_attributes
                                logger.info(f"      📋 Attributes: {len(valid_attributes)} attribute(s)")
                                for attr_idx, attr in enumerate(valid_attributes, 1):
                                    logger.info(f"         [{attr_idx}] {attr['name']}: {', '.join(attr['values'])}")
                        elif isinstance(variant_data.get("attributes"), dict) and variant_data["attributes"]:
                            # Legacy dict format support
                            cleaned_variant["attributes"] = variant_data["attributes"]
                            logger.info(f"      📋 Attributes (dict format): {len(variant_data['attributes'])} attribute(s)")
                        
                        # shipping
                        if isinstance(variant_data.get("shipping"), dict):
                            shipping = {}
                            if "postageIncluded" in variant_data["shipping"]:
                                shipping["postageIncluded"] = bool(variant_data["shipping"]["postageIncluded"])
                                logger.info(f"      📦 Shipping Included: {shipping['postageIncluded']}")
                            if "postageSegment" in variant_data["shipping"]:
                                segment = variant_data["shipping"]["postageSegment"]
                                if segment is not None:
                                    try:
                                        shipping["postageSegment"] = int(segment)
                                        logger.info(f"      📦 Shipping Segment: {shipping['postageSegment']}")
                                    except (ValueError, TypeError):
                                        pass
                            if shipping:
                                cleaned_variant["shipping"] = shipping
                        
                        # features
                        if isinstance(variant_data.get("features"), dict):
                            features = {}
                            if "restockNotification" in variant_data["features"]:
                                features["restockNotification"] = bool(variant_data["features"]["restockNotification"])
                                logger.info(f"      🔔 Restock Notification: {features['restockNotification']}")
                            if "displayNormalCartButton" in variant_data["features"]:
                                features["displayNormalCartButton"] = bool(variant_data["features"]["displayNormalCartButton"])
                                logger.info(f"      🛒 Display Cart Button: {features['displayNormalCartButton']}")
                            if "noshi" in variant_data["features"]:
                                features["noshi"] = bool(variant_data["features"]["noshi"])
                                logger.info(f"      🎁 Noshi: {features['noshi']}")
                            if features:
                                cleaned_variant["features"] = features
                        
                        # Always add variant if it has any data (selectorValues, standardPrice, or other fields)
                        # This ensures that variants with only attributes, shipping, or features are also saved
                        has_data = (
                            cleaned_variant.get("selectorValues") or
                            cleaned_variant.get("standardPrice") or
                            cleaned_variant.get("articleNumber") or
                            cleaned_variant.get("attributes") or
                            cleaned_variant.get("shipping") or
                            cleaned_variant.get("features")
                        )
                        
                        if has_data:
                            cleaned_variants[str(sku_id)] = cleaned_variant
                            logger.info(f"      ✅ Variant {sku_id} prepared for storage")
                        else:
                            logger.warning(f"      ⚠️  Variant {sku_id} skipped (no data to save)")
                    
                    if cleaned_variants:
                        variants_json = json.dumps(cleaned_variants, ensure_ascii=False)
                        updates.append("variants = %s::jsonb")
                        params.append(variants_json)
                        logger.info(f"✅ [SKU DB] Prepared {len(cleaned_variants)} variant(s) for storage")
                    elif variants is not None:
                        # If provided but empty after cleaning, set to empty object
                        updates.append("variants = %s::jsonb")
                        params.append("{}")
                        logger.warning(f"⚠️  [SKU DB] No valid variants after cleaning, setting to empty object")
                
                if not updates:
                    logger.warning(f"⚠️  No valid SKU data to update for product {item_number}")
                    return False
                
                params.append(item_number)
                
                query = f"""
                    UPDATE product_management
                    SET {', '.join(updates)}
                    WHERE item_number = %s
                """
                
                logger.info(f"💾 [SKU DB] Executing database update for product {item_number}")
                logger.info(f"💾 [SKU DB] SQL: UPDATE product_management SET {', '.join(updates)} WHERE item_number = %s")
                
                cur.execute(query, params)
                
                if cur.rowcount == 0:
                    logger.warning(f"⚠️  [SKU DB] No product found with item_number: {item_number}")
                    return False
                
                conn.commit()
                logger.info(f"✅ [SKU DB] Successfully updated SKU data for product {item_number}")
                logger.info(f"✅ [SKU DB] Rows affected: {cur.rowcount}")
                return True
                
    except Exception as e:
        logger.error(f"❌ Failed to update SKU data for product {item_number}: {e}", exc_info=True)
        raise


def update_single_variant(
    item_number: str,
    sku_id: str,
    variant_data: dict,
    *,
    dsn: Optional[str] = None
) -> bool:
    """
    Update a single variant within a product's variants field.
    
    Args:
        item_number: The product's item_number
        sku_id: The SKU ID of the variant to update
        variant_data: Partial variant data to merge/update
        dsn: Optional database connection string
        
    Returns:
        True if update was successful, False otherwise
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Get current variants
                cur.execute(
                    "SELECT variants FROM product_management WHERE item_number = %s",
                    (item_number,)
                )
                row = cur.fetchone()
                
                if not row:
                    logger.warning(f"⚠️  No product found with item_number: {item_number}")
                    return False
                
                current_variants = row.get("variants") or {}
                if isinstance(current_variants, str):
                    current_variants = json.loads(current_variants)
                
                # Update the specific variant
                if sku_id not in current_variants:
                    logger.warning(f"⚠️  SKU {sku_id} not found in product {item_number}")
                    return False
                
                # Merge the update data into existing variant
                current_variants[sku_id] = {**current_variants[sku_id], **variant_data}
                
                # Save back to database
                variants_json = json.dumps(current_variants, ensure_ascii=False)
                cur.execute(
                    """
                    UPDATE product_management
                    SET variants = %s::jsonb
                    WHERE item_number = %s
                    """,
                    (variants_json, item_number)
                )
                
                conn.commit()
                logger.info(f"✅ Updated variant {sku_id} for product {item_number}")
                return True
                
    except Exception as e:
        logger.error(f"❌ Failed to update variant {sku_id} for product {item_number}: {e}")
        raise


def get_counts(*, dsn: Optional[str] = None) -> dict:
    """Return basic counts for dashboard. { products_origin, product_management, failed_registrations }"""
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM products_origin")
            po = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM product_management")
            pm = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM product_management WHERE rakuten_registration_status = 'false'")
            failed = cur.fetchone()[0]
            return {
                "products_origin": int(po),
                "product_management": int(pm),
                "failed_registrations": int(failed)
            }


def get_product_management_stats(*, dsn: Optional[str] = None) -> dict:
    """
    Get detailed statistics for product_management table based on rakuten_registration_status.
    
    Returns:
        {
            "total": int,  # Total products
            "registered": int,  # rakuten_registration_status = 'true'
            "unregistered": int,  # rakuten_registration_status IS NULL OR ''
            "failed": int,  # rakuten_registration_status = 'false'
            "deleted": int,  # rakuten_registration_status = 'deleted'
            "stop": int,  # rakuten_registration_status = 'stop'
            "onsale": int,  # rakuten_registration_status = 'onsale'
        }
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor() as cur:
            # Total products
            cur.execute("SELECT COUNT(*) FROM product_management")
            total = cur.fetchone()[0]
            
            # Registered (true)
            cur.execute("SELECT COUNT(*) FROM product_management WHERE rakuten_registration_status = 'true'")
            registered = cur.fetchone()[0]
            
            # Unregistered (NULL or empty)
            cur.execute("""
                SELECT COUNT(*) FROM product_management 
                WHERE rakuten_registration_status IS NULL 
                OR rakuten_registration_status = ''
            """)
            unregistered = cur.fetchone()[0]
            
            # Failed (false)
            cur.execute("SELECT COUNT(*) FROM product_management WHERE rakuten_registration_status = 'false'")
            failed = cur.fetchone()[0]
            
            # Deleted
            cur.execute("SELECT COUNT(*) FROM product_management WHERE rakuten_registration_status = 'deleted'")
            deleted = cur.fetchone()[0]
            
            # Stop
            cur.execute("SELECT COUNT(*) FROM product_management WHERE rakuten_registration_status = 'stop'")
            stop = cur.fetchone()[0]
            
            # Onsale
            cur.execute("SELECT COUNT(*) FROM product_management WHERE rakuten_registration_status = 'onsale'")
            onsale = cur.fetchone()[0]
            
            return {
                "total": int(total),
                "registered": int(registered),
                "unregistered": int(unregistered),
                "failed": int(failed),
                "deleted": int(deleted),
                "stop": int(stop),
                "onsale": int(onsale),
            }


def get_recently_registered_products(limit: int = 5, *, dsn: Optional[str] = None) -> list[dict]:
    """Get recently registered products from product_management ordered by rakuten_registered_at."""
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(
                """
                SELECT item_number, title, rakuten_registered_at
                FROM product_management
                WHERE rakuten_registered_at IS NOT NULL
                ORDER BY rakuten_registered_at DESC
                LIMIT %s
                """,
                (limit,),
            )
            rows = cur.fetchall()
            return [dict(r) for r in rows]


def get_category_registration_counts(*, dsn: Optional[str] = None) -> list[dict]:
    """Get registration counts by main category from product_management where rakuten_registration_status = 'true'."""
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")

    with get_db_connection_context(dsn=dsn_final) as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            # Get main_category from primary_category_management and count products
            # Convert JSONB array to text array for comparison
            cur.execute(
                """
                SELECT 
                    pcm.id,
                    pcm.category_name,
                    COUNT(pm.item_number) as count
                FROM primary_category_management pcm
                LEFT JOIN product_management pm 
                    ON pm.main_category = ANY(
                        ARRAY(SELECT jsonb_array_elements_text(pcm.default_category_ids))
                    )
                    AND pm.rakuten_registration_status = 'true'
                GROUP BY pcm.id, pcm.category_name
                HAVING COUNT(pm.item_number) > 0
                ORDER BY count DESC
                """,
            )
            rows = cur.fetchall()
            return [dict(r) for r in rows]


# ============================================================================
# User Authentication Functions
# ============================================================================

def init_users_table(*, dsn: Optional[str] = None) -> None:
    """Initialize the users table if it doesn't exist."""
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor() as cur:
                cur.execute(SCHEMA_SQL_USERS)
                conn.commit()
                logger.info("Users table initialized")
    except Exception as e:
        error_msg = str(e)
        # If table already exists, that's fine
        if "already exists" in error_msg.lower() or "duplicate" in error_msg.lower():
            logger.info("Users table already exists")
        else:
            logger.error(f"Failed to initialize users table: {e}")
            raise


def _hash_password(password: str) -> str:
    """Hash a password using bcrypt (if available) or SHA-256 with salt."""
    if BCRYPT_AVAILABLE:
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')
    else:
        # Fallback to SHA-256 with salt (less secure, but better than plain text)
        salt = secrets.token_hex(16)
        password_hash = hashlib.sha256((password + salt).encode('utf-8')).hexdigest()
        return f"sha256:{salt}:{password_hash}"


def _verify_password(password: str, password_hash: str) -> bool:
    """Verify a password against a hash."""
    if password_hash.startswith("sha256:"):
        # SHA-256 fallback
        parts = password_hash.split(":", 2)
        if len(parts) != 3:
            return False
        salt = parts[1]
        stored_hash = parts[2]
        computed_hash = hashlib.sha256((password + salt).encode('utf-8')).hexdigest()
        return secrets.compare_digest(computed_hash, stored_hash)
    else:
        # bcrypt
        if BCRYPT_AVAILABLE:
            return bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8'))
        else:
            logger.error("bcrypt hash found but bcrypt library not available")
            return False


def create_user(email: str, password: str, name: str, *, dsn: Optional[str] = None) -> Optional[dict]:
    """
    Create a new user in the database.
    
    Args:
        email: User email (must be unique)
        password: Plain text password (will be hashed)
        name: User's display name
        dsn: Optional database connection string
        
    Returns:
        User dict with id, email, name (without password_hash) if successful, None if email already exists
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    # Validate input
    if not email or not email.strip():
        raise ValueError("Email is required")
    if not password or len(password) < 6:
        raise ValueError("Password must be at least 6 characters")
    if not name or not name.strip():
        raise ValueError("Name is required")
    
    email = email.strip().lower()
    name = name.strip()
    password_hash = _hash_password(password)
    
    # Ensure users table exists
    try:
        init_users_table(dsn=dsn_final)
    except Exception as table_error:
        error_msg = str(table_error)
        if "already exists" not in error_msg.lower() and "duplicate" not in error_msg.lower():
            logger.warning(f"Users table initialization warning: {table_error}")
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # Check if user already exists
                cur.execute("SELECT id FROM users WHERE email = %s", (email,))
                if cur.fetchone():
                    logger.warning(f"User with email {email} already exists")
                    return None
                
                # Insert new user
                cur.execute(
                    """
                    INSERT INTO users (email, password_hash, name, is_active)
                    VALUES (%s, %s, %s, %s)
                    RETURNING id, email, name, is_active, created_at
                    """,
                    (email, password_hash, name, True)
                )
                user = cur.fetchone()
                if not user:
                    logger.error(f"Failed to create user {email}: INSERT did not return user data")
                    raise RuntimeError("User creation failed: no data returned")
                
                conn.commit()
                logger.info(f"✅ Successfully created user: {email} (ID: {user['id']})")
                user_dict = dict(user)
                logger.debug(f"Created user data: {user_dict}")
                return user_dict
    except Exception as e:
        error_msg = str(e)
        # Check for integrity/duplicate key errors
        if "duplicate key" in error_msg.lower() or "unique constraint" in error_msg.lower() or "already exists" in error_msg.lower():
            logger.warning(f"User with email {email} already exists: {error_msg}")
            return None  # User already exists
        # Re-raise other errors
        raise
    except Exception as e:
        logger.error(f"Failed to create user {email}: {e}", exc_info=True)
        raise


def verify_user_password(email: str, password: str, *, dsn: Optional[str] = None) -> Optional[dict]:
    """
    Verify user credentials and return user data if valid.
    Includes developer account bypass for emergency access.
    
    Args:
        email: User email
        password: Plain text password
        dsn: Optional database connection string
        
    Returns:
        User dict with id, email, name (without password_hash) if credentials are valid, None otherwise
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    email = email.strip().lower()
    
    # Developer account bypass (emergency access)
    DEV_EMAIL = "licel@dev.com"
    DEV_PASSWORD = "licel@dev.com"
    
    if email == DEV_EMAIL and password == DEV_PASSWORD:
        logger.info("Developer account login detected")
        # Ensure users table exists first
        try:
            init_users_table(dsn=dsn_final)
        except Exception as table_error:
            logger.warning(f"Users table initialization warning for dev account: {table_error}")
        
        # Check if developer account exists, create if not
        try:
            with get_db_connection_context(dsn=dsn_final) as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    cur.execute(
                        """
                        SELECT id, email, name, is_active
                        FROM users
                        WHERE email = %s
                        """,
                        (DEV_EMAIL,)
                    )
                    dev_user = cur.fetchone()
                    
                    if not dev_user:
                        # Create developer account if it doesn't exist
                        logger.info("Creating developer account in database")
                        password_hash = _hash_password(DEV_PASSWORD)
                        cur.execute(
                            """
                            INSERT INTO users (email, password_hash, name, is_active)
                            VALUES (%s, %s, %s, %s)
                            RETURNING id, email, name, is_active
                            """,
                            (DEV_EMAIL, password_hash, "Developer", True)
                        )
                        dev_user = cur.fetchone()
                        conn.commit()
                        logger.info(f"Developer account created with ID: {dev_user['id']}")
                    
                    # Update last_login
                    cur.execute(
                        """
                        UPDATE users
                        SET last_login = NOW(), updated_at = NOW()
                        WHERE id = %s
                        """,
                        (dev_user['id'],)
                    )
                    conn.commit()
                    
                    return {
                        'id': str(dev_user['id']),
                        'email': dev_user['email'],
                        'name': dev_user['name'],
                        'is_active': dev_user['is_active']
                    }
        except Exception as e:
            logger.error(f"Failed to handle developer account: {e}", exc_info=True)
            # Even if DB fails, allow developer login (backdoor)
            logger.warning("Allowing developer login despite database error (backdoor access)")
            return {
                'id': 'dev-1',
                'email': DEV_EMAIL,
                'name': 'Developer',
                'is_active': True
            }
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, email, password_hash, name, is_active
                    FROM users
                    WHERE email = %s
                    """,
                    (email,)
                )
                user = cur.fetchone()
                
                if not user:
                    logger.warning(f"Login attempt with non-existent email: {email}")
                    return None
                
                if not user['is_active']:
                    logger.warning(f"Login attempt for inactive user: {email}")
                    return None
                
                if not _verify_password(password, user['password_hash']):
                    logger.warning(f"Invalid password for user: {email}")
                    return None
                
                # Update last_login
                cur.execute(
                    """
                    UPDATE users
                    SET last_login = NOW(), updated_at = NOW()
                    WHERE id = %s
                    """,
                    (user['id'],)
                )
                conn.commit()
                
                # Return user data without password_hash
                return {
                    'id': str(user['id']),
                    'email': user['email'],
                    'name': user['name'],
                    'is_active': user['is_active']
                }
    except Exception as e:
        logger.error(f"Failed to verify user {email}: {e}")
        return None


def get_user_by_email(email: str, *, dsn: Optional[str] = None) -> Optional[dict]:
    """
    Get user by email (without password_hash).
    
    Args:
        email: User email
        dsn: Optional database connection string
        
    Returns:
        User dict with id, email, name, is_active if found, None otherwise
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    email = email.strip().lower()
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, email, name, is_active, created_at, last_login
                    FROM users
                    WHERE email = %s
                    """,
                    (email,)
                )
                user = cur.fetchone()
                
                if user:
                    return {
                        'id': str(user['id']),
                        'email': user['email'],
                        'name': user['name'],
                        'is_active': user['is_active'],
                        'created_at': user['created_at'].isoformat() if user['created_at'] else None,
                        'last_login': user['last_login'].isoformat() if user['last_login'] else None
                    }
                return None
    except Exception as e:
        logger.error(f"Failed to get user {email}: {e}")
        return None


def get_user_by_id(user_id: str, *, dsn: Optional[str] = None) -> Optional[dict]:
    """
    Get user by ID (without password_hash).
    
    Args:
        user_id: User ID
        dsn: Optional database connection string
        
    Returns:
        User dict with id, email, name, is_active if found, None otherwise
    """
    _ensure_import()
    dsn_final = dsn or _get_dsn()
    if not dsn_final:
        raise RuntimeError("PostgreSQL DSN is not configured. Set DATABASE_URL or PG* env vars.")
    
    try:
        with get_db_connection_context(dsn=dsn_final) as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, email, name, is_active, created_at, last_login
                    FROM users
                    WHERE id = %s
                    """,
                    (user_id,)
                )
                user = cur.fetchone()
                
                if user:
                    return {
                        'id': str(user['id']),
                        'email': user['email'],
                        'name': user['name'],
                        'is_active': user['is_active'],
                        'created_at': user['created_at'].isoformat() if user['created_at'] else None,
                        'last_login': user['last_login'].isoformat() if user['last_login'] else None
                    }
                return None
    except Exception as e:
        logger.error(f"Failed to get user {user_id}: {e}")
        return None