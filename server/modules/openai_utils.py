"""
OpenAI utilities for generating product titles and marketing content for Rakuten products.

This module provides comprehensive content generation for Rakuten marketplace:
- Product titles (90-100 chars, noun-only, SEO optimized)
- Product descriptions (600+ chars)
- Sales descriptions (600+ chars)
- Taglines (50-60 chars)

All content is optimized for Rakuten guidelines compliance and SEO.
"""

import os
import logging
import re
import time
from typing import Optional, Dict, Any, List, Pattern
from dataclasses import dataclass

try:
    from openai import OpenAI
    from openai import APIError, RateLimitError, APIConnectionError
except ImportError:
    OpenAI = None
    APIError = Exception
    RateLimitError = Exception
    APIConnectionError = Exception

# Optional .env loading
try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None
else:
    try:
        load_dotenv()
    except Exception:
        pass

logger = logging.getLogger(__name__)

# ============================================================================
# CONSTANTS - All magic numbers and configuration values
# ============================================================================

# Title generation constants
TITLE_MIN_LENGTH = 90
TITLE_MAX_LENGTH = 100
TITLE_TEMPERATURE = 0.2  # Lower for more consistent, noun-only titles
TITLE_MAX_TOKENS = 200

# Description generation constants
DESCRIPTION_MIN_LENGTH = 500  # Minimum length (simplified - no strict enforcement)
DESCRIPTION_MAX_LENGTH = 10000  # No maximum limit
DESCRIPTION_TEMPERATURE = 0.7
DESCRIPTION_MAX_TOKENS = 2000

# Tagline constants (updated for better SEO and purchase rate)
TAGLINE_MIN_LENGTH = 60
TAGLINE_MAX_LENGTH = 70

# Retry and timeout constants
MAX_RETRIES = 1
RETRY_DELAY_SECONDS = 1
API_TIMEOUT_SECONDS = 60.0
MAX_KEYWORDS_TO_PAD = 5

# OpenAI API configuration
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL", "https://api.openai.com/v1")

# Initialize OpenAI client
_openai_client: Optional[Any] = None

# ============================================================================
# PRE-COMPILED REGEX PATTERNS for performance optimization
# ============================================================================

# URL and image detection patterns
URL_PATTERN = re.compile(r'https?://|www\.|\.(jpg|jpeg|png|gif)', re.IGNORECASE)
IMAGE_EXT_PATTERN = re.compile(r'\.(jpg|jpeg|png|gif|webp|bmp|svg)', re.IGNORECASE)
DOMAIN_PATTERN = re.compile(r'\.(com|net|org|co|jp|cn|io)', re.IGNORECASE)

# Title cleaning patterns
DECORATIVE_PATTERNS = [
    re.compile(r'【生成タイトル】', re.IGNORECASE),
    re.compile(r'【タイトル】', re.IGNORECASE),
    re.compile(r'【商品タイトル】', re.IGNORECASE),
    re.compile(r'生成タイトル[：:]?\s*', re.IGNORECASE),
    re.compile(r'タイトル[：:]?\s*', re.IGNORECASE),
    re.compile(r'商品タイトル[：:]?\s*', re.IGNORECASE),
    re.compile(r'^【.*?】\s*', re.IGNORECASE),
    re.compile(r'^\[.*?\]\s*', re.IGNORECASE),
    re.compile(r'^「.*?」\s*', re.IGNORECASE),
    re.compile(r'^\s*【', re.IGNORECASE),
    re.compile(r'^\s*\[', re.IGNORECASE),
    re.compile(r'^\s*「', re.IGNORECASE),
    re.compile(r'【', re.IGNORECASE),
    re.compile(r'】', re.IGNORECASE),
    re.compile(r'\[', re.IGNORECASE),
    re.compile(r'\]', re.IGNORECASE),
]

# Year and price patterns (updated to support current and future years)
YEAR_PATTERN = re.compile(r'20\d{2}年')
YEAR_NUMBER_PATTERN = re.compile(r'20\d{2}')
PRICE_YEN_PATTERN = re.compile(r'\d+円')
PRICE_MAN_YEN_PATTERN = re.compile(r'\d+万円')
PRICE_KEYWORD_PATTERN = re.compile(r'価格|金額')

# Color variation number patterns
COLOR_CODE_PATTERN_1 = re.compile(r'\b[A-Z]\d{3,4}\b')
COLOR_CODE_PATTERN_2 = re.compile(r'\b[89]\d{3}\b')
COLOR_CODE_PATTERN_3 = re.compile(r'\b\d{3,4}[A-Z]\b')

# Text cleaning patterns
QUOTE_PATTERN = re.compile(r'^["\'\s]+|["\'\s]+$')
WHITESPACE_PATTERN = re.compile(r'\s+')
BR_TAG_PATTERN = re.compile(r'(<br>\s*)+')
# Add <br> after Japanese/English sentence-ending punctuation (。！？!? and .)
# This ensures each sentence ends with <br> for proper line breaks on Rakuten pages.
SENTENCE_END_PATTERN = re.compile(r'([。！？!?\.])(?!<br>)')

# Delivery message constants and patterns
DELIVERY_MESSAGE = "<br>ご到着まで、6～9営業日ほどかかります。"
DELIVERY_PATTERNS = [
    re.compile(r'到着までに[^。]*6[^。]*9[^。]*営業[^。]*', re.IGNORECASE),
    re.compile(r'<>着までに[^。]*6[^。]*9[^。]*営業[^。]*', re.IGNORECASE),
    re.compile(r'到着まで[^。]*6[^。]*9[^。]*営業[^。]*', re.IGNORECASE),
    re.compile(r'ご到着まで[^。]*6[^。]*9[^。]*営業[^。]*', re.IGNORECASE),
    # Catch malformed delivery messages - more comprehensive patterns
    re.compile(r'ご着まで[^。]*6[^。]*9[^。]*営業[^。]*', re.IGNORECASE),  # Missing "到"
    re.compile(r'[^。]*6[^。]*~[^。]*9[^。]*営業[^。]*', re.IGNORECASE),  # Using ~ instead of ～
    re.compile(r'[^。]*6[^。]*9[^。]*営業[^。]*ほど[^。]*', re.IGNORECASE),  # Missing "日"
    re.compile(r'[^。]*6[^。]*9[^。]*営業日[^。]*', re.IGNORECASE),  # Any variation with 6-9営業日
    re.compile(r'[^。]*ご着[^。]*6[^。]*~[^。]*9[^。]*営業[^。]*', re.IGNORECASE),  # Combined malformed
    re.compile(r'[^。]*6[^。]*~[^。]*9[^。]*営業[^。]*ほど[^。]*', re.IGNORECASE),  # Combined malformed
    re.compile(r'[^。]*ご到着[^。]*6[^。]*~[^。]*9[^。]*営業[^。]*', re.IGNORECASE),  # Using ~
    re.compile(r'[^。]*6[^。]*9[^。]*営業[^。]*かかります[^。]*', re.IGNORECASE),  # Any variation with かかります
]



def ensure_br_tags_global(text: str) -> str:
    """
    Ensure that each sentence in the given text ends with a <br> tag.
    Sentences are detected by SENTENCE_END_PATTERN (。！？!? and .).
    Consecutive <br> tags are normalized to a single <br>.
    
    Args:
        text: Input text to process
        
    Returns:
        Text with <br> tags properly inserted after sentences
    """
    if not text:
        return text
    # Add <br> after sentence-ending punctuation if not already present
    text = SENTENCE_END_PATTERN.sub(r'\1<br>', text)
    # Normalize multiple consecutive <br> tags
    text = BR_TAG_PATTERN.sub('<br>', text)
    return text.strip()


def add_delivery_message(text: str) -> str:
    """
    Remove any existing delivery messages and add the correct delivery message at the end.
    
    This function ensures that:
    1. Any existing delivery messages are removed to prevent duplicates
    2. The correct delivery message is added at the end
    3. Proper formatting is maintained
    
    Args:
        text: Input text that may contain delivery messages
        
    Returns:
        Text with delivery message properly added at the end
        
    Examples:
        >>> add_delivery_message("商品説明文です。")
        '商品説明文です。<br>ご到着まで、6～9営業日ほどかかります。'
        >>> add_delivery_message("商品説明文です。到着までに、6～9営業日ほどかかります。")
        '商品説明文です。<br>ご到着まで、6～9営業日ほどかかります。'
    """
    if not text:
        return DELIVERY_MESSAGE.lstrip('<br>')
    
    # Remove any existing delivery messages using pre-compiled patterns
    cleaned_text = text
    for pattern in DELIVERY_PATTERNS:
        cleaned_text = pattern.sub('', cleaned_text)
    
    # Remove trailing <br> and whitespace
    cleaned_text = cleaned_text.rstrip('<br>').rstrip()
    
    # Remove orphaned "ご。" patterns that may be left after delivery message removal
    # This can happen if the delivery message pattern matches part of the text incorrectly
    cleaned_text = re.sub(r'ご。', '', cleaned_text)
    cleaned_text = re.sub(r'ご\s*。', '', cleaned_text)
    cleaned_text = re.sub(r'。\s*ご\s*。', '。', cleaned_text)
    # Remove any remaining orphaned punctuation patterns
    cleaned_text = re.sub(r'([^。\s])ご\s*。', r'\1。', cleaned_text)
    
    # Clean up any double periods or malformed punctuation
    cleaned_text = re.sub(r'。\s*。+', '。', cleaned_text)
    cleaned_text = cleaned_text.rstrip('<br>').rstrip()
    
    # Add correct delivery message
    return f"{cleaned_text}{DELIVERY_MESSAGE}"



# Japanese character validation
JAPANESE_CHAR_PATTERN = re.compile(r'[^\u3040-\u309F\u30A0-\u30FF\u4E00-\u9FAF\w\s\-/()（）【】]')

# Excluded key patterns for detail extraction
EXCLUDED_KEY_PATTERNS = [
    re.compile(r'image', re.IGNORECASE),
    re.compile(r'img', re.IGNORECASE),
    re.compile(r'url', re.IGNORECASE),
    re.compile(r'pic', re.IGNORECASE),
    re.compile(r'photo', re.IGNORECASE),
    re.compile(r'画像', re.IGNORECASE),
    re.compile(r'写真', re.IGNORECASE),
    re.compile(r'price', re.IGNORECASE),
    re.compile(r'価格', re.IGNORECASE),
    re.compile(r'金額', re.IGNORECASE),
    re.compile(r'code', re.IGNORECASE),
    re.compile(r'コード', re.IGNORECASE),
    re.compile(r'番号$', re.IGNORECASE),
]


def truncate_at_word_boundary(text: str, max_length: int, min_length: Optional[int] = None) -> str:
    """
    Truncate text at word/keyword boundaries to prevent meaningless characters at the end.
    Works with both Japanese (spaces, punctuation) and English text.
    
    Args:
        text: Text to truncate
        max_length: Maximum character length
        min_length: Minimum character length (optional)
        
    Returns:
        Truncated text ending at a word boundary
        
    Examples:
        >>> truncate_at_word_boundary("これはテストの商品タイトルです", 10)
        'これはテストの商品'
        >>> truncate_at_word_boundary("This is a test product title", 15)
        'This is a test'
    """
    if not text or len(text) <= max_length:
        return text
    
    # Try to find a good break point
    # Japanese word boundaries: spaces, punctuation (。、！？), katakana/hiragana boundaries
    # English word boundaries: spaces, punctuation
    
    # First, try to truncate at max_length and find the last word boundary
    truncated = text[:max_length]
    
    # Find word boundaries (spaces, punctuation, CJK character boundaries)
    # Priority order: space > punctuation > character boundary
    
    # 1. Try to find last space (most common boundary)
    last_space = truncated.rfind(' ')
    if last_space > 0 and (min_length is None or last_space >= min_length):
        return truncated[:last_space].strip()
    
    # 2. Try to find last punctuation (Japanese: 。、！？, English: .!?,)
    punctuation_chars = '。、！？.!?,'
    last_punct_idx = -1
    for char in punctuation_chars:
        idx = truncated.rfind(char)
        if idx > last_punct_idx:
            last_punct_idx = idx
    
    if last_punct_idx > 0 and (min_length is None or last_punct_idx + 1 >= min_length):
        return truncated[:last_punct_idx + 1].strip()
    
    # 3. For Japanese: try to find last CJK character boundary (before punctuation/spaces)
    # Look for pattern: CJK char followed by non-CJK char or space
    import unicodedata
    for i in range(len(truncated) - 1, max(0, max_length - 10), -1):
        if i < min_length or min_length is None:
            break
        char = truncated[i]
        # Check if this is a good break point (space, punctuation, or CJK boundary)
        if char == ' ' or char in punctuation_chars:
            return truncated[:i].strip()
        # Check if it's a CJK character boundary (non-CJK before CJK)
        if i > 0:
            prev_char = truncated[i-1]
            char_type = unicodedata.category(char)
            prev_char_type = unicodedata.category(prev_char)
            # Break between non-CJK and CJK characters
            if (prev_char_type.startswith('L') and char_type.startswith('C')) or \
               (not prev_char_type.startswith('C') and char_type.startswith('C')):
                return truncated[:i].strip()
    
    # 4. If no good break point found, try to truncate at least at min_length if specified
    if min_length and len(truncated) < min_length and len(text) > min_length:
        # Try to find first space/punctuation after min_length
        for i in range(min_length, min(max_length, len(text))):
            if text[i] == ' ' or text[i] in punctuation_chars:
                return text[:i].strip()
    
    # 5. Last resort: truncate at character boundary (but this should rarely happen)
    # Try to avoid cutting in the middle of a multi-byte character
    while len(truncated) > 0:
        try:
            truncated.encode('utf-8')
            break
        except UnicodeEncodeError:
            truncated = truncated[:-1]
    
    return truncated.strip() if truncated else text[:max_length]


def _get_openai_client():
    """Get or create OpenAI client with timeout and retry configuration.

    Returns:
        OpenAI client instance, or None if the library/API key is not available.
    """
    global _openai_client

    # Re-read API key in case environment was updated after import
    api_key = os.getenv("OPENAI_API_KEY") or OPENAI_API_KEY

    if _openai_client is not None:
        return _openai_client

    if OpenAI is None:
        logger.error("OpenAI library is not installed. Please install it with: pip install openai")
        return None

    if not api_key:
        logger.error("OPENAI_API_KEY is not set. Please set it in your environment variables.")
        return None

    try:
        _openai_client = OpenAI(
            api_key=api_key,
            base_url=OPENAI_BASE_URL if OPENAI_BASE_URL else None,
            timeout=API_TIMEOUT_SECONDS,
            max_retries=MAX_RETRIES,
        )
        logger.info("✅ OpenAI client initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize OpenAI client: {e}")
        _openai_client = None

    return _openai_client


def _sanitize_for_prompt(text: str, max_length: int = 2000) -> str:
    """
    Sanitize user input to prevent prompt injection attacks.
    
    This function removes potential prompt injection patterns that could
    compromise the security and integrity of OpenAI API calls.
    
    Args:
        text: Input text to sanitize
        max_length: Maximum length to allow (default: 2000)
        
    Returns:
        Sanitized text safe for prompt construction
        
    Note:
        This is a basic sanitization. For production use, consider additional
        security measures based on your threat model.
    """
    if not text:
        return ""
    
    # Remove potential prompt injection patterns
    injection_patterns = [
        r'```',
        r'\[INST\]',
        r'\[/INST\]',
        r'<\|im_start\|>',
        r'<\|im_end\|>',
    ]
    
    sanitized = text
    for pattern in injection_patterns:
        sanitized = re.sub(pattern, '', sanitized, flags=re.IGNORECASE)
    
    # Limit length
    sanitized = sanitized[:max_length].strip()
    
    return sanitized


@dataclass
class RakutenContent:
    """Container for Rakuten product content."""
    title: str
    catchphrase: str  # tagline (50-60 chars)
    description: str  # product description (600+ chars)
    sales_description: str  # sales description (600+ chars) - REQUIRED


@dataclass
class ProductDescriptions:
    """Container for product descriptions, sales description, and tagline."""
    product_description: str  # 商品説明文 (600+ chars)
    sales_description: str  # 販売説明文 (600+ chars)
    tagline: str  # タグライン (50-60 chars)


def extract_product_details_from_detail_json(
    detail_payload: Optional[Dict[str, Any]]
) -> Dict[str, str]:
    """
    Extract product details from detail_json based on keyT values.
    
    This function extracts ALL relevant product information from detail_json,
    supporting all product categories (clothing, electronics, home goods, food, beauty, etc.).
    
    Common keyT values that may be extracted:
    - カラー/色 (Color) - applicable to all categories
    - サイズ (Size) - applicable to all categories
    - 製品カテゴリ/カテゴリー (Product Category)
    - ブランド (Brand)
    - 素材/材質 (Material) - for clothing, home goods
    - 機能/特徴 (Features/Characteristics) - for electronics, appliances
    - 容量/サイズ (Capacity/Size) - for electronics, food, cosmetics
    - 対象性別/対象年齢層 (Target Gender/Age) - for clothing, cosmetics
    - 規格型番/モデル (Model Number) - for electronics
    - 重量/質量 (Weight) - for various categories
    - その他すべての有効なkeyT値
    
    Args:
        detail_payload: The detail_json dictionary from product_origin table
        
    Returns:
        Dictionary mapping keyT names to their valueT values (all relevant fields extracted)
    """
    if not detail_payload or not isinstance(detail_payload, dict):
        return {}
    
    # Universal keyT values applicable to all product categories
    # These are common across all categories
    universal_keys = {
        "カラー": ["カラー", "色", "カラーバリエーション"],
        "サイズ": ["サイズ", "規格サイズ", "容量サイズ"],
        "製品カテゴリ": ["製品カテゴリ", "製品カテゴリー", "カテゴリ", "カテゴリー"],
        "規格型番": ["規格型番", "型番", "モデル", "モデル番号", "製品番号"],
        "ブランド": ["ブランド", "メーカー", "製造元"],
        "対象性別": ["対象性別", "性別", "対象"],
        "対象年齢層": ["対象年齢層", "対象年齢", "年齢層"],
    }
    
    # Category-specific keys (optional, will be included if found)
    category_specific_keys = {
        # Clothing-specific
        "袖の長さ": ["袖の長さ", "袖丈"],
        "スタイル": ["スタイル", "デザインスタイル"],
        "スタイルタイプ": ["スタイルタイプ"],
        "適用性別": ["適用性別"],
        "適用グループ": ["適用グループ"],
        # Material/Features (universal)
        "素材": ["素材", "材質", "原材料"],
        "機能": ["機能", "特徴", "性能"],
        # Electronics/Appliances
        "容量": ["容量", "ストレージ", "メモリ"],
        "重量": ["重量", "質量"],
        "寸法": ["寸法", "サイズ", "大きさ"],
    }
    
    # Combine all target keys
    target_keys = {**universal_keys, **category_specific_keys}
    
    # Create a lookup map for faster matching
    key_lookup = {}
    for main_key, variations in target_keys.items():
        for var in variations:
            key_lookup[var] = main_key
    
    extracted_details = {}
    
    # Try to find goodsInfo object (check multiple possible names)
    goods_info = None
    for key in ["goodsInfo", "goodsinfo", "goods_info", "productInfo", "product_info"]:
        if key in detail_payload:
            goods_info = detail_payload.get(key)
            break
    
    # If not found with exact keys, try case-insensitive search
    if not goods_info:
        for key, value in detail_payload.items():
            if isinstance(value, dict) and key.lower() in ["goodsinfo", "productinfo"]:
                goods_info = value
                break
    
    # Extract detail array from goodsInfo
    detail_array = []
    if goods_info and isinstance(goods_info, dict):
        detail_array = goods_info.get("detail", [])
        if not isinstance(detail_array, list):
            detail_array = []
    
    # Also check root level for detail array
    if not detail_array and "detail" in detail_payload:
        detail_array = detail_payload.get("detail", [])
    if not isinstance(detail_array, list):
        detail_array = []
    
    # Extract values for target keys AND all other relevant keyT values
    # Strategy: Extract known keys first, then include all other meaningful keyT values
    excluded_key_patterns = [
        r'image', r'img', r'url', r'pic', r'photo', r'画像', r'写真',  # Images/URLs
        r'price', r'価格', r'金額',  # Prices
        r'code', r'コード', r'番号$',  # Codes (but keep model numbers)
    ]
    
    for item in detail_array:
        if not isinstance(item, dict):
            continue
        
        key_t = item.get("keyT") or item.get("keyt")
        value_t = item.get("valueT") or item.get("valuet")
        
        if not key_t or not value_t:
            continue
        
        key_t_str = str(key_t).strip()
        value_t_str = str(value_t).strip()
        
        # Skip if key contains excluded patterns (using pre-compiled patterns)
        key_lower = key_t_str.lower()
        should_exclude = any(pattern.search(key_lower) for pattern in EXCLUDED_KEY_PATTERNS)
        if should_exclude:
            continue
        
        # Skip if value contains URL patterns (using pre-compiled pattern)
        if URL_PATTERN.search(value_t_str):
            continue
        
        # Check if this keyT matches any of our target keys (exact match or in lookup)
        matched_key = None
        if key_t_str in key_lookup:
            matched_key = key_lookup[key_t_str]
        else:
            # Try partial matching
            for var_key, main_key in key_lookup.items():
                if var_key in key_t_str or key_t_str in var_key:
                    matched_key = main_key
                    break
        
        # If no match found in target keys, use the original keyT name
        # This allows extraction of category-specific or new fields
        if not matched_key:
            # Use original keyT as-is if it seems meaningful
            # Filter out very short or generic keys
            if len(key_t_str) >= 2 and key_t_str not in ['ID', 'id', 'ID号', 'id号']:
                matched_key = key_t_str
        
        if matched_key:
            # Clean up the value (handle comma-separated values, remove extra spaces)
            # If value contains commas, split and clean each part
            if ',' in value_t_str:
                # Handle comma-separated values (e.g., "RED,Green,黒,オフホワイト")
                parts = [part.strip() for part in value_t_str.split(',')]
                # Filter out empty parts and clean each
                cleaned_parts = []
                for part in parts:
                    part_clean = re.sub(r'\s+', ' ', part).strip()
                    if part_clean:
                        cleaned_parts.append(part_clean)
                if cleaned_parts:
                    value_t_clean = ' '.join(cleaned_parts)
                else:
                    value_t_clean = re.sub(r'\s+', ' ', value_t_str).strip()
            else:
                value_t_clean = re.sub(r'\s+', ' ', value_t_str).strip()
            
            if value_t_clean:
                # Store with the main key name (normalized key or original keyT)
                extracted_details[matched_key] = value_t_clean
    
    return extracted_details


# Pre-compiled patterns for title filtering (performance optimization)
_TITLE_FILTER_PATTERNS = [
    re.compile(r'の'),  # particle "no" - remove ALL occurrences
    re.compile(r'\s+な\s+'),  # particle/adjective "na"
    re.compile(r'\s+を\s+'),  # particle "wo"
    re.compile(r'\s+に\s+'),  # particle "ni"
    re.compile(r'\s+が\s+'),  # particle "ga"
    re.compile(r'\s+へ\s+'),  # particle "he"
    re.compile(r'\s+で\s+'),  # particle "de"
    re.compile(r'\s+から\s+'),  # particle "kara"
    re.compile(r'\s+まで\s+'),  # particle "made"
    re.compile(r'\s+も\s+'),  # particle "mo"
    re.compile(r'\s+は\s+'),  # particle "ha/wa"
    re.compile(r'と'),  # particle "to" - remove ALL occurrences
    re.compile(r'\s+や\s+'),  # particle "ya"
    re.compile(r'\s+か\s+'),  # particle "ka"
    re.compile(r'\s+です\s+'),  # copula "desu"
    re.compile(r'\s+だ\s+'),  # copula "da"
    re.compile(r'\s+である\s+'),  # copula "de aru"
    re.compile(r'\s+ます\s+'),  # verb ending "masu"
    re.compile(r'\s+する\s+'),  # verb "suru"
    re.compile(r'\s+ある\s+'),  # verb "aru"
    re.compile(r'\s+いる\s+'),  # verb "iru"
    re.compile(r'\s+この\s+'),  # demonstrative "kono"
    re.compile(r'\s+その\s+'),  # demonstrative "sono"
    re.compile(r'\s+あの\s+'),  # demonstrative "ano"
    re.compile(r'\s+とても\s+'),  # adverb "totemo"
    re.compile(r'\s+すぐ\s+'),  # adverb "sugu"
    re.compile(r'\s+ゆっくり\s+'),  # adverb "yukkuri"
    re.compile(r'\s+そして\s+'),  # conjunction "soshite"
    re.compile(r'\s+しかし\s+'),  # conjunction "shikashi"
    re.compile(r'\s+だから\s+'),  # conjunction "dakara"
    re.compile(r'20\d{2}年'),  # year patterns (supports all years 2000-2099)
    re.compile(r'20\d{2}'),  # year numbers (supports all years 2000-2099)
    re.compile(r'\d+円'),  # price patterns
    re.compile(r'\d+万円'),  # price patterns
    re.compile(r'価格'),  # "price"
    re.compile(r'金額'),  # "amount"
    # Chinese-derived terms
    re.compile(r'青少年'),  # "youth/adolescent"
    re.compile(r'子羊'),  # "lamb"
    re.compile(r'子羊毛'),  # "lamb wool"
    re.compile(r'新型'),  # "new type"
    re.compile(r'潮'),  # "tide"
    re.compile(r'厚い'),  # adjective "thick"
    # Chinese direct translation terms
    re.compile(r'気質通勤'),  # "temperament commute"
    re.compile(r'通勤風'),  # "commute style"
    re.compile(r'洋気'),  # "foreign atmosphere"
    re.compile(r'暴権'),  # inappropriate term
    re.compile(r'气质'),  # "temperament"
    re.compile(r'显瘦'),  # "look slim"
    re.compile(r'美観'),  # "beautiful view"
    re.compile(r'爆発的人気'),  # "explosive popularity"
    re.compile(r'インフルエンサー'),  # "influencer"
    re.compile(r'スリムに見える'),  # "look slim"
    re.compile(r'爆款'),  # "explosive hit"
    re.compile(r'爆売れ'),  # "explosive sales"
    # Unrelated keywords
    re.compile(r'技術者服'),  # unrelated keyword
    re.compile(r'足治療'),  # unrelated keyword
    re.compile(r'技術者'),  # unrelated keyword
]

_TRAILING_PARTICLES = ['の', 'な', 'を', 'に', 'が', 'へ', 'で', 'から', 'まで', 'も', 'は', 'と', 'や', 'か']


def _filter_non_nouns_from_title(title: str) -> str:
    """
    Filter out common particles, verbs, adjectives, and other non-noun words from title.
    Uses pre-compiled patterns for optimal performance.
    
    Args:
        title: Generated title string
        
    Returns:
        Filtered title with obvious non-nouns removed
    """
    if not title:
        return title
    
    filtered_title = title
    
    # Remove patterns using pre-compiled regex (much faster)
    for pattern in _TITLE_FILTER_PATTERNS:
        filtered_title = pattern.sub(' ', filtered_title)
    
    # Clean up multiple spaces
    filtered_title = WHITESPACE_PATTERN.sub(' ', filtered_title).strip()
    
    # Remove trailing particles at word boundaries
    words = filtered_title.split(' ')
    cleaned_words = []
    for word in words:
        # Remove trailing particles
        while word and word[-1] in _TRAILING_PARTICLES and len(word) > 1:
            word = word[:-1]
        # Also remove leading particles
        while word and word[0] in _TRAILING_PARTICLES and len(word) > 1:
            word = word[1:]
        if word and word.strip():
            cleaned_words.append(word)
    
    filtered_title = ' '.join(cleaned_words)
    
    # Final pass: remove any remaining "の" or "と" characters
    filtered_title = filtered_title.replace('の', ' ')
    filtered_title = filtered_title.replace('と', ' ')
    filtered_title = WHITESPACE_PATTERN.sub(' ', filtered_title).strip()
    
    return filtered_title


# System message for OpenAI title generation - OPTIMIZED VERSION
_SYSTEM_MESSAGE = """あなたは楽天市場向け商品タイトル生成の専門AIです。楽天市場の審査を100%通過し、SEO最適化された完璧なタイトルを生成します。

【絶対遵守の3大ルール】
1. 文字数：必ず90〜100文字（この範囲を絶対に超えない、下回らない）
2. 品詞：名詞のみ（助詞・動詞・形容詞・副詞は一切禁止）
3. 出力：タイトルのみ（説明文・補足・装飾は一切不要）

【生成前の必須チェックリスト】
□ 文字数が90〜100文字の範囲内か
□ 名詞のみで構成されているか（助詞「の」「と」「が」などがないか）
□ 商品に関連するキーワードのみか（無関係なキーワードがないか）
□ 中国語文字が含まれていないか
□ 価格・年号・商品コードが含まれていないか
□ 誇張語（新作、人気、爆款など）が含まれていないか
□ 装飾記号（【】、[]、「」など）が含まれていないか

【キーワード配置の最適順序】
1. カテゴリー名（レディース、メンズ、家電、食品など）
2. 商品名・商品タイプ（コート、スマートフォン、チョコレートなど）
3. ブランド名（該当する場合のみ）
4. 特徴・機能（防寒、5G対応、無添加など）
5. 仕様・スペック（サイズ、容量、色、素材など）

【完全禁止事項（即座に削除）】
❌ 助詞：の、が、に、を、で、へ、から、まで、は、も、と、や、か
❌ 動詞：する、使う、着る、買う、見る、食べる（すべての動詞）
❌ 形容詞：美しい、厚い、新しい、大きい（すべての「〜い」で終わる語）
❌ 形容動詞：便利だ、きれいだ、簡単だ（すべての「〜だ」「〜な」で終わる語）
❌ 副詞：とても、すぐ、ゆっくり
❌ 年号：2020、2021、2022、2023、2024、2025
❌ 価格：価格、金額、円、数字+円
❌ 誇張語：新作、爆款、人気、売れ筋、ベストセラー、大人気
❌ 商品コード：A170、A171、8001、8007
❌ プラットフォーム名：1688、Wish、Lazada、AliExpress、Temu、Taobao
❌ URL：https://、http://、www.
❌ 中国語文字：すべて日本語に変換
❌ 装飾記号：【】、[]、「」

【中国語用語の変換マップ】
気質/气质 → エレガント/上品/優雅
通勤風 → 通勤/オフィス/ビジネス
显瘦 → スリム/スリムフィット
子羊毛 → ラムウール
智能 → スマート/インテリジェント
有机 → オーガニック/有機
爆款/热卖 → 削除（誇張表現）

【完璧なタイトルの例】
✅ レディース コート ロング ラムウール フード付き ベルト 秋冬 防寒 保温 ブラック グレー ベージュ 3色構成
✅ スマートフォン iPhone 15 Pro 256GB タイタニウム 5G対応 A17 Proチップ 6.1インチ 3カメラ バッテリー最適化
✅ チョコレート バレンタイン プレゼント 詰め合わせ 高級 無添加 個包装 10種類 24個入り ギフトボックス
✅ メンズ パーカー フード付き 厚手 長袖 コットン カジュアル 秋冬 防寒 サイドポケット ジッパー グレー ネイビー 2色構成

【悪い例（絶対に避ける）】
❌ 商品名の カテゴリ名な ブランド名を（助詞が含まれている）
❌ 美しい商品 便利なアイテム（形容詞が含まれている）
❌ 2025年 新作 レディース コート 価格 36円（年号、誇張語、価格情報）
❌ 智能 热卖 爆款 特价 スマホ（中国語、誇張語）

【出力形式】
タイトルのみを出力してください。説明文、補足、装飾、ラベルは一切不要です。"""


def _build_title_prompt(
    product_name: str,
    details_text: str,
) -> str:
    """
    Build the prompt for product title generation.
    
    Args:
        product_name: The product name
        details_text: Formatted product details string
        
    Returns:
        Formatted prompt string
    """
    # Optimized prompt components - clear, concise, and effective
    prompt_parts = [
        "【タスク】",
        "以下の商品情報から、楽天市場審査通過可能な90〜100文字の名詞のみのタイトルを生成してください。",
        "",
        "【元の商品名】",
        product_name or "不明",
        "",
        "【商品詳細情報】",
        details_text if details_text else "詳細情報なし",
        "",
        "【生成ルール（絶対遵守）】",
        f"1. 文字数：必ず{TITLE_MIN_LENGTH}〜{TITLE_MAX_LENGTH}文字（この範囲を絶対に超えない、下回らない）",
        "2. 品詞：名詞のみ（助詞「の」「と」「が」、動詞、形容詞、副詞は一切禁止）",
        "3. キーワード：商品に関連するキーワードのみ（無関係なキーワードは絶対禁止）",
        "4. 出力：タイトルのみ（説明文、補足、装飾は一切不要）",
        "",
        "【カテゴリー自動識別とキーワード推測】",
        "商品情報からカテゴリーを自動識別し、適切なキーワードを推測・復元してください：",
        "- ファッション：レディース/メンズ/キッズ、商品名、素材、色、サイズ、季節、機能",
        "- 電子機器：商品名、ブランド、型番、機能、スペック、容量、色",
        "- 食品：商品名、ブランド、種類、味、容量、特徴、パッケージ",
        "- 日用品：商品名、用途、素材、サイズ、機能、色",
        "- コスメ：商品名、ブランド、種類、効果、容量、対象、色・タイプ",
        "",
        "【キーワード配置の最適順序】",
        "1. カテゴリー名（レディース、メンズ、家電など）",
        "2. 商品名・商品タイプ（コート、スマートフォンなど）",
        "3. ブランド名（該当する場合）",
        "4. 特徴・機能（カテゴリーに応じた重要キーワード）",
        "5. 仕様・スペック（サイズ、容量、色、素材など）",
        "",
        "【完全禁止事項（即座に削除）】",
        "❌ 助詞：の、が、に、を、で、へ、から、まで、は、も、と、や、か",
        "❌ 動詞：する、使う、着る、買う、見る、食べる（すべての動詞）",
        "❌ 形容詞：美しい、厚い、新しい、大きい（すべての「〜い」で終わる語）",
        "❌ 形容動詞：便利だ、きれいだ、簡単だ（すべての「〜だ」「〜な」で終わる語）",
        "❌ 副詞：とても、すぐ、ゆっくり",
        "❌ 年号：2020、2021、2022、2023、2024、2025",
        "❌ 価格：価格、金額、円、数字+円",
        "❌ 誇張語：新作、爆款、人気、売れ筋、ベストセラー、大人気",
        "❌ 商品コード：A170、A171、8001、8007",
        "❌ プラットフォーム名：1688、Wish、Lazada、AliExpress、Temu、Taobao",
        "❌ URL：https://、http://、www.",
        "❌ 中国語文字：すべて日本語に変換",
        "❌ 装飾記号：【】、[]、「」",
        "",
        "【中国語用語の変換マップ】",
        "気質/气质 → エレガント/上品/優雅",
        "通勤風 → 通勤/オフィス/ビジネス",
        "显瘦 → スリム/スリムフィット",
        "子羊毛 → ラムウール",
        "智能 → スマート/インテリジェント",
        "有机 → オーガニック/有機",
        "爆款/热卖 → 削除（誇張表現）",
        "",
        "【完璧なタイトルの例】",
        "✅ レディース コート ロング ラムウール フード付き ベルト 秋冬 防寒 保温 ブラック グレー ベージュ 3色構成",
        "✅ スマートフォン iPhone 15 Pro 256GB タイタニウム 5G対応 A17 Proチップ 6.1インチ 3カメラ バッテリー最適化",
        "✅ チョコレート バレンタイン プレゼント 詰め合わせ 高級 無添加 個包装 10種類 24個入り ギフトボックス",
        "✅ メンズ パーカー フード付き 厚手 長袖 コットン カジュアル 秋冬 防寒 サイドポケット ジッパー グレー ネイビー 2色構成",
        "",
        "【悪い例（絶対に避ける）】",
        "❌ 商品名の カテゴリ名な ブランド名を（助詞が含まれている）",
        "❌ 美しい商品 便利なアイテム（形容詞が含まれている）",
        "❌ 2025年 新作 レディース コート 価格 36円（年号、誇張語、価格情報）",
        "❌ 智能 热卖 爆款 特价 スマホ（中国語、誇張語）",
        "",
        "【出力形式（厳守）】",
        "タイトルのみを出力してください。説明文、補足、装飾、ラベルは一切不要です。",
        "",
        "【生成前の最終チェック】",
        "□ 文字数が90〜100文字か",
        "□ 名詞のみで構成されているか（助詞がないか）",
        "□ 商品に関連するキーワードのみか",
        "□ 中国語文字、価格、年号、商品コードが含まれていないか",
        "□ 装飾記号が含まれていないか",
    ]
    
    return "\n".join(prompt_parts)


def generate_product_title_with_openai(
    product_name: str,
    product_details: Dict[str, str],
    *,
    model: str = "gpt-4o-mini",
) -> str:
    """
    Generate a product title using OpenAI GPT-4 model.
    
    The title will be 90-100 characters, optimized for Rakuten marketplace SEO and compliant with Rakuten guidelines.
    
    Args:
        product_name: The product name from product_origin.product_name
        product_details: Dictionary of extracted product details (from extract_product_details_from_detail_json)
        model: OpenAI model to use (default: gpt-4)
        
    Returns:
        Generated product title (90-100 characters, half-width spaces between keywords, Rakuten guideline compliant)
    """
    client = _get_openai_client()
    if not client:
        logger.warning("OpenAI client not available, returning original product name")
        return product_name or ""
    
    # Build product details string for prompt (with sanitization)
    details_list = []
    for key, value in product_details.items():
        if value:
            # Clean up the value (remove special characters that might confuse the model)
            value_clean = re.sub(r'[^\w\s\-/,，、]', '', str(value))
            if value_clean:
                details_list.append(f"{key}: {value_clean}")
    
    details_text = "\n".join(details_list) if details_list else "なし"
    
    # Sanitize inputs to prevent prompt injection
    sanitized_product_name = _sanitize_for_prompt(product_name, max_length=500)
    sanitized_details_text = _sanitize_for_prompt(details_text, max_length=2000)
    
    try:
        # Build prompt using unified helper function (no retry logic)
        prompt = _build_title_prompt(
            product_name=sanitized_product_name,
            details_text=sanitized_details_text,
        )
        
        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": _SYSTEM_MESSAGE
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=TITLE_TEMPERATURE,
            max_tokens=TITLE_MAX_TOKENS,
        )
        
        generated_title = response.choices[0].message.content.strip()
        
        # Clean up the generated title using pre-compiled patterns
        # Remove any leading/trailing quotes or whitespace
        generated_title = QUOTE_PATTERN.sub('', generated_title)
        
        # CRITICAL: Remove decorative keywords and markers using pre-compiled patterns
        for pattern in DECORATIVE_PATTERNS:
            generated_title = pattern.sub('', generated_title)
        
        # Remove any remaining decorative markers at the start
        generated_title = re.sub(r'^(生成タイトル|タイトル|商品タイトル)[：:\s]*', '', generated_title, flags=re.IGNORECASE)
        
        # Remove year and price information using pre-compiled patterns
        generated_title = YEAR_PATTERN.sub('', generated_title)
        generated_title = YEAR_NUMBER_PATTERN.sub('', generated_title)
        generated_title = PRICE_YEN_PATTERN.sub('', generated_title)
        generated_title = PRICE_MAN_YEN_PATTERN.sub('', generated_title)
        generated_title = PRICE_KEYWORD_PATTERN.sub('', generated_title)
        
        # Remove color variation numbers using pre-compiled patterns
        generated_title = COLOR_CODE_PATTERN_1.sub('', generated_title)
        generated_title = COLOR_CODE_PATTERN_2.sub('', generated_title)
        generated_title = COLOR_CODE_PATTERN_3.sub('', generated_title)
        
        # Remove Chinese promotional phrases and keywords
        chinese_phrases = [
            '热卖推荐', '热销', '推荐', '新品', '特价', '限时', '抢购', '热卖', '爆款', '畅销', '人气', 
            '热销款', '热销推荐', '子羊毛', '青少年', '新型', '潮',
            # Additional Chinese fashion-specific terms
            '气质', '显瘦', '美観', '爆発的人気', 'インフルエンサー'
        ]
        for phrase in chinese_phrases:
            generated_title = generated_title.replace(phrase, '')
        
        # Replace Chinese direct translation terms with natural Japanese
        chinese_translation_replacements = {
            '気質通勤': '',
            '通勤風': '',
            '洋気': '',
            '暴権': '',
            # These will be replaced with natural Japanese equivalents
        }
        for chinese_term, replacement in chinese_translation_replacements.items():
            if replacement:
                generated_title = generated_title.replace(chinese_term, replacement)
            else:
                # If no replacement, just remove it
                generated_title = re.sub(r'\b' + re.escape(chinese_term) + r'\b', '', generated_title)
        
        # Remove unrelated keywords (Rakuten guideline violation)
        unrelated_keywords = [
            '技術者服', '足治療', '技術者',  # unrelated keywords that should not appear
            '爆発的人気', 'インフルエンサー', 'スリムに見える', '美観',  # Low relevance keywords
            '爆款', '爆売れ',  # Chinese promotional terms
        ]
        for keyword in unrelated_keywords:
            generated_title = re.sub(r'\b' + re.escape(keyword) + r'\b', '', generated_title)
        
        # Replace Chinese fashion-specific terms with natural Japanese
        fashion_term_replacements = {
            '気質': '',  # Remove, replace contextually
            '通勤風': 'ビジネス',
            '洋気': '',  # Remove, high-class concept should be expressed differently
            '显瘦': 'スリム',  # Slim-fitting
        }
        for chinese_term, japanese_term in fashion_term_replacements.items():
            if japanese_term:
                generated_title = re.sub(r'\b' + re.escape(chinese_term) + r'\b', japanese_term, generated_title)
            else:
                generated_title = re.sub(r'\b' + re.escape(chinese_term) + r'\b', '', generated_title)
        
        # Replace inappropriate sexual expressions with safer alternatives
        generated_title = re.sub(r'\bセクシー\b(?!風)', '', generated_title)  # Remove standalone "セクシー" but keep "セクシー風"
        
        # Replace ambiguous expressions
        generated_title = re.sub(r'\b欧米風\b', '', generated_title)  # Remove ambiguous "欧米風"
        generated_title = re.sub(r'\b婦人服\b', 'レディース', generated_title)  # Replace "婦人服" with "レディース"
        
        # Filter out obvious non-nouns (particles, verbs, etc.)
        generated_title = _filter_non_nouns_from_title(generated_title)
        
        # CRITICAL: Remove ALL instances of "の" and "と" particles (most common error)
        generated_title = generated_title.replace('の', ' ')
        generated_title = generated_title.replace('と', ' ')
        
        # Ensure half-width spaces between keywords
        generated_title = WHITESPACE_PATTERN.sub(' ', generated_title)
        
        # Remove any non-Japanese characters that shouldn't be in the title
        generated_title = JAPANESE_CHAR_PATTERN.sub('', generated_title)
        
        # Check length and adjust if needed using constants
        title_length = len(generated_title)
        
        if TITLE_MIN_LENGTH <= title_length <= TITLE_MAX_LENGTH:
            logger.info(f"✅ Generated perfect title ({title_length} chars): {generated_title[:50]}...")
            return generated_title
        elif title_length < TITLE_MIN_LENGTH:
            # Title is too short, pad with product details if available
            logger.warning(f"⚠️  Generated title is too short ({title_length} chars), padding with product details...")
            if product_details:
                keywords = [v for v in product_details.values() if v]
                if keywords:
                    additional = ' '.join(keywords[:MAX_KEYWORDS_TO_PAD])
                    padded = f"{generated_title} {additional}"
                    padded = WHITESPACE_PATTERN.sub(' ', padded).strip()
                    if TITLE_MIN_LENGTH <= len(padded) <= TITLE_MAX_LENGTH:
                        return padded[:TITLE_MAX_LENGTH]
            logger.warning(f"⚠️  Could not extend title to {TITLE_MIN_LENGTH}+ chars, returning {title_length} char title")
            return generated_title
        elif title_length > TITLE_MAX_LENGTH:
            # Title is too long, truncate intelligently at word boundary
            logger.warning(f"⚠️  Generated title is too long ({title_length} chars), truncating at word boundary...")
            truncated = truncate_at_word_boundary(generated_title, TITLE_MAX_LENGTH, TITLE_MIN_LENGTH)
            
            # Ensure minimum length after truncation
            if len(truncated) < TITLE_MIN_LENGTH:
                logger.warning(f"⚠️  After truncation, title is too short ({len(truncated)} chars), using first {TITLE_MIN_LENGTH} chars")
                truncated = truncate_at_word_boundary(generated_title, TITLE_MIN_LENGTH + 10, TITLE_MIN_LENGTH)
                if len(truncated) < TITLE_MIN_LENGTH:
                    truncated = generated_title[:TITLE_MIN_LENGTH]
            
            logger.info(f"✅ Truncated title ({len(truncated)} chars): {truncated[:50]}...")
            return truncated
        
    except Exception as e:
        logger.error(f"Error generating title with OpenAI: {e}", exc_info=True)
        return product_name or ""
    
    # Fallback: return product name if generation failed
    return product_name or ""


def generate_product_descriptions_with_openai(
    product_name: str,
    detail_payload: Optional[Dict[str, Any]],
    existing_description: str = "",
    *,
    model: str = "gpt-4o-mini",
) -> ProductDescriptions:
    """
    Generate product description, sales description, and tagline using OpenAI GPT-4o-mini.
    
    Args:
        product_name: The product name from product_origin.product_name
        detail_payload: The detail_json dictionary from product_origin table
        existing_description: Existing product description (optional)
        model: OpenAI model to use (default: gpt-4o-mini)
        
    Returns:
        ProductDescriptions object with product_description (600+ chars), 
        sales_description (600+ chars), and tagline (50-60 chars)
    """
    client = _get_openai_client()
    if not client:
        logger.warning("OpenAI client not available, using fallback")
        # Fallback: create professional descriptions with proper content
        safe_name = product_name or "商品"
        safe_desc = existing_description or ""
        
        # CRITICAL: Clean existing_description of all HTML except <br> before using
        if safe_desc:
            # Remove all HTML except <br> tags
            safe_desc = re.sub(r'<(?!br\s*/?>)[^>]+>', '', safe_desc, flags=re.IGNORECASE)
            safe_desc = re.sub(r'[a-z]+\s*=\s*"[^"]*"', '', safe_desc, flags=re.IGNORECASE)
            safe_desc = re.sub(r'&[a-z]+;', '', safe_desc, flags=re.IGNORECASE)
            safe_desc = WHITESPACE_PATTERN.sub(' ', safe_desc)
            safe_desc = BR_TAG_PATTERN.sub('<br>', safe_desc)
        
        # Simplified fallback: Always add extension text (no length checks)
        product_extension_text = "楽天市場での販売実績も豊富で、お客様から高い評価をいただいております。<br>使いやすさと機能性を兼ね備えた設計で、多くのお客様にご愛用いただいています。"
        sales_extension_text = "この商品は、お求めやすい価格で、高い満足度を実現しています。<br>商品の魅力をぜひお試しください。<br>楽天市場での販売実績も多数あり、お客様からのレビューでも高い評価をいただいております。<br>この機会にぜひお買い求めください。"
        
        # Create base descriptions - always add extension text
        if safe_desc:
            fallback_product_desc = f"{safe_desc}<br>{product_extension_text}"
            fallback_sales_desc = f"{safe_desc}<br>{sales_extension_text}"
        else:
            fallback_product_desc = product_extension_text
            fallback_sales_desc = sales_extension_text
        
        # Ensure each sentence ends with <br>
        fallback_product_desc = ensure_br_tags_global(fallback_product_desc)
        fallback_sales_desc = ensure_br_tags_global(fallback_sales_desc)
        
        # Generate tagline in keyword format (half-width space separated)
        # Convert product name to keyword format
        keywords = re.split(r'[の・、,，\s]+', safe_name)
        keywords = [k.strip() for k in keywords if k.strip()]
        fallback_tagline = ' '.join(keywords)
        
        # Add additional keywords if needed
        if len(fallback_tagline) < TAGLINE_MIN_LENGTH:
            fallback_tagline = f"{fallback_tagline} 楽天市場 人気 商品"
        
        # Validate length: 70文字以下の場合はそのまま、70文字を超える場合のみ60-70文字に調整
        if len(fallback_tagline) > TAGLINE_MAX_LENGTH:
            fallback_tagline = truncate_at_word_boundary(fallback_tagline, TAGLINE_MAX_LENGTH, TAGLINE_MIN_LENGTH)
        
        # IMPORTANT: Remove delivery message from product_description in fallback
        if fallback_product_desc:
            for pattern in DELIVERY_PATTERNS:
                fallback_product_desc = pattern.sub('', fallback_product_desc)
            # Remove orphaned "ご。" patterns that may be left after delivery message removal
            fallback_product_desc = re.sub(r'ご。', '', fallback_product_desc)
            fallback_product_desc = re.sub(r'ご\s*。', '', fallback_product_desc)
            fallback_product_desc = re.sub(r'。\s*ご\s*。', '。', fallback_product_desc)
            fallback_product_desc = re.sub(r'([^。\s])ご\s*。', r'\1。', fallback_product_desc)
            fallback_product_desc = re.sub(r'。\s*。+', '。', fallback_product_desc)
            fallback_product_desc = fallback_product_desc.rstrip('<br>').rstrip()
        # IMPORTANT: Add delivery message to sales_description in fallback
        fallback_sales_desc = add_delivery_message(fallback_sales_desc) if fallback_sales_desc else DELIVERY_MESSAGE.lstrip('<br>')
        
        return ProductDescriptions(
            product_description=fallback_product_desc,
            sales_description=fallback_sales_desc,
            tagline=fallback_tagline,
        )
    
    # Build product details string from detail_json
    # CRITICAL: ONLY use goodsInfo.detail values - ignore all other fields in detail_json
    # This ensures we don't include unwanted HTML tags, images, or other markup
    details_text = ""
    if detail_payload:
        try:
            # STRICT: Only extract from goodsInfo.detail - ignore all other parts of detail_json
            goods_info = detail_payload.get("goodsInfo") if isinstance(detail_payload, dict) else None
            if not isinstance(goods_info, dict):
                logger.warning("goodsInfo not found or not a dict in detail_json, skipping detail extraction")
                details_text = ""
            else:
                detail_array = goods_info.get("detail", [])
                if not isinstance(detail_array, list):
                    logger.warning("goodsInfo.detail is not a list, skipping detail extraction")
                    details_text = ""
                else:
                    detail_items = []
                    for item in detail_array:
                        if not isinstance(item, dict):
                            continue
                        
                        key_t = item.get("keyT", "")
                        value_t = item.get("valueT", "")
                        
                        # Skip if key or value is empty
                        if not key_t or not value_t:
                            continue
                        
                        # Convert to string for processing
                        key_t_str = str(key_t).strip()
                        value_t_str = str(value_t).strip()
                        
                        # CRITICAL: Remove any HTML tags from valueT (should only contain plain text)
                        # Remove all HTML tags except <br> (though ideally there should be none)
                        value_t_str = re.sub(r'<(?!br\s*/?>)[^>]+>', '', value_t_str, flags=re.IGNORECASE)
                        value_t_str = re.sub(r'<br\s*/?>', ' ', value_t_str, flags=re.IGNORECASE)  # Convert <br> to space
                        
                        # Remove HTML attributes
                        value_t_str = re.sub(r'\s*style\s*=\s*"[^"]*"', '', value_t_str, flags=re.IGNORECASE)
                        value_t_str = re.sub(r'\s*id\s*=\s*"[^"]*"', '', value_t_str, flags=re.IGNORECASE)
                        value_t_str = re.sub(r'\s*class\s*=\s*"[^"]*"', '', value_t_str, flags=re.IGNORECASE)
                        value_t_str = re.sub(r'\s*title\s*=\s*"[^"]*"', '', value_t_str, flags=re.IGNORECASE)
                        
                        # Clean up the value
                        value_t_str = WHITESPACE_PATTERN.sub(' ', value_t_str).strip()
                        
                        # Skip if value is empty after cleaning
                        if not value_t_str:
                            continue
                        
                        # Skip if value contains URL patterns (image URLs, web URLs, etc.)
                        url_patterns = [
                            r'https?://',  # HTTP/HTTPS URLs
                            r'www\.',  # www URLs
                            r'\.(jpg|jpeg|png|gif|webp|bmp|svg)',  # Image file extensions
                            r'\.(com|net|org|co|jp|cn)',  # Domain extensions
                            r'images?',  # "image" or "images" keyword
                            r'img',  # "img" keyword
                            r'picUrl',  # picUrl keyword
                            r'imageUrl',  # imageUrl keyword
                        ]
                        
                        # Check if value_t contains URL patterns
                        contains_url = False
                        for pattern in url_patterns:
                            if re.search(pattern, value_t_str, re.IGNORECASE):
                                contains_url = True
                                break
                        
                        # Skip fields that are image or URL related
                        if contains_url:
                            logger.debug(f"Skipping URL/image field: {key_t_str}")
                            continue
                        
                        # Skip if key contains image/URL related keywords
                        key_lower = key_t_str.lower()
                        excluded_keywords = ['image', 'img', 'url', 'pic', 'photo', '画像', '写真']
                        if any(keyword in key_lower for keyword in excluded_keywords):
                            logger.debug(f"Skipping image/URL related key: {key_t_str}")
                            continue
                        
                        # Add to details if it passes all filters
                        detail_items.append(f"{key_t_str}: {value_t_str}")
                    
                    if detail_items:
                        details_text = "\n".join(detail_items)
                    else:
                        logger.warning("No valid detail items found in goodsInfo.detail after filtering")
        except Exception as e:
            logger.warning(f"Error extracting details from detail_json: {e}")
    
    # Optimized system prompt - concise and effective
    system_prompt = """あなたは楽天市場向け商品説明文・販売説明文・タグライン作成の専門家です。

【絶対遵守の3大ルール】
1. 文字数：商品説明文・販売説明文は最低500文字以上、タグラインは60-70文字（70文字以下の場合はそのまま、70文字を超える場合のみ60-70文字に調整）
2. フォーマット：各文の終わり（。！？の後）に必ず<br>を挿入、タグラインはキーワードを半角スペースで区切った形式
3. 言語：完全なネイティブ日本語のみ（中国語文字は絶対禁止）

【生成前の必須チェックリスト】
□ 商品説明文が500文字以上か
□ 販売説明文が500文字以上か
□ タグラインがキーワード形式（半角スペース区切り）か
□ タグラインが70文字以下、または60-70文字の範囲内か
□ 各文の終わりに<br>が挿入されているか
□ 中国語文字が含まれていないか
□ 価格・商品コード・URLが含まれていないか
□ 自然で読みやすい日本語か

【作成する3つのコンテンツ】
1. 商品説明文（product_description）:
   - 最低500文字以上
   - 商品カテゴリーを自動識別し、カテゴリーに適した詳細情報を含む
   - 事実に基づいた正確な情報
   - 各文の終わりに<br>を挿入

2. 販売説明文（sales_description）:
   - 最低500文字以上
   - 購買意欲を高める魅力的な説明
   - おすすめポイント、使用シーン、メリットを詳しく説明
   - 各文の終わりに<br>を挿入

3. タグライン（tagline）:
   - 60-70文字（70文字以下の場合はそのまま、70文字を超える場合のみ60-70文字に調整）
   - キーワードを半角スペースで区切った形式（SEO最適化のため）
   - 例：「秋冬 男性 半高襟 シャツ 長袖 カジュアル ビジネス」
   - 文章形式ではなく、検索されやすいキーワードの組み合わせ

【完全禁止事項（即座に削除）】
❌ 中国語文字：すべて日本語に翻訳
❌ 価格情報：価格、金額、円、数字+円、税込、税抜
❌ 商品コード：A170、A171、8001、8007などすべてのコード
❌ プラットフォーム名：1688、Wish、Lazada、AliExpress、Temu、Taobao
❌ URL：https://、http://、www.、画像URL
❌ 年号：2020、2021、2022、2023、2024、2025
❌ 過度な誇張表現：世界一、最高級、史上最高
❌ HTMLタグ（<br>以外）：<div>、<table>、<span>などすべて禁止
❌ 配送メッセージ：商品説明文・販売説明文に「到着まで」「ご到着まで」などの配送に関する記述を絶対に含めないこと（後で自動追加されます）
❌ 不正な記号：,,!、,?、、?,?、??、?,?などの連続する記号は絶対に使用しないこと

【カテゴリー別の推奨内容】
● ファッション：素材、サイズ、デザイン、手入れ方法、対象性別
● 電子機器：技術仕様、機能、性能、接続方法、付属品
● 食品：原材料、成分、賞味期限、保存方法、産地、容量
● 日用品：用途、素材、サイズ、機能、メンテナンス方法
● コスメ：成分、効果、使用方法、容量、対象肌質

【出力形式（厳守）】
以下の形式で出力してください：

商品説明文：
[ここに500文字以上の商品説明文を記述。各文の終わりに<br>を挿入。配送に関する記述（到着まで、ご到着までなど）は絶対に含めないこと。]

販売説明文：
[ここに500文字以上の販売説明文を記述。各文の終わりに<br>を挿入。配送に関する記述（到着まで、ご到着までなど）は絶対に含めないこと。]

タグライン：
[ここにキーワードを半角スペースで区切った形式で記述。70文字以下の場合はそのまま、70文字を超える場合のみ60-70文字に調整。例：「秋冬 男性 半高襟 シャツ 長袖 カジュアル ビジネス」。文章形式ではなく、検索されやすいキーワードの組み合わせで記述すること。]

【重要】「商品説明文：」「販売説明文：」「タグライン：」というラベルは必ず含めてください。
【絶対禁止】商品説明文・販売説明文に配送メッセージ（「到着まで」「ご到着まで」など）を絶対に含めないでください。後で自動的に追加されます。"""

    try:
        # Sanitize inputs to prevent prompt injection
        sanitized_product_name = _sanitize_for_prompt(product_name, max_length=500)
        sanitized_details_text = _sanitize_for_prompt(details_text, max_length=2000)
        sanitized_existing_desc = _sanitize_for_prompt(existing_description, max_length=1000)
        
        # Optimized user prompt - clear and concise
        user_prompt = f"""以下の商品情報から、楽天市場向けの商品説明文・販売説明文・タグラインを作成してください。

【商品名】
{sanitized_product_name or "商品名未設定"}

【商品詳細情報】
{sanitized_details_text if sanitized_details_text else "詳細情報なし"}

【既存の商品説明】
{sanitized_existing_desc if sanitized_existing_desc else "既存の説明なし"}

【作成指示】
商品カテゴリー（衣類、電子機器、食品、日用品、コスメなど）を自動識別し、以下3つのコンテンツを作成してください：

1. 商品説明文：最低{DESCRIPTION_MIN_LENGTH}文字以上
   - カテゴリーに適した詳細情報（素材、サイズ、機能、仕様など）
   - 事実に基づいた正確な情報
   - 各文の終わりに<br>を挿入

2. 販売説明文：最低{DESCRIPTION_MIN_LENGTH}文字以上
   - 購買意欲を高める魅力的な説明
   - おすすめポイント、使用シーン、メリットを詳しく説明
   - 各文の終わりに<br>を挿入

3. タグライン：{TAGLINE_MIN_LENGTH}-{TAGLINE_MAX_LENGTH}文字（70文字以下の場合はそのまま、70文字を超える場合のみ60-70文字に調整）
   - キーワードを半角スペースで区切った形式（SEO最適化のため）
   - 例：「秋冬 男性 半高襟 シャツ 長袖 カジュアル ビジネス」
   - 文章形式ではなく、検索されやすいキーワードの組み合わせ
   - 商品名、カテゴリー、特徴、用途などの重要なキーワードを含める

【出力形式（厳守）】
以下の形式で出力してください：

商品説明文：
[ここに{DESCRIPTION_MIN_LENGTH}文字以上の商品説明文を記述。各文の終わりに<br>を挿入。配送に関する記述（到着まで、ご到着までなど）は絶対に含めないこと。]

販売説明文：
[ここに{DESCRIPTION_MIN_LENGTH}文字以上の販売説明文を記述。各文の終わりに<br>を挿入。配送に関する記述（到着まで、ご到着までなど）は絶対に含めないこと。]

タグライン：
[ここにキーワードを半角スペースで区切った形式で記述。70文字以下の場合はそのまま、70文字を超える場合のみ{TAGLINE_MIN_LENGTH}-{TAGLINE_MAX_LENGTH}文字に調整。例：「秋冬 男性 半高襟 シャツ 長袖 カジュアル ビジネス」。文章形式ではなく、検索されやすいキーワードの組み合わせで記述すること。]

【重要】「商品説明文：」「販売説明文：」「タグライン：」というラベルは必ず含めてください。
【絶対禁止】商品説明文・販売説明文に配送メッセージ（「到着まで」「ご到着まで」など）を絶対に含めないでください。後で自動的に追加されます。"""
        
        response = client.chat.completions.create(
            model=model,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=DESCRIPTION_TEMPERATURE,
            max_tokens=DESCRIPTION_MAX_TOKENS,
        )
        
        output_text = response.choices[0].message.content.strip()
        
        # Parse the structured output with improved error handling
        product_description = ""
        sales_description = ""
        tagline = ""
        
        try:
            # Primary parsing method: Use regex to extract sections
            # Extract 商品説明文 (product_description)
            prod_desc_patterns = [
                r'商品説明文[：:]\s*(.+?)(?=\n\n|販売説明文|タグライン|$)',
                r'商品説明文[：:]\s*(.+?)(?=販売説明文|タグライン|$)',
                r'product_description[：:]\s*(.+?)(?=\n\n|sales_description|tagline|$)',
            ]
            for pattern in prod_desc_patterns:
                match = re.search(pattern, output_text, re.DOTALL | re.IGNORECASE)
                if match:
                    product_description = match.group(1).strip()
                    # Clean up brackets if present
                    product_description = re.sub(r'^\[|\]$', '', product_description).strip()
                    break
            
            # Extract 販売説明文 (sales_description) - REQUIRED
            sales_desc_patterns = [
                r'販売説明文[：:]\s*(.+?)(?=\n\n|タグライン|商品説明文|$)',
                r'販売説明文[：:]\s*(.+?)(?=タグライン|商品説明文|$)',
                r'sales_description[：:]\s*(.+?)(?=\n\n|tagline|product_description|$)',
            ]
            for pattern in sales_desc_patterns:
                match = re.search(pattern, output_text, re.DOTALL | re.IGNORECASE)
                if match:
                    sales_description = match.group(1).strip()
                    # Clean up brackets if present
                    sales_description = re.sub(r'^\[|\]$', '', sales_description).strip()
                    break
            
            # Extract タグライン (tagline)
            tagline_patterns = [
                r'タグライン[：:]\s*(.+?)(?=\n\n|商品説明文|販売説明文|$)',
                r'タグライン[：:]\s*(.+?)(?=商品説明文|販売説明文|$)',
                r'tagline[：:]\s*(.+?)(?=\n\n|product_description|sales_description|$)',
            ]
            for pattern in tagline_patterns:
                match = re.search(pattern, output_text, re.DOTALL | re.IGNORECASE)
                if match:
                    tagline = match.group(1).strip()
                    # Clean up brackets if present
                    tagline = re.sub(r'^\[|\]$', '', tagline).strip()
                    break
            
            # Secondary parsing method: Line-by-line parsing if regex failed
            if not all([product_description, sales_description, tagline]):
                logger.warning("Primary parsing method incomplete, trying line-by-line parsing...")
                lines = output_text.split('\n')
                current_section = None
                collected_text = []
                
                for line in lines:
                    line = line.strip()
                    # Skip empty lines and section markers
                    if not line or line.startswith('【') or (line.startswith('[') and line.endswith(']')):
                        continue
                    
                    # Detect section headers
                    line_lower = line.lower()
                    if '商品説明文' in line or 'product_description' in line_lower:
                        # Save previous section
                        if collected_text and current_section:
                            text = ' '.join(collected_text).strip()
                            if current_section == 'product':
                                product_description = text
                            elif current_section == 'sales':
                                sales_description = text
                            elif current_section == 'tagline':
                                tagline = text
                        current_section = 'product'
                        collected_text = []
                    elif '販売説明文' in line or 'sales_description' in line_lower:
                        # Save previous section
                        if collected_text and current_section:
                            text = ' '.join(collected_text).strip()
                            if current_section == 'product':
                                product_description = text
                            elif current_section == 'sales':
                                sales_description = text
                            elif current_section == 'tagline':
                                tagline = text
                        current_section = 'sales'
                        collected_text = []
                    elif 'タグライン' in line or 'tagline' in line_lower:
                        # Save previous section
                        if collected_text and current_section:
                            text = ' '.join(collected_text).strip()
                            if current_section == 'product':
                                product_description = text
                            elif current_section == 'sales':
                                sales_description = text
                            elif current_section == 'tagline':
                                tagline = text
                        current_section = 'tagline'
                        collected_text = []
                    elif current_section:
                        # Collect content for current section
                        # Skip section markers
                        if not (line.startswith('【') or (line.startswith('[') and ']' in line)):
                            collected_text.append(line)
                
                # Save last section
                if collected_text and current_section:
                    text = ' '.join(collected_text).strip()
                    if current_section == 'tagline':
                        tagline = text
                    elif current_section == 'sales':
                        sales_description = text
                    elif current_section == 'product':
                        product_description = text
                        
        except Exception as parse_error:
            logger.error(f"Error parsing OpenAI response: {parse_error}", exc_info=True)
            # Continue with empty strings, will be handled by fallback logic below
        
        # Remove ALL HTML elements except <br> tags (CRITICAL)
        def remove_all_html_except_br(text: str) -> str:
            """
            Remove ALL HTML tags, attributes, and elements EXCEPT <br> tags.
            Only <br> tags are kept for line breaks. All other HTML is completely removed.
            """
            if not text:
                return text
            
            # Step 1: Remove all HTML tags EXCEPT <br> and </br> (keep <br> for formatting)
            # This removes: <div>, <table>, <tr>, <td>, <span>, <p>, <a>, <img>, <script>, <style>, etc.
            text = re.sub(r'<(?!br\s*/?>|/br>)[^>]+>', '', text, flags=re.IGNORECASE)
            
            # Step 2: Remove shop tool positioning tags and markers (even if not in HTML tags)
            text = re.sub(r'SHOPTOOL_[^\s<>"]+', '', text, flags=re.IGNORECASE)
            text = re.sub(r'offer-template-\d+', '', text, flags=re.IGNORECASE)
            text = re.sub(r'关联营销[^\s<>"]*', '', text, flags=re.IGNORECASE)
            text = re.sub(r'SHOPTOOL_POSITION[^\s<>"]*', '', text, flags=re.IGNORECASE)
            
            # Step 3: Remove all HTML attributes (style, id, class, title, etc.)
            # These might appear as standalone attributes
            text = re.sub(r'\s*style\s*=\s*"[^"]*"', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\s*id\s*=\s*"[^"]*"', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\s*class\s*=\s*"[^"]*"', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\s*title\s*=\s*"[^"]*"', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\s*border\s*=\s*"[^"]*"', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\s*cellpadding\s*=\s*"[^"]*"', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\s*cellspacing\s*=\s*"[^"]*"', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\s*max-width\s*:\s*[^;]+;?', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\s*font-size\s*:\s*[^;]+;?', '', text, flags=re.IGNORECASE)
            text = re.sub(r'\s*line-height\s*:\s*[^;]+;?', '', text, flags=re.IGNORECASE)
            
            # Step 4: Remove HTTP/HTTPS URLs (full URLs)
            text = re.sub(r'https?://[^\s<>"\'()<br>]+', '', text)
            # Remove www URLs
            text = re.sub(r'www\.[^\s<>"\'()<br>]+', '', text)
            # Remove image file references using pre-compiled pattern
            text = IMAGE_EXT_PATTERN.sub('', text)
            # Remove image-related patterns
            text = re.sub(r'images?[^\s<>"\'()<br>]*', '', text, flags=re.IGNORECASE)
            text = re.sub(r'img[^\s<>"\'()<br>]*', '', text, flags=re.IGNORECASE)
            text = re.sub(r'picUrl[^\s<>"\'()<br>]*', '', text, flags=re.IGNORECASE)
            text = re.sub(r'imageUrl[^\s<>"\'()<br>]*', '', text, flags=re.IGNORECASE)
            # Remove any remaining URL patterns using pre-compiled pattern
            text = DOMAIN_PATTERN.sub('', text)
            
            # Step 5: Remove HTML entities (but keep common ones like &nbsp; converted to space)
            text = re.sub(r'&nbsp;', ' ', text, flags=re.IGNORECASE)
            text = re.sub(r'&[a-z]+;', '', text, flags=re.IGNORECASE)
            text = re.sub(r'&#\d+;', '', text)
            
            # Step 6: Remove any remaining HTML-like patterns
            text = re.sub(r'<[^>]*>', '', text)  # Catch any remaining HTML tags
            
            # Step 7: Remove newlines and carriage returns (replace with space)
            text = text.replace('\r\n', ' ').replace('\n', ' ').replace('\r', ' ')
            
            # Step 8: Normalize <br> tags (ensure consistent format)
            text = re.sub(r'<br\s*/?>', '<br>', text, flags=re.IGNORECASE)
            text = re.sub(r'</br>', '<br>', text, flags=re.IGNORECASE)
            
            # Step 9: Clean up multiple spaces and normalize <br> tags using pre-compiled patterns
            text = WHITESPACE_PATTERN.sub(' ', text)
            text = BR_TAG_PATTERN.sub('<br>', text)
            
            # Step 10: Final pass - remove any remaining HTML-like content
            # Remove any text that looks like HTML attributes or tags
            text = re.sub(r'[a-z]+\s*=\s*"[^"]*"', '', text, flags=re.IGNORECASE)
            
            return text.strip()
        
        # CRITICAL: Remove ALL HTML elements except <br> tags from descriptions
        # This ensures no unwanted markup from detail_json or generated content leaks through
        # Only <br> tags are kept for line breaks - all other HTML is completely removed
        if product_description:
            product_description = remove_all_html_except_br(product_description)
            # Ensure each sentence ends with <br>
            product_description = ensure_br_tags_global(product_description)
            # Final cleanup: ensure ONLY <br> tags remain, all other HTML removed
            product_description = re.sub(r'<(?!br\s*/?>)[^>]+>', '', product_description, flags=re.IGNORECASE)
            product_description = WHITESPACE_PATTERN.sub(' ', product_description)
            product_description = BR_TAG_PATTERN.sub('<br>', product_description)
            # Remove any remaining HTML attributes or entities
            product_description = re.sub(r'[a-z]+\s*=\s*"[^"]*"', '', product_description, flags=re.IGNORECASE)
            product_description = re.sub(r'&[a-z]+;', '', product_description, flags=re.IGNORECASE)
        if sales_description:
            sales_description = remove_all_html_except_br(sales_description)
            # Ensure each sentence ends with <br>
            sales_description = ensure_br_tags_global(sales_description)
            # Final cleanup: ensure ONLY <br> tags remain, all other HTML removed
            sales_description = re.sub(r'<(?!br\s*/?>)[^>]+>', '', sales_description, flags=re.IGNORECASE)
            sales_description = WHITESPACE_PATTERN.sub(' ', sales_description)
            sales_description = BR_TAG_PATTERN.sub('<br>', sales_description)
            # Remove any remaining HTML attributes or entities
            sales_description = re.sub(r'[a-z]+\s*=\s*"[^"]*"', '', sales_description, flags=re.IGNORECASE)
            sales_description = re.sub(r'&[a-z]+;', '', sales_description, flags=re.IGNORECASE)
        
        # Simplified: Always add extension text to both descriptions (no length checks)
        # Extension texts for product and sales descriptions
        product_extension_text = "楽天市場での販売実績も豊富で、お客様から高い評価をいただいております。<br>使いやすさと機能性を兼ね備えた設計で、多くのお客様にご愛用いただいています。"
        sales_extension_text = "この商品は、お求めやすい価格で、高い満足度を実現しています。<br>商品の魅力をぜひお試しください。<br>楽天市場での販売実績も多数あり、お客様からのレビューでも高い評価をいただいております。<br>この機会にぜひお買い求めください。"
        
        # Always add extension text to product_description (regardless of length)
        if product_description:
            product_description = f"{product_description}<br>{product_extension_text}"
        else:
            product_description = product_extension_text
        
        # Always add extension text to sales_description (regardless of length)
        if sales_description:
            sales_description = f"{sales_description}<br>{sales_extension_text}"
        else:
            sales_description = sales_extension_text
        
        # Ensure tagline exists
        if not tagline:
            # Convert product name to keyword format (space-separated)
            safe_name = product_name or "商品"
            # Split by common separators and join with half-width spaces
            keywords = re.split(r'[の・、,，\s]+', safe_name)
            keywords = [k.strip() for k in keywords if k.strip()]
            tagline = ' '.join(keywords)
            if len(tagline) < TAGLINE_MIN_LENGTH:
                tagline = f"{tagline} 楽天市場 人気 商品"
        
        # Convert tagline to keyword format (ensure half-width spaces)
        # Replace full-width spaces and other separators with half-width spaces
        tagline = re.sub(r'[の・、,，\s]+', ' ', tagline)  # Replace separators with half-width space
        tagline = re.sub(r'\s+', ' ', tagline)  # Normalize multiple spaces to single space
        tagline = tagline.strip()
        
        # Validate tagline length (70文字以下の場合はそのまま、70文字を超える場合のみ60-70文字に調整)
        if len(tagline) <= TAGLINE_MAX_LENGTH:
            # 70文字以下の場合はそのまま使用（最小長チェックなし）
            if len(tagline) < TAGLINE_MIN_LENGTH:
                logger.info(
                    f"ℹ️  Generated tagline is {len(tagline)} chars (below {TAGLINE_MIN_LENGTH}), but keeping as-is per requirements"
                )
        else:
            # 70文字を超える場合のみ60-70文字に調整
            logger.warning(
                f"⚠️  Generated tagline is too long ({len(tagline)} chars), truncating to {TAGLINE_MIN_LENGTH}-{TAGLINE_MAX_LENGTH} chars..."
            )
            # Truncate at word boundary (preserve keywords)
            tagline = truncate_at_word_boundary(tagline, TAGLINE_MAX_LENGTH, TAGLINE_MIN_LENGTH)
            if len(tagline) < TAGLINE_MIN_LENGTH:
                # If still too short after truncation, pad with keywords
                remaining = TAGLINE_MIN_LENGTH - len(tagline)
                tagline = f"{tagline} 商品"[:TAGLINE_MAX_LENGTH]
        
        # FINAL FORMAT ENFORCEMENT:
        # IMPORTANT: Remove any delivery messages from product_description (商品説明文)
        # Delivery message should ONLY be in sales_description (販売説明文)
        if product_description:
            # Remove any existing delivery messages from product_description
            for pattern in DELIVERY_PATTERNS:
                product_description = pattern.sub('', product_description)
            # Remove orphaned "ご。" patterns that may be left after delivery message removal
            product_description = re.sub(r'ご。', '', product_description)
            product_description = re.sub(r'ご\s*。', '', product_description)
            product_description = re.sub(r'。\s*ご\s*。', '。', product_description)
            product_description = re.sub(r'([^。\s])ご\s*。', r'\1。', product_description)
            product_description = re.sub(r'。\s*。+', '。', product_description)
            product_description = product_description.rstrip('<br>').rstrip()
        
        # IMPORTANT: Add delivery message ONLY to sales_description (販売説明文)
        sales_description = add_delivery_message(sales_description) if sales_description else DELIVERY_MESSAGE.lstrip('<br>')
        
        logger.info(
            f"✅ Generated descriptions: product_desc={len(product_description)} chars, "
            f"sales_desc={len(sales_description)} chars, tagline={len(tagline)} chars"
        )
        
        return ProductDescriptions(
            product_description=product_description,
            sales_description=sales_description,
            tagline=tagline,
        )
        
    except Exception as e:
        logger.error(f"Error generating product descriptions: {e}", exc_info=True)
        # Fallback: create professional fallback descriptions with guaranteed minimum lengths
        safe_name = product_name or "商品"
        safe_desc = existing_description or ""
        
        # Simplified fallback: Always add extension text (no length checks)
        product_extension_text = "楽天市場での販売実績も豊富で、お客様から高い評価をいただいております。<br>使いやすさと機能性を兼ね備えた設計で、多くのお客様にご愛用いただいています。"
        sales_extension_text = "この商品は、お求めやすい価格で、高い満足度を実現しています。<br>商品の魅力をぜひお試しください。<br>楽天市場での販売実績も多数あり、お客様からのレビューでも高い評価をいただいております。<br>この機会にぜひお買い求めください。"
        
        # Create base descriptions - always add extension text
        if safe_desc:
            fallback_prod_desc = f"{safe_desc}<br>{product_extension_text}"
            fallback_sales_desc = f"{safe_desc}<br>{sales_extension_text}"
        else:
            fallback_prod_desc = product_extension_text
            fallback_sales_desc = sales_extension_text
        
        # Ensure each sentence ends with <br>
        fallback_prod_desc = ensure_br_tags_global(fallback_prod_desc)
        fallback_sales_desc = ensure_br_tags_global(fallback_sales_desc)
        
        # Create fallback tagline in keyword format (half-width space separated)
        # Convert product name to keyword format
        keywords = re.split(r'[の・、,，\s]+', safe_name)
        keywords = [k.strip() for k in keywords if k.strip()]
        fallback_tagline = ' '.join(keywords)
        
        # Add additional keywords if needed
        if len(fallback_tagline) < TAGLINE_MIN_LENGTH:
            fallback_tagline = f"{fallback_tagline} 楽天市場 人気 商品"
        
        # Validate length: 70文字以下の場合はそのまま、70文字を超える場合のみ60-70文字に調整
        if len(fallback_tagline) > TAGLINE_MAX_LENGTH:
            fallback_tagline = truncate_at_word_boundary(fallback_tagline, TAGLINE_MAX_LENGTH, TAGLINE_MIN_LENGTH)
        
        # IMPORTANT: Remove delivery message from product_description in fallback
        if fallback_prod_desc:
            for pattern in DELIVERY_PATTERNS:
                fallback_prod_desc = pattern.sub('', fallback_prod_desc)
            # Remove orphaned "ご。" patterns that may be left after delivery message removal
            fallback_prod_desc = re.sub(r'ご。', '', fallback_prod_desc)
            fallback_prod_desc = re.sub(r'ご\s*。', '', fallback_prod_desc)
            fallback_prod_desc = re.sub(r'。\s*ご\s*。', '。', fallback_prod_desc)
            fallback_prod_desc = re.sub(r'([^。\s])ご\s*。', r'\1。', fallback_prod_desc)
            fallback_prod_desc = re.sub(r'。\s*。+', '。', fallback_prod_desc)
            fallback_prod_desc = fallback_prod_desc.rstrip('<br>').rstrip()
        # IMPORTANT: Add delivery message to sales_description in fallback
        fallback_sales_desc = add_delivery_message(fallback_sales_desc) if fallback_sales_desc else DELIVERY_MESSAGE.lstrip('<br>')
        
        return ProductDescriptions(
            product_description=fallback_prod_desc,
            sales_description=fallback_sales_desc,
            tagline=fallback_tagline,
        )


def generate_rakuten_product_content(
    product_name: str,
    detail_payload: Optional[Dict[str, Any]],
    existing_description: str = "",
) -> RakutenContent:
    """
    Generate complete Rakuten product content including title, catchphrase, and description.
    
    Args:
        product_name: The product name from product_origin.product_name
        detail_payload: The detail_json dictionary from product_origin table
        existing_description: Existing product description (optional)
        
    Returns:
        RakutenContent object with title, catchphrase, and description
    """
    try:
        # Extract product details from detail_json
        product_details = extract_product_details_from_detail_json(detail_payload)
        
        # Generate title using OpenAI GPT-4
        title = generate_product_title_with_openai(
            product_name=product_name,
            product_details=product_details,
            model="gpt-4o-mini",  # Use GPT-4 as specified
        )
        
        # Validate title - ensure it's not empty
        if not title or len(title.strip()) == 0:
            logger.warning(f"⚠️  Generated title is empty, using product_name as fallback")
            title = product_name or "商品名未設定"
            
        # Generate descriptions using GPT-4o-mini
        descriptions = generate_product_descriptions_with_openai(
            product_name=product_name,
            detail_payload=detail_payload,
            existing_description=existing_description,
            model="gpt-4o-mini",
        )
        
        # Map ProductDescriptions to RakutenContent format
        catchphrase = descriptions.tagline
        description = descriptions.product_description
        sales_description = descriptions.sales_description

        # FINAL SAFEGUARD: ensure every sentence in description and sales_description ends with <br>
        # This guarantees correct line breaks on Rakuten pages even if upstream processing changes.
        if description:
            description = ensure_br_tags_global(description)
        if sales_description:
            sales_description = ensure_br_tags_global(sales_description)
        
        # IMPORTANT: Remove any delivery messages from description (product_description)
        # Delivery message should ONLY be in sales_description (販売説明文)
        if description:
            # Remove any existing delivery messages from description
            for pattern in DELIVERY_PATTERNS:
                description = pattern.sub('', description)
            # Remove orphaned "ご。" patterns that may be left after delivery message removal
            description = re.sub(r'ご。', '', description)
            description = re.sub(r'ご\s*。', '', description)
            description = re.sub(r'。\s*ご\s*。', '。', description)
            description = re.sub(r'([^。\s])ご\s*。', r'\1。', description)
            description = re.sub(r'。\s*。+', '。', description)
            description = description.rstrip('<br>').rstrip()
        
        # IMPORTANT: Add delivery message ONLY to sales_description (販売説明文)
        sales_description = add_delivery_message(sales_description) if sales_description else DELIVERY_MESSAGE.lstrip('<br>')
        
    except Exception as e:
        logger.error(f"Error generating product content: {e}", exc_info=True)
        # Fallback: ensure all required fields are populated
        safe_name = product_name or "商品名未設定"
        safe_desc = existing_description or ""
        
        title = safe_name
        # Convert to keyword format (half-width space separated)
        keywords = re.split(r'[の・、,，\s]+', safe_name)
        keywords = [k.strip() for k in keywords if k.strip()]
        catchphrase = ' '.join(keywords)
        if len(catchphrase) < TAGLINE_MIN_LENGTH:
            catchphrase = f"{catchphrase} 楽天市場 人気 商品"
        # 70文字を超える場合のみ60-70文字に調整
        if len(catchphrase) > TAGLINE_MAX_LENGTH:
            catchphrase = truncate_at_word_boundary(catchphrase, TAGLINE_MAX_LENGTH, TAGLINE_MIN_LENGTH)
        
        # Simplified: Always add extension text (no length checks)
        product_extension_text = "楽天市場での販売実績も豊富で、お客様から高い評価をいただいております。<br>使いやすさと機能性を兼ね備えた設計で、多くのお客様にご愛用いただいています。"
        sales_extension_text = "この商品は、お求めやすい価格で、高い満足度を実現しています。<br>商品の魅力をぜひお試しください。<br>楽天市場での販売実績も多数あり、お客様からのレビューでも高い評価をいただいております。<br>この機会にぜひお買い求めください。"
        
        # Create base descriptions - always add extension text
        if safe_desc:
            description = f"{safe_desc}<br>{product_extension_text}"
            sales_description = f"{safe_desc}<br>{sales_extension_text}"
        else:
            description = product_extension_text
            sales_description = sales_extension_text
        
        # Ensure each sentence ends with <br>
        description = ensure_br_tags_global(description)
        sales_description = ensure_br_tags_global(sales_description)
        
        # IMPORTANT: Remove any delivery messages from description (product_description)
        # Delivery message should ONLY be in sales_description (販売説明文)
        if description:
            for pattern in DELIVERY_PATTERNS:
                description = pattern.sub('', description)
            # Remove orphaned "ご。" patterns that may be left after delivery message removal
            description = re.sub(r'ご。', '', description)
            description = re.sub(r'ご\s*。', '', description)
            description = re.sub(r'。\s*ご\s*。', '。', description)
            description = re.sub(r'([^。\s])ご\s*。', r'\1。', description)
            description = re.sub(r'。\s*。+', '。', description)
            description = description.rstrip('<br>').rstrip()
        
        # IMPORTANT: Add delivery message ONLY to sales_description (販売説明文)
        sales_description = add_delivery_message(sales_description) if sales_description else DELIVERY_MESSAGE.lstrip('<br>')
    
    return RakutenContent(
        title=title,
        catchphrase=catchphrase,
        description=description,
        sales_description=sales_description,
    )


def generate_marketing_copy(
    product_name: str,
    product_description: str = "",
    *,
    model: str = "gpt-4o-mini",
) -> str:
    """
    Generate marketing copy for a product (legacy function for backward compatibility).
    
    Args:
        product_name: The product name
        product_description: Product description
        model: OpenAI model to use
        
    Returns:
        Generated marketing copy
    """
    client = _get_openai_client()
    if not client:
        logger.warning("OpenAI client not available, returning product description")
        return product_description or ""
    
    try:
        prompt = f"""以下の商品情報を元に、楽天市場向けの商品説明文を生成してください。

商品名: {product_name}

商品説明: {product_description or "なし"}

簡潔で魅力的な商品説明文を日本語で作成してください。
"""

        response = client.chat.completions.create(
            model=model,
            messages=[
                {
                    "role": "system",
                    "content": "あなたは楽天市場向けの商品説明文作成の専門家です。"
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.6,
            max_tokens=2000,
        )
        
        return response.choices[0].message.content.strip()
        
    except Exception as e:
        logger.error(f"Error generating marketing copy: {e}")
        return product_description or ""

