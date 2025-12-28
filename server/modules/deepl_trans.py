"""
DeepL Translation Module for E-commerce Product Data

This module provides translation functionality using the DeepL API with specialized
support for Chinese-to-Japanese translation of product variant selectors (colors, sizes).

Key Features:
- Chinese/Japanese/English text translation via DeepL API
- Comprehensive normalization map for fixing translation errors
- Text cleaning for Rakuten API compatibility (32-byte limit, no machine-dependent chars)
- Language detection and automatic source language handling
- Translation caching and rate limiting

Usage:
    from modules.deepl_trans import translate_to_japanese, clean_variant_value
    
    # Translate Chinese to Japanese
    result = translate_to_japanese("黑色")  # Returns "ブラック"
    
    # Clean variant value for Rakuten API
    value = clean_variant_value("黑色加绒", key="color")  # Returns "ブラック"
"""

from __future__ import annotations

import logging
import os
import re
import string
import time
import unicodedata
import json
import pathlib
from typing import Any, Dict, List, Optional, Set, Tuple

# =============================================================================
# OPTIONAL IMPORTS
# =============================================================================

try:
    from deepl import Translator
    DEEPL_AVAILABLE = True
except ImportError:
    Translator = None  # type: ignore
    DEEPL_AVAILABLE = False

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

logger = logging.getLogger(__name__)


# =============================================================================
# CONFIGURATION LOADING
# =============================================================================

def _get_config_path() -> pathlib.Path:
    """Get path to translation config JSON file."""
    module_dir = pathlib.Path(__file__).resolve().parent
    project_root = module_dir.parent
    config_path = project_root / "translation_config.json"
    return config_path


def _load_translation_config() -> Dict[str, Any]:
    """Load translation configuration from JSON file."""
    config_path = _get_config_path()
    
    if not config_path.exists():
        logger.warning(f"Translation config file not found at {config_path}, using defaults")
        return {}
    
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            config = json.load(f)
        logger.debug(f"Translation config loaded from {config_path}")
        return config
    except Exception as e:
        logger.error(f"Failed to load translation config: {e}, using defaults")
        return {}


# Load configuration
_translation_config = _load_translation_config()


def reload_translation_config() -> bool:
    """Reload translation configuration from JSON file."""
    global _translation_config, TRANSLATION_MAP, PATTERN_DICTIONARY, REMOVAL_PATTERNS
    global CHINESE_BRACKETS, CHINESE_INDICATORS, SIZE_VALUES
    global CJK_RANGE, CJK_EXT_A_RANGE, HIRAGANA_RANGE, KATAKANA_RANGE, PRIVATE_USE_RANGE
    global _SORTED_MAP
    
    try:
        _translation_config = _load_translation_config()
        
        # Reload all configuration values
        _reload_config_values()
        
        logger.info("Translation configuration reloaded successfully")
        return True
    except Exception as e:
        logger.error(f"Failed to reload translation config: {e}")
        return False


def _reload_config_values():
    """Reload all configuration values from _translation_config."""
    global TRANSLATION_MAP, PATTERN_DICTIONARY, REMOVAL_PATTERNS
    global CHINESE_BRACKETS, CHINESE_INDICATORS, SIZE_VALUES
    global CJK_RANGE, CJK_EXT_A_RANGE, HIRAGANA_RANGE, KATAKANA_RANGE, PRIVATE_USE_RANGE
    global _SORTED_MAP
    
    # Initialize dictionaries if they don't exist yet
    if 'TRANSLATION_MAP' not in globals():
        TRANSLATION_MAP = {}
    if 'PATTERN_DICTIONARY' not in globals():
        PATTERN_DICTIONARY = {}
    if 'REMOVAL_PATTERNS' not in globals():
        REMOVAL_PATTERNS = {}
    
    # Load translation map
    TRANSLATION_MAP.clear()
    if "translation_map" in _translation_config:
        TRANSLATION_MAP.update(_translation_config["translation_map"])
    
    # Load pattern dictionary
    PATTERN_DICTIONARY.clear()
    if "pattern_dictionary" in _translation_config:
        PATTERN_DICTIONARY.update(_translation_config["pattern_dictionary"])
    
    # Load removal patterns
    REMOVAL_PATTERNS.clear()
    if "removal_patterns" in _translation_config:
        REMOVAL_PATTERNS.update(_translation_config["removal_patterns"])
    
    # Load Chinese brackets
    if "chinese_brackets" in _translation_config:
        global CHINESE_BRACKETS
        CHINESE_BRACKETS = tuple(_translation_config["chinese_brackets"])
    
    # Load Chinese indicators
    if "chinese_indicators" in _translation_config:
        global CHINESE_INDICATORS
        CHINESE_INDICATORS = set(_translation_config["chinese_indicators"])
    
    # Load size values
    if "size_values" in _translation_config:
        global SIZE_VALUES
        SIZE_VALUES = set(_translation_config["size_values"])
    
    # Load Unicode ranges
    if "unicode_ranges" in _translation_config:
        ranges = _translation_config["unicode_ranges"]
        global CJK_RANGE, CJK_EXT_A_RANGE, HIRAGANA_RANGE, KATAKANA_RANGE, PRIVATE_USE_RANGE
        CJK_RANGE = tuple(ranges.get("cjk_range", [0x4E00, 0x9FFF]))
        CJK_EXT_A_RANGE = tuple(ranges.get("cjk_ext_a_range", [0x3400, 0x4DBF]))
        HIRAGANA_RANGE = tuple(ranges.get("hiragana_range", [0x3040, 0x309F]))
        KATAKANA_RANGE = tuple(ranges.get("katakana_range", [0x30A0, 0x30FF]))
        PRIVATE_USE_RANGE = tuple(ranges.get("private_use_range", [0xE000, 0xF8FF]))
    
    # Rebuild sorted map
    if 'TRANSLATION_MAP' in globals() and TRANSLATION_MAP:
        global _SORTED_MAP
        _SORTED_MAP = sorted(
            TRANSLATION_MAP.items(),
            key=lambda x: len(x[0]),
            reverse=True
        )


# =============================================================================
# CONFIGURATION
# =============================================================================

class Config:
    """Centralized configuration for DeepL translation module."""
    
    @staticmethod
    def _get_config() -> Dict[str, Any]:
        """Get config section from translation config."""
        return _translation_config.get("config", {})
    
    @staticmethod
    def get_API_KEY() -> Optional[str]:
        config = Config._get_config()
        return config.get("api_key") or os.getenv("DEEPL_API_KEY")
    
    @staticmethod
    def get_SERVER_URL() -> Optional[str]:
        config = Config._get_config()
        return config.get("server_url") or os.getenv("DEEPL_SERVER_URL")
    
    @staticmethod
    def get_RATE_LIMIT_DELAY() -> float:
        config = Config._get_config()
        return float(config.get("rate_limit_delay", os.getenv("DEEPL_RATE_LIMIT_DELAY", "0.1")))
    
    @staticmethod
    def get_MAX_RETRIES() -> int:
        config = Config._get_config()
        return int(config.get("max_retries", os.getenv("DEEPL_MAX_RETRIES", "3")))
    
    @staticmethod
    def get_RETRY_DELAY() -> float:
        config = Config._get_config()
        return float(config.get("retry_delay", os.getenv("DEEPL_RETRY_DELAY", "1.0")))
    
    @staticmethod
    def get_CACHE_MAX_SIZE() -> int:
        config = Config._get_config()
        return int(config.get("cache_max_size", 10000))
    
    @staticmethod
    def get_MAX_BYTES() -> int:
        config = Config._get_config()
        return int(config.get("max_bytes", 32))
    
    # Properties for backward compatibility
    @property
    def API_KEY(self) -> Optional[str]:
        return Config.get_API_KEY()
    
    @property
    def SERVER_URL(self) -> Optional[str]:
        return Config.get_SERVER_URL()
    
    @property
    def RATE_LIMIT_DELAY(self) -> float:
        return Config.get_RATE_LIMIT_DELAY()
    
    @property
    def MAX_RETRIES(self) -> int:
        return Config.get_MAX_RETRIES()
    
    @property
    def RETRY_DELAY(self) -> float:
        return Config.get_RETRY_DELAY()
    
    @property
    def CACHE_MAX_SIZE(self) -> int:
        return Config.get_CACHE_MAX_SIZE()
    
    @property
    def MAX_BYTES(self) -> int:
        return Config.get_MAX_BYTES()


# Create Config instance for backward compatibility
Config = Config()


# =============================================================================
# UNICODE CONSTANTS
# =============================================================================

# Initialize from config with defaults
def _init_unicode_ranges():
    """Initialize Unicode ranges from config."""
    if "unicode_ranges" in _translation_config:
        ranges = _translation_config["unicode_ranges"]
        return (
            tuple(ranges.get("cjk_range", [0x4E00, 0x9FFF])),
            tuple(ranges.get("cjk_ext_a_range", [0x3400, 0x4DBF])),
            tuple(ranges.get("hiragana_range", [0x3040, 0x309F])),
            tuple(ranges.get("katakana_range", [0x30A0, 0x30FF])),
            tuple(ranges.get("private_use_range", [0xE000, 0xF8FF]))
        )
    return (
        (0x4E00, 0x9FFF),
        (0x3400, 0x4DBF),
        (0x3040, 0x309F),
        (0x30A0, 0x30FF),
        (0xE000, 0xF8FF)
    )

CJK_RANGE, CJK_EXT_A_RANGE, HIRAGANA_RANGE, KATAKANA_RANGE, PRIVATE_USE_RANGE = _init_unicode_ranges()

# Characters rejected by Rakuten API
def _init_chinese_brackets() -> Tuple[str, ...]:
    """Initialize Chinese brackets from config."""
    if "chinese_brackets" in _translation_config:
        return tuple(_translation_config["chinese_brackets"])
    # Return empty tuple - all values should come from JSON config file
    return tuple()

CHINESE_BRACKETS: Tuple[str, ...] = _init_chinese_brackets()

# Chinese character indicators for language detection
def _init_chinese_indicators() -> Set[str]:
    """Initialize Chinese indicators from config."""
    if "chinese_indicators" in _translation_config:
        return set(_translation_config["chinese_indicators"])
    # Return empty set - all values should come from JSON config file
    return set()

CHINESE_INDICATORS: Set[str] = _init_chinese_indicators()

# Standard size values
def _init_size_values() -> Set[str]:
    """Initialize size values from config."""
    if "size_values" in _translation_config:
        return set(_translation_config["size_values"])
    # Return empty set - all values should come from JSON config file
    return set()

SIZE_VALUES: Set[str] = _init_size_values()


# =============================================================================
# PATTERN DICTIONARY
# =============================================================================

# Character pattern dictionary for word boundary detection and text processing
# This dictionary can be continuously extended with new patterns as needed
def _init_pattern_dictionary() -> Dict[str, List[str]]:
    """Initialize pattern dictionary from config."""
    if "pattern_dictionary" in _translation_config:
        return dict(_translation_config["pattern_dictionary"])
    # Return empty dict - all values should come from JSON config file
    return {}


def get_patterns(category: Optional[str] = None) -> List[str]:
    """
    Get patterns from the pattern dictionary.
    
    Args:
        category: Specific category to retrieve (e.g., 'colors', 'neck_styles').
                  If None, returns all patterns from all categories.
        
    Returns:
        List of patterns. If category is specified, returns patterns for that category.
        If category is None, returns all patterns from all categories.
        
    Example:
        >>> get_patterns('colors')
        ['ホワイト', 'ブラック', 'グレー', ...]
        >>> get_patterns()
        ['ネック', 'ハイネック', 'ホワイト', ...]  # All patterns
    """
    if category:
        return PATTERN_DICTIONARY.get(category, [])
    
    # Return all patterns from all categories
    all_patterns = []
    for patterns in PATTERN_DICTIONARY.values():
        all_patterns.extend(patterns)
    return all_patterns


def add_pattern(category: str, pattern: str) -> bool:
    """
    Add a new pattern to the pattern dictionary.
    
    Args:
        category: Category name (e.g., 'colors', 'neck_styles').
                  If category doesn't exist, it will be created.
        pattern: Pattern string to add.
        
    Returns:
        True if pattern was added, False if it already exists.
        
    Example:
        >>> add_pattern('colors', 'ターコイズ')
        True
    """
    if category not in PATTERN_DICTIONARY:
        PATTERN_DICTIONARY[category] = []
    
    if pattern not in PATTERN_DICTIONARY[category]:
        PATTERN_DICTIONARY[category].append(pattern)
        return True
    
    return False


def remove_pattern(category: str, pattern: str) -> bool:
    """
    Remove a pattern from the pattern dictionary.
    
    Args:
        category: Category name.
        pattern: Pattern string to remove.
        
    Returns:
        True if pattern was removed, False if it didn't exist.
    """
    if category in PATTERN_DICTIONARY:
        if pattern in PATTERN_DICTIONARY[category]:
            PATTERN_DICTIONARY[category].remove(pattern)
            return True
    return False


def get_all_categories() -> List[str]:
    """
    Get all available pattern categories.
    
    Returns:
        List of category names.
    """
    return list(PATTERN_DICTIONARY.keys())


# =============================================================================
# TRANSLATION MAP
# =============================================================================

# Comprehensive Chinese-to-Japanese translation map
# Note: Order matters for dict iteration. Longer terms should match first via _SORTED_MAP
def _init_translation_map() -> Dict[str, str]:
    """Initialize translation map from config."""
    if "translation_map" in _translation_config:
        return dict(_translation_config["translation_map"])
    # Return empty dict - all values should come from JSON config file
    return {}

TRANSLATION_MAP: Dict[str, str] = _init_translation_map()


# =============================================================================
# REMOVAL PATTERNS DICTIONARY
# =============================================================================

# Dictionary of patterns to remove before translation
# This dictionary can be continuously extended with new removal patterns
def _init_removal_patterns() -> Dict[str, List[str]]:
    """Initialize removal patterns from config."""
    if "removal_patterns" in _translation_config:
        return dict(_translation_config["removal_patterns"])
    # Return empty dict - all values should come from JSON config file
    return {}

REMOVAL_PATTERNS: Dict[str, List[str]] = _init_removal_patterns()


def get_removal_patterns(category: Optional[str] = None) -> List[str]:
    """
    Get removal patterns from the removal patterns dictionary.
    
    Args:
        category: Specific category to retrieve (e.g., 'symbols', 'brackets').
                  If None, returns all patterns from all categories.
        
    Returns:
        List of patterns to remove.
    """
    if category:
        return REMOVAL_PATTERNS.get(category, [])
    
    # Return all patterns from all categories
    all_patterns = []
    for patterns in REMOVAL_PATTERNS.values():
        all_patterns.extend(patterns)
    return all_patterns


def add_removal_pattern(category: str, pattern: str) -> bool:
    """
    Add a new removal pattern to the removal patterns dictionary.
    
    Args:
        category: Category name (e.g., 'symbols', 'brackets').
                  If category doesn't exist, it will be created.
        pattern: Pattern string to add.
        
    Returns:
        True if pattern was added, False if it already exists.
    """
    if category not in REMOVAL_PATTERNS:
        REMOVAL_PATTERNS[category] = []
    
    if pattern not in REMOVAL_PATTERNS[category]:
        REMOVAL_PATTERNS[category].append(pattern)
        return True
    
    return False


def remove_unwanted_patterns(text: str) -> str:
    """
    Remove unwanted patterns (symbols, brackets, quality terms, etc.) from text before translation.
    
    This function removes patterns defined in REMOVAL_PATTERNS dictionary,
    including product codes, brackets with content, quality terms, and symbols.
    
    Args:
        text: Input text that may contain unwanted patterns
        
    Returns:
        Text with unwanted patterns removed
        
    Example:
        >>> remove_unwanted_patterns("K19-ミリタリーグリーン【高品質】")
        'ミリタリーグリーン'
        >>> remove_unwanted_patterns("K19-ネイビーブルー【高品質】")
        'ネイビーブルー'
    """
    if not text:
        return text
    
    result = text
    
    # Remove product codes (pattern: letter(s) + numbers + dash/hyphen at start)
    # Examples: K19-, K19, K-19, K_19, A123-, etc.
    result = re.sub(r'^[A-Za-z]+\d+[-_・]?', '', result)
    result = re.sub(r'^[A-Za-z]+\d+[-_・]\s*', '', result)
    
    # Remove brackets and their contents (Chinese and Japanese brackets)
    # Handle various bracket types: 【】, [], （）, ()
    result = re.sub(r'【[^】]*】', '', result)
    result = re.sub(r'\[[^\]]*\]', '', result)
    result = re.sub(r'（[^）]*）', '', result)
    result = re.sub(r'\([^)]*\)', '', result)
    result = re.sub(r'「[^」]*」', '', result)
    result = re.sub(r'『[^』]*』', '', result)
    result = re.sub(r'〔[^〕]*〕', '', result)
    result = re.sub(r'〖[^〗]*〗', '', result)
    
    # Remove standalone bracket characters
    for bracket in CHINESE_BRACKETS:
        result = result.replace(bracket, '')
    
    # Dynamically process all removal pattern categories from config
    # This allows custom categories to be added through the UI
    for category_name, patterns in REMOVAL_PATTERNS.items():
        if not patterns:
            continue
        
        # Sort by length (longest first) to avoid partial matches
        sorted_patterns = sorted(patterns, key=len, reverse=True)
        
        # Special handling for 'common_suffixes' - only remove at the end
        if category_name == 'common_suffixes':
            for suffix in sorted_patterns:
                if result.endswith(suffix):
                    result = result[:-len(suffix)]
        # Special handling for 'symbols' - replace with space
        elif category_name == 'symbols':
            for symbol in sorted_patterns:
                result = result.replace(symbol, ' ')
        # Default: remove the pattern entirely
        else:
            for pattern in sorted_patterns:
                result = result.replace(pattern, '')
    
    # Clean up multiple spaces and trim
    result = re.sub(r'\s+', ' ', result).strip()
    
    return result

# Pre-sorted for longest-first matching (prevents partial replacements)
def _init_sorted_map() -> List[Tuple[str, str]]:
    """Initialize sorted translation map."""
    return sorted(
        TRANSLATION_MAP.items(),
        key=lambda x: len(x[0]),
        reverse=True
    )

_SORTED_MAP: List[Tuple[str, str]] = _init_sorted_map()

# Dictionary for multi-language lookups (used by translate function)
_DICTIONARY: Dict[str, Dict[str, str]] = {
    "尺码": {"EN": "size", "JA": "サイズ"},
    "颜色": {"EN": "color", "JA": "カラー"},
    "规格": {"EN": "specification", "JA": "仕様"},
    "款式": {"EN": "style", "JA": "スタイル"},
    "材质": {"EN": "material", "JA": "素材"},
    "品牌": {"EN": "brand", "JA": "ブランド"},
    "均码": {"EN": "free size", "JA": "フリーサイズ"},
    "黑色": {"EN": "black", "JA": "ブラック"},
    "白色": {"EN": "white", "JA": "ホワイト"},
    "灰色": {"EN": "gray", "JA": "グレー"},
    "红色": {"EN": "red", "JA": "レッド"},
    "蓝色": {"EN": "blue", "JA": "ブルー"},
    "绿色": {"EN": "green", "JA": "グリーン"},
    "黄色": {"EN": "yellow", "JA": "イエロー"},
    "粉色": {"EN": "pink", "JA": "ピンク"},
    "紫色": {"EN": "purple", "JA": "パープル"},
    "橙色": {"EN": "orange", "JA": "オレンジ"},
    "棕色": {"EN": "brown", "JA": "ブラウン"},
    "银色": {"EN": "silver", "JA": "シルバー"},
    "金色": {"EN": "gold", "JA": "ゴールド"},
    "サイズ": {"EN": "size"},
    "カラー": {"EN": "color"},
}


# =============================================================================
# MODULE STATE
# =============================================================================

_translator: Optional[Any] = None
_cache: Dict[str, str] = {}


# =============================================================================
# CHARACTER DETECTION HELPERS
# =============================================================================

def _in_range(code: int, range_tuple: Tuple[int, int]) -> bool:
    """Check if Unicode code point is within range."""
    return range_tuple[0] <= code <= range_tuple[1]


def _is_cjk(char: str) -> bool:
    """Check if character is CJK (Chinese/Japanese/Korean)."""
    return _in_range(ord(char), CJK_RANGE)


def _is_hiragana(char: str) -> bool:
    """Check if character is Hiragana."""
    return _in_range(ord(char), HIRAGANA_RANGE)


def _is_katakana(char: str) -> bool:
    """Check if character is Katakana."""
    return _in_range(ord(char), KATAKANA_RANGE)


def _is_kana(char: str) -> bool:
    """Check if character is Japanese kana (Hiragana or Katakana)."""
    return _is_hiragana(char) or _is_katakana(char)


def _is_japanese_safe(char: str) -> bool:
    """Check if character is safe for Japanese text (kana or ASCII)."""
    code = ord(char)
    return _is_kana(char) or (0x20 <= code <= 0x7E)


def _has_chinese_chars(text: str) -> bool:
    """Check if text contains Chinese indicator characters."""
    return bool(CHINESE_INDICATORS & set(text))


def _get_byte_length(text: str) -> int:
    """Get UTF-8 byte length of text."""
    return len(text.encode('utf-8')) if text else 0


# =============================================================================
# TEXT NORMALIZATION
# =============================================================================

def normalize(text: str) -> str:
    """
    Apply translation normalization map to fix Chinese-to-Japanese errors.
    
    Maps Chinese terms to Japanese equivalents and removes filler text.
    Uses longest-first matching to prevent partial replacements.
    
    IMPORTANT: For mixed Chinese-Japanese text (e.g., "高领ベージュホワイト"),
    only the Chinese parts are translated, preserving Japanese parts.
    
    Args:
        text: Input text that may contain Chinese characters
        
    Returns:
        Normalized text with corrections applied
        
    Example:
        >>> normalize("黑色加绒")
        'ブラック'
        >>> normalize("高领ベージュホワイト")
        'ハイネックベージュホワイト'  # Chinese part translated, Japanese preserved
        >>> normalize("ダークグレー")
        'ダークグレー'  # Pure Japanese text returned as-is
    """
    if not text:
        return text
    
    # Step 1: Remove unwanted patterns (symbols, brackets, quality terms, etc.)
    result = remove_unwanted_patterns(text)
    
    if not result:
        return ""
    
    # Check if text contains Chinese characters (even if mixed with Japanese)
    has_chinese = any(_is_cjk(c) and c not in '・' for c in result)
    has_japanese_kana = any(_is_kana(c) for c in result)
    
    # If pure Japanese (no Chinese characters), return as-is
    if has_japanese_kana and not has_chinese:
        return result.strip()
    
    # Apply normalization map to translate Chinese parts
    # This works for both pure Chinese and mixed Chinese-Japanese text
    for term, replacement in _SORTED_MAP:
        if term in result:
            result = result.replace(term, replacement)
    
    # Remove LLQ suffix variants
    result = re.sub(r'L\s*L\s*Q', '', result, flags=re.IGNORECASE)
    
    # Clean up whitespace
    result = re.sub(r'\s{2,}', ' ', result).strip()
    return result


# Backward compatibility alias
_apply_normalization_map = normalize


# =============================================================================
# TEXT CLEANING
# =============================================================================

def clean_for_rakuten(text: str, strict: bool = False) -> str:
    """
    Clean text for Rakuten API compatibility.
    
    Removes:
    - Control characters (except standard whitespace)
    - Private use area characters
    - Zero-width characters
    - Chinese brackets 【】
    - Variation selectors
    
    Args:
        text: Input text
        strict: If True, only allow ASCII + Japanese kana/kanji
        
    Returns:
        Cleaned text safe for Rakuten API
    """
    if not text:
        return text
    
    # Unicode normalization
    result = unicodedata.normalize('NFKC', text)
    
    # Remove Chinese brackets and their contents
    result = re.sub(r'【[^】]*】', '', result)
    for bracket in CHINESE_BRACKETS:
        result = result.replace(bracket, '')
    
    if strict:
        # Remove promotional phrases
        for phrase in ('热卖推荐', '热销', '推荐', '新品', '特价', '限时', '抢购', '热卖', '爆款', '畅销', '人气'):
            result = result.replace(phrase, '')
        
        # Extract color part after dash (e.g., "8888-黑色" -> "黑色")
        if '-' in result:
            parts = result.split('-')
            if len(parts) > 1:
                last = parts[-1].strip()
                if any(_is_cjk(c) for c in last) and 2 <= len(last) <= 15:
                    result = last
        
        # Remove product codes
        result = re.sub(r'^\s*[89]\d{3}\s*[-・\s]*', '', result)
        result = re.sub(r'[-・\s]+[89]\d{3}\s*$', '', result)
        result = re.sub(r'^\d+', '', result)
    
    # Filter problematic characters
    cleaned = []
    for char in result:
        code = ord(char)
        # Skip control characters (except tab, newline, carriage return, space)
        if code <= 0x1F and code not in {0x09, 0x0A, 0x0D, 0x20}:
            continue
        # Skip extended control characters
        if 0x7F <= code <= 0x9F:
            continue
        # Skip private use area
        if _in_range(code, PRIVATE_USE_RANGE):
            continue
        cleaned.append(char)
    
    result = ''.join(cleaned)
    
    # Remove zero-width and variation selector characters
    result = re.sub(r'[\u200B-\u200D\uFEFF\u2060\uFE00-\uFE0F]', '', result)
    result = result.replace('\u3000', ' ')  # Ideographic space to regular space
    
    if strict:
        # Only allow ASCII + Japanese characters
        result = ''.join(
            c for c in result
            if ord(c) < 0x80 or _is_kana(c) or _is_cjk(c) or _in_range(ord(c), CJK_EXT_A_RANGE)
        )
    
    # Convert full-width ASCII to half-width
    for i in range(0x21, 0x7F):
        result = result.replace(chr(0xFF00 + i - 0x20), chr(i))
    
    return result.strip()


# Backward compatibility aliases
clean_text_for_rakuten = clean_for_rakuten
clean_translation_result = lambda text, target_lang="JA", strict=False: clean_for_rakuten(text, strict or target_lang == "JA")


# =============================================================================
# TEXT TRUNCATION
# =============================================================================

def _get_char_type(char: str) -> str:
    """Get character type for word boundary detection."""
    if _is_katakana(char):
        return 'katakana'
    elif _is_hiragana(char):
        return 'hiragana'
    elif _is_cjk(char):
        return 'cjk'
    elif char.isalnum() and ord(char) < 128:
        return 'ascii'
    elif char.isspace():
        return 'space'
    else:
        return 'other'


def _split_long_katakana_sequence(text: str) -> List[str]:
    """
    Split long katakana sequences into meaningful units.
    
    Attempts to split at common word boundaries in katakana text.
    Uses patterns from PATTERN_DICTIONARY for word boundary detection.
    """
    if not text or len(text) < 6:
        return [text]
    
    # Get all patterns from the pattern dictionary
    # Prioritize longer patterns first to avoid partial matches
    patterns = get_patterns()
    patterns = sorted(patterns, key=len, reverse=True)
    
    words = []
    remaining = text
    i = 0
    
    while remaining and i < 20:  # Safety limit
        i += 1
        best_match = None
        best_pos = -1
        
        # Find the earliest matching pattern
        for pattern in patterns:
            pos = remaining.find(pattern)
            if pos != -1 and (best_pos == -1 or pos < best_pos):
                best_match = pattern
                best_pos = pos
        
        if best_match and best_pos >= 0:
            # Add text before the pattern
            if best_pos > 0:
                words.append(remaining[:best_pos])
            # Add the pattern itself
            words.append(best_match)
            # Continue with remaining text
            remaining = remaining[best_pos + len(best_match):]
        else:
            # No more patterns found
            if remaining:
                words.append(remaining)
            break
    
    return [w for w in words if w] if words else [text]


def _split_into_words(text: str) -> List[str]:
    """
    Split text into words based on character type boundaries and semantic units.
    
    Word boundaries occur at:
    - Transitions between character types (katakana, hiragana, CJK, ASCII)
    - Spaces
    - Punctuation marks
    - Long katakana sequences are split into meaningful units
    """
    if not text:
        return []
    
    words = []
    current_word = []
    prev_type = None
    
    for char in text:
        char_type = _get_char_type(char)
        
        # Space always breaks words
        if char_type == 'space':
            if current_word:
                words.append(''.join(current_word))
                current_word = []
            prev_type = None
            continue
        
        # Punctuation breaks words
        if char in string.punctuation or char in '・':
            if current_word:
                words.append(''.join(current_word))
                current_word = []
            prev_type = None
            continue
        
        # Check for type transition (word boundary)
        if prev_type is not None and char_type != prev_type:
            # Special case: katakana and hiragana can be grouped together
            if (prev_type in ('katakana', 'hiragana') and char_type in ('katakana', 'hiragana')):
                current_word.append(char)
            else:
                # Type change - start new word
                if current_word:
                    words.append(''.join(current_word))
                current_word = [char]
        else:
            current_word.append(char)
        
        prev_type = char_type
    
    # Add remaining word
    if current_word:
        words.append(''.join(current_word))
    
    # Post-process: split long katakana sequences into meaningful units
    final_words = []
    for word in words:
        if word and _is_katakana(word[0]):
            # Check if it's a long katakana sequence
            if len(word) >= 6:
                # Try to split into meaningful units
                split_words = _split_long_katakana_sequence(word)
                final_words.extend(split_words)
            else:
                final_words.append(word)
        else:
            final_words.append(word)
    
    return final_words


def truncate_to_bytes(text: str, max_bytes: int = 32, fallback: str = "Value") -> str:
    """
    Truncate text to fit within byte limit, preserving word boundaries.
    
    For Japanese/Chinese text, word boundaries are detected based on character
    type transitions (katakana, hiragana, CJK, ASCII) rather than spaces.
    
    Args:
        text: Text to truncate
        max_bytes: Maximum byte length (default: 32 for Rakuten)
        fallback: Fallback value if text is empty
        
    Returns:
        Truncated text within byte limit, cut at word boundaries
        
    Example:
        >>> truncate_to_bytes("ハイネックベージュホワイト", 32)
        'ハイネックベージュホワイト'  # If within limit
        >>> truncate_to_bytes("ハイネックベージュホワイト", 20)
        'ハイネックベージュ'  # Cut at word boundary
    """
    if not text:
        return fallback[:10] if fallback else "Value"
    
    if _get_byte_length(text) <= max_bytes:
        return text
    
    # Split into words based on character type boundaries
    words = _split_into_words(text)
    
    if not words:
        # No words found, fall back to character-by-character
        result = text
        while _get_byte_length(result) > max_bytes and result:
            result = result[:-1]
        return result.strip() or (fallback[:10] if fallback else "Value")
    
    # Try to fit words one by one
    result_words = []
    for word in words:
        # Check if adding this word would exceed limit
        candidate = ''.join(result_words + [word])
        candidate_bytes = _get_byte_length(candidate)
        
        if candidate_bytes <= max_bytes:
            result_words.append(word)
        else:
            # This word doesn't fit - stop here
            break
    
    # If we have at least one word, return it
    if result_words:
        result = ''.join(result_words)
        # Ensure it's within limit (should be, but double-check)
        if _get_byte_length(result) > max_bytes:
            # Edge case: last word made it exceed, remove it
            if len(result_words) > 1:
                result_words.pop()
                result = ''.join(result_words)
        
        # Final safety check - if still too long, we need to truncate the last word
        # But try to preserve word boundaries by removing the last word entirely
        while _get_byte_length(result) > max_bytes and result_words:
            result_words.pop()
            result = ''.join(result_words)
        
        # If still too long after removing words, truncate character by character
        # This should only happen if a single word exceeds the limit
        while _get_byte_length(result) > max_bytes and result:
            result = result[:-1]
        
        return result.strip() or (fallback[:10] if fallback else "Value")
    
    # No words fit - try to fit at least part of the first word
    # This is a last resort, but we try to preserve meaning by keeping
    # complete character sequences (e.g., complete katakana sequences)
    if words:
        first_word = words[0]
        result = ""
        for char in first_word:
            candidate = result + char
            if _get_byte_length(candidate) <= max_bytes:
                result = candidate
            else:
                break
        
        if result:
            return result.strip() or (fallback[:10] if fallback else "Value")
    
    # Absolute fallback: character-by-character truncation
    result = text
    while _get_byte_length(result) > max_bytes and result:
        result = result[:-1]
    
    return result.strip() or (fallback[:10] if fallback else "Value")


# Backward compatibility aliases
_shorten_selector_value = lambda text, original_value=None, key=None, max_bytes=32: truncate_to_bytes(text, max_bytes, original_value or "Value")
_trim_text_to_byte_limit = lambda text, max_bytes, fallback=None: truncate_to_bytes(text, max_bytes, fallback or "")


# =============================================================================
# LANGUAGE DETECTION
# =============================================================================

def detect_language(text: str) -> str:
    """
    Detect source language of text.
    
    Args:
        text: Text to analyze
        
    Returns:
        Language code: "ZH" (Chinese), "JA" (Japanese), or "EN" (English/default)
    """
    if not text or not isinstance(text, str):
        return "EN"
    
    text = text.strip()
    if not text:
        return "EN"
    
    # Check for Japanese kana (definitive Japanese indicator)
    if any(_is_kana(c) for c in text):
        return "JA"
    
    # ASCII-only is English
    if all(ord(c) < 128 or c.isspace() for c in text):
        return "EN"
    
    # Check for CJK characters
    has_cjk = any(_is_cjk(c) for c in text)
    if not has_cjk:
        return "EN"
    
    # CJK without kana - check for Chinese indicators
    if _has_chinese_chars(text):
        return "ZH"
    
    # Pure CJK without kana is more likely Chinese
    non_space = [c for c in text if not c.isspace() and c not in string.punctuation]
    if non_space and all(_is_cjk(c) for c in non_space):
        return "ZH"
    
    return "JA"


# Backward compatibility alias
detect_source_language = detect_language


# =============================================================================
# DEEPL API
# =============================================================================

def _get_translator() -> Any:
    """Get or create DeepL translator instance."""
    global _translator
    
    if not DEEPL_AVAILABLE:
        raise RuntimeError("deepl library not installed. Install with: pip install deepl")
    
    api_key = Config.get_API_KEY()
    if not api_key:
        raise RuntimeError("DEEPL_API_KEY not configured")
    
    if _translator is None:
        kwargs = {"auth_key": api_key}
        server_url = Config.get_SERVER_URL()
        if server_url:
            kwargs["server_url"] = server_url
        _translator = Translator(**kwargs)
        logger.debug("DeepL translator initialized")
    
    return _translator


def translate(
    text: str,
    source_lang: str = "JA",
    target_lang: str = "EN",
    use_cache: bool = True
) -> Optional[str]:
    """
    Translate text using DeepL API.
    
    Args:
        text: Text to translate
        source_lang: Source language code (default: JA)
        target_lang: Target language code (default: EN)
        use_cache: Whether to use translation cache
        
    Returns:
        Translated text, or None if translation fails
    """
    if not text or not text.strip():
        return None
    
    original_text = text.strip()
    
    # Check dictionary FIRST (before any normalization)
    # This ensures accurate translations for common terms like "颜色" -> "color", "尺码" -> "size"
    if original_text in _DICTIONARY:
        target_upper = target_lang.upper()
        if target_upper in _DICTIONARY[original_text]:
            return _DICTIONARY[original_text][target_upper]
        # Also check for "EN" if target is "EN-US"
        if target_upper == "EN-US" and "EN" in _DICTIONARY[original_text]:
            return _DICTIONARY[original_text]["EN"]
    
    # If translating to Japanese and source is already Japanese, return as-is
    if target_lang.upper() in ("JA", "JP"):
        detected_lang = detect_language(original_text)
        if detected_lang == "JA":
            return original_text
    
    # Only apply normalize for Japanese translations (to fix Chinese->Japanese errors)
    # For English translations, do NOT normalize (to avoid Chinese->Japanese->English)
    if target_lang.upper() in ("JA", "JP"):
        text = normalize(original_text)
    else:
        text = original_text
    
    # Check cache
    cache_key = f"{source_lang}:{target_lang}:{text}"
    if use_cache and cache_key in _cache:
        return _cache[cache_key]
    
    # Require API key for DeepL calls
    api_key = Config.get_API_KEY()
    if not api_key:
        logger.warning("DEEPL_API_KEY not configured")
        return None
    
    time.sleep(Config.get_RATE_LIMIT_DELAY())
    
    try:
        translator = _get_translator()
        
        # Normalize language codes
        src = "JA" if source_lang == "JP" else source_lang.upper()
        tgt = "JA" if target_lang == "JP" else target_lang.upper()
        if tgt == "EN":
            tgt = "EN-US"
        
        max_retries = Config.get_MAX_RETRIES()
        for attempt in range(max_retries):
            try:
                result = translator.translate_text(text, source_lang=src, target_lang=tgt)
                
                if result and hasattr(result, 'text') and result.text:
                    translated = clean_for_rakuten(result.text.strip())
                    if translated:
                        if use_cache and len(_cache) < Config.get_CACHE_MAX_SIZE():
                            _cache[cache_key] = translated
                        return translated
                return None
                
            except Exception as e:
                if "rate limit" in str(e).lower() or "429" in str(getattr(e, 'status_code', '')):
                    wait = Config.get_RETRY_DELAY() * (2 ** attempt)
                    logger.warning(f"Rate limited, waiting {wait}s")
                    time.sleep(wait)
                    continue
                raise
                
    except Exception as e:
        logger.error(f"Translation failed: {e}")
        return None
    
    return None


# Backward compatibility alias
translate_text = translate


def translate_to_japanese(text: str, source_lang: Optional[str] = None) -> Optional[str]:
    """
    Translate text to Japanese.
    
    If source is already Japanese, returns original text as-is without any processing.
    Prioritizes dictionary lookup over API calls.
    
    Args:
        text: Text to translate
        source_lang: Source language code (auto-detected if None)
        
    Returns:
        Japanese text (original if already Japanese, translated otherwise)
    """
    if not text or not text.strip():
        return text
    
    original_text = text.strip()
    
    # Step 1: Check dictionary FIRST (before any normalization or language detection)
    # This ensures accurate translations for common terms like "颜色" -> "カラー", "尺码" -> "サイズ"
    if original_text in _DICTIONARY and "JA" in _DICTIONARY[original_text]:
        return _DICTIONARY[original_text]["JA"]
    
    # Step 2: Detect language
    if source_lang is None:
        source_lang = detect_language(original_text)
    
    # Step 3: Check if text is mixed Chinese-Japanese (e.g., "120夏速乾网格セット装")
    # If it contains both Chinese characters and Japanese kana, we need to translate Chinese parts
    has_chinese = any(_is_cjk(c) and c not in '・' for c in original_text)
    has_japanese_kana = any(_is_kana(c) for c in original_text)
    is_mixed = has_chinese and has_japanese_kana
    
    # Step 4: If already pure Japanese (no Chinese characters), return as-is
    if source_lang == "JA" and not has_chinese:
        return original_text
    
    # Step 5: For Chinese text (pure or mixed), apply normalization map first
    if source_lang == "ZH" or is_mixed:
        # Apply normalization map to fix common Chinese-to-Japanese translation errors
        normalized = normalize(original_text)
        # If normalization changed the text, use it; otherwise translate original
        if normalized != original_text:
            text_to_translate = normalized
        else:
            text_to_translate = original_text
    else:
        # For non-Chinese text, translate directly without normalization
        text_to_translate = original_text
    
    # Step 6: Translate to Japanese via DeepL API
    # For mixed text, use Chinese as source language to ensure Chinese parts are translated
    result = translate(text_to_translate, source_lang="ZH" if is_mixed else source_lang, target_lang="JA")
    
    # Step 7: Apply normalization to result to fix any translation errors
    if result:
        return normalize(result)
    else:
        return original_text


# Backward compatibility alias
translate_text_to_japanese = translate_to_japanese


def translate_to_english(text: str, source_lang: Optional[str] = None) -> Optional[str]:
    """
    Translate text to English.
    
    IMPORTANT: Does NOT use normalize() to avoid converting Chinese to Japanese first.
    Uses dictionary lookup first, then DeepL API directly from source language to English.
    """
    if not text or not text.strip():
        return text
    
    original_text = text.strip()
    
    # Step 1: Check dictionary first (fastest, most accurate)
    if original_text in _DICTIONARY and "EN" in _DICTIONARY[original_text]:
        return _DICTIONARY[original_text]["EN"]
    
    # Step 2: Detect source language
    if source_lang is None:
        source_lang = detect_language(original_text)
    
    # Step 3: If already English, return as-is
    if source_lang == "EN":
        return original_text
    
    # Step 4: Translate directly from source language to English (NO normalization)
    # This ensures Chinese -> English, not Chinese -> Japanese -> English
    return translate(original_text, source_lang=source_lang, target_lang="EN")


# Backward compatibility alias
translate_text_to_english = translate_to_english


def translate_key_to_english(text: str, source_lang: Optional[str] = None) -> Optional[str]:
    """Translate text to English and normalize as a key."""
    translated = translate_to_english(text, source_lang)
    if not translated:
        return None

    key = translated.lower().replace(" ", "_").replace("-", "_")
    key = re.sub(r"[^a-z0-9_]", "", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key or "key"


# =============================================================================
# BATCH TRANSLATION
# =============================================================================

def translate_batch(
    texts: List[str],
    source_lang: str = "JA",
    target_lang: str = "EN",
    use_cache: bool = True
) -> List[Optional[str]]:
    """
    Translate multiple texts in a single API call.
    
    Args:
        texts: List of texts to translate
        source_lang: Source language code
        target_lang: Target language code
        use_cache: Whether to use cache
        
    Returns:
        List of translations (None for failed translations)
    """
    if not texts:
        return []
    
    # Filter valid texts
    valid_texts = [t.strip() for t in texts if t and isinstance(t, str) and t.strip()]
    if not valid_texts:
        return [None] * len(texts)
    
    # Separate cached and uncached
    results: Dict[int, Optional[str]] = {}
    to_translate: List[Tuple[int, str]] = []
    
    for i, text in enumerate(valid_texts):
        cache_key = f"{source_lang}:{target_lang}:{text}"
        if use_cache and cache_key in _cache:
            results[i] = _cache[cache_key]
        else:
            to_translate.append((i, text))
    
    if not to_translate:
        return [results.get(i) for i in range(len(valid_texts))]
    
    api_key = Config.get_API_KEY()
    if not api_key:
        return [None] * len(texts)
    
    time.sleep(Config.get_RATE_LIMIT_DELAY())
    
    try:
        translator = _get_translator()
        
        src = "JA" if source_lang == "JP" else source_lang.upper()
        tgt = "EN-US" if target_lang.upper() == "EN" else target_lang.upper()
        
        texts_to_send = [text for _, text in to_translate]
        api_results = translator.translate_text(texts_to_send, source_lang=src, target_lang=tgt)
        
        if api_results:
            if not isinstance(api_results, list):
                api_results = [api_results]
            
            for idx, result in enumerate(api_results):
                if idx < len(to_translate):
                    orig_idx, orig_text = to_translate[idx]
                    if result and hasattr(result, 'text') and result.text:
                        translated = clean_for_rakuten(result.text.strip())
                        results[orig_idx] = translated
                        if use_cache and translated and len(_cache) < Config.get_CACHE_MAX_SIZE():
                            _cache[f"{source_lang}:{target_lang}:{orig_text}"] = translated
                    else:
                        results[orig_idx] = None
        
        return [results.get(i) for i in range(len(valid_texts))]
                
    except Exception as e:
        logger.error(f"Batch translation failed: {e}")
        return [None] * len(texts)
    

# =============================================================================
# CACHE MANAGEMENT
# =============================================================================

def clear_cache() -> None:
    """Clear translation cache."""
    global _cache
    _cache.clear()
    logger.info("Translation cache cleared")


def get_cache_stats() -> Dict[str, Any]:
    """Get cache statistics."""
    return {"size": len(_cache), "max_size": Config.get_CACHE_MAX_SIZE()}


def clear_cache_for_text(text: str, source_lang: str = "ZH", target_lang: str = "JA") -> None:
    """Clear cache entry for specific text."""
    global _cache
    _cache.pop(f"{source_lang}:{target_lang}:{text.strip()}", None)


# =============================================================================
# VARIANT SELECTOR PROCESSING
# =============================================================================

def _is_color_key(key: Optional[str]) -> bool:
    """Check if key indicates a color selector."""
    if not key:
        return False
    return key.lower() in ('color', 'colour', 'カラー', '色', 'colors', 'colours')


def _is_size_key(key: Optional[str]) -> bool:
    """Check if key indicates a size selector."""
    if not key:
        return False
    return key.lower() in ('size', 'サイズ', 'sizes', '尺码', '尺寸')


def _extract_size_from_text(text: str) -> Optional[str]:
    """
    Extract size value from text.
    
    Extracts size patterns from SIZE_VALUES (e.g., "2XL" from "2XL建议155～175斤").
    Also handles Chinese size terms (e.g., "均码" -> "フリーサイズ" from "0223均码【80-120均码】").
    Uses patterns from PATTERN_DICTIONARY for size detection.
    
    Args:
        text: Text like "M（100-110斤）" or "3XL" or "2XL建议155～175斤" or "0223均码【80-120均码】"
        
    Returns:
        Extracted size like "M", "2XL", or "フリーサイズ" (for "均码"), or None
    """
    if not text:
        return None
    
    text_original = text.strip()
    text_upper = text_original.upper()
    
    # Step 1: Direct match with SIZE_VALUES (exact match)
    if text_upper in SIZE_VALUES:
        # Return in original case if it was uppercase, otherwise return uppercase
        if text_original.upper() == text_original:
            return text_upper
        return text_original
    
    # Step 2: Remove parentheses and brackets content, then check
    cleaned = re.sub(r'\([^)]*\)', '', text_original)
    cleaned = re.sub(r'（[^）]*）', '', cleaned)
    cleaned = re.sub(r'\[[^\]]*\]', '', cleaned)
    cleaned = re.sub(r'【[^】]*】', '', cleaned)
    cleaned = cleaned.strip()
    cleaned_upper = cleaned.upper()
    
    if cleaned_upper in SIZE_VALUES:
        if cleaned.upper() == cleaned:
            return cleaned_upper
        return cleaned
    
    # Step 3: Check for Chinese size terms from PATTERN_DICTIONARY (e.g., "均码" -> "フリーサイズ")
    # Check in both original text and cleaned text (after removing brackets)
    # Get Chinese size terms from pattern dictionary (filter Chinese characters)
    size_patterns_all = get_patterns('sizes')
    chinese_size_terms = [term for term in size_patterns_all if any('\u4e00' <= c <= '\u9fff' for c in term)]
    # Priority: longer terms first to match "均码" before "码"
    for term in sorted(chinese_size_terms, key=len, reverse=True):
        if term in text_original:
            # Found Chinese size term, return Japanese translation from TRANSLATION_MAP
            if term in TRANSLATION_MAP:
                return TRANSLATION_MAP[term]
        if term in cleaned:
            # Also check cleaned text (after removing brackets)
            if term in TRANSLATION_MAP:
                return TRANSLATION_MAP[term]
    
    # Step 4: Pattern match from SIZE_VALUES (longest first to match "2XL" before "XL")
    # Use word boundary or start of string, and allow end of string or non-alphanumeric after
    for size in sorted(SIZE_VALUES, key=len, reverse=True):
        # Match at start of string, or after non-alphanumeric, or at word boundary
        # Allow end of string or non-alphanumeric (including Chinese characters) after
        pattern = rf'(?:^|[^A-Z0-9]){re.escape(size)}(?=[^A-Z0-9]|$)'
        match = re.search(pattern, text_upper, re.IGNORECASE)
        if match:
            # Extract the matched size
            matched_text = text_upper[match.start():match.end()].strip()
            # Remove any leading non-alphanumeric characters
            matched_text = re.sub(r'^[^A-Z0-9]+', '', matched_text)
            if matched_text in SIZE_VALUES:
                return matched_text
    
    # Step 5: Check pattern dictionary for sizes (case-insensitive)
    size_patterns = get_patterns('sizes')
    for size_pattern in sorted(size_patterns, key=len, reverse=True):
        # Match at start of string, or after non-alphanumeric, or at word boundary
        pattern = rf'(?:^|[^A-Z0-9]){re.escape(size_pattern.upper())}(?=[^A-Z0-9]|$)'
        if re.search(pattern, text_upper, re.IGNORECASE):
            # Return in original case if it was uppercase, otherwise return uppercase
            if size_pattern.isupper():
                return size_pattern
            return size_pattern.upper()
    
    return None


def clean_variant_value(
    value: str,
    key: Optional[str] = None,
    max_bytes: int = 32,
    context: Optional[Any] = None
) -> str:
    """
    Clean and translate variant selector value for Rakuten API.
    
    This is the main function for processing variant values (colors, sizes).
    Handles translation, cleaning, and byte-limit enforcement.
    Removes unwanted patterns (symbols, brackets, quality terms) before translation.
    
    Args:
        value: Original value to process
        key: Selector key ("color", "size", etc.) for context
        max_bytes: Maximum byte length (default: 32 for Rakuten)
        context: Optional context (ignored, for backward compatibility)
        
    Returns:
        Cleaned value within byte limit
    """
    if not value or not isinstance(value, str):
        return value or ""
    
    original = value.strip()
    if not original:
        return ""
    
    # Handle sizes specially - check for Chinese size terms BEFORE removing unwanted patterns
    # This prevents "码" from being removed by remove_unwanted_patterns
    if _is_size_key(key):
        # First, try to extract size from original text (before removing patterns)
        size = _extract_size_from_text(original)
        if size:
            return size
        # If not found, try after removing unwanted patterns
        cleaned = remove_unwanted_patterns(original)
        if cleaned and cleaned != original:
            size = _extract_size_from_text(cleaned)
            if size:
                return size
        logger.warning(f"Could not extract size from: '{original}'")
        return ""
    
    # Step 1: Remove unwanted patterns (symbols, brackets, quality terms, product codes)
    cleaned = remove_unwanted_patterns(original)
    if not cleaned:
        cleaned = original  # Fallback to original if removal left nothing
    
    # Check if text contains Chinese characters (even if mixed with Japanese)
    has_chinese = any(_is_cjk(c) and c not in '・' for c in cleaned)
    has_japanese_kana = any(_is_kana(c) for c in cleaned)
    
    # If mixed Chinese-Japanese text, normalize Chinese parts first
    if has_chinese:
        # Apply normalization map to translate Chinese parts
        normalized = normalize(cleaned)
        
        # If normalization changed the text, use it
        if normalized != cleaned:
            result = normalized
        else:
            # If normalization didn't help, try DeepL translation
            lang = detect_language(cleaned)
            try:
                translated = translate_to_japanese(cleaned, source_lang=lang)
                result = translated.strip() if translated else cleaned
            except Exception as e:
                logger.debug(f"Translation failed: {e}")
                result = cleaned
    elif has_japanese_kana:
        # Pure Japanese: return as-is, only truncate if needed
        result = cleaned
    else:
        # Other languages: translate to Japanese
        lang = detect_language(cleaned)
        try:
            translated = translate_to_japanese(cleaned, source_lang=lang)
            result = translated.strip() if translated else cleaned
        except Exception as e:
            logger.debug(f"Translation failed: {e}")
            result = cleaned
    
    max_bytes_config = Config.get_MAX_BYTES()
    return truncate_to_bytes(result, max_bytes if max_bytes else max_bytes_config, original)


# Backward compatibility aliases
def _translate_variant_value_with_context(
    value: str,
    key: Optional[str] = None,
    context: Optional[Any] = None,
    max_bytes: int = 32
) -> str:
    """
    Backward compatibility wrapper for clean_variant_value.
    Accepts context parameter but ignores it.
    """
    return clean_variant_value(value, key, max_bytes)

def _clean_selector_text(value: str, key: Optional[str] = None) -> str:
    """Clean selector text - if Japanese, return as-is without normalization."""
    if not value:
        return ""
    value = value.strip()
    # If Japanese, return as-is; otherwise normalize first
    if detect_language(value) == "JA":
        return value
    return clean_variant_value(normalize(value), key)


def clean_chinese_color_for_rakuten(
    text: str,
    original_value: Optional[str] = None,
    max_bytes: int = 32
) -> str:
    """
    Clean Chinese color names for Rakuten API registration.
    
    Comprehensive cleaning pipeline:
    1. Apply normalization map
    2. Remove brackets and promotional text
    3. Translate remaining Chinese via DeepL
    4. Remove all Chinese characters as fallback
    5. Enforce byte limit
    
    Args:
        text: Input text with Chinese color names
        original_value: Fallback value
        max_bytes: Maximum byte length
        
    Returns:
        Cleaned Japanese color name
    """
    fallback = (original_value[:10] if original_value else "Value")
    
    if not text or not isinstance(text, str):
        return fallback
    
    text = text.strip()
    
    # Check if text is already Japanese - if so, return as-is (only truncate if needed)
    if detect_language(text) == "JA":
        return truncate_to_bytes(text, max_bytes, fallback)
    
    # Step 1: Normalize (only for non-Japanese text)
    result = normalize(text)
    if not result:
        return fallback
    
    # Step 2: Remove brackets
    result = re.sub(r'【[^】]*】|\[[^\]]*\]|\([^)]*\)|（[^）]*）', '', result).strip()
    
    # Step 3: Remove product codes
    if re.match(r'^\s*\d{3,5}\s*$', result):
        return fallback
    result = re.sub(r'^\s*[89]\d{3}\s*[-・\s]*', '', result)
    result = re.sub(r'[-・\s]+[89]\d{3}\s*$', '', result).strip()
    
    if not result:
        return fallback
    
    # Step 4: Clean for Rakuten
    result = clean_for_rakuten(result, strict=True)
    
    # Only normalize if not Japanese (normalize already checks for Japanese)
    result = normalize(result)
    
    # Step 5: Handle remaining Chinese characters
    if _has_chinese_chars(result):
        # Try to translate
        try:
            translated = translate_to_japanese(result, source_lang="ZH")
            if translated and translated != result:
                result = translated
        except Exception:
            pass
        
        # Remove any remaining Chinese chars
        result = ''.join(c for c in result if ord(c) < 0x80 or _is_kana(c))
    
    result = clean_for_rakuten(result, strict=True).strip()
    
    if not result:
        return fallback
    
    return truncate_to_bytes(result, max_bytes, fallback)


def _extract_important_keywords(text: str, max_bytes: int = 32) -> str:
    """Extract important keywords (color, size) from text within byte limit."""
    if not text:
        return ""
    
    # Build color set from translation map
    colors = {v for v in TRANSLATION_MAP.values() if v and any(
        c in v for c in ('レッド', 'ブルー', 'グレー', 'ブラック', 'ホワイト', 'グリーン',
                        'ピンク', 'オレンジ', 'パープル', 'ブラウン', 'ベージュ', 'イエロー',
                        'シルバー', 'ゴールド', 'ローズ', 'ネイビー', 'ライト', 'ダーク')
    )}
    
    # Find colors in text (longest first)
    for color in sorted(colors, key=len, reverse=True):
        if color in text and _get_byte_length(color) <= max_bytes:
            return color
    
    # Try extracting first Japanese word
    for part in re.split(r'[・\s\u3000\-]+', text):
        part = part.strip()
        if part and _get_byte_length(part) <= max_bytes and any(_is_kana(c) for c in part):
            return part
    
    return ""


# =============================================================================
# BACKWARD COMPATIBILITY HELPERS
# =============================================================================

def _remove_chinese_brackets(text: str) -> str:
    """Remove Chinese brackets and their contents."""
    if not text:
        return text
    result = re.sub(r'【[^】]*】|\[[^\]]*\]', '', text)
    for bracket in CHINESE_BRACKETS:
        result = result.replace(bracket, '')
    return result.strip()


def _filter_japanese_characters_only(text: str) -> str:
    """Filter text to only allow Japanese characters."""
    if not text:
        return text
    return ''.join(c for c in text if _is_japanese_safe(c) or c in '.,- ')


def _normalize_key(text: str) -> str:
    """Normalize text to key format."""
    key = text.lower().strip().replace(" ", "_").replace("-", "_")
    key = re.sub(r"[^a-z0-9_]", "", key)
    key = re.sub(r"_+", "_", key).strip("_")
    return key or "key"


def _is_valid_translation(source: str, translated: str, source_lang: str, target_lang: str) -> bool:
    """Validate translation result against known bad translations."""
    if not translated:
        return False
    
    # Known bad DeepL translations
    bad_translations = {
        ("黑色", "JA"): {"鉄", "悲観的", "鉄色"},
        ("灰色", "JA"): {"悲観的", "鉄"},
    }
    
    bad_set = bad_translations.get((source, target_lang))
    return translated not in bad_set if bad_set else True


# =============================================================================
# MODULE INITIALIZATION
# =============================================================================

# Initialize all dictionaries first, then reload from config
# This ensures dictionaries exist before _reload_config_values() tries to use them
TRANSLATION_MAP = _init_translation_map()
PATTERN_DICTIONARY = _init_pattern_dictionary()
REMOVAL_PATTERNS = _init_removal_patterns()
_SORTED_MAP = _init_sorted_map()

# Now reload from JSON config file (will override defaults if config exists)
_reload_config_values()

if not DEEPL_AVAILABLE:
    logger.warning("deepl library not installed. Install with: pip install deepl")
elif not Config.get_API_KEY():
    logger.warning("DEEPL_API_KEY not configured")
else:
    logger.info("DeepL translation module initialized")


# =============================================================================
# PUBLIC API
# =============================================================================

__all__ = [
    # Core functions
    'normalize',
    'translate',
    'translate_to_japanese',
    'translate_to_english',
    'translate_key_to_english',
    'translate_batch',
    'detect_language',
    'clean_for_rakuten',
    'truncate_to_bytes',
    
    # Variant processing
    'clean_variant_value',
    'clean_chinese_color_for_rakuten',
    
    # Cache management
    'clear_cache',
    'get_cache_stats',
    'clear_cache_for_text',
    
    # Configuration
    'Config',
    'TRANSLATION_MAP',
    'reload_translation_config',
    
    # Backward compatibility
    'translate_text',
    'translate_text_to_japanese',
    'translate_text_to_english',
    'detect_source_language',
    'clean_text_for_rakuten',
    '_apply_normalization_map',
    '_translate_variant_value_with_context',
    '_clean_selector_text',
    '_shorten_selector_value',
    '_trim_text_to_byte_limit',
    '_is_color_key',
    '_is_size_key',
    '_extract_size_from_text',
    '_remove_chinese_brackets',
    '_filter_japanese_characters_only',
    # Pattern dictionary functions
    'get_patterns',
    'add_pattern',
    'remove_pattern',
    'get_all_categories',
    'PATTERN_DICTIONARY',
    # Removal patterns functions
    'get_removal_patterns',
    'add_removal_pattern',
    'remove_unwanted_patterns',
    'REMOVAL_PATTERNS',
]

