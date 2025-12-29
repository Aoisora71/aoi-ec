import io
import json
import os
import logging
import re
import time
from urllib.parse import urlparse

import cv2
import numpy as np
import requests
from requests.adapters import HTTPAdapter
try:
    from urllib3.util.retry import Retry
except ImportError:
    try:
        from requests.packages.urllib3.util.retry import Retry
    except ImportError:
        Retry = None
import boto3
from google import genai
from google.genai import types
from PIL import Image

# Configure logging (only if not already configured)
if not logging.getLogger().handlers:
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Global flag to track if 429/quota errors occurred (to avoid showing error modals)
_last_error_was_quota = False

# ============================================================================
# TEMPORARY FLAG: Image Processing Control
# ============================================================================
# To temporarily disable image processing (S3 uploads and Gemini processing):
#   - Set environment variable: IMAGE_PROCESSING_ENABLED=false
#   - Or change the default below to: IMAGE_PROCESSING_ENABLED = False
#
# When disabled:
#   - All image processing code remains intact
#   - Functions will return early without processing
#   - No S3 uploads will occur
#   - No Gemini API calls will be made
#   - Log messages will indicate that processing is skipped
# ============================================================================
IMAGE_PROCESSING_ENABLED = os.getenv("IMAGE_PROCESSING_ENABLED", "true").lower() == "true"

# Check for API key (prefer GEMINI_API_KEY, fallback to GOOGLE_API_KEY)
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
if not GEMINI_API_KEY:
    logger.error("=" * 80)
    logger.error("❌ GEMINI API KEY NOT SET")
    logger.error("=" * 80)
    logger.error("API key is required. Please set either GEMINI_API_KEY or GOOGLE_API_KEY environment variable")
    logger.error("")
    logger.error("Please:")
    logger.error("1. Get your API key from: https://aistudio.google.com/app/apikey")
    logger.error("2. Set it in PowerShell: $env:GEMINI_API_KEY = 'your_api_key_here'")
    logger.error("3. Or set it in CMD: set GEMINI_API_KEY=your_api_key_here")
    logger.error("")
    logger.error("=" * 80)
    raise ValueError("API key environment variable is required")

# Validate API key format (should start with AIza)
if not GEMINI_API_KEY.startswith("AIza"):
    logger.warning("⚠️  Warning: API key doesn't start with 'AIza'. Please verify your API key is correct.")
    logger.warning(f"   API key (first 20 chars): {GEMINI_API_KEY[:20]}...")
    logger.warning("   Get a valid key from: https://aistudio.google.com/app/apikey")

# Log which API key is being used
if os.getenv("GEMINI_API_KEY") and os.getenv("GOOGLE_API_KEY"):
    logger.info("Both GEMINI_API_KEY and GOOGLE_API_KEY are set. Using GEMINI_API_KEY.")
elif os.getenv("GOOGLE_API_KEY") and not os.getenv("GEMINI_API_KEY"):
    logger.info("Using GOOGLE_API_KEY for authentication.")
else:
    logger.info("Using GEMINI_API_KEY for authentication.")

client = genai.Client(api_key=GEMINI_API_KEY)

# Create a session with retry strategy and proper headers
def create_session_with_retry() -> requests.Session:
    """
    Create a requests session with retry strategy and proper headers for image downloads.
    """
    session = requests.Session()
    
    # Retry strategy (if available)
    if Retry is not None:
        try:
            retry_strategy = Retry(
                total=3,
                backoff_factor=1,
                status_forcelist=[429, 500, 502, 503, 504, 420],
                allowed_methods=["GET", "HEAD"]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
        except Exception as e:
            logger.warning(f"Could not set up retry strategy: {e}. Continuing without retry adapter.")
    
    # Set headers to mimic a real browser (helps avoid rate limiting)
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'image/webp,image/apng,image/*,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.google.com/',
        'Sec-Fetch-Dest': 'image',
        'Sec-Fetch-Mode': 'no-cors',
        'Sec-Fetch-Site': 'cross-site',
    })
    
    return session

# Global session
download_session = create_session_with_retry()


def generate_product_image_code(item_number: str) -> str:
    """
    Deterministically convert a product/item number into an 8-digit code suitable for S3 paths.
    Uses weighted mixing and checksum hashing to minimise the chance of collisions while
    keeping the result numeric.
    """
    if item_number is None:
        raise ValueError("item_number is required to generate product image code")

    raw = str(item_number).strip()
    digits_only = ''.join(ch for ch in raw if ch.isdigit())

    # If no digits are present, fall back to character codes to guarantee determinism
    if not digits_only:
        digits_only = ''.join(f"{ord(ch) % 10}" for ch in raw)

    # Use the last 12 digits to keep consistency while still incorporating most variance
    digits_only = digits_only[-12:].rjust(12, '0')

    blocks = [int(digits_only[i:i + 3]) for i in range(0, 12, 3)]

    weighted_mix = (
        blocks[0] * 971 +
        blocks[1] * 673 +
        blocks[2] * 353 +
        blocks[3] * 199
    ) % 10_000_000

    checksum = sum((idx + 1) * int(d) for idx, d in enumerate(digits_only)) % 10_000_000

    code = (weighted_mix ^ checksum) % 10_000_000
    # Add leading digits to the mix to further minimise collisions
    code = (code + int(digits_only[:4])) % 10_000_000

    if code == 0:
        code = 1  # avoid returning all zeros

    return f"{code:08d}"

def get_default_prompt_tags() -> str:
    """
    Get the default prompt tags for Gemini image processing.
    
    Returns:
        Default prompt string for removing text and applying face blur only to clearly visible human faces
    """
    return "Remove all Chinese text, English text, and any background text from this product image. Create a completely natural product image without any text overlays, watermarks, or background text. Keep the product itself unchanged but remove all text elements. Generate a clean, natural product image with high quality. Only apply a blur effect to clearly visible human faces. Do not blur any other areas, product parts, or non-human face regions. Maintain the original image quality and sharpness for all non-human face areas."

def _extract_images_from_response(response) -> list[np.ndarray]:
    """Extract inline image data from a Gemini response."""
    images: list[np.ndarray] = []
    try:
        if response and getattr(response, "candidates", None):
            for candidate in response.candidates:
                content = getattr(candidate, "content", None)
                if not content or not getattr(content, "parts", None):
                    continue
                for part in content.parts:
                    if hasattr(part, "inline_data") and part.inline_data:
                        img_data = part.inline_data.data
                        img_array = cv2.imdecode(np.frombuffer(img_data, dtype=np.uint8), cv2.IMREAD_COLOR)
                        if img_array is not None:
                            images.append(img_array)
                    elif hasattr(part, "text") and part.text:
                        # Some models may return text-only; ignore but log at debug
                        logger.debug(f"Gemini returned text instead of image: {part.text[:200]}")
    except Exception as e:
        logger.warning(f"Failed to parse Gemini response: {e}")
    return images


def execute_gemini(
    image_data: bytes | str,
    prompt_tags: str,
) -> list[np.ndarray] | None:
    """
    Execute Google Generative AI (Gemini) for image editing

    Args:
        image_data: Image data as bytes or file path (bytes preferred for AWS)
        prompt_tags: Tags to use for prompts

    Returns:
        fixed_images: List of fixed images as numpy arrays
    """
    # TEMPORARY: Skip Gemini processing if disabled
    if not IMAGE_PROCESSING_ENABLED:
        logger.info("⚠️  Image processing is temporarily disabled. Skipping Gemini processing.")
        return None

    prompt = f"Please return only images, do not return any text. {prompt_tags}"
    logger.info("=" * 80)
    logger.info("🤖 GEMINI IMAGE PROCESSING PROMPT")
    logger.info("=" * 80)
    logger.info(f"Prompt: {prompt}")
    logger.info("=" * 80)

    # Handle both bytes and file path for backward compatibility
    if isinstance(image_data, str):
        # File path (legacy mode)
        with open(image_data, "rb") as f:
            image_bytes = f.read()
    else:
        # Bytes (AWS server mode - no local files)
        image_bytes = image_data

    input_image = Image.open(io.BytesIO(image_bytes))

    try:
        if isinstance(image_data, str):
            logger.info(f"Processing image: {image_data}")
        else:
            logger.info(f"Processing image from memory ({len(image_bytes)} bytes)")
        # Prefer the latest image-capable model; fall back to legacy if needed
        response = None
        used_model = None
        primary_error = None
        try:
            response = client.models.generate_content(
                model='gemini-3-pro-image-preview',
                contents=[prompt, input_image],
                config=types.GenerateContentConfig(
                    response_modalities=['IMAGE']
                )
            )
            used_model = 'gemini-3-pro-image-preview'
            logger.info(f"Using model: {used_model}")
            images = _extract_images_from_response(response)
            if images:
                logger.info(f"Generated {len(images)} cleaned image(s)")
                return images
            logger.warning("No image content returned from gemini-3-pro-image-preview; falling back to legacy model.")
        except Exception as e:
            primary_error = e
            logger.warning(f"Primary model gemini-3-pro-image-preview failed: {e}. Falling back to gemini-2.5-flash-image-preview.")

        # Fallback attempt with legacy model and allow text modality to improve chances
        try:
            response = client.models.generate_content(
                model='gemini-2.5-flash-image-preview',
                contents=[prompt, input_image],
                config=types.GenerateContentConfig(
                    response_modalities=['IMAGE', 'TEXT']
                )
            )
            used_model = 'gemini-2.5-flash-image-preview'
            logger.info(f"Using model: {used_model}")
            images = _extract_images_from_response(response)
            if images:
                logger.info(f"Generated {len(images)} cleaned image(s) (fallback)")
                return images
            logger.warning("Fallback model returned no images.")
        except Exception as fallback_error:
            logger.error(f"Fallback model failed: {fallback_error}")
            if primary_error:
                logger.error(f"Primary error was: {primary_error}")

        if response and getattr(response, "candidates", None):
            # Log finish reasons and part counts to aid debugging when images are missing
            try:
                finish_reasons = [
                    getattr(c, "finish_reason", None) for c in response.candidates
                ]
                parts_counts = [
                    len(getattr(getattr(c, "content", None), "parts", []) or [])
                    for c in response.candidates
                ]
                logger.warning(f"Finish reasons: {finish_reasons}")
                logger.warning(f"Parts count per candidate: {parts_counts}")
            except Exception as dbg_err:
                logger.debug(f"Failed to log candidate details: {dbg_err}")

        if response and getattr(response, "candidates", None) and response.candidates[0].content.parts:
            fixed_images = []
            for part in response.candidates[0].content.parts:
                if hasattr(part, 'inline_data') and part.inline_data:
                    # Data is already in binary format, not base64 encoded
                    img_data = part.inline_data.data
                    img_array = cv2.imdecode(np.frombuffer(img_data, dtype=np.uint8), cv2.IMREAD_COLOR)
                    if img_array is not None:
                        fixed_images.append(img_array)
                        logger.info(f"Successfully processed image part")
                    else:
                        logger.warning(f"Failed to decode image data")
                elif hasattr(part, 'text') and part.text:
                    logger.warning(f"The model returned text instead of image: {part.text}")
            
            if fixed_images:
                logger.info(f"Generated {len(fixed_images)} cleaned image(s)")
                return fixed_images
            else:
                logger.warning(f"No valid images were generated")
                return None
        else:
            logger.warning(f"No content returned from the model")
            return None

    except Exception as e:
        error_msg = str(e)
        error_dict = {}
        
        # Try to extract error details from the exception
        if hasattr(e, 'response') and hasattr(e.response, 'json'):
            try:
                error_dict = e.response.json()
            except:
                pass
        
        # Try to parse error dict from error message string (Google GenAI sometimes includes it)
        dict_match = re.search(r"\{'error':\s*\{[^}]+\}\}", error_msg)
        if dict_match:
            try:
                # Convert single quotes to double quotes for JSON parsing
                error_str = dict_match.group(0).replace("'", '"')
                error_dict = json.loads(error_str)
            except:
                pass
        
        # Check for location restriction error
        error_message_lower = error_msg.lower()
        error_code = error_dict.get('error', {}).get('status', '') if error_dict else ''
        error_message_text = error_dict.get('error', {}).get('message', '') if error_dict else ''
        
        # Check for 429/RESOURCE_EXHAUSTED errors (rate limit/quota exceeded)
        # These should be logged but not raise errors to avoid showing error modals
        global _last_error_was_quota
        if (error_code == "RESOURCE_EXHAUSTED" or 
            "RESOURCE_EXHAUSTED" in error_msg or 
            "429" in error_msg or
            "quota" in error_message_lower or
            "rate limit" in error_message_lower or
            "rate_limit" in error_message_lower):
            _last_error_was_quota = True
            logger.warning("=" * 80)
            logger.warning("⚠️  GEMINI API RATE LIMIT / QUOTA EXCEEDED")
            logger.warning("=" * 80)
            logger.warning("Gemini API quota has been exceeded or rate limit reached.")
            logger.warning("This is expected when the daily quota is reached.")
            logger.warning("Processing will continue silently without showing error modals.")
            logger.warning("")
            logger.warning("Error details:")
            if error_dict:
                logger.warning(f"  Status: {error_code}")
                logger.warning(f"  Message: {error_message_text}")
            logger.warning("=" * 80)
            # Return None silently - don't raise error to avoid showing error modals
            return None
        else:
            _last_error_was_quota = False
        
        if ("location is not supported" in error_message_lower or 
            "location is not supported" in error_message_text.lower() or
            (error_code == "FAILED_PRECONDITION" and "location" in error_message_text.lower())):
            logger.error("=" * 80)
            logger.error("❌ GEMINI API LOCATION RESTRICTION ERROR")
            logger.error("=" * 80)
            logger.error("Your current location is not supported for Gemini API use.")
            logger.error("")
            logger.error("SOLUTIONS:")
            logger.error("1. Use a VPN to connect from a supported region (e.g., US, EU, Japan)")
            logger.error("2. Deploy your application to a server in a supported region")
            logger.error("3. Use Google Cloud Run or other Google Cloud services in supported regions")
            logger.error("")
            logger.error("Supported regions typically include:")
            logger.error("  - United States")
            logger.error("  - European Union countries")
            logger.error("  - Japan")
            logger.error("  - Other regions as per Google's current policy")
            logger.error("")
            logger.error("For the latest information, check:")
            logger.error("  https://ai.google.dev/available_regions")
            logger.error("=" * 80)
        # Check for API key errors
        elif "API key" in error_msg or "API_KEY" in error_msg or "INVALID_ARGUMENT" in error_msg:
            if "not valid" in error_msg or "invalid" in error_msg.lower():
                logger.error("=" * 80)
                logger.error("❌ GEMINI API KEY ERROR")
                logger.error("=" * 80)
                logger.error("Your Gemini API key is invalid or expired.")
                logger.error("")
                logger.error("Please:")
                logger.error("1. Get a valid API key from: https://aistudio.google.com/app/apikey")
                logger.error("2. Set it using: $env:GEMINI_API_KEY = 'your_api_key_here'")
                logger.error("3. Make sure the API key has access to Gemini 2.5 Flash Image Preview")
                logger.error("")
                logger.error(f"Current API key (first 10 chars): {GEMINI_API_KEY[:10]}...")
                logger.error("=" * 80)
            else:
                logger.error(f"API key error: {error_msg}")
        else:
            logger.error(f"Failed to execute the model: {error_msg}")
            # Log full error details if available
            if error_dict:
                logger.error(f"Error details: {error_dict}")
        return None

def _s3_client(region_name: str | None = None):
    region = region_name or os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-2"
    return boto3.client("s3", region_name=region)

def upload_image_bytes_to_s3(
    image_bytes: bytes,
    bucket_name: str,
    s3_key: str,
    content_type: str = "image/jpeg",
) -> str:
    """
    Upload image bytes to S3 and return the public URL.
    """
    # TEMPORARY: Skip S3 upload if image processing is disabled
    if not IMAGE_PROCESSING_ENABLED:
        logger.info("⚠️  Image processing is temporarily disabled. Skipping S3 upload.")
        return ""
    
    s3 = _s3_client()
    extra_args = {
        "ACL": "public-read",
        "ContentType": content_type,
        "ContentDisposition": "inline",
    }
    s3.upload_fileobj(io.BytesIO(image_bytes), bucket_name, s3_key, ExtraArgs=extra_args)
    region = os.getenv("AWS_DEFAULT_REGION") or "ap-southeast-2"
    return f"https://{bucket_name}.s3.{region}.amazonaws.com/{s3_key}"

def process_single_image_with_retry(
    url: str,
    product_id: str,
    bucket_name: str,
    folder_name: str,
    prompt_tags: str,
    upload_original: bool,
    max_retries: int,
    retry_delay: int,
    *,
    product_image_code: str,
    image_index: int,
) -> dict:
    """
    Process a single image with retry logic. Retries the entire process (download + process + upload) if it fails.
    
    Args:
        product_image_code: 8-digit code generated from the product id for S3 naming.
        image_index: 1-based index of the image in the current batch (used for S3 naming).

    Returns:
        Dictionary with result:
            - source_url
            - original_url
            - processed_url
            - status
            - error
            - index (original image order)
    """
    image_result = {
        "source_url": url,
        "original_url": None,
        "processed_url": None,
        "status": "failed",
        "error": None,
        "index": image_index,
    }
    
    # Enforce a hard cap of 2 attempts per image
    effective_max_retries = min(max_retries, 2)

    for attempt in range(1, effective_max_retries + 1):
        try:
            logger.info(f"🔄 Attempt {attempt}/{effective_max_retries} for: {url}")
            
            # Step 1: Download image to memory
            logger.info(f"⏬ Downloading image to memory...")
            try:
                r = download_session.get(url, timeout=30, stream=True)
                
                if r.status_code == 420 or r.status_code == 429:
                    if attempt < effective_max_retries:
                        wait_time = retry_delay * attempt
                        logger.warning(f"⚠️  Rate limited ({r.status_code}). Waiting {wait_time}s before retry...")
                        time.sleep(wait_time)
                        continue
                    else:
                        raise requests.exceptions.HTTPError(f"Rate limited ({r.status_code}) after {effective_max_retries} attempts")
                elif r.status_code >= 400:
                    r.raise_for_status()
                
                image_bytes = r.content
                content_type = r.headers.get("Content-Type", "image/jpeg")
                logger.info(f"✅ Downloaded {len(image_bytes)} bytes")
            except requests.exceptions.RequestException as download_error:
                if attempt < effective_max_retries:
                    wait_time = retry_delay * attempt
                    logger.warning(f"⚠️  Download failed: {download_error}. Retrying in {wait_time}s... (attempt {attempt}/{effective_max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    raise
            
            # Step 2: Upload original image (if enabled)
            if upload_original:
                filename = os.path.basename(urlparse(url).path) or "image.jpg"
                original_s3_key = f"{folder_name}/original/{filename}"
                try:
                    original_s3_url = upload_image_bytes_to_s3(
                        image_bytes, 
                        bucket_name, 
                        original_s3_key, 
                        content_type
                    )
                    image_result["original_url"] = original_s3_url
                    logger.info(f"✅ Original uploaded: {original_s3_url}")
                except Exception as e:
                    logger.warning(f"⚠️  Original upload failed: {e}")
                    # Continue with processing even if original upload fails
            
            # Step 3: Process with Gemini
            logger.info(f"🤖 Processing with Gemini...")
            outputs = execute_gemini(image_bytes, prompt_tags=prompt_tags)
            
            if not outputs:
                # Check if this is a 429/quota error
                global _last_error_was_quota
                if _last_error_was_quota:
                    # For quota/rate limit errors, don't set error message to avoid showing error modals
                    logger.info(f"⚠️  Quota/rate limit reached - skipping image processing silently (attempt {attempt}/{effective_max_retries})")
                    if attempt < effective_max_retries:
                        wait_time = retry_delay * attempt
                        logger.info(f"⏳ Retrying in {wait_time}s... (attempt {attempt}/{effective_max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        # Don't set error for quota/rate limit issues - just return with status failed
                        # This prevents error modals from showing
                        image_result["status"] = "failed"
                        image_result["error"] = None  # Don't set error message to avoid showing error modals
                        return image_result
                else:
                    # Normal error - set error message
                    error_msg = "Gemini processing failed - no images generated"
                    logger.warning(f"⚠️  {error_msg} (attempt {attempt}/{effective_max_retries})")
                    if attempt < effective_max_retries:
                        wait_time = retry_delay * attempt
                        logger.info(f"⏳ Retrying in {wait_time}s... (attempt {attempt}/{effective_max_retries})")
                        time.sleep(wait_time)
                        continue
                    else:
                        image_result["error"] = error_msg
                        return image_result
            
            # Step 4: Upload processed image
            logger.info(f"📤 Uploading processed image to S3...")
            base_name = os.path.splitext(os.path.basename(urlparse(url).path) or "image")[0]
            first_img = outputs[0]
            
            ok, buf = cv2.imencode(".jpg", first_img, [int(cv2.IMWRITE_JPEG_QUALITY), 95])
            if not ok:
                error_msg = "Failed to encode processed image to JPEG"
                logger.error(f"❌ {error_msg}")
                if attempt < effective_max_retries:
                    wait_time = retry_delay * attempt
                    logger.info(f"⏳ Retrying in {wait_time}s... (attempt {attempt}/{effective_max_retries})")
                    time.sleep(wait_time)
                    continue
                else:
                    image_result["error"] = error_msg
                    return image_result
            
            # Add "img" prefix to product_image_code if it starts with a number (to match Rakuten Cabinet folder structure)
            # Pattern: "01430022" -> "img01430022" for S3 path
            s3_folder_name = product_image_code
            if product_image_code and product_image_code[0].isdigit():
                s3_folder_name = "img" + product_image_code
            
            s3_key = f"{folder_name}/{s3_folder_name}/{product_image_code}_{image_index}.jpg"
            s3_url = upload_image_bytes_to_s3(buf.tobytes(), bucket_name, s3_key, "image/jpeg")
            image_result["processed_url"] = s3_url
            image_result["product_image_code"] = product_image_code
            image_result["status"] = "success"
            image_result["error"] = None
            logger.info(f"✅ Successfully processed and uploaded: {s3_url}")
            return image_result
            
        except requests.RequestException as e:
            error_msg = f"Network error: {str(e)}"
            logger.error(f"❌ {error_msg}")
            if attempt < effective_max_retries:
                wait_time = retry_delay * attempt
                logger.info(f"⏳ Retrying in {wait_time}s... (attempt {attempt}/{effective_max_retries})")
                time.sleep(wait_time)
                continue
            else:
                image_result["error"] = error_msg
                return image_result
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"❌ {error_msg}")
            if attempt < effective_max_retries:
                wait_time = retry_delay * attempt
                logger.info(f"⏳ Retrying in {wait_time}s... (attempt {attempt}/{effective_max_retries})")
                time.sleep(wait_time)
                continue
            else:
                image_result["error"] = error_msg
                return image_result
    
    # If we get here, all retries failed
    if image_result["error"] is None:
        image_result["error"] = f"Failed after {effective_max_retries} attempts"
    return image_result

def process_and_upload_images_from_urls(
    image_urls: list[str],
    product_id: str,
    bucket_name: str,
    folder_name: str,
    prompt_tags: str,
    upload_original: bool = True,
    max_retries: int = 2,
    retry_delay: int = 2,
) -> tuple[list[str], list[tuple[str, str, int]], dict]:
    """
    For each URL: download to memory → upload original to S3 → process with Gemini → upload processed to S3.
    Processed files are saved under:
        {folder_name}/{product_id}/{product_image_code}/{product_image_code}_{index}.jpg
    No local files are created - everything is processed in memory (AWS server optimized).
    Retries each image until successful or max retries reached.
    
    Args:
        image_urls: List of image URLs to process
        product_id: Product ID for S3 folder structure
        bucket_name: S3 bucket name
        folder_name: S3 folder/prefix
        prompt_tags: Gemini prompt for image processing
        upload_original: Whether to upload original image to S3 (default: True)
        max_retries: Maximum number of retries per image (default: 3)
        retry_delay: Delay in seconds between retries (default: 3)
    
    Returns:
        Tuple of:
        - List of processed S3 URLs only
        - List of failed URLs with error messages
        - Dictionary with detailed results per image
    """
    # TEMPORARY: Skip image processing if disabled
    if not IMAGE_PROCESSING_ENABLED:
        logger.info("⚠️  Image processing is temporarily disabled. Skipping S3 uploads and Gemini processing.")
        logger.info(f"   Would have processed {len(image_urls)} image(s) for product {product_id}")
        return [], [(url, "Image processing disabled", idx) for idx, url in enumerate(image_urls, 1)], {}
    
    # Hard cap retries to 2 even if caller passes larger value
    effective_max_retries = min(max_retries, 2)
    
    processed_urls: list[str] = []  # Only processed URLs
    failed_urls: list[tuple[str, str, int]] = []  # Store (url, error_message, index) for failed images
    image_results: dict = {}  # Store detailed results per image
    total_images = len(image_urls)
    product_image_code = generate_product_image_code(product_id)
    
    logger.info("=" * 80)
    logger.info(f" Starting processing of {total_images} image(s)")
    logger.info(f" Mode: In-memory processing (AWS server optimized)")
    logger.info(f" Upload original: {upload_original}")
    logger.info(f" Max retries per image: {effective_max_retries}")
    logger.info(f" Retry delay: {retry_delay}s")
    logger.info(f" Product image code: {product_image_code}")
    logger.info("=" * 80)
    
    for idx, url in enumerate(image_urls, 1):
        logger.info("")
        logger.info("=" * 80)
        logger.info(f"📸 Processing image {idx}/{total_images}")
        logger.info(f"   URL: {url}")
        logger.info("=" * 80)
        
        # Add delay between images to avoid rate limiting
        if idx > 1:
            if 'alicdn.com' in url:
                delay = 2.0  # 2 seconds for AliExpress
            else:
                delay = 1.0  # 1 second for others
            logger.info(f"⏳ Waiting {delay}s before processing to avoid rate limiting...")
            time.sleep(delay)
        
        # Process image with retry logic
        image_result = process_single_image_with_retry(
            url=url,
            product_id=product_id,
            bucket_name=bucket_name,
            folder_name=folder_name,
            prompt_tags=prompt_tags,
            upload_original=upload_original,
            max_retries=effective_max_retries,
            retry_delay=retry_delay,
            product_image_code=product_image_code,
            image_index=idx,
        )
        
        # Store result
        image_results[url] = image_result
        
        # Extract processed URL (only this is returned)
        if image_result["status"] == "success" and image_result["processed_url"]:
            processed_urls.append(image_result["processed_url"])
            logger.info(f" Image {idx}/{total_images} completed successfully")
        else:
            failed_urls.append((url, image_result.get("error", "Unknown error"), idx))
            logger.warning(f"  Image {idx}/{total_images} failed: {image_result.get('error', 'Unknown error')}")
        
        # Log progress
        logger.info(f" Progress: {idx}/{total_images} images processed, {len(processed_urls)} processed successfully, {len(failed_urls)} failed")
    
    # Final summary
    logger.info("")
    logger.info("=" * 80)
    logger.info(" PROCESSING SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Total images: {total_images}")
    logger.info(f"Successfully processed: {len(processed_urls)}")
    logger.info(f"Failed: {len(failed_urls)}")
    if failed_urls:
        logger.warning("")
        logger.warning("⚠️  Failed images (after all retries):")
        for url, error, idx in failed_urls:
            logger.warning(f"   - {url}")
            logger.warning(f"     Error: {error}")
    logger.info("=" * 80)
    
    # Return only processed URLs
    return processed_urls, failed_urls, image_results