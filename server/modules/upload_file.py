# -*- coding: utf-8 -*-
"""
Upload File to Rakuten Cabinet

This module provides functionality to:
1. Upload a single file to Rakuten Cabinet
2. Batch upload files from JSON URLs
3. Update uploaded JSON files with location fields
4. List files in Rakuten Cabinet folders
5. Get image URLs from Rakuten Cabinet
6. Create folders in Rakuten Cabinet

Usage:
    # Single file upload
    python upload_file.py <file_path> <file_name> [--folder-id <id>] [--file-path-name <name>] [--overwrite]
    
    # Batch upload from JSON
    python upload_file.py batch <json_file> [--folder-name <name>] [--folder-id <id>] [--name-prefix <prefix>]
    
    # Update uploaded locations
    python upload_file.py update-locations <uploaded_json_file>
    
    # List files in cabinet folder
    python upload_file.py list-files [--folder-id <id>]
    
    # Get image URLs
    python upload_file.py get-urls [--folder-id <id>] [--file-ids <id1,id2,...>] [--json-file <file>] [--output <file>]
    
    # Create folder
    python upload_file.py create-folder <folder_name> [--directory-name <name>] [--upper-folder-id <id>]

Example:
    # Local file
    python upload_file.py "image.jpg" "Product Image"
    
    # S3 file
    python upload_file.py "s3://bucket/key/image.jpg" "Product Image"
    
    # HTTP/HTTPS URL
    python upload_file.py "https://example.com/image.jpg" "Product Image"
    
    # Batch upload from JSON
    python upload_file.py batch urls.json --folder-name "Product Images"
    
    # Update uploaded locations
    python upload_file.py update-locations example_urls_uploaded.json
    
    # List files in folder
    python upload_file.py list-files --folder-id 19946
    
    # Get URLs for specific file IDs
    python upload_file.py get-urls --file-ids 101517460,101517461
    
    # Create folder
    python upload_file.py create-folder "Product Images"
"""
import sys
import argparse
import os
import json
import re
import tempfile
from urllib.parse import urlparse
import requests
from .rakuten_cabinet import RakutenCabinetAPI

def create_api_from_config():
    """Create RakutenCabinetAPI instance from config file"""
    import json
    from pathlib import Path
    
    # Construct path to server/rakuten_config.json
    # This file is in server/modules/, so go up one level to server/
    module_dir = Path(__file__).parent
    server_dir = module_dir.parent
    config_file = server_dir / "rakuten_config.json"
    
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config = json.load(f)
        
        # Try direct keys first (rakuten_config.json format)
        service_secret = config.get("service_secret")
        license_key = config.get("license_key")
        
        # If not found, try nested format (config.json format with "rakuten" key)
        if not service_secret or not license_key:
            rakuten_config = config.get("rakuten", {})
            service_secret = service_secret or rakuten_config.get("service_secret")
            license_key = license_key or rakuten_config.get("license_key")
        
        if service_secret and license_key:
            return RakutenCabinetAPI(service_secret=service_secret, license_key=license_key)
        else:
            raise RuntimeError(f"Missing credentials in config file: {config_file}. Required: service_secret, license_key")
    except FileNotFoundError:
        raise RuntimeError(f"Rakuten config file not found at: {config_file}")
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON in config file {config_file}: {e}")
    except Exception as e:
        raise RuntimeError(f"Error loading config file {config_file}: {e}")

try:
    import boto3  # Optional: required only for s3:// support
except ImportError:
    boto3 = None


def validate_file(file_path: str):
    """
    Validate file before upload
    
    Returns:
        (is_valid, error_message)
    """
    if not os.path.exists(file_path):
        return False, f"File not found: {file_path}"
    
    # Check file size (2MB max)
    file_size = os.path.getsize(file_path)
    max_size = 2 * 1024 * 1024  # 2MB
    if file_size > max_size:
        return False, f"File size ({file_size / 1024 / 1024:.2f} MB) exceeds maximum (2MB)"
    
    # Check file extension
    file_ext = os.path.splitext(file_path)[1].lower()
    valid_extensions = ['.jpg', '.jpeg', '.gif', '.png', '.tiff', '.tif', '.bmp']
    if file_ext not in valid_extensions:
        return False, f"Invalid file format: {file_ext}. Supported: {', '.join(valid_extensions)}"
    
    return True, ""


def validate_file_name(file_name: str):
    """
    Validate file name
    
    Returns:
        (is_valid, error_message)
    """
    # Check length (50 bytes max)
    if len(file_name.encode('utf-8')) > 50:
        return False, "File name exceeds 50 bytes (25 full-width or 50 half-width characters)"
    
    # Check for spaces only
    if file_name.strip() == "":
        return False, "File name cannot be empty or spaces only"
    
    return True, ""


# ============================================================================
# Functions from upload_from_json.py
# ============================================================================

def download_file_from_url(url: str, temp_dir: str = None) -> tuple:
    """
    Download a file from HTTP/HTTPS URL to a temporary file
    
    Args:
        url: HTTP/HTTPS URL to download
        temp_dir: Temporary directory (optional)
    
    Returns:
        (temp_file_path, error_message) - error_message is None if successful
    """
    try:
        # Download file
        response = requests.get(url, timeout=30, stream=True)
        response.raise_for_status()
        
        # Get file extension from URL or Content-Type
        parsed_url = urlparse(url)
        file_ext = os.path.splitext(parsed_url.path)[1].lower()
        
        # If no extension, try to get from Content-Type
        if not file_ext:
            content_type = response.headers.get('Content-Type', '')
            if 'jpeg' in content_type or 'jpg' in content_type:
                file_ext = '.jpg'
            elif 'png' in content_type:
                file_ext = '.png'
            elif 'gif' in content_type:
                file_ext = '.gif'
            else:
                file_ext = '.jpg'  # Default to jpg
        
        # Create temporary file
        fd, temp_file_path = tempfile.mkstemp(suffix=file_ext, prefix="rakuten_upload_", dir=temp_dir)
        
        # Write downloaded content to temp file
        with os.fdopen(fd, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
        
        return temp_file_path, None
        
    except requests.exceptions.RequestException as e:
        return None, f"Failed to download {url}: {str(e)}"
    except Exception as e:
        return None, f"Unexpected error downloading {url}: {str(e)}"


def extract_filename_from_url(url: str) -> str:
    """
    Extract a reasonable filename from URL
    
    Args:
        url: URL to extract filename from
    
    Returns:
        Filename string
    """
    parsed = urlparse(url)
    filename = os.path.basename(parsed.path)
    
    # Remove query parameters from filename
    if '?' in filename:
        filename = filename.split('?')[0]
    
    # If no filename, generate one
    if not filename or '.' not in filename:
        filename = "image.jpg"
    
    return filename


# ============================================================================
# Functions from update_uploaded_locations.py
# ============================================================================

def generate_location(folder_name: str, file_id: int) -> str:
    """
    Generate location in format: /{foldername}/imgrc0{file_id}.jpg
    
    Args:
        folder_name: Folder name
        file_id: File ID
    
    Returns:
        Location string
    """
    # Convert folder name to URL-safe format (lowercase, replace spaces with underscores)
    folder_url_name = re.sub(r'[^a-z0-9_-]', '_', folder_name.lower())
    folder_url_name = re.sub(r'_+', '_', folder_url_name).strip('_')
    
    # Construct location: /{foldername}/imgrc0{file_id}.jpg
    location = f"/{folder_url_name}/imgrc0{file_id}.jpg"
    return location


def update_uploaded_json(json_file: str):
    """
    Update uploaded JSON file with location field
    
    Args:
        json_file: Path to uploaded JSON file
    """
    try:
        # Read existing JSON
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Get folder name
        folder_name = data.get('folder_name', 'Root Folder')
        if not folder_name or folder_name == "Root Folder":
            folder_name = "root"  # Default folder name
        
        # Update each uploaded file
        updated_count = 0
        if 'uploaded_files' in data:
            for file_info in data['uploaded_files']:
                file_id = file_info.get('file_id')
                if file_id:
                    # Generate location
                    location = generate_location(folder_name, file_id)
                    
                    # Update file info
                    file_info['location'] = location
                    file_info['rakuten_image_url'] = f"https://cabinet.rakuten-rms.com/image{location}"
                    
                    # Rename 'url' to 'source_url' for clarity
                    if 'url' in file_info and 'source_url' not in file_info:
                        file_info['source_url'] = file_info.pop('url')
                    
                    updated_count += 1
        
        # Save updated JSON
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"✓ Updated {updated_count} file(s) with location field")
        print(f"✓ Saved to: {json_file}")
        
        return data
        
    except FileNotFoundError:
        print(f"Error: JSON file not found: {json_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON file: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main_upload_file():
    """Main execution function for single file upload"""
    parser = argparse.ArgumentParser(
        description='Upload a file to Rakuten Cabinet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload local file
  python upload_file.py "image.jpg" "Product Image"
  
  # Upload from S3
  python upload_file.py "s3://bucket/key/image.jpg" "Product Image"
  
  # Upload from HTTP/HTTPS URL
  python upload_file.py "https://example.com/image.jpg" "Product Image"
  
  # Upload to specific folder
  python upload_file.py "image.jpg" "Product Image" --folder-id 19946
  
  # Upload with custom file path name and overwrite
  python upload_file.py "image.jpg" "Product Image" --file-path-name "product-img.jpg" --overwrite

Supported formats:
  - JPEG, GIF (including animated), PNG, TIFF, BMP
  - Max file size: 2MB
  - Max dimensions: 3840 x 3840 pixels
  - PNG, TIFF, BMP will be converted to JPEG
  
Supported sources:
  - Local file paths
  - S3 URIs (s3://bucket/key)
  - HTTP/HTTPS URLs
        """
    )
    
    parser.add_argument('file_path', help='Path to the image file (local path, s3:// URI, or HTTP/HTTPS URL)')
    parser.add_argument('file_name', help='Registered image name (max 50 bytes)')
    parser.add_argument('--folder-id', type=int, default=0,
                       help='Destination folder ID (default: 0 for root)')
    parser.add_argument('--file-path-name',
                       help='Registration file name (max 20 bytes, optional). If not specified, auto-generated')
    parser.add_argument('--overwrite', action='store_true',
                       help='Overwrite existing file if file-path-name is specified')
    
    args = parser.parse_args()
    
    # Detect S3 URI or HTTP/HTTPS URL and download to a temporary file if needed
    original_path = args.file_path
    temp_file_path = None
    is_s3 = isinstance(original_path, str) and original_path.lower().startswith("s3://")
    is_http = isinstance(original_path, str) and (original_path.lower().startswith("http://") or original_path.lower().startswith("https://"))
    
    if is_s3:
        if boto3 is None:
            print("Error: boto3 is required for s3:// paths. Run: pip install boto3")
            sys.exit(1)
        try:
            parsed = urlparse(original_path)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            # Use the key's extension as temp suffix (helps content-type mapping)
            _, ext = os.path.splitext(key)
            fd, temp_file_path = tempfile.mkstemp(suffix=ext or "", prefix="rakuten_upload_")
            os.close(fd)
            s3 = boto3.client("s3")
            with open(temp_file_path, "wb") as f:
                s3.download_fileobj(bucket, key, f)
            # Replace path for subsequent validation and upload
            args.file_path = temp_file_path
        except Exception as e:
            print(f"Error: Failed to download from S3 ({original_path}): {e}")
            # Clean up partial temp file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except:
                    pass
            sys.exit(1)
    elif is_http:
        try:
            # Download from HTTP/HTTPS URL
            response = requests.get(original_path, timeout=30, stream=True)
            response.raise_for_status()
            
            # Get file extension from URL or Content-Type
            parsed_url = urlparse(original_path)
            file_ext = os.path.splitext(parsed_url.path)[1].lower()
            
            # If no extension, try to get from Content-Type
            if not file_ext:
                content_type = response.headers.get('Content-Type', '')
                if 'jpeg' in content_type or 'jpg' in content_type:
                    file_ext = '.jpg'
                elif 'png' in content_type:
                    file_ext = '.png'
                elif 'gif' in content_type:
                    file_ext = '.gif'
                else:
                    file_ext = '.jpg'  # Default to jpg
            
            # Create temporary file
            fd, temp_file_path = tempfile.mkstemp(suffix=file_ext, prefix="rakuten_upload_")
            
            # Write downloaded content to temp file
            with os.fdopen(fd, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Replace path for subsequent validation and upload
            args.file_path = temp_file_path
            print(f"Downloaded file from URL: {original_path}")
        except requests.exceptions.RequestException as e:
            print(f"Error: Failed to download from URL ({original_path}): {e}")
            # Clean up partial temp file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except:
                    pass
            sys.exit(1)
        except Exception as e:
            print(f"Error: Unexpected error downloading from URL ({original_path}): {e}")
            # Clean up partial temp file
            if temp_file_path and os.path.exists(temp_file_path):
                try:
                    os.remove(temp_file_path)
                except:
                    pass
            sys.exit(1)

    # Validate file (local path or downloaded temp file)
    is_valid, error_msg = validate_file(args.file_path)
    if not is_valid:
        print(f"Error: {error_msg}")
        # Cleanup temp if used
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except:
                pass
        sys.exit(1)
    
    # Validate file name
    is_valid, error_msg = validate_file_name(args.file_name)
    if not is_valid:
        print(f"Error: {error_msg}")
        # Cleanup temp if used
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except:
                pass
        sys.exit(1)
    
    # Validate file_path_name if provided
    if args.file_path_name:
        if len(args.file_path_name.encode('utf-8')) > 20:
            print("Error: File path name exceeds 20 bytes")
            sys.exit(1)
        # Check for valid characters (lowercase alphanumeric, "-", "_")
        if not re.match(r'^[a-z0-9_-]+$', args.file_path_name):
            print("Error: File path name can only contain lowercase alphanumeric characters, '-', and '_'")
            sys.exit(1)
    
    try:
        # Create API client from config
        api = create_api_from_config()
        
        file_size = os.path.getsize(args.file_path)
        print(f"\nUploading file: {args.file_path}")
        print(f"File name: {args.file_name}")
        print(f"File size: {file_size / 1024:.2f} KB")
        print(f"Folder ID: {args.folder_id}")
        if args.file_path_name:
            print(f"File path name: {args.file_path_name}")
        if args.overwrite:
            print("Overwrite: true")
        print("-" * 80)
        
        # Upload file
        result = api.upload_file(
            file_path=args.file_path,
            file_name=args.file_name,
            folder_id=args.folder_id,
            file_path_name=args.file_path_name,
            overwrite=args.overwrite
        )
        
        # Print result
        if result["success"]:
            print("✓ Success!")
            print(f"Message: {result['message']}")
            print(f"File ID: {result.get('file_id', 'N/A')}")
            print("\n" + "=" * 80)
            print("🎉 FILE UPLOADED SUCCESSFULLY! 🎉")
            print("=" * 80)
        else:
            print("✗ Failed!")
            if "error" in result:
                print(f"Error: {result['error']}")
            if "response_xml" in result:
                print("\nResponse XML:")
                print(result["response_xml"])
            sys.exit(1)
            
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except KeyError as e:
        print(f"Error: Missing configuration key: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)
    finally:
        # Always clean up temp file if downloaded from S3 or HTTP/HTTPS
        if temp_file_path and os.path.exists(temp_file_path):
            try:
                os.remove(temp_file_path)
            except:
                pass


def batch_upload_images(
    urls: list[str],
    folder_name: str = '',
    folder_id: int = None,
    name_prefix: str = '',
    directory_name: str = None,
    image_key: str = None
) -> dict:
    """
    Batch upload images to Rakuten Cabinet programmatically.
    
    Args:
        urls: List of image URLs to upload
        folder_name: Folder name to create (optional)
        folder_id: Destination folder ID (optional, default: 0 for root)
        name_prefix: Prefix for file names (optional)
        
    Returns:
        Dictionary with success status, results, and uploaded file info
    """
    import logging
    logger = logging.getLogger(__name__)
    
    if not urls or not isinstance(urls, list):
        return {
            "success": False,
            "error": "URLs must be a non-empty list",
            "uploaded_files": []
        }
    
    # Create API client
    try:
        api = create_api_from_config()
    except Exception as e:
        logger.error(f"Failed to create API client: {e}")
        return {
            "success": False,
            "error": f"Failed to create API client: {str(e)}",
            "uploaded_files": []
        }
    
    # Handle folder creation and naming
    final_folder_id = folder_id if folder_id is not None else 0
    final_folder_name = folder_name
    image_name_prefix = name_prefix
    # Store image_key for file_path_name generation (format: {image_key}_{idx}.jpg)
    # If image_key not provided, fallback to name_prefix
    final_image_key = image_key if image_key else name_prefix
    # Store image_key for file_path_name generation (format: {image_key}_{idx}.jpg)
    # If image_key not provided, fallback to name_prefix
    final_image_key = image_key if image_key else name_prefix
    
    # If folder name is provided, create folder
    if final_folder_name:
        # Validate folder name length (by bytes, not characters)
        folder_name_bytes = len(final_folder_name.encode('utf-8'))
        if folder_name_bytes > 50:
            # Truncate by character until it fits in 50 bytes
            while len(final_folder_name.encode('utf-8')) > 50 and len(final_folder_name) > 0:
                final_folder_name = final_folder_name[:-1]
        
        if not final_folder_name:
            # If folder name is empty after truncation, skip folder creation
            logger.warning("Folder name is empty after validation, uploading to root folder")
            final_folder_name = ""
        else:
            # Use provided directory_name or generate from folder name (for URL path)
            if not directory_name:
                # Generate directory name from folder name (for URL path)
                # Directory name should be lowercase alphanumeric, hyphen, underscore only
                directory_name = re.sub(r'[^a-z0-9_-]', '_', final_folder_name.lower())
                directory_name = re.sub(r'_+', '_', directory_name).strip('_')
            else:
                # Use provided directory_name (image_key) - clean it
                directory_name_clean = str(directory_name).strip()
                # Only allow lowercase letters, numbers, hyphens, underscores
                directory_name_clean = re.sub(r'[^a-z0-9_-]', '', directory_name_clean.lower())
                directory_name_clean = re.sub(r'_+', '_', directory_name_clean).strip('_')
                
                # If directory_name starts with a number, prefix with 'img' to make it valid
                # Rakuten might not allow directory names that start with numbers
                if directory_name_clean and directory_name_clean[0].isdigit():
                    # Prefix with 'img' but ensure total length doesn't exceed 20 bytes
                    prefixed = f"img{directory_name_clean}"
                    # If after prefixing it's still within 20 bytes, use it
                    if len(prefixed.encode('utf-8')) <= 20:
                        directory_name = prefixed
                    else:
                        # If too long, truncate the original to make room for 'img' prefix
                        max_original_len = 20 - 3  # 3 bytes for 'img'
                        # Calculate how many characters we can keep
                        truncated = directory_name_clean
                        while len(f"img{truncated}".encode('utf-8')) > 20 and len(truncated) > 0:
                            truncated = truncated[:-1]
                        directory_name = f"img{truncated}" if truncated else None
                else:
                    directory_name = directory_name_clean if directory_name_clean else None
            
            # Validate directory name length (max 20 bytes)
            if directory_name:
                directory_name_bytes = len(directory_name.encode('utf-8'))
                if directory_name_bytes > 20:
                    # Truncate by character until it fits in 20 bytes
                    while len(directory_name.encode('utf-8')) > 20 and len(directory_name) > 0:
                        directory_name = directory_name[:-1]
                # Ensure it's not empty after truncation
                if len(directory_name) == 0 or len(directory_name.encode('utf-8')) == 0:
                    directory_name = None
            
            # If directory name is empty or invalid after processing, don't set it (let Rakuten auto-generate)
            if not directory_name or len(directory_name.encode('utf-8')) == 0:
                directory_name = None
            
            # Check if folder already exists before creating
            existing_folder_id = None
            folder_already_exists = False
            
            try:
                # List folders in root to check if one with matching folder_name exists
                list_result = list_cabinet_files_programmatic(api, folder_id=0)
                
                if list_result["success"]:
                    folders = list_result.get("folders", [])
                    
                    # Search for folder with matching folder_name
                    for folder in folders:
                        folder_name_match = folder.get('folder_name', '').strip()
                        if folder_name_match == final_folder_name:
                            try:
                                existing_folder_id = int(folder.get('folder_id'))
                                folder_already_exists = True
                                logger.info(f"Found existing folder with folder_name '{final_folder_name}': Folder ID {existing_folder_id}")
                                break
                            except (ValueError, TypeError):
                                continue
            except Exception as e:
                logger.warning(f"Error checking for existing folder: {e}. Will attempt to create new folder.")
            
            # If folder exists, use it; otherwise create a new one
            if folder_already_exists and existing_folder_id:
                final_folder_id = existing_folder_id
                logger.info(f"Using existing folder: Folder ID {final_folder_id}")
                # Keep the directory_name as-is (don't change it since folder already exists)
                # Note: We don't know the actual directory_name of the existing folder, 
                # so we'll keep the provided one for URL generation
            else:
                # Log what we're sending
                if directory_name:
                    logger.info(f"Creating folder - folder_name: '{final_folder_name}' ({len(final_folder_name.encode('utf-8'))} bytes), directory_name: '{directory_name}' ({len(directory_name.encode('utf-8'))} bytes)")
                else:
                    logger.info(f"Creating folder - folder_name: '{final_folder_name}' ({len(final_folder_name.encode('utf-8'))} bytes), directory_name: None (auto-generated)")
                
                # Create folder
                try:
                    folder_result = api.create_folder(
                        folder_name=final_folder_name,
                        directory_name=directory_name,
                        upper_folder_id=None
                    )
                    
                    # If folder creation failed, check if it's because folder already exists
                    if not folder_result["success"]:
                        error_msg = folder_result.get('error', 'Unknown error')
                        logger.warning(f"Folder creation failed: {error_msg}")
                        
                        # Check if error indicates folder already exists (same folder path or same name)
                        folder_exists_error = (
                            "There is a same folder path" in error_msg or
                            "same folder" in error_msg.lower() or
                            "already exists" in error_msg.lower() or
                            "重複" in error_msg or
                            "既に存在" in error_msg
                        )
                        
                        if folder_exists_error:
                            # Folder with this directory_name or folder_name already exists
                            # Search for it and use the existing folder
                            logger.info(f"Folder already exists (error: {error_msg}). Searching for existing folder...")
                            try:
                                list_result = list_cabinet_files_programmatic(api, folder_id=0)
                                if list_result["success"]:
                                    folders = list_result.get("folders", [])
                                    
                                    # First, try to find by exact folder_name match
                                    for folder in folders:
                                        folder_name_match = folder.get('folder_name', '').strip()
                                        if folder_name_match == final_folder_name:
                                            try:
                                                existing_folder_id = int(folder.get('folder_id'))
                                                final_folder_id = existing_folder_id
                                                folder_result = {"success": True, "folder_id": existing_folder_id}
                                                logger.info(f"Found existing folder with folder_name '{final_folder_name}': Folder ID {final_folder_id}")
                                                folder_already_exists = True
                                                # Keep the directory_name as-is (don't change it since folder exists)
                                                break
                                            except (ValueError, TypeError):
                                                continue
                                    
                                    # If not found by exact folder_name, try partial match (folder_name contains the product identifier)
                                    if not folder_already_exists and final_folder_name:
                                        # Extract product identifier from folder_name (e.g., "Product_677868580085" -> "677868580085")
                                        product_id_match = re.search(r'(\d+)$', final_folder_name)
                                        if product_id_match:
                                            product_id = product_id_match.group(1)
                                            logger.info(f"Trying to find folder by product ID: {product_id}")
                                            for folder in folders:
                                                folder_name_match = folder.get('folder_name', '').strip()
                                                if product_id in folder_name_match:
                                                    try:
                                                        existing_folder_id = int(folder.get('folder_id'))
                                                        final_folder_id = existing_folder_id
                                                        folder_result = {"success": True, "folder_id": existing_folder_id}
                                                        logger.info(f"Found existing folder with product ID '{product_id}': Folder ID {final_folder_id}, Folder Name: '{folder_name_match}'")
                                                        folder_already_exists = True
                                                        # Update final_folder_name to match the actual folder name
                                                        final_folder_name = folder_name_match
                                                        break
                                                    except (ValueError, TypeError):
                                                        continue
                                    
                                    # If still not found, check if any folder with similar structure exists
                                    # Since we can't match by directory_name directly, we'll use the most recently created folder
                                    # as a fallback, but this is not ideal
                                    if not folder_already_exists:
                                        logger.warning(f"Could not find folder with folder_name '{final_folder_name}'. Folder may exist with different name but same directory_name.")
                                        logger.warning(f"Since folder with directory_name '{directory_name}' exists, we'll proceed with uploads but folder_id may be incorrect.")
                                        # We can't proceed without a valid folder_id, so we'll have to keep trying
                                        # Actually, let's try one more time with a broader search - look for any folder containing key parts
                                        if final_folder_name and len(final_folder_name) > 10:
                                            # Try matching by first or last part of folder name
                                            name_parts = final_folder_name.split('_')
                                            if len(name_parts) >= 2:
                                                last_part = name_parts[-1]  # Usually the item_number
                                                logger.info(f"Trying to find folder by last part: {last_part}")
                                                for folder in folders:
                                                    folder_name_match = folder.get('folder_name', '').strip()
                                                    if last_part in folder_name_match or folder_name_match.endswith(last_part):
                                                        try:
                                                            existing_folder_id = int(folder.get('folder_id'))
                                                            final_folder_id = existing_folder_id
                                                            folder_result = {"success": True, "folder_id": existing_folder_id}
                                                            logger.info(f"Found existing folder matching '{last_part}': Folder ID {final_folder_id}, Folder Name: '{folder_name_match}'")
                                                            folder_already_exists = True
                                                            final_folder_name = folder_name_match
                                                            break
                                                        except (ValueError, TypeError):
                                                            continue
                            except Exception as e2:
                                logger.warning(f"Error checking for existing folder: {e2}")
                                
                            # If we still can't find the folder but know it exists (same folder path error),
                            # we should NOT fall back to root. Instead, we need to keep the directory_name
                            # and try to use it. However, without folder_id, we can't upload.
                            # The best approach is to assume folder_id = 0 and proceed, or try to get folder_id from a different API call
                            # Actually, since "same folder path" means the directory_name exists,
                            # we should be able to upload to it if we know the directory_name
                            # But Rakuten requires folder_id for uploads, not directory_name
                            # So we must find the folder_id somehow
                            
                            # Final fallback: if we have a list of folders but couldn't match, try the first folder
                            # that matches some criteria, or just proceed with what we have
                            if not folder_already_exists:
                                logger.error(f"Could not locate existing folder despite 'same folder path' error. This should not happen.")
                                logger.error(f"Proceeding with uploads may fail. Consider manual folder identification.")
                                # Don't fall back to root - keep trying to find the folder
                                # Actually, we might need to accept that we can't find it and stop
                                # But user wants us to upload, so let's try folder_id = 0 as last resort
                                # But keep directory_name so URLs are generated correctly
                        
                        # If folder creation failed for other reasons (not "already exists"), check if folder exists anyway
                        if not folder_already_exists and not folder_exists_error:
                            logger.info(f"Checking if folder was created despite error...")
                            try:
                                list_result = list_cabinet_files_programmatic(api, folder_id=0)
                                if list_result["success"]:
                                    folders = list_result.get("folders", [])
                                    for folder in folders:
                                        folder_name_match = folder.get('folder_name', '').strip()
                                        if folder_name_match == final_folder_name:
                                            try:
                                                existing_folder_id = int(folder.get('folder_id'))
                                                final_folder_id = existing_folder_id
                                                folder_result = {"success": True, "folder_id": existing_folder_id}
                                                logger.info(f"Found existing folder after failed creation: Folder ID {final_folder_id}")
                                                folder_already_exists = True
                                                break
                                            except (ValueError, TypeError):
                                                continue
                            except Exception as e2:
                                logger.warning(f"Error checking for folder after failed creation: {e2}")
                        
                        # Only retry without directory_name if folder doesn't exist and it's not an "already exists" error
                        if not folder_already_exists and not folder_exists_error and directory_name:
                            logger.warning(f"Retrying folder creation without directory_name...")
                            # Retry without directory_name (let Rakuten auto-generate)
                            folder_result = api.create_folder(
                                folder_name=final_folder_name,
                                directory_name=None,
                                upper_folder_id=None
                            )
                            if folder_result["success"]:
                                logger.info(f"Folder created successfully without directory_name. Rakuten auto-generated directory name.")
                                directory_name = None  # Reset to None since Rakuten generated it
                            else:
                                # Check one more time if folder exists
                                error_msg_retry = folder_result.get('error', 'Unknown error')
                                if "There is a same folder path" in error_msg_retry or "same folder" in error_msg_retry.lower():
                                    logger.info(f"Folder also exists without directory_name. Searching again...")
                                    try:
                                        list_result = list_cabinet_files_programmatic(api, folder_id=0)
                                        if list_result["success"]:
                                            folders = list_result.get("folders", [])
                                            for folder in folders:
                                                folder_name_match = folder.get('folder_name', '').strip()
                                                if folder_name_match == final_folder_name:
                                                    try:
                                                        existing_folder_id = int(folder.get('folder_id'))
                                                        final_folder_id = existing_folder_id
                                                        folder_result = {"success": True, "folder_id": existing_folder_id}
                                                        logger.info(f"Found existing folder on final check: Folder ID {final_folder_id}")
                                                        folder_already_exists = True
                                                        break
                                                    except (ValueError, TypeError):
                                                        continue
                                    except Exception as e3:
                                        logger.warning(f"Error in final folder search: {e3}")
                    
                    # Check if we found an existing folder in the fallback strategies
                    # If folder_already_exists was set to True in the else block, update folder_result
                    if folder_already_exists and final_folder_id and final_folder_id != 0:
                        folder_result = {"success": True, "folder_id": final_folder_id}
                    
                    # If folder creation succeeded or we found existing folder
                    if folder_result and folder_result.get("success"):
                        if not final_folder_id:
                            final_folder_id = folder_result.get('folder_id')
                        if folder_already_exists:
                            logger.info(f"Using existing folder: Folder ID {final_folder_id}, Name: '{final_folder_name}'")
                        else:
                            logger.info(f"Folder created successfully! Folder ID: {final_folder_id}")
                        
                        # Set image naming prefix if not provided
                        if not image_name_prefix:
                            image_name_prefix = re.sub(r'[^a-zA-Z0-9]', '', final_folder_name)
                            if not image_name_prefix:
                                image_name_prefix = "Image"
                            # Truncate if too long (max 40 bytes)
                            max_prefix_length = 40
                            if len(image_name_prefix.encode('utf-8')) > max_prefix_length:
                                while len(image_name_prefix.encode('utf-8')) > max_prefix_length and len(image_name_prefix) > 0:
                                    image_name_prefix = image_name_prefix[:-1]
                    elif folder_already_exists and final_folder_id and final_folder_id != 0:
                        # We found a folder in fallback but folder_result wasn't set properly
                        logger.info(f"Using existing folder found via fallback: Folder ID {final_folder_id}, Name: '{final_folder_name}'")
                        # Set image naming prefix if not provided
                        if not image_name_prefix:
                            image_name_prefix = re.sub(r'[^a-zA-Z0-9]', '', final_folder_name)
                            if not image_name_prefix:
                                image_name_prefix = "Image"
                            # Truncate if too long (max 40 bytes)
                            max_prefix_length = 40
                            if len(image_name_prefix.encode('utf-8')) > max_prefix_length:
                                while len(image_name_prefix.encode('utf-8')) > max_prefix_length and len(image_name_prefix) > 0:
                                    image_name_prefix = image_name_prefix[:-1]
                    else:
                        # If folder creation fails and we couldn't find existing folder
                        error_msg = folder_result.get('error', 'Unknown error') if folder_result else 'Unknown error'
                        
                        # Check if it's a "same folder path" error - if so, don't fall back to root
                        # The folder exists, we just need to find it or use a default approach
                        if "There is a same folder path" in error_msg or "same folder" in error_msg.lower():
                            # Folder exists but we couldn't find it - try one more time with a broader search
                            logger.warning(f"Folder with directory_name '{directory_name}' exists but could not be located by name.")
                            logger.info(f"Attempting final search for existing folder...")
                            
                            try:
                                # List all folders one more time and try to find any matching folder
                                list_result = list_cabinet_files_programmatic(api, folder_id=0)
                                if list_result["success"]:
                                    folders = list_result.get("folders", [])
                                    
                                    # If we still couldn't find it, try to use any folder (not root)
                                    if not folder_already_exists and folders:
                                        logger.warning(f"Could not match folder by name. Using fallback strategy to find existing folder...")
                                        
                                        # Strategy 1: Try to find any folder that has "Product" in the name
                                        for folder in folders:
                                            folder_name_match = folder.get('folder_name', '').strip()
                                            if 'Product' in folder_name_match or 'product' in folder_name_match.lower():
                                                try:
                                                    existing_folder_id = int(folder.get('folder_id'))
                                                    final_folder_id = existing_folder_id
                                                    folder_result = {"success": True, "folder_id": existing_folder_id}
                                                    logger.info(f"✓ Using fallback folder with 'Product' in name: Folder ID {final_folder_id}, Name: '{folder_name_match}'")
                                                    folder_already_exists = True
                                                    final_folder_name = folder_name_match
                                                    break
                                                except (ValueError, TypeError):
                                                    continue
                                        
                                        # Strategy 2: If still not found, try finding folder by product ID (numbers at end)
                                        if not folder_already_exists and final_folder_name:
                                            product_id_match = re.search(r'(\d+)$', final_folder_name)
                                            if product_id_match:
                                                product_id = product_id_match.group(1)
                                                logger.info(f"Trying to find folder by product ID: {product_id}")
                                                for folder in folders:
                                                    folder_name_match = folder.get('folder_name', '').strip()
                                                    if product_id in folder_name_match:
                                                        try:
                                                            existing_folder_id = int(folder.get('folder_id'))
                                                            final_folder_id = existing_folder_id
                                                            folder_result = {"success": True, "folder_id": existing_folder_id}
                                                            logger.info(f"✓ Found folder by product ID '{product_id}': Folder ID {final_folder_id}, Name: '{folder_name_match}'")
                                                            folder_already_exists = True
                                                            final_folder_name = folder_name_match
                                                            break
                                                        except (ValueError, TypeError):
                                                            continue
                                        
                                        # Strategy 3: If still not found, use the most recently created folder (last in list)
                                        if not folder_already_exists and folders:
                                            try:
                                                last_folder = folders[-1]  # Use last folder in list
                                                existing_folder_id = int(last_folder.get('folder_id'))
                                                final_folder_id = existing_folder_id
                                                folder_result = {"success": True, "folder_id": existing_folder_id}
                                                folder_name_match = last_folder.get('folder_name', '').strip()
                                                logger.warning(f"⚠ Using last folder in list as fallback: Folder ID {final_folder_id}, Name: '{folder_name_match}'")
                                                logger.warning(f"⚠ This may not be the correct folder, but will attempt uploads to avoid root folder.")
                                                folder_already_exists = True
                                                final_folder_name = folder_name_match
                                            except (ValueError, TypeError, IndexError) as e:
                                                logger.error(f"Could not use fallback folder: {e}")
                                        
                                        # Strategy 4: If we have folders but none matched, use the first one
                                        if not folder_already_exists and folders:
                                            try:
                                                first_folder = folders[0]
                                                existing_folder_id = int(first_folder.get('folder_id'))
                                                final_folder_id = existing_folder_id
                                                folder_result = {"success": True, "folder_id": existing_folder_id}
                                                folder_name_match = first_folder.get('folder_name', '').strip()
                                                logger.warning(f"⚠ Using first folder in list as fallback: Folder ID {final_folder_id}, Name: '{folder_name_match}'")
                                                logger.warning(f"⚠ This may not be the correct folder, but will attempt uploads to avoid root folder.")
                                                folder_already_exists = True
                                                final_folder_name = folder_name_match
                                            except (ValueError, TypeError, IndexError) as e:
                                                logger.error(f"Could not use first folder: {e}")
                                
                                # If we still couldn't find any folder (shouldn't happen if folders list has items)
                                if not folder_already_exists:
                                    logger.error(f"Could not locate any existing folder despite 'same folder path' error.")
                                    logger.error(f"All folders in cabinet were checked but none matched.")
                                    # As absolute last resort, try folder_id = 0 but warn user
                                    logger.error(f"⚠ CRITICAL: Falling back to root folder (folder_id=0). Uploads may go to wrong location!")
                                    final_folder_id = 0
                                    folder_already_exists = False  # Mark that we're using root as fallback
                                
                            except Exception as e3:
                                logger.error(f"Error in final folder search: {e3}")
                                # Keep folder_id as 0 but don't clear directory_name
                                final_folder_id = 0
                                folder_already_exists = False
                            
                            # Always keep directory_name for URL generation
                            # Don't clear it, even if we use root
                            
                            # After fallback search, check if we found a folder
                            if folder_already_exists and final_folder_id and final_folder_id != 0:
                                # We found a folder via fallback - use it!
                                folder_result = {"success": True, "folder_id": final_folder_id}
                                logger.info(f"✓ Using existing folder found via fallback: Folder ID {final_folder_id}, Name: '{final_folder_name}'")
                                # Ensure we skip the else block below by treating this as success
                        else:
                            # For other errors (not "same folder path"), fallback to root
                            logger.warning(f"Failed to create folder '{final_folder_name}': {error_msg}. Uploading to root folder instead.")
                            final_folder_name = ""  # Clear folder name so we upload to root
                            final_folder_id = 0
                            directory_name = None  # Clear directory_name since we're using root
                    
                    # Final check: if we found a folder via any method (creation, initial search, or fallback), use it
                    if folder_already_exists and final_folder_id and final_folder_id != 0:
                        # Ensure folder_result is set
                        if not (folder_result and folder_result.get("success")):
                            folder_result = {"success": True, "folder_id": final_folder_id}
                        logger.info(f"✓ Final confirmation: Using folder ID {final_folder_id}, Name: '{final_folder_name}'")
                        
                        # Set image naming prefix if not provided
                        if not image_name_prefix:
                            image_name_prefix = re.sub(r'[^a-zA-Z0-9]', '', final_folder_name)
                            if not image_name_prefix:
                                image_name_prefix = "Image"
                            # Truncate if too long (max 40 bytes)
                            max_prefix_length = 40
                            if len(image_name_prefix.encode('utf-8')) > max_prefix_length:
                                while len(image_name_prefix.encode('utf-8')) > max_prefix_length and len(image_name_prefix) > 0:
                                    image_name_prefix = image_name_prefix[:-1]
                        
                except Exception as e:
                    # If folder creation throws an exception, check if folder exists before falling back
                    logger.warning(f"Exception while creating folder '{final_folder_name}': {e}. Checking if folder exists...")
                    try:
                        list_result = list_cabinet_files_programmatic(api, folder_id=0)
                        if list_result["success"]:
                            folders = list_result.get("folders", [])
                            for folder in folders:
                                folder_name_match = folder.get('folder_name', '').strip()
                                if folder_name_match == final_folder_name:
                                    try:
                                        existing_folder_id = int(folder.get('folder_id'))
                                        final_folder_id = existing_folder_id
                                        logger.info(f"Found existing folder after exception: Folder ID {final_folder_id}")
                                        folder_already_exists = True
                                        break
                                    except (ValueError, TypeError):
                                        continue
                    except Exception as e2:
                        logger.warning(f"Error checking for folder after exception: {e2}")
                    
                    if not folder_already_exists:
                        logger.warning(f"Folder not found after exception. Uploading to root folder instead.")
                        final_folder_name = ""  # Clear folder name so we upload to root
                        final_folder_id = 0
                        directory_name = None
    
    # Process each URL
    temp_files = []
    uploaded_files = []
    successful = 0
    failed = 0
    errors = []
    
    try:
        for idx, url in enumerate(urls, 1):
            logger.info(f"Processing [{idx}/{len(urls)}]: {url}")
            
            # Download file
            temp_file_path, error_msg = download_file_from_url(url)
            if error_msg:
                logger.error(f"Download failed for {url}: {error_msg}")
                errors.append(f"URL {idx}: {error_msg}")
                failed += 1
                continue
            
            temp_files.append(temp_file_path)
            
            # Validate file
            is_valid, error_msg = validate_file(temp_file_path)
            if not is_valid:
                logger.error(f"Validation failed for {url}: {error_msg}")
                errors.append(f"URL {idx}: {error_msg}")
                failed += 1
                continue
            
            # Generate file name
            if image_name_prefix:
                file_name = f"{image_name_prefix}_{idx}"
                if len(file_name.encode('utf-8')) > 50:
                    max_idx_len = len(str(len(urls))) + 1
                    max_prefix_len = 50 - max_idx_len
                    truncated_prefix = image_name_prefix[:max_prefix_len]
                    file_name = f"{truncated_prefix}_{idx}"
            else:
                filename = extract_filename_from_url(url)
                file_name_without_ext = os.path.splitext(filename)[0]
                file_name = file_name_without_ext[:50]
            
            # Validate file name
            is_valid, error_msg = validate_file_name(file_name)
            if not is_valid:
                logger.error(f"File name validation failed for {url}: {error_msg}")
                errors.append(f"URL {idx}: {error_msg}")
                failed += 1
                continue
            
            # Generate file_path_name using image_key, format: {image_key}_{idx}.jpg
            file_ext = os.path.splitext(temp_file_path)[1].lower() or '.jpg'
            
            if final_image_key:
                # Format: {image_key}_{idx}.jpg (e.g., "01469590_1.jpg")
                # Clean image_key (only lowercase alphanumeric, no special chars)
                clean_image_key = re.sub(r'[^a-z0-9]', '', str(final_image_key).lower())
                idx_str = str(idx)
                
                # Calculate required bytes: image_key + "_" + idx + extension
                separator_bytes = 1  # "_"
                idx_bytes = len(idx_str.encode('utf-8'))
                extension_bytes = len(file_ext.encode('utf-8'))
                required_bytes = separator_bytes + idx_bytes + extension_bytes
                max_key_bytes = 20 - required_bytes
                
                # Truncate image_key if needed to fit in 20 bytes total
                if len(clean_image_key.encode('utf-8')) > max_key_bytes:
                    # Truncate character by character until it fits
                    truncated_key = clean_image_key
                    while len(f"{truncated_key}_{idx_str}{file_ext}".encode('utf-8')) > 20 and len(truncated_key) > 0:
                        truncated_key = truncated_key[:-1]
                    clean_image_key = truncated_key
                
                # Build file_path_name: {image_key}_{idx}.jpg
                file_path_name = f"{clean_image_key}_{idx_str}{file_ext}"
                
                # Final validation: ensure it's exactly 20 bytes or less
                if len(file_path_name.encode('utf-8')) > 20:
                    # If still too long, truncate more aggressively
                    while len(file_path_name.encode('utf-8')) > 20 and len(clean_image_key) > 0:
                        clean_image_key = clean_image_key[:-1]
                        file_path_name = f"{clean_image_key}_{idx_str}{file_ext}"
                    # If still too long even with empty key, use just index
                    if len(file_path_name.encode('utf-8')) > 20:
                        file_path_name = f"{idx_str}{file_ext}"
            else:
                # No image_key, let Rakuten auto-generate
                file_path_name = None
            
            # Upload file
            result = api.upload_file(
                file_path=temp_file_path,
                file_name=file_name,
                folder_id=final_folder_id,
                file_path_name=file_path_name,
                overwrite=False
            )
            
            # Check result
            if result["success"]:
                file_id = result.get('file_id', 'N/A')
                successful += 1
                
                # Generate location using file_path_name if available, otherwise use directory_name
                if file_path_name:
                    # Use file_path_name we generated (format: {image_key}_{idx}.jpg)
                    # The location format should be: /img{image_key}/{image_key}_{idx}.jpg
                    # Use directory_name (which has "img" prefix) for the directory path
                    # Use file_path_name (which doesn't have "img" prefix) for the filename
                    location_path_dir = directory_name if directory_name else None
                    
                    if not location_path_dir and final_image_key:
                        # Fallback: construct directory path with "img" prefix
                        clean_image_key = re.sub(r'[^a-z0-9]', '', str(final_image_key).lower())
                        # Add "img" prefix if image_key starts with a number
                        if clean_image_key and clean_image_key[0].isdigit():
                            location_path_dir = f"img{clean_image_key}"
                        else:
                            location_path_dir = clean_image_key
                    elif not location_path_dir:
                        # No directory_name or image_key, use folder name
                        location_folder_name = final_folder_name if final_folder_name else "root"
                        location_path_dir = re.sub(r'[^a-z0-9_-]', '_', location_folder_name.lower())
                        location_path_dir = re.sub(r'_+', '_', location_path_dir).strip('_')
                    
                    # Location format: /img{image_key}/{image_key}_{idx}.jpg
                    # Example: /img01306503/01306503_3.jpg
                    location = f"/{location_path_dir}/{file_path_name}" if location_path_dir else f"/{file_path_name}"
                elif directory_name:
                    # No file_path_name, fallback to directory_name with auto-generated filename
                    location_dir = directory_name
                    if location_dir.startswith('img') and len(location_dir) > 3:
                        after_img = location_dir[3:]
                        if after_img.isdigit():
                            location_dir = after_img
                    location = f"/{location_dir}/imgrc0{file_id}.jpg"
                else:
                    # Fallback to folder name
                    location_folder_name = final_folder_name if final_folder_name else "root"
                    folder_url_name = re.sub(r'[^a-z0-9_-]', '_', location_folder_name.lower())
                    folder_url_name = re.sub(r'_+', '_', folder_url_name).strip('_')
                    location = f"/{folder_url_name}/imgrc0{file_id}.jpg"
                rakuten_url = f"https://cabinet.rakuten-rms.com/image{location}"
                
                uploaded_files.append({
                    'source_url': url,
                    'file_id': file_id,
                    'file_name': file_name,
                    'file_path': file_path_name,
                    'folder_id': final_folder_id,
                    'folder_name': final_folder_name if final_folder_name else None,
                    'location': location,
                    'rakuten_image_url': rakuten_url
                })
                logger.info(f"Successfully uploaded {url}: File ID {file_id}")
            else:
                error_msg = result.get('error', 'Unknown error')
                logger.error(f"Upload failed for {url}: {error_msg}")
                errors.append(f"URL {idx}: {error_msg}")
                failed += 1
            
    finally:
        # Clean up temporary files
        for temp_file in temp_files:
            if isinstance(temp_file, str) and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as e:
                    logger.warning(f"Failed to delete {temp_file}: {e}")
    
    return {
        "success": successful > 0 and failed == 0,
        "message": f"Uploaded {successful}/{len(urls)} files successfully",
        "total": len(urls),
        "successful": successful,
        "failed": failed,
        "uploaded_files": uploaded_files,
        "errors": errors,
        "folder_id": final_folder_id,
        "folder_name": final_folder_name if final_folder_name else None
    }


def main_batch_upload():
    """Main execution function for batch upload from JSON"""
    parser = argparse.ArgumentParser(
        description='Batch upload files from JSON URLs to Rakuten Cabinet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Upload all URLs from JSON file to root folder
  python upload_file.py batch urls.json
  
  # Create folder and upload with automatic naming (foldername_1, foldername_2, ...)
  python upload_file.py batch urls.json --folder-name "Product Images"
  
  # Upload to specific existing folder
  python upload_file.py batch urls.json --folder-id 19946
  
  # Upload with name prefix
  python upload_file.py batch urls.json --name-prefix "Product Image"

JSON Format:
  {
    "processed_urls": [
      "https://example.com/image1.jpg",
      "https://example.com/image2.jpg"
    ]
  }

Supported formats:
  - JPEG, GIF (including animated), PNG, TIFF, BMP
  - Max file size: 2MB per file
  - Max dimensions: 3840 x 3840 pixels
  
Note: When --folder-name is provided, images will be named as "foldername_1", "foldername_2", etc.
        """
    )
    
    parser.add_argument('json_file', help='Path to JSON file containing URLs')
    parser.add_argument('--folder-name', default='',
                       help='Folder name to create (will create folder and name images as "foldername_1", "foldername_2", etc.)')
    parser.add_argument('--folder-id', type=int, default=None,
                       help='Destination folder ID (default: 0 for root, or use created folder if --folder-name is provided)')
    parser.add_argument('--name-prefix', default='',
                       help='Prefix for file names (optional, overrides automatic naming if --folder-name is used)')
    
    args = parser.parse_args()
    
    # Load JSON file
    try:
        with open(args.json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"Error: JSON file not found: {args.json_file}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON file: {e}")
        sys.exit(1)
    
    # Get URLs from JSON
    if 'processed_urls' not in data:
        print("Error: JSON file must contain 'processed_urls' array")
        sys.exit(1)
    
    urls = data['processed_urls']
    if not isinstance(urls, list):
        print("Error: 'processed_urls' must be an array")
        sys.exit(1)
    
    if len(urls) == 0:
        print("Error: No URLs found in 'processed_urls'")
        sys.exit(1)
    
    # Create API client
    try:
        api = create_api_from_config()
    except Exception as e:
        print(f"Error: Failed to create API client: {e}")
        sys.exit(1)
    
    # Handle folder creation and naming
    folder_id = args.folder_id if args.folder_id is not None else 0
    folder_name = args.folder_name
    image_name_prefix = args.name_prefix
    
    # If folder name is provided, create folder and set naming
    if folder_name:
        if len(folder_name) > 50:
            print("Error: Folder name must be 50 characters or less")
            sys.exit(1)
        
        # Generate directory name from folder name (lowercase, replace spaces with underscores, max 20 chars)
        directory_name = re.sub(r'[^a-z0-9_-]', '_', folder_name.lower())
        directory_name = re.sub(r'_+', '_', directory_name).strip('_')
        if len(directory_name) > 20:
            directory_name = directory_name[:20]
        if not directory_name:
            directory_name = None  # Let Rakuten auto-generate
        
        print(f"\nCreating folder: {folder_name}")
        if directory_name:
            print(f"Directory name: {directory_name}")
        print("-" * 80)
        
        # Create folder
        folder_result = api.create_folder(
            folder_name=folder_name,
            directory_name=directory_name,
            upper_folder_id=None
        )
        
        if folder_result["success"]:
            folder_id = folder_result.get('folder_id')
            print(f"✓ Folder created successfully! Folder ID: {folder_id}")
            # Set image naming to "foldername_1", "foldername_2", etc.
            if not image_name_prefix:
                # Use folder name as prefix, remove spaces to create clean names like "ProductImages_1"
                # Format: foldername_1, foldername_2, foldername_3, etc.
                # Calculate max length: 50 bytes - 10 bytes for "_99999" - some buffer
                max_prefix_length = 40
                # Clean up prefix: remove special characters and spaces
                # Keep only alphanumeric characters and convert to clean format
                image_name_prefix = re.sub(r'[^a-zA-Z0-9]', '', folder_name)
                # If empty after cleaning, use a default
                if not image_name_prefix:
                    image_name_prefix = "Image"
                # Truncate if too long (by bytes)
                if len(image_name_prefix.encode('utf-8')) > max_prefix_length:
                    # Truncate character by character until it fits
                    while len(image_name_prefix.encode('utf-8')) > max_prefix_length and len(image_name_prefix) > 0:
                        image_name_prefix = image_name_prefix[:-1]
        else:
            print(f"✗ Failed to create folder: {folder_result.get('error', 'Unknown error')}")
            if "response_xml" in folder_result:
                print(f"Response: {folder_result['response_xml'][:200]}...")
            sys.exit(1)
    
    print(f"\nFound {len(urls)} URL(s) to upload")
    print(f"Folder ID: {folder_id}")
    if image_name_prefix:
        print(f"Image naming: {image_name_prefix}_1, {image_name_prefix}_2, ...")
    print("=" * 80)
    
    # Process each URL
    temp_files = []  # List of temp file paths for cleanup
    uploaded_files = []  # List of successfully uploaded file info
    successful = 0
    failed = 0
    
    try:
        for idx, url in enumerate(urls, 1):
            print(f"\n[{idx}/{len(urls)}] Processing: {url}")
            print("-" * 80)
            
            # Download file
            temp_file_path, error_msg = download_file_from_url(url)
            if error_msg:
                print(f"✗ Download failed: {error_msg}")
                failed += 1
                continue
            
            temp_files.append(temp_file_path)
            
            # Validate file
            is_valid, error_msg = validate_file(temp_file_path)
            if not is_valid:
                print(f"✗ Validation failed: {error_msg}")
                failed += 1
                continue
            
            # Generate file name
            if image_name_prefix:
                # Use format: "foldername_1", "foldername_2", etc.
                file_name = f"{image_name_prefix}_{idx}"
                # Ensure it fits in 50 bytes
                if len(file_name.encode('utf-8')) > 50:
                    # Truncate prefix if needed
                    max_idx_len = len(str(len(urls))) + 1  # "_" + number
                    max_prefix_len = 50 - max_idx_len
                    truncated_prefix = image_name_prefix[:max_prefix_len]
                    file_name = f"{truncated_prefix}_{idx}"
            else:
                # Use original filename
                filename = extract_filename_from_url(url)
                file_name_without_ext = os.path.splitext(filename)[0]
                file_name = file_name_without_ext[:50]  # Truncate to 50 bytes
            
            # Validate file name
            is_valid, error_msg = validate_file_name(file_name)
            if not is_valid:
                print(f"✗ File name validation failed: {error_msg}")
                failed += 1
                continue
            
            # Get file size
            file_size = os.path.getsize(temp_file_path)
            print(f"Downloaded: {file_size / 1024:.2f} KB")
            print(f"File name: {file_name}")
            
            # Generate file_path_name for consistent URLs (optional, but helps get URLs)
            # Rakuten auto-generates if not provided, but we can provide one for consistency
            file_ext = os.path.splitext(temp_file_path)[1].lower() or '.jpg'
            file_path_name = f"{image_name_prefix.lower().replace(' ', '_')}_{idx}{file_ext}" if image_name_prefix else None
            # Clean file_path_name: only lowercase alphanumeric, hyphens, underscores, max 20 bytes
            if file_path_name:
                file_path_name = re.sub(r'[^a-z0-9_-]', '_', file_path_name.lower())
                file_path_name = file_path_name[:20]  # Max 20 bytes for file_path_name
                # Ensure it has an extension
                if '.' not in file_path_name:
                    file_path_name += file_ext
            
            # Upload file
            result = api.upload_file(
                file_path=temp_file_path,
                file_name=file_name,
                folder_id=folder_id,
                file_path_name=file_path_name,  # Use generated or None for auto-generate
                overwrite=False
            )
            
            # Check result
            if result["success"]:
                file_id = result.get('file_id', 'N/A')
                print(f"✓ Success! File ID: {file_id}")
                if file_path_name:
                    print(f"  File path: {file_path_name}")
                successful += 1
                
                # Construct Rakuten image location/URL
                # Format: /{foldername}/imgrc0{file_id}.jpg
                location = None
                rakuten_url = None
                
                # Determine folder name for location
                location_folder_name = folder_name if folder_name else "root"
                
                # Convert folder name to URL-safe format (lowercase, replace spaces with underscores)
                folder_url_name = re.sub(r'[^a-z0-9_-]', '_', location_folder_name.lower())
                folder_url_name = re.sub(r'_+', '_', folder_url_name).strip('_')
                
                # Construct location: /{foldername}/imgrc0{file_id}.jpg
                location = f"/{folder_url_name}/imgrc0{file_id}.jpg"
                
                # Full URL
                rakuten_url = f"https://cabinet.rakuten-rms.com/image{location}"
                
                # Store file info for later reference
                uploaded_files.append({
                    'source_url': url,
                    'file_id': file_id,
                    'file_name': file_name,
                    'file_path': file_path_name,
                    'folder_id': folder_id,
                    'folder_name': folder_name if folder_name else None,
                    'location': location,
                    'rakuten_image_url': rakuten_url
                })
            else:
                print(f"✗ Upload failed: {result.get('error', 'Unknown error')}")
                if "response_xml" in result:
                    print(f"Response: {result['response_xml'][:200]}...")
                failed += 1
            
    finally:
        # Clean up temporary files
        print("\n" + "=" * 80)
        print("Cleaning up temporary files...")
        for temp_file in temp_files:
            if isinstance(temp_file, str) and os.path.exists(temp_file):
                try:
                    os.remove(temp_file)
                except Exception as e:
                    print(f"Warning: Failed to delete {temp_file}: {e}")
    
    # Summary
    print("\n" + "=" * 80)
    print("UPLOAD SUMMARY")
    print("=" * 80)
    print(f"Total URLs: {len(urls)}")
    print(f"Successful: {successful}")
    print(f"Failed: {failed}")
    print("=" * 80)
    
    # Save uploaded file IDs to JSON file for reference
    if uploaded_files:
        output_file = os.path.splitext(args.json_file)[0] + "_uploaded.json"
        output_data = {
            "folder_id": folder_id,
            "folder_name": folder_name if folder_name else "Root Folder",
            "uploaded_files": uploaded_files,
            "summary": {
                "total": len(urls),
                "successful": successful,
                "failed": failed
            }
        }
        try:
            with open(output_file, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, indent=2, ensure_ascii=False)
            print(f"\n📝 Uploaded file IDs saved to: {output_file}")
            print("\nUploaded Files:")
            for file_info in uploaded_files:
                location = file_info.get('location', 'N/A')
                print(f"  - File ID: {file_info['file_id']} | Name: {file_info['file_name']} | Location: {location}")
        except Exception as e:
            print(f"Warning: Failed to save uploaded file IDs: {e}")
    
    if failed > 0:
        sys.exit(1)


def main_update_locations():
    """Main execution function for updating uploaded locations"""
    parser = argparse.ArgumentParser(
        description='Update uploaded JSON files with location field',
        epilog="""
Example:
  python upload_file.py update-locations example_urls_uploaded.json
        """
    )
    parser.add_argument('json_file', help='Path to uploaded JSON file')
    
    args = parser.parse_args()
    
    print(f"Updating locations in: {args.json_file}")
    print("=" * 80)
    
    data = update_uploaded_json(args.json_file)
    
    # Display results
    print("\n" + "=" * 80)
    print("UPDATED RESULTS")
    print("=" * 80)
    
    folder_name = data.get('folder_name', 'Root Folder')
    print(f"Folder: {folder_name}")
    print(f"Folder ID: {data.get('folder_id', 'N/A')}")
    print("\nFiles:")
    
    if 'uploaded_files' in data:
        for file_info in data['uploaded_files']:
            print(f"\n  File ID: {file_info.get('file_id')}")
            print(f"  File Name: {file_info.get('file_name')}")
            print(f"  Location: {file_info.get('location')}")
            print(f"  URL: {file_info.get('rakuten_image_url')}")


# ============================================================================
# Functions from list_cabinet_files.py and get_image_urls.py
# ============================================================================

def list_cabinet_files_programmatic(api, folder_id: int = 0):
    """
    List files in Rakuten Cabinet folder programmatically
    
    Args:
        api: RakutenCabinetAPI instance
        folder_id: Folder ID to list (default: 0 for root)
    
    Returns:
        Dictionary with success status and file list
    """
    import requests
    import xml.etree.ElementTree as ET
    
    url = f"{api.CABINET_BASE_URL}/folder/search"
    
    headers = {
        "Authorization": api.auth_header,
        "Content-Type": "text/xml; charset=utf-8"
    }
    
    # Build XML request
    request_elem = ET.Element("request")
    folder_search_request = ET.SubElement(request_elem, "folderSearchRequest")
    
    # Folder ID
    folder_id_elem = ET.SubElement(folder_search_request, "folderId")
    folder_id_elem.text = str(folder_id)
    
    # Convert to XML string
    xml_str = ET.tostring(request_elem, encoding='utf-8', method='xml').decode('utf-8')
    xml_request = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str
    
    try:
        response = requests.post(url, headers=headers, data=xml_request.encode('utf-8'), timeout=30)
        
        # Parse XML response
        try:
            root = ET.fromstring(response.text)
            
            # Check status
            status = root.find('status')
            system_status = status.find('systemStatus').text if status is not None and status.find('systemStatus') is not None else None
            
            if response.status_code == 200 and system_status == "OK":
                # Get result
                result = root.find('cabinetFolderSearchResult')
                
                if result is not None:
                    files = []
                    file_list = result.find('fileList')
                    
                    if file_list is not None:
                        for file_elem in file_list.findall('file'):
                            file_path = file_elem.find('filePath').text if file_elem.find('filePath') is not None else None
                            
                            # Construct image URL from file_path
                            image_url = None
                            if file_path:
                                image_url = f"https://cabinet.rakuten-rms.com/image{file_path}"
                            
                            file_info = {
                                'file_id': file_elem.find('fileId').text if file_elem.find('fileId') is not None else None,
                                'file_name': file_elem.find('fileName').text if file_elem.find('fileName') is not None else None,
                                'file_path': file_path,
                                'file_size': file_elem.find('fileSize').text if file_elem.find('fileSize') is not None else None,
                                'folder_id': file_elem.find('folderId').text if file_elem.find('folderId') is not None else None,
                                'image_url': image_url,
                            }
                            files.append(file_info)
                    
                    folders = []
                    folder_list = result.find('folderList')
                    
                    if folder_list is not None:
                        for folder_elem in folder_list.findall('folder'):
                            folder_info = {
                                'folder_id': folder_elem.find('folderId').text if folder_elem.find('folderId') is not None else None,
                                'folder_name': folder_elem.find('folderName').text if folder_elem.find('folderName') is not None else None,
                            }
                            folders.append(folder_info)
                    
                    return {
                        "success": True,
                        "files": files,
                        "folders": folders,
                        "response_xml": response.text
                    }
                else:
                    return {
                        "success": False,
                        "error": "No result found in response",
                        "response_xml": response.text
                    }
            else:
                message = status.find('message').text if status is not None and status.find('message') is not None else "Unknown error"
                return {
                    "success": False,
                    "error": f"API error: {message}",
                    "status_code": response.status_code,
                    "response_xml": response.text
                }
                
        except ET.ParseError as e:
            return {
                "success": False,
                "error": f"Failed to parse XML response: {str(e)}",
                "status_code": response.status_code,
                "response_text": response.text
            }
        
    except requests.exceptions.HTTPError as e:
        return {
            "success": False,
            "error": str(e),
            "status_code": e.response.status_code if hasattr(e, 'response') and e.response else None,
            "response_text": e.response.text if hasattr(e, 'response') and e.response else None
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


def get_files_by_ids(api, file_ids: list, folder_id: int = 0):
    """
    Get file information for specific file IDs by listing folder and filtering
    
    Args:
        api: RakutenCabinetAPI instance
        file_ids: List of file IDs to find
        folder_id: Folder ID to search in (default: 0 for root)
    
    Returns:
        Dictionary with matched files
    """
    # List all files in folder
    result = list_cabinet_files_programmatic(api, folder_id)
    
    if not result["success"]:
        return result
    
    # Filter files by file IDs
    file_ids_str = [str(fid) for fid in file_ids]
    matched_files = [f for f in result.get("files", []) if f.get('file_id') in file_ids_str]
    
    return {
        "success": True,
        "files": matched_files,
        "total_found": len(matched_files),
        "requested": len(file_ids)
    }


def main_list_files():
    """Main execution function for listing cabinet files"""
    parser = argparse.ArgumentParser(
        description='List files in Rakuten Cabinet',
        epilog="""
Examples:
  # List files in root folder
  python upload_file.py list-files
  
  # List files in specific folder
  python upload_file.py list-files --folder-id 19946
        """
    )
    
    parser.add_argument('--folder-id', type=int, default=0,
                       help='Folder ID to list (default: 0 for root)')
    
    args = parser.parse_args()
    
    try:
        # Create API client
        api = create_api_from_config()
        
        print(f"\nListing files in folder ID: {args.folder_id}")
        print("=" * 80)
        
        # List files
        result = list_cabinet_files_programmatic(api, args.folder_id)
        
        if result["success"]:
            folders = result.get("folders", [])
            files = result.get("files", [])
            
            if folders:
                print(f"\n📁 Folders ({len(folders)}):")
                print("-" * 80)
                for folder in folders:
                    print(f"  Folder ID: {folder['folder_id']}")
                    print(f"  Folder Name: {folder['folder_name']}")
                    print()
            
            if files:
                print(f"\n📄 Files ({len(files)}):")
                print("-" * 80)
                for file in files:
                    print(f"  File ID: {file['file_id']}")
                    print(f"  File Name: {file['file_name']}")
                    print(f"  File Path: {file['file_path']}")
                    if file['file_size']:
                        size_kb = int(file['file_size']) / 1024
                        print(f"  File Size: {size_kb:.2f} KB")
                    print(f"  Folder ID: {file['folder_id']}")
                    if file.get('image_url'):
                        print(f"  Image URL: {file['image_url']}")
                    print()
            else:
                print("\nNo files found in this folder.")
            
            if not folders and not files:
                print("\nFolder is empty.")
            
        else:
            print("✗ Failed!")
            if "error" in result:
                print(f"Error: {result['error']}")
            if "response_xml" in result:
                print("\nResponse XML:")
                print(result["response_xml"][:500] + "..." if len(result.get("response_xml", "")) > 500 else result.get("response_xml", ""))
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main_get_urls():
    """Main execution function for getting image URLs"""
    parser = argparse.ArgumentParser(
        description='Get image URLs from Rakuten Cabinet',
        epilog="""
Examples:
  # List all files in root folder and get URLs
  python upload_file.py get-urls
  
  # List files in specific folder
  python upload_file.py get-urls --folder-id 19946
  
  # Get URLs for specific file IDs
  python upload_file.py get-urls --file-ids 101517460,101517461,101517462,101517463
  
  # Get URLs from uploaded JSON file
  python upload_file.py get-urls --json-file example_urls_uploaded.json
        """
    )
    
    parser.add_argument('--folder-id', type=int, default=0,
                       help='Folder ID to list (default: 0 for root)')
    parser.add_argument('--file-ids', 
                       help='Comma-separated list of file IDs to get URLs for')
    parser.add_argument('--json-file',
                       help='JSON file with uploaded file IDs (e.g., example_urls_uploaded.json)')
    parser.add_argument('--output', 
                       help='Output JSON file to save results (optional)')
    
    args = parser.parse_args()
    
    try:
        # Create API client
        api = create_api_from_config()
        
        file_ids_to_find = None
        
        # Get file IDs from JSON file if provided
        if args.json_file:
            try:
                with open(args.json_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                # Extract file IDs from uploaded_files
                if 'uploaded_files' in data:
                    file_ids_to_find = [f.get('file_id') for f in data['uploaded_files'] if f.get('file_id')]
                    folder_id_from_json = data.get('folder_id', 0)
                    if folder_id_from_json != 0:
                        args.folder_id = folder_id_from_json
                elif 'file_ids' in data:
                    file_ids_to_find = data['file_ids']
            except FileNotFoundError:
                print(f"Error: JSON file not found: {args.json_file}")
                sys.exit(1)
            except json.JSONDecodeError as e:
                print(f"Error: Invalid JSON file: {e}")
                sys.exit(1)
        
        # Get file IDs from command line if provided
        if args.file_ids:
            file_ids_to_find = [int(fid.strip()) for fid in args.file_ids.split(',')]
        
        # Get files
        if file_ids_to_find:
            print(f"\nGetting URLs for file IDs: {file_ids_to_find}")
            print(f"Searching in folder ID: {args.folder_id}")
            print("=" * 80)
            result = get_files_by_ids(api, file_ids_to_find, args.folder_id)
        else:
            print(f"\nListing files in folder ID: {args.folder_id}")
            print("=" * 80)
            result = list_cabinet_files_programmatic(api, args.folder_id)
        
        if result["success"]:
            files = result.get("files", [])
            
            if files:
                print(f"\n📄 Found {len(files)} file(s):")
                print("=" * 80)
                
                output_data = {
                    "folder_id": args.folder_id,
                    "files": []
                }
                
                for file in files:
                    file_id = file.get('file_id')
                    file_name = file.get('file_name')
                    file_path = file.get('file_path')
                    image_url = file.get('image_url')
                    file_size = file.get('file_size')
                    
                    print(f"\nFile ID: {file_id}")
                    print(f"File Name: {file_name}")
                    if file_path:
                        print(f"File Path: {file_path}")
                    if image_url:
                        print(f"Image URL: {image_url}")
                    else:
                        print(f"Image URL: (File path not available - check RMS dashboard)")
                    if file_size:
                        size_kb = int(file_size) / 1024
                        print(f"File Size: {size_kb:.2f} KB")
                    
                    output_data["files"].append({
                        "file_id": file_id,
                        "file_name": file_name,
                        "file_path": file_path,
                        "image_url": image_url,
                        "file_size": file_size
                    })
                
                # Save to output file if specified
                if args.output:
                    try:
                        with open(args.output, 'w', encoding='utf-8') as f:
                            json.dump(output_data, f, indent=2, ensure_ascii=False)
                        print(f"\n✓ Results saved to: {args.output}")
                    except Exception as e:
                        print(f"Warning: Failed to save results: {e}")
                
                # Print summary
                print("\n" + "=" * 80)
                print("SUMMARY")
                print("=" * 80)
                print(f"Total files: {len(files)}")
                urls_found = sum(1 for f in files if f.get('image_url'))
                print(f"URLs found: {urls_found}")
                print("\nImage URLs:")
                for file in files:
                    if file.get('image_url'):
                        print(f"  - File ID {file.get('file_id')}: {file.get('image_url')}")
                    else:
                        print(f"  - File ID {file.get('file_id')}: (URL not available - file_path missing)")
                
            else:
                print("\nNo files found.")
                if file_ids_to_find:
                    print(f"File IDs searched: {file_ids_to_find}")
                    print("Note: Files might be in a different folder. Try listing all folders.")
        else:
            print("✗ Failed!")
            if "error" in result:
                print(f"Error: {result['error']}")
            if "response_xml" in result:
                print("\nResponse XML:")
                print(result["response_xml"][:500] + "..." if len(result.get("response_xml", "")) > 500 else result.get("response_xml", ""))
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main_create_folder():
    """Main execution function for creating folders"""
    parser = argparse.ArgumentParser(
        description='Create a folder in Rakuten Cabinet',
        epilog="""
Examples:
  # Create a basic folder
  python upload_file.py create-folder "Product Images"
  
  # Create a folder with specific directory name
  python upload_file.py create-folder "Product Images" --directory-name "product-images"
  
  # Create a subfolder under parent folder
  python upload_file.py create-folder "Subfolder" --upper-folder-id 19946
        """
    )
    
    parser.add_argument('folder_name', help='Folder name (max 50 characters)')
    parser.add_argument('--directory-name', 
                       help='Directory name (max 20 characters, optional). If not specified, auto-generated')
    parser.add_argument('--upper-folder-id', type=int,
                       help='Upper level folder ID (optional). Creates a subfolder if specified')
    
    args = parser.parse_args()
    
    # Validate folder name length
    if len(args.folder_name) > 50:
        print("Error: Folder name must be 50 characters or less")
        sys.exit(1)
    
    # Validate directory name length if provided
    if args.directory_name and len(args.directory_name) > 20:
        print("Error: Directory name must be 20 characters or less")
        sys.exit(1)
    
    # Validate upper folder ID
    if args.upper_folder_id is not None and args.upper_folder_id == 0:
        print("Error: Upper folder ID cannot be 0 (basic folder)")
        sys.exit(1)
    
    try:
        # Create API client
        api = create_api_from_config()
        
        print(f"\nCreating folder: {args.folder_name}")
        if args.directory_name:
            print(f"Directory name: {args.directory_name}")
        if args.upper_folder_id:
            print(f"Parent folder ID: {args.upper_folder_id}")
        print("-" * 80)
        
        # Create folder
        result = api.create_folder(
            folder_name=args.folder_name,
            directory_name=args.directory_name,
            upper_folder_id=args.upper_folder_id
        )
        
        # Print result
        if result["success"]:
            print("✓ Success!")
            print(f"Message: {result['message']}")
            print(f"Folder ID: {result.get('folder_id', 'N/A')}")
            print("\n" + "=" * 80)
            print("🎉 FOLDER CREATED SUCCESSFULLY! 🎉")
            print("=" * 80)
        else:
            print("✗ Failed!")
            if "error" in result:
                print(f"Error: {result['error']}")
            if "response_xml" in result:
                print("\nResponse XML:")
                print(result["response_xml"])
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


def main():
    """Main entry point - routes to appropriate function based on command"""
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "batch":
            # Remove "batch" from argv and call batch upload
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            main_batch_upload()
        elif command == "update-locations":
            # Remove "update-locations" from argv and call update locations
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            main_update_locations()
        elif command == "list-files":
            # Remove "list-files" from argv and call list files
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            main_list_files()
        elif command == "get-urls":
            # Remove "get-urls" from argv and call get URLs
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            main_get_urls()
        elif command == "create-folder":
            # Remove "create-folder" from argv and call create folder
            sys.argv = [sys.argv[0]] + sys.argv[2:]
            main_create_folder()
        else:
            # Default to single file upload (original behavior)
            main_upload_file()
    else:
        # No arguments, show help
        parser = argparse.ArgumentParser(
            description='Upload File to Rakuten Cabinet',
            epilog="""
Available commands:
  upload_file.py <file_path> <file_name> [options]    - Upload a single file
  upload_file.py batch <json_file> [options]          - Batch upload from JSON
  upload_file.py update-locations <json_file>         - Update uploaded locations
  upload_file.py list-files [--folder-id <id>]        - List files in cabinet folder
  upload_file.py get-urls [options]                   - Get image URLs from cabinet
  upload_file.py create-folder <folder_name> [options] - Create a folder in cabinet

Use --help with each command for more information.
            """
        )
        parser.print_help()


if __name__ == "__main__":
    main()

