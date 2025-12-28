# -*- coding: utf-8 -*-
"""
Rakuten Cabinet API Client

This module provides functionality for managing files and folders in Rakuten Cabinet
(file storage system), including folder creation and file uploads.
"""
import os
import requests
import xml.etree.ElementTree as ET
from typing import Dict, Any, Optional


class RakutenCabinetAPI:
    """Rakuten Cabinet (File Storage) API Client"""
    
    CABINET_BASE_URL = "https://api.rms.rakuten.co.jp/es/1.0/cabinet"
    
    def __init__(self, service_secret: str, license_key: str):
        """
        Initialize the Rakuten Cabinet API client
        
        Args:
            service_secret: Rakuten service secret
            license_key: Rakuten license key
        """
        self.service_secret = service_secret
        self.license_key = license_key
        self.auth_header = self._create_auth_header()
    
    def _create_auth_header(self) -> str:
        """Create ESA Base64 encoded authorization header"""
        import base64
        credentials = f"{self.service_secret}:{self.license_key}"
        encoded = base64.b64encode(credentials.encode('utf-8')).decode('ascii')
        return f"ESA {encoded}"
    
    def create_folder(
        self,
        folder_name: str,
        directory_name: Optional[str] = None,
        upper_folder_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Create a folder in Rakuten Cabinet (file storage)
        
        Args:
            folder_name: Folder name (max 50 characters, mandatory)
            directory_name: Directory name (max 20 characters, optional)
                          If not specified, an automatic number will be generated
            upper_folder_id: Upper level folder ID (optional)
                           If specified, creates a subfolder under the parent folder
                           Cannot be 0 (basic folder)
        
        Returns:
            Response dictionary with success status and folder_id
        """
        url = f"{self.CABINET_BASE_URL}/folder/insert"
        
        headers = {
            "Authorization": self.auth_header,
            "Content-Type": "text/xml; charset=utf-8"
        }
        
        # Build XML request
        request_elem = ET.Element("request")
        folder_insert_request = ET.SubElement(request_elem, "folderInsertRequest")
        folder = ET.SubElement(folder_insert_request, "folder")
        
        # Mandatory: folderName
        folder_name_elem = ET.SubElement(folder, "folderName")
        folder_name_elem.text = folder_name
        
        # Optional: directoryName
        if directory_name:
            directory_name_elem = ET.SubElement(folder, "directoryName")
            directory_name_elem.text = directory_name
        
        # Optional: upperFolderId
        if upper_folder_id is not None:
            upper_folder_id_elem = ET.SubElement(folder, "upperFolderId")
            upper_folder_id_elem.text = str(upper_folder_id)
        
        # Convert to XML string
        xml_str = ET.tostring(request_elem, encoding='utf-8', method='xml').decode('utf-8')
        # Add XML declaration
        xml_request = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str
        
        try:
            response = requests.post(url, headers=headers, data=xml_request.encode('utf-8'), timeout=30)
            
            # Parse XML response
            try:
                root = ET.fromstring(response.text)
                
                # Check status
                status = root.find('status')
                system_status = status.find('systemStatus').text if status is not None and status.find('systemStatus') is not None else None
                
                # Get result
                result = root.find('cabinetFolderInsertResult')
                
                if response.status_code == 200 and system_status == "OK":
                    result_code = int(result.find('resultCode').text) if result is not None and result.find('resultCode') is not None else -1
                    folder_id = int(result.find('FolderId').text) if result is not None and result.find('FolderId') is not None else None
                    
                    if result_code == 0:
                        return {
                            "success": True,
                            "message": "Folder created successfully",
                            "folder_id": folder_id,
                            "result_code": result_code,
                            "response_xml": response.text
                        }
                    else:
                        return {
                            "success": False,
                            "error": f"API returned error code: {result_code}",
                            "folder_id": folder_id,
                            "result_code": result_code,
                            "response_xml": response.text
                        }
                else:
                    # Error response
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
            error_response = None
            error_text = None
            
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_text = e.response.text
                    # Try to parse as XML
                    try:
                        error_root = ET.fromstring(error_text)
                        error_response = ET.tostring(error_root, encoding='utf-8').decode('utf-8')
                    except:
                        error_response = {"raw_response": error_text}
                except:
                    error_text = None
                
                response_headers = dict(e.response.headers)
            else:
                response_headers = None
            
            return {
                "success": False,
                "error": str(e),
                "status_code": e.response.status_code if hasattr(e, 'response') and e.response else None,
                "error_data": error_response,
                "error_text": error_text,
                "response_headers": response_headers,
                "url": url
            }
        except Exception as e:
            return {"success": False, "error": str(e)}
    
    def upload_file(
        self,
        file_path: str,
        file_name: str,
        folder_id: int = 0,
        file_path_name: Optional[str] = None,
        overwrite: bool = False
    ) -> Dict[str, Any]:
        """
        Upload a file (image) to Rakuten Cabinet
        
        Args:
            file_path: Path to the image file on local filesystem
            file_name: Registered image name (max 50 bytes, mandatory)
                    50 bytes or less (25 full-width chars or 50 half-width chars)
                    Prohibited: Control codes, half-width katakana
                    Spaces will be converted/trimmed
            folder_id: Destination folder ID (default: 0 for root)
            file_path_name: Registration file name (max 20 bytes, optional)
                          Up to 20 half-width characters
                          Default: auto-generated (img[8-digit].jpg/gif)
                          Allowed: lowercase alphanumeric, "-", "_"
            overwrite: Overwrite flag (default: False)
                     If True and file_path_name is specified, overwrites existing file
        
        Returns:
            Response dictionary with success status and file_id
        
        Supported formats:
            - JPEG, GIF (including animated), PNG, TIFF, BMP
            - Max file size: 2MB per file
            - Max dimensions: 3840 x 3840 pixels
            - PNG, TIFF, BMP will be converted to JPEG
        """
        url = f"{self.CABINET_BASE_URL}/file/insert"
        
        # Validate file exists
        if not os.path.exists(file_path):
            return {"success": False, "error": f"File not found: {file_path}"}
        
        # Check file size (2MB = 2 * 1024 * 1024 bytes)
        file_size = os.path.getsize(file_path)
        max_size = 2 * 1024 * 1024  # 2MB
        if file_size > max_size:
            return {"success": False, "error": f"File size ({file_size} bytes) exceeds maximum (2MB)"}
        
        # Validate file name length
        if len(file_name.encode('utf-8')) > 50:
            return {"success": False, "error": "File name exceeds 50 bytes"}
        
        # Validate file_path_name if provided
        if file_path_name and len(file_path_name.encode('utf-8')) > 20:
            return {"success": False, "error": "File path name exceeds 20 bytes"}
        
        # Build XML request
        request_elem = ET.Element("request")
        file_insert_request = ET.SubElement(request_elem, "fileInsertRequest")
        file_elem = ET.SubElement(file_insert_request, "file")
        
        # Mandatory: fileName
        file_name_xml = ET.SubElement(file_elem, "fileName")
        file_name_xml.text = file_name
        
        # Mandatory: folderId
        folder_id_xml = ET.SubElement(file_elem, "folderId")
        folder_id_xml.text = str(folder_id)
        
        # Optional: filePath
        if file_path_name:
            file_path_xml = ET.SubElement(file_elem, "filePath")
            file_path_xml.text = file_path_name
        
        # Optional: overWrite
        if overwrite:
            overwrite_xml = ET.SubElement(file_elem, "overWrite")
            overwrite_xml.text = "true"
        
        # Convert to XML string
        xml_str = ET.tostring(request_elem, encoding='utf-8', method='xml').decode('utf-8')
        xml_request = '<?xml version="1.0" encoding="UTF-8"?>\n' + xml_str
        
        # Prepare multipart/form-data
        headers = {
            "Authorization": self.auth_header
        }
        
        # Determine content type based on file extension
        file_ext = os.path.splitext(file_path)[1].lower()
        content_type_map = {
            '.jpg': 'image/jpeg',
            '.jpeg': 'image/jpeg',
            '.gif': 'image/gif',
            '.png': 'image/png',
            '.tiff': 'image/tiff',
            '.tif': 'image/tiff',
            '.bmp': 'image/bmp'
        }
        content_type = content_type_map.get(file_ext, 'application/octet-stream')
        
        try:
            # Open file and prepare multipart data
            with open(file_path, 'rb') as f:
                files = {
                    'file': (os.path.basename(file_path), f, content_type)
                }
                data = {
                    'xml': xml_request
                }
                
                response = requests.post(url, headers=headers, files=files, data=data, timeout=60)
            
            # Parse XML response
            try:
                root = ET.fromstring(response.text)
                
                # Check status
                status = root.find('status')
                system_status = status.find('systemStatus').text if status is not None and status.find('systemStatus') is not None else None
                
                # Get result
                result = root.find('cabinetFileInsertResult')
                
                if response.status_code == 200 and system_status == "OK":
                    result_code = int(result.find('resultCode').text) if result is not None and result.find('resultCode') is not None else -1
                    file_id = int(result.find('FileId').text) if result is not None and result.find('FileId') is not None else None
                    
                    if result_code == 0:
                        return {
                            "success": True,
                            "message": "File uploaded successfully",
                            "file_id": file_id,
                            "result_code": result_code,
                            "response_xml": response.text
                        }
                    else:
                        return {
                            "success": False,
                            "error": f"API returned error code: {result_code}",
                            "file_id": file_id,
                            "result_code": result_code,
                            "response_xml": response.text
                        }
                else:
                    # Error response
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
            
        except FileNotFoundError:
            return {"success": False, "error": f"File not found: {file_path}"}
        except IOError as e:
            return {"success": False, "error": f"File read error: {str(e)}"}
        except requests.exceptions.HTTPError as e:
            error_response = None
            error_text = None
            
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_text = e.response.text
                    # Try to parse as XML
                    try:
                        error_root = ET.fromstring(error_text)
                        error_response = ET.tostring(error_root, encoding='utf-8').decode('utf-8')
                    except:
                        error_response = {"raw_response": error_text}
                except:
                    error_text = None
                
                response_headers = dict(e.response.headers)
            else:
                response_headers = None
            
            return {
                "success": False,
                "error": str(e),
                "status_code": e.response.status_code if hasattr(e, 'response') and e.response else None,
                "error_data": error_response,
                "error_text": error_text,
                "response_headers": response_headers,
                "url": url
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

