"""
Script to verify Rakuten API credentials and test connection
"""
import base64
import json
import requests
import sys
import io
from main import load_config, create_api_from_config

# UTF-8 encoding fix for Windows
if sys.platform == 'win32':
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')


def main():
    print("=" * 80)
    print("RAKUTEN API CREDENTIALS VERIFICATION")
    print("=" * 80)
    
    try:
        # Load configuration
        config = load_config()
        credentials = config["rakuten"]
        SERVICE_SECRET = credentials["service_secret"]
        LICENSE_KEY = credentials["license_key"]
        
        # Show credentials (masked)
        print(f"\nService Secret: {SERVICE_SECRET[:10]}...{SERVICE_SECRET[-5:]}")
        print(f"License Key: {LICENSE_KEY[:10]}...{LICENSE_KEY[-5:]}")
        
        # Create API instance to get auth header
        api = create_api_from_config()
        auth_header = api.auth_header
    
        print(f"\nAuth Header: {auth_header[:30]}...{auth_header[-20:]}")
        
        # Try a simple API call with minimal data
        url = "https://api.rms.rakuten.co.jp/es/2.0/items/manage-numbers/test-product-minimal"
        
        headers = {
            "Authorization": auth_header,
            "Content-Type": "application/json"
        }
        
        # Minimal test payload
        minimal_product = {
            "itemType": "NORMAL",
            "itemNumber": "test-minimal",
            "title": "Test Product Minimal"
        }
        
        print("\n" + "-" * 80)
        print("TEST 1: Minimal Product Registration")
        print(f"URL: {url}")
        print(f"Payload: {json.dumps(minimal_product, indent=2)}")
        
        try:
            response = requests.put(url, headers=headers, json=minimal_product, timeout=30)
            
            print(f"\nResponse Status: {response.status_code}")
            print(f"Response Headers:")
            for key, value in response.headers.items():
                if key.lower() in ['content-type', 'x-rms-error-code', 'www-authenticate']:
                    print(f"  {key}: {value}")
            
            print(f"\nResponse Body:")
            try:
                response_json = response.json()
                print(json.dumps(response_json, indent=2, ensure_ascii=False))
            except:
                print(response.text if response.text else "[Empty]")
                
            # Analyze response
            if response.status_code == 401:
                print("\n" + "-" * 80)
                print("DIAGNOSIS: Authentication Failed (401 Unauthorized)")
                print("-" * 80)
                print("\nThe API returned GA0001 error code.")
                print("This means the authentication format is CORRECT, but the")
                print("credentials themselves are either:")
                print("\n1. INVALID (wrong Service Secret or License Key)")
                print("2. EXPIRED (credentials need to be regenerated)")
                print("3. NOT ACTIVATED (API access not enabled for this account)")
                print("\nAction Required:")
                print("1. Log in to Rakuten RMS: https://rms.rakuten.co.jp/")
                print("2. Go to: 店舗様向け情報・サービス > WEB APIサービス")
                print("3. Check your Service Secret and License Key")
                print("4. Verify WEB API service is activated")
                print("5. Check if credentials have expired")
                print("-" * 80)
                
            elif response.status_code in [201, 204]:
                print("\n" + "-" * 80)
                print("SUCCESS! Credentials are valid and working!")
                print("-" * 80)
                
            elif response.status_code == 400:
                print("\n" + "-" * 80)
                print("DIAGNOSIS: Authentication Succeeded but Request Invalid")
                print("-" * 80)
                print("Good news: Your credentials are VALID!")
                print("The 400 error is due to missing required fields in the test payload.")
                print("You can proceed with full product registration.")
                print("-" * 80)
                
        except requests.exceptions.RequestException as e:
            print(f"\nNetwork Error: {e}")
            print("\nPlease check your internet connection.")
        
        print("\n" + "=" * 80)
        print("VERIFICATION COMPLETE")
        print("=" * 80)
        
    except FileNotFoundError as e:
        print(f"Error: {e}")
        sys.exit(1)
    except KeyError as e:
        print(f"Error: Missing configuration key: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()

