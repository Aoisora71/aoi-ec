from typing import Optional, Dict, Any
import requests


def safe_post_json(
    url: str,
    *,
    data: Optional[Dict[str, Any]] = None,
    files: Optional[Dict[str, Any]] = None,
    timeout: int = 15,
) -> Optional[Dict[str, Any]]:
    try:
        resp = requests.post(url, data=data, files=files, timeout=timeout)
        resp.raise_for_status()
    except requests.Timeout:
        print(f" Request to {url} timed out after {timeout}s")
        return None
    except requests.RequestException as exc:
        print(f" Network error calling {url}: {exc}")
        return None
    try:
        return resp.json()
    except ValueError:
        print(" Failed to parse JSON from response")
        return None


