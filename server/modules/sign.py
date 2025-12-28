import hashlib
import hmac


def md5_sign(app_key: str, app_secret: str, timestamp: str) -> str:
    raw = (app_key + app_secret + timestamp).encode("utf-8")
    return hashlib.md5(raw).hexdigest()


def hmac_sha256_sign(app_key: str, app_secret: str, timestamp: str) -> str:
    message = (app_key + timestamp).encode("utf-8")
    secret = app_secret.encode("utf-8")
    return hmac.new(secret, message, hashlib.sha256).hexdigest()


