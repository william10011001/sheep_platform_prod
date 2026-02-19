import os
import re
import json
import hmac
import base64
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Tuple

import bcrypt
from cryptography.fernet import Fernet, InvalidToken


DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)

SECRET_FILE = DATA_DIR / "secret.key"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _load_or_create_fernet_key() -> bytes:
    env_key = os.environ.get("SHEEP_SECRET_KEY", "").strip()
    if env_key:
        try:
            raw = env_key.encode("utf-8")
            # Support raw 32-byte urlsafe base64 key or plain base64 without padding
            if len(raw) in (44, 43, 45):
                return raw
            # If provided as hex, convert
            if re.fullmatch(r"[0-9a-fA-F]{64}", env_key):
                key_bytes = bytes.fromhex(env_key)
                return base64.urlsafe_b64encode(key_bytes)
        except Exception:
            pass

    if SECRET_FILE.exists():
        return SECRET_FILE.read_bytes().strip()

    key = Fernet.generate_key()
    SECRET_FILE.write_bytes(key)
    return key


_FERNET: Optional[Fernet] = None


def get_fernet() -> Fernet:
    global _FERNET
    if _FERNET is None:
        _FERNET = Fernet(_load_or_create_fernet_key())
    return _FERNET


def encrypt_text(text: Optional[str]) -> Optional[bytes]:
    if text is None:
        return None
    text = str(text)
    return get_fernet().encrypt(text.encode("utf-8"))


def decrypt_text(token: Optional[bytes]) -> Optional[str]:
    if token is None:
        return None
    try:
        return get_fernet().decrypt(token).decode("utf-8")
    except InvalidToken:
        return None


def hash_password(password: str) -> str:
    pw = password.encode("utf-8")
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(pw, salt).decode("utf-8")


def verify_password(password: str, pw_hash: str) -> bool:
    try:
        return bcrypt.checkpw(password.encode("utf-8"), pw_hash.encode("utf-8"))
    except Exception:
        return False


USERNAME_RE = re.compile(r"^[a-zA-Z0-9_]{3,32}$")


def normalize_username(username: str) -> str:
    return str(username or "").strip()


def validate_username(username: str) -> Tuple[bool, str]:
    u = normalize_username(username)
    if not USERNAME_RE.fullmatch(u):
        return False, "帳號格式需為 3-32 字元，僅允許英數與底線。"
    return True, ""


def validate_password_strength(password: str) -> Tuple[bool, str]:
    pw = str(password or "")
    if len(pw) < 10:
        return False, "密碼長度至少 10 字元。"
    if pw.lower() == pw or pw.upper() == pw:
        return False, "密碼需包含大小寫字母。"
    if not any(ch.isdigit() for ch in pw):
        return False, "密碼需包含數字。"
    return True, ""


def validate_wallet_address(addr: str) -> Tuple[bool, str]:
    a = str(addr or "").strip()
    if not a:
        return False, "地址不可為空。"

    # Minimal format checks for common USDT networks; do not enforce network correctness here
    if a.startswith("0x") and len(a) == 42 and re.fullmatch(r"0x[0-9a-fA-F]{40}", a):
        return True, ""
    if a.startswith("T") and 26 <= len(a) <= 36 and re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]+", a):
        return True, ""
    if 26 <= len(a) <= 64 and re.fullmatch(r"[0-9A-Za-z]+", a):
        return True, ""
    return False, "地址格式不符合常見規則。"


def random_token(nbytes: int = 32) -> str:
    return secrets.token_urlsafe(nbytes)



def get_hmac_key() -> bytes:
    raw = _load_or_create_fernet_key()
    try:
        pad = b"=" * (-len(raw) % 4)
        key_bytes = base64.urlsafe_b64decode(raw + pad)
    except Exception:
        key_bytes = raw
    return hmac.new(key_bytes, b"sheep_platform_hmac", digestmod="sha256").digest()


def stable_hmac_sha256(key: bytes, message: str) -> str:
    mac = hmac.new(key, message.encode("utf-8"), digestmod="sha256").digest()
    return base64.urlsafe_b64encode(mac).decode("utf-8").rstrip("=")


def json_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
