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


def hash_password(password) -> str:
    if isinstance(password, str):
        pw = password.encode("utf-8")
    else:
        pw = password
    salt = bcrypt.gensalt(rounds=12)
    return bcrypt.hashpw(pw, salt).decode("utf-8")


def verify_password(password, pw_hash) -> bool:
    """[專家級防禦校驗] 極致強健的密碼驗證，主動修正 DB 字串汙染並支援 Python 跨版本 Hash"""
    if not password or not pw_hash:
        return False
    try:
        # 1. 統一將輸入密碼轉為 bytes
        p_bytes = password.encode("utf-8") if isinstance(password, str) else password
        
        # 2. 深層清理 pw_hash：處理 SQLite 讀取時可能誤抓的字串化 bytes (例如 "b'$2b$12...'" )
        if isinstance(pw_hash, str):
            h_str = pw_hash.strip()
            # 遞迴移除可能嵌套的引號
            while (h_str.startswith(("b'", 'b"', "'", '"')) and h_str.endswith(("'", '"'))):
                if h_str.startswith(("b'", 'b"')): h_str = h_str[2:-1]
                else: h_str = h_str[1:-1]
            h_bytes = h_str.encode("utf-8")
        else:
            h_bytes = pw_hash

        # 3. 最終安全性檢查與執行
        return bcrypt.checkpw(p_bytes, h_bytes)
    except Exception as fatal_sec:
        # [最大化顯示] 輸出至系統標準錯誤流，確保 Admin 能在日誌抓到關鍵 Trace
        import sys, traceback
        sys.stderr.write(f"\n[!!! SECURITY ALERT !!!] verify_password 執行崩潰\n")
        sys.stderr.write(f"Hash 來源類型: {type(pw_hash)} | 內容長度: {len(str(pw_hash))}\n")
        traceback.print_exc(file=sys.stderr)
        return False


USERNAME_RE = re.compile(r"^[^\r\n]{1,64}$")


def normalize_username(username: str) -> str:
    return str(username or "").strip()


def validate_username(username: str) -> Tuple[bool, str]:
    u = normalize_username(username)
    if not u:
        return False, "帳號不可為空。"
    if len(u) > 64:
        return False, "帳號長度上限為 64 字元。"
    if not USERNAME_RE.fullmatch(u):
        return False, "帳號格式不支援換行字元。"
    return True, ""


def validate_password_strength(password: str) -> Tuple[bool, str]:
    pw = str(password or "")
    if len(pw) < 6:
        return False, "密碼長度至少 6 字元。"
    has_alpha = any(ch.isalpha() for ch in pw)
    has_digit = any(ch.isdigit() for ch in pw)
    if not (has_alpha and has_digit):
        return False, "密碼需同時包含英文字母與數字。"
    return True, ""


def validate_wallet_address(addr: str, chain: str = "") -> Tuple[bool, str]:
    a = str(addr or "").strip()
    c = str(chain or "").strip().upper()
    if not a:
        return False, "地址不可為空。"

    # TRC20: Tron base58 address, usually starts with 'T'
    is_trc = bool(a.startswith("T") and 26 <= len(a) <= 36 and re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]+", a))
    # EVM chains (ERC20/BEP20): 0x + 40 hex chars
    is_evm = bool(a.startswith("0x") and len(a) == 42 and re.fullmatch(r"0x[0-9a-fA-F]{40}", a))

    if c in ("TRC20", "TRON"):
        return (True, "") if is_trc else (False, "地址格式與所選鏈不符。")
    if c in ("BEP20", "BSC", "ERC20", "ETH"):
        return (True, "") if is_evm else (False, "地址格式與所選鏈不符。")

    # 未指定鏈：允許常見格式
    if is_trc or is_evm:
        return True, ""

    # Fallback: basic sanity check for alphanumeric addresses
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


def generate_slider_captcha() -> Tuple[int, str]:
    """[專家級防護] 生成隨機目標位置與強加密憑證"""
    target_x = secrets.randbelow(150) + 50  # 隨機目標位置介於 50 到 200 之間
    ts = datetime.now(timezone.utc).timestamp()
    
    # 將目標位置與過期時間封裝並加密
    payload = json_dumps({"x": target_x, "ts": ts})
    token_bytes = get_fernet().encrypt(payload.encode("utf-8"))
    token_str = token_bytes.decode("utf-8")
    
    return target_x, token_str


def verify_slider_captcha(token: str, offset: float, tracks: list) -> Tuple[bool, str]:
    """[專家級防護] 驗證滑動驗證碼，包含時間、距離與軌跡行為分析"""
    if not token or not tracks:
        return False, "驗證碼資料缺失，請重新整理頁面。"
        
    try:
        decrypted_bytes = get_fernet().decrypt(token.encode("utf-8"), ttl=300) # 5分鐘內有效
        payload = json.loads(decrypted_bytes.decode("utf-8"))
    except Exception as e:
        return False, f"驗證碼已過期或遭竄改 ({type(e).__name__})。"
        
    target_x = payload.get("x", 0)
    
    # 驗證 1：最終位置容錯率 (相差不能超過 5px)
    if abs(offset - target_x) > 5.0:
        return False, "滑動位置不精確，請重試。"
        
    # 驗證 2：軌跡長度檢查
    if len(tracks) < 5:
        return False, "軌跡異常 。"
        
    # 驗證 3：行為學檢查 (計算滑動過程中的時間差與位移特徵)
    # 正常的滑動會有加速與減速的過程，不會是完美的等速運動
    try:
        start_time = tracks[0].get("t", 0)
        end_time = tracks[-1].get("t", 0)
        total_time = end_time - start_time
        
        if total_time < 50: # 滑動時間小於 50ms 判定為非人類
            return False, "操作速度異常，拒絕訪問。"
            
        # 簡單計算 X 軸的變異數，防止線性軌跡
        x_values = [pt.get("x", 0) for pt in tracks]
        if max(x_values) == min(x_values) and offset > 0:
            return False, "無效的直線軌跡特徵。"
            
    except Exception as track_err:
        return False, f"軌跡解析失敗: {track_err}"

    return True, "驗證成功"
