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
    """[專家級防護] 生成隨機目標位置與強加密憑證，加入隨機鹽值防止重放特徵分析"""
    try:
        target_x = secrets.randbelow(150) + 50  # 隨機缺口位置介於 50 到 200 之間
        ts = datetime.now(timezone.utc).timestamp()
        nonce = secrets.token_hex(8)  # 注入隨機擾動防止加密後特徵被分析
        
        # 將目標位置、過期時間與擾動鹽值封裝並加密
        payload = json_dumps({"x": target_x, "ts": ts, "n": nonce})
        token_bytes = get_fernet().encrypt(payload.encode("utf-8"))
        
        return target_x, token_bytes.decode("utf-8")
    except Exception as e:
        import sys, traceback
        sys.stderr.write(f"\n[!!! CAPTCHA ERROR !!!] 生成驗證碼失敗\n")
        traceback.print_exc(file=sys.stderr)
        raise RuntimeError(f"驗證碼底層生成崩潰: {str(e)}")


def verify_slider_captcha(token: str, offset: float, tracks: list) -> Tuple[bool, str]:
    """[專家級防護] 嚴格驗證滑動驗證碼，包含高階物理行為學軌跡分析與最大化錯誤追蹤"""
    if not token or not tracks:
        return False, "驗證資料缺失，請重新整理頁面。"
        
    try:
        decrypted_bytes = get_fernet().decrypt(token.encode("utf-8"), ttl=300) # 5分鐘內嚴格有效
        payload = json.loads(decrypted_bytes.decode("utf-8"))
    except Exception as e:
        # 最大化拋出例外細節，確保管理員能分辨是逾時還是竄改
        return False, f"驗證憑證已過期或被竄改: {type(e).__name__} - {str(e)}"
        
    target_x = payload.get("x", 0)
    gen_ts = payload.get("ts", 0)
    
    # [專家級防護] 時間差攻擊阻斷 (Time-to-Solve)
    # 機器人腳本通常在獲取 token 後毫秒級內送出。人類視覺反應加上拖曳絕對超過 1.5 秒
    solve_time = datetime.now(timezone.utc).timestamp() - gen_ts
    if solve_time < 1.5:
        return False, f"操作時間極度異常 (耗時僅 {solve_time:.2f} 秒)，已攔截惡意自動化腳本。"
        
    # 驗證 1：最終位置容錯率 (收緊至 4px)
    diff = abs(offset - target_x)
    if diff > 4.0:
        return False, f"滑動位置不精確 (誤差值: {diff:.1f}px)，請重試。"
        
    # 驗證 2：軌跡長度檢查 (人類滑動不可能少於 10 個採樣點)
    if len(tracks) < 10:
        return False, f"軌跡特徵點過少 (僅捕捉到 {len(tracks)} 點)，疑似非人類腳本。"
        
    # 驗證 3：物理行為學檢查 (防抖動、極限邊界與變異加速度)
    try:
        start_time = tracks[0].get("t", 0)
        end_time = tracks[-1].get("t", 0)
        total_time = end_time - start_time
        
        if total_time < 150 or total_time > 8000:
            return False, f"操作時間異常 (耗時 {total_time}ms)，不在人類常規操作速度範圍。"
            
        x_values = []
        y_values = []
        for pt in tracks:
            if "x" not in pt or "y" not in pt or "t" not in pt:
                return False, f"軌跡資料結構損毀: 缺少必要座標參數 (x,y,t)。目前資料: {pt}"
            x_values.append(pt["x"])
            y_values.append(pt["y"])
            
        # 驗證 4：X 軸軌跡邊界約束 (不該出現極端負數或超過前端容器的數值)
        if min(x_values) < -20 or max(x_values) > 350:
            return False, f"X 軸軌跡超出物理邊界限制 (Min: {min(x_values)}, Max: {max(x_values)})。"
            
        # 驗證 5：Y 軸人類微小防手震特徵與腳本隨機數破解
        # 惡意腳本會用 random.randint(-1, 2) 產生極端規律的抖動，或是 y 永遠為 0
        y_range = max(y_values) - min(y_values)
        if y_range == 0:
            return False, "Y 軸缺乏人類微小手抖特徵 (判定為自動化腳本強制直行)。"
        # [專家級修正] 人類在手機或滑鼠拖曳時，拇指弧度造成 Y 軸偏移 50~200px 是極度正常的自然現象。
        # 只有當 Y 軸偏移超過整個螢幕高度的一大半 (例如 > 500)，才判定為腳本塞入的異常亂數。
        if y_range > 500:
            return False, "Y 軸抖動幅度超越物理極限，判定為腳本亂數生成。"
            
        # 驗證 6：加速度變異與軌跡線性度分析 (阻殺所有等速與微小亂數腳本)
        velocities = []
        x_deltas = []
        for i in range(1, len(tracks)):
            dx = tracks[i]["x"] - tracks[i-1]["x"]
            dt = tracks[i]["t"] - tracks[i-1]["t"]
            x_deltas.append(dx)
            if dt <= 0: continue
            velocities.append(dx / dt)
            
        if len(velocities) > 2:
            avg_v = sum(velocities) / len(velocities)
            variance_v = sum((v - avg_v)**2 for v in velocities) / len(velocities)
            
            # 人類拉動必定會有加速起步與減速對準的過程，腳本通常呈現超低變異數
            if variance_v < 0.0001:
                return False, f"滑動呈現超自然完美等速運動 (變異數: {variance_v:.6f})，直接攔截。"
            
            # 檢查是否過度規律 (腳本每次都移動固定步長 ±2，人類步長變化極大)
            # [專家級修正] 加入 max(x_deltas) > 3 條件，避免誤殺慢慢滑動(dx永遠只有 0, 1, 2)的真實人類
            unique_dx = len(set(x_deltas))
            if unique_dx <= 3 and max(x_deltas) > 3 and len(tracks) > 15:
                return False, "位移步長過度規律缺乏自然變化，判定為腳本生成的線性軌跡。"

    except Exception as track_err:
        import sys, traceback
        sys.stderr.write(f"\n[!!! CAPTCHA SECURITY ALARM !!!] 軌跡解析過程中發生非預期例外\n")
        traceback.print_exc(file=sys.stderr)
        return False, f"軌跡特徵深度解析崩潰: {str(track_err)}"

    return True, "驗證成功"
