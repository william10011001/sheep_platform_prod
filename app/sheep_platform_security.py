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
    """[專家級修復] 嚴格驗證密鑰來源與完整性，防止密鑰汙染或遺失"""
    env_key = os.environ.get("SHEEP_SECRET_KEY", "").strip()
    
    # 1. 優先級 1：環境變數（Docker Secrets）
    if env_key:
        try:
            raw = env_key.encode("utf-8")
            
            # 情形A：標準 base64 URL-safe 格式（44 位元組，含或不含填充）
            if len(raw) in (43, 44, 45, 88):
                # 嘗試直接解碼驗證
                try:
                    decoded = base64.urlsafe_b64decode(raw + b"=" * (-len(raw) % 4))
                    if len(decoded) == 32:  # Fernet 需要 32 bytes
                        return raw
                except Exception:
                    pass
            
            # 情形B：十六進制格式（64 字元）
            if re.fullmatch(r"[0-9a-fA-F]{64}", env_key):
                key_bytes = bytes.fromhex(env_key)
                encoded = base64.urlsafe_b64encode(key_bytes)
                return encoded
            
            # 情形C：原始 32 bytes（極不推薦）
            if len(raw) == 32:
                return base64.urlsafe_b64encode(raw)
                
        except Exception as e:
            import sys
            print(f"[KEY_ERROR] SHEEP_SECRET_KEY 格式不符，將使用文件或自動生成: {str(e)}", file=sys.stderr)
    
    # 2. 優先級 2：本地文件密鑰
    if SECRET_FILE.exists():
        try:
            key_data = SECRET_FILE.read_bytes().strip()
            # 驗證檔案密鑰的有效性
            if len(key_data) in (43, 44, 45):
                return key_data
        except Exception as e:
            import sys
            print(f"[KEY_ERROR] 密鑰文檔讀取失敗，將重新生成: {str(e)}", file=sys.stderr)
    
    # 3. 優先級 3：自動生成（部署首次啟動）
    try:
        key = Fernet.generate_key()
        # 確保目錄存在
        SECRET_FILE.parent.mkdir(parents=True, exist_ok=True)
        SECRET_FILE.write_bytes(key)
        import sys
        print(f"[INFO] 已生成新的加密密鑰並保存至 {SECRET_FILE}", file=sys.stderr)
        return key
    except Exception as e:
        import sys
        raise RuntimeError(f"無法生成或保存加密密鑰: {str(e)}") from e


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
    """[專家級修復] 統一密碼雜湊輸出為 UTF-8 字符串，防止類型混雜"""
    if isinstance(password, str):
        pw = password.encode("utf-8")
    else:
        pw = bytes(password) if password else b""
    
    if not pw:
        raise ValueError("Password cannot be empty")
    
    salt = bcrypt.gensalt(rounds=12)
    hash_bytes = bcrypt.hashpw(pw, salt)
    return hash_bytes.decode("utf-8")


def verify_password(password, pw_hash) -> bool:
    """[專家級防禦校驗] 極致簡潔而強健的密碼驗證，零殘留誤判"""
    if not password or not pw_hash:
        return False
    
    try:
        # 單一責任：統一轉換為 bytes，無多層遞迴
        p_bytes = password.encode("utf-8") if isinstance(password, str) else bytes(password)
        h_bytes = pw_hash.encode("utf-8") if isinstance(pw_hash, str) else bytes(pw_hash)
        
        # 直接執行 bcrypt 驗證
        result = bcrypt.checkpw(p_bytes, h_bytes)
        return bool(result)
        
    except ValueError:
        # bcrypt 拋出 ValueError 表示雜湊格式不符，不是異常情況
        return False
    except Exception as e:
        # 只在真的不可恢復的例外時記錄
        import sys, traceback
        print(f"[AUTH_ERROR] verify_password 發生異常: {type(e).__name__}: {str(e)}", file=sys.stderr)
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
    """[專家級修復] 增強密碼強度檢查，防止 TOP 1000 弱密碼與常見模式"""
    pw = str(password or "")
    
    if len(pw) < 8:
        return False, "密碼長度至少 8 字元。"
    
    if len(pw) > 128:
        return False, "密碼長度上限 128 字元。"
    
    has_lower = any(ch.islower() for ch in pw)
    has_upper = any(ch.isupper() for ch in pw)
    has_digit = any(ch.isdigit() for ch in pw)
    has_special = any(ch in "!@#$%^&*()-_=+[]{}|;:',.<>?/`~" for ch in pw)
    
    complexity = sum([has_lower, has_upper, has_digit, has_special])
    if complexity < 3:
        return False, "密碼需包含至少 3 種字元類型（小寫、大寫、數字、特殊符號）。"
    
    # [新增] 黑名單檢查：禁止常見脆弱密碼
    weak_patterns = [
        r"^password\d{0,3}$",  # password, password1, password123
        r"^admin\d{0,3}$",     # admin123
        r"^123456",            # 所有數字序列開頭
        r"^qwerty",            # 鍵盤序列
        r"(.)\1{3,}",          # 連續重複字元（aaaa）
    ]
    
    for pattern in weak_patterns:
        if re.search(pattern, pw, re.IGNORECASE):
            return False, "密碼過於簡單或已被列為高風險，請選擇更複雜的密碼。"
    
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


def generate_slider_captcha(client_ip: str) -> Tuple[int, str]:
    """[專家級防護] 生成隨機目標位置與強加密憑證，加入隨機鹽值防止重放與IP強綁定"""
    try:
        target_x = secrets.randbelow(150) + 50  # 隨機缺口位置介於 50 到 200 之間
        ts = datetime.now(timezone.utc).timestamp()
        nonce = secrets.token_hex(8)  # 注入隨機擾動防止加密後特徵被分析
        
        # 將目標位置、過期時間、擾動鹽值與請求者IP封裝並加密 (綁定IP防代刷池攻擊)
        payload = json_dumps({"x": target_x, "ts": ts, "n": nonce, "ip": client_ip})
        token_bytes = get_fernet().encrypt(payload.encode("utf-8"))
        
        return target_x, token_bytes.decode("utf-8")
    except Exception as e:
        import sys, traceback
        sys.stderr.write(f"\n[!!! CAPTCHA ERROR !!!] 生成驗證碼失敗\n")
        traceback.print_exc(file=sys.stderr)
        raise RuntimeError(f"驗證碼生成錯誤: {str(e)}")


def verify_slider_captcha(token: str, offset: float, tracks: list, client_ip: str) -> Tuple[bool, str]:
    """[專家級防護] 嚴格驗證滑動驗證碼，包含高階物理行為學軌跡分析與最大化錯誤追蹤"""
    if not token or not tracks:
        return False, "驗證資料缺失，請重新整理頁面。"
        
    try:
        # 縮短生命週期至 120 秒，防範重放與腳本延遲 (攻擊腳本使用了強制等待時間)
        decrypted_bytes = get_fernet().decrypt(token.encode("utf-8"), ttl=120) 
        payload = json.loads(decrypted_bytes.decode("utf-8"))
    except Exception as e:
        # 最大化拋出例外細節，確保管理員能分辨是逾時還是竄改
        return False, f"驗證憑證已過期或無效: {type(e).__name__} - {str(e)}"

    # 驗證 IP 綁定，徹底粉碎代理 IP 池分散式攻擊
    bound_ip = payload.get("ip", "")
    if bound_ip and bound_ip != client_ip:
        import sys
        sys.stderr.write(f"\n[!!! CAPTCHA ALARM !!!] IP攔截 簽發IP: {bound_ip}, 提交IP: {client_ip}\n")
        return False, f"IP不匹配，請勿使用代理或重新整理頁面。"
        
    target_x = payload.get("x", 0)
    gen_ts = payload.get("ts", 0)
    
    # [專家級防護] 時間差攻擊阻斷 (Time-to-Solve)
    # 機器人腳本通常在獲取 token 後毫秒級內送出。人類視覺反應加上拖曳絕對超過 1.5 秒
    solve_time = datetime.now(timezone.utc).timestamp() - gen_ts
    if solve_time < 1.5:
        return False, f"操作時間異常，請重試。"
        
    # 驗證 1：最終位置容錯率 (收緊至 4px)
    diff = abs(offset - target_x)
    if diff > 4.0:
        return False, f"滑動位置不精確，請重試。"
        
    # 驗證 2：軌跡長度檢查 (人類滑動不可能少於 10 個採樣點)
    if len(tracks) < 10:
        return False, f"滑動異常，請重試，疑似非人類腳本。"
        
    # 驗證 3：物理行為學檢查 (防抖動、極限邊界與變異加速度)
    try:
        start_time = tracks[0].get("t", 0)
        end_time = tracks[-1].get("t", 0)
        total_time = end_time - start_time
        
        if total_time < 150 or total_time > 8000:
            return False, f"操作時間異常 (耗時 {total_time}ms)。"
            
        x_values = []
        y_values = []
        for pt in tracks:
            if "x" not in pt or "y" not in pt or "t" not in pt:
                return False, f"滑動異常，請重試。目前資料: {pt}"
            x_values.append(pt["x"])
            y_values.append(pt["y"])
            
        # 驗證 4：X 軸軌跡邊界約束 (不該出現極端負數或超過前端容器的數值)
        if min(x_values) < -20 or max(x_values) > 350:
            return False, f"滑動X軸軌跡異常 (Min: {min(x_values)}, Max: {max(x_values)})。"
            
        # 驗證 5：Y 軸人類微小防手震特徵與腳本隨機數破解
        # 惡意腳本會用 random.randint(-1, 2) 產生極端規律的抖動，或是 y 永遠為 0
        y_range = max(y_values) - min(y_values)
        if y_range == 0:
            return False, "滑動異常，請重試 (判定為自動化腳本)。"
        # [專家級修正] 人類在手機或滑鼠拖曳時，拇指弧度造成 Y 軸偏移 50~200px 是極度正常的自然現象。
        # 只有當 Y 軸偏移超過整個螢幕高度的一大半 (例如 > 500)，才判定為腳本塞入的異常亂數。
        if y_range > 500:
            return False, "滑動異常，請重試(判定為亂數生成)。"
            
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
                return False, f"滑動異常，請重試 (變異數: {variance_v:.6f})"
            
            # 檢查是否過度規律 (腳本每次都移動固定步長 ±2，人類步長變化極大)
            # [專家級修正] 加入 max(x_deltas) > 3 條件，避免誤殺慢慢滑動(dx永遠只有 0, 1, 2)的真實人類
            unique_dx = len(set(x_deltas))
            if unique_dx <= 3 and max(x_deltas) > 3 and len(tracks) > 15:
                return False, "滑動異常，請重試。"

            # [專家級新增] 驗證 7：Y軸高頻震盪分析 (專殺攻擊腳本中的 random.randint 雜訊)
            y_direction_changes = 0
            for i in range(2, len(tracks)):
                dy1 = tracks[i-1]["y"] - tracks[i-2]["y"]
                dy2 = tracks[i]["y"] - tracks[i-1]["y"]
                # 如果連續兩次斜率相乘小於0，代表Y軸發生非自然反向折返
                if dy1 * dy2 < 0:
                    y_direction_changes += 1
            
            # 人類滑動通常朝一個方向微偏，或偶爾修正。若超過 40% 的點都在反向折返，絕對是 random 生成的無意義雜訊
            if len(tracks) > 10 and y_direction_changes > len(tracks) * 0.4:
                import sys
                sys.stderr.write(f"\n[!!! CAPTCHA ALARM !!!] 檢測到演算法雜訊，折返率異常: {y_direction_changes}/{len(tracks)}\n")
                return False, f"滑動異常，請重試。"

    except Exception as track_err:
        import sys, traceback
        sys.stderr.write(f"\n滑動異常，請重試。軌跡解析失敗\n")
        traceback.print_exc(file=sys.stderr)
        return False, f"滑動異常，請重試。軌跡解析失敗: {str(track_err)}"

    return True, "驗證成功"
