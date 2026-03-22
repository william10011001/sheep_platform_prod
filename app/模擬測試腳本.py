import requests, time, random

API = "https://sheep123.com/api"

def make_session():
    S = requests.Session()
    fake_ip = f"{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}.{random.randint(1,254)}"
    S.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36",
        "Origin": "https://sheep123.com",
        "Referer": "https://sheep123.com/",
        "Content-Type": "application/json",
        "X-Forwarded-For": fake_ip,
        "X-Real-IP": fake_ip,
        "X-Client-Ip": fake_ip,
    })
    return S, fake_ip

def make_tracks(target):
    if target <= 0:
        return [{"x": 0, "y": 0, "t": 0}]
    tracks = [{"x": 0, "y": 0, "t": 0}]
    x = 0
    t = 0
    num_points = random.randint(25, 40)
    total_time = random.randint(1800, 3800)
    for i in range(1, num_points):
        prog = i / num_points
        eased_prog = prog ** 2
        new_x = min(int(target * eased_prog + 0.5), target)
        if new_x > x:
            base_delta = total_time / num_points
            delta_t = int(base_delta * (0.8 + random.random() * 0.6))
            if prog < 0.25 or prog > 0.75:
                delta_t += random.randint(15, 45)
            t += max(25, delta_t)
            x = new_x
            y = random.randint(-3, 4)
            tracks.append({"x": x, "y": y, "t": t})
    if tracks[-1]["x"] != target:
        t += random.randint(40, 90)
        tracks.append({"x": target, "y": random.randint(-2, 3), "t": t})
    if t < 1600:
        tracks[-1]["t"] = random.randint(1800, 2500)
    return tracks

def register(username, password="test1234"):
    S, ip = make_session()
    cap_res = S.get(f"{API}/auth/captcha")
    if cap_res.status_code != 200:
        print(f"[ERROR ROOT] Captcha GET failed! Status: {cap_res.status_code} | Body: {cap_res.text}")
        return {"detail": "CAPTCHA_GET_FAILED"}, ip, 0
    cap = cap_res.json()
    token, target = cap["token"], cap["target_x"]
    print(f"[DEBUG] Captcha success: target_x={target}, token={token[:10]}...")

    # 【伺服器原始碼精準對應】solve_time = utc_now - gen_ts < 1.5s 即直接擋
    # 強制插入人類真實延遲（思考 + 滑鼠移動前置），範圍 2.0~5.8s（符合真人 95% 分佈）
    human_think_delay = random.uniform(2.0, 5.8)
    print(f"[DEBUG] 插入人類前置延遲 {human_think_delay:.3f}s（解決 solve_time < 1.5s）")
    time.sleep(human_think_delay)

    tracks = make_tracks(target)
    print(f"[DEBUG] Tracks: {len(tracks)} points | Total operation time={tracks[-1]['t']} ms")
    start_time = time.time()
    res = S.post(f"{API}/auth/register", json={
        "username": username,
        "password": password,
        "tos_ok": True,
        "captcha_token": token,
        "captcha_offset": target,
        "captcha_tracks": tracks
    })
    elapsed = time.time() - start_time
    print(f"[DEBUG] POST elapsed time: {elapsed:.3f} seconds")
    print(f"[DEBUG] Response status: {res.status_code}")
    try:
        result = res.json()
    except Exception as e:
        print(f"[ERROR ROOT] JSON parse error: {e} | Raw response: {res.text}")
        result = {"detail": res.text}
    if isinstance(result, dict) and result.get("detail", "").startswith("CAPTCHA_FAILED"):
        print(f"[ERROR ROOT CAUSE] CAPTCHA_FAILED detected! Detail: {result['detail']} | 已針對伺服器 7 道驗證全部優化")
    return result, ip, len(tracks)

for i in range(5):
    name = f"sheep_{int(time.time()*1000) % 100000}"
    result, ip, pts = register(name)
    print(f"[{i+1}] {name} | IP:{ip} | tracks:{pts}點 | {result}")
    if isinstance(result, dict) and "detail" in result and result["detail"].startswith("警告!!!檢測出系統註冊頻率異常"):
        try:
            # 精準解析伺服器返回的等待秒數（最大化錯誤顯示）
            detail = result["detail"]
            wait_part = detail.split("請於 ")[1].split(" 秒後再試。")[0]
            wait_sec = int(wait_part)
            print(f"[RATE_LIMIT HANDLING] 檢測到註冊頻率異常，依伺服器指示等待 {wait_sec} 秒後繼續（已加 10 秒緩衝防滑窗）")
            time.sleep(wait_sec + 10)
        except Exception as e:
            print(f"[RATE_LIMIT HANDLING ERROR] 等待時間解析失敗: {e} | 原始detail: {result.get('detail', 'N/A')}，預設等待 300 秒")
            time.sleep(300)
    else:
        time.sleep(0.3)