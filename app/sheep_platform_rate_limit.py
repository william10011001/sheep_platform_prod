import time
import threading
from dataclasses import dataclass
from typing import Dict, Optional, Tuple


@dataclass
class _Bucket:
    capacity: float
    fill_rate_per_s: float
    tokens: float
    updated_at: float


class RateLimiter:
    def __init__(self, rate_per_minute: float, burst: float, ttl_s: float = 600.0) -> None:
        self._rate_per_minute = float(max(0.0, rate_per_minute))
        self._burst = float(max(1.0, burst))
        self._ttl_s = float(max(60.0, ttl_s))
        self._lock = threading.Lock()
        self._buckets: Dict[str, _Bucket] = {}
        # 使用 monotonic time 避免系統時間校正造成 token bucket 計算錯誤
        self._last_cleanup = time.monotonic()

    def configure(self, rate_per_minute: float, burst: float) -> None:
        with self._lock:
            self._rate_per_minute = float(max(0.0, rate_per_minute))
            self._burst = float(max(1.0, burst))

    def _refill(self, b: _Bucket, now: float) -> None:
        if now <= b.updated_at:
            return
        dt = now - b.updated_at
        b.tokens = min(b.capacity, b.tokens + dt * b.fill_rate_per_s)
        b.updated_at = now

    def _cleanup(self, now: float) -> None:
        if now - self._last_cleanup < 5.0:
            return
        self._last_cleanup = now
        cutoff = now - self._ttl_s
        for k in list(self._buckets.keys()):
            if self._buckets[k].updated_at < cutoff:
                self._buckets.pop(k, None)

    def check(self, key: str, cost: float = 1.0) -> Tuple[bool, Optional[float]]:
        key = str(key or "")
        if not key:
            return True, None
        cost = float(max(0.0, cost))
        if self._rate_per_minute <= 0:
            return True, None

        now = time.monotonic()
        with self._lock:
            self._cleanup(now)

            fill_rate_per_s = self._rate_per_minute / 60.0
            b = self._buckets.get(key)
            if b is None:
                b = _Bucket(capacity=self._burst, fill_rate_per_s=fill_rate_per_s, tokens=self._burst, updated_at=now)
                self._buckets[key] = b
            else:
                b.capacity = self._burst
                b.fill_rate_per_s = fill_rate_per_s
                self._refill(b, now)

            if b.tokens >= cost:
                b.tokens -= cost
                return True, None

            missing = cost - b.tokens
            retry_after = missing / b.fill_rate_per_s if b.fill_rate_per_s > 0 else 1.0
            return False, float(max(0.05, retry_after))
