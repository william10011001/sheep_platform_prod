from __future__ import annotations

import os
from typing import Any, Iterable, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from sheep_secrets import redact_text


TRANSIENT_HTTP_STATUSES = (408, 425, 429, 500, 502, 503, 504, 521, 522, 523, 524)


def env_truthy(name: str, default: bool = False) -> bool:
    raw = str(os.environ.get(name, "") or "").strip().lower()
    if not raw:
        return bool(default)
    return raw in {"1", "true", "yes", "on"}


def resolve_tls_verify(
    *,
    allow_insecure_env: str = "SHEEP_ALLOW_INSECURE_TLS",
    ca_bundle_env: str = "SHEEP_CA_BUNDLE",
    default: bool = True,
) -> bool | str:
    ca_bundle = str(os.environ.get(ca_bundle_env, "") or "").strip()
    if ca_bundle:
        return ca_bundle
    if env_truthy(allow_insecure_env, default=False):
        return False
    return bool(default)


def create_retry_session(
    *,
    user_agent: str = "",
    total_retries: int = 3,
    backoff_factor: float = 0.5,
    status_forcelist: Optional[Iterable[int]] = None,
    pool_connections: int = 16,
    pool_maxsize: int = 16,
) -> requests.Session:
    session = requests.Session()
    if user_agent and hasattr(getattr(session, "headers", None), "update"):
        session.headers.update({"User-Agent": user_agent})
    retry = Retry(
        total=int(total_retries),
        read=int(total_retries),
        connect=int(total_retries),
        backoff_factor=float(backoff_factor),
        status_forcelist=tuple(status_forcelist or TRANSIENT_HTTP_STATUSES),
        allowed_methods=frozenset({"DELETE", "GET", "HEAD", "OPTIONS", "PATCH", "POST", "PUT"}),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=int(pool_connections), pool_maxsize=int(pool_maxsize))
    if hasattr(session, "mount"):
        session.mount("http://", adapter)
        session.mount("https://", adapter)
    return session


def request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    timeout: Any,
    verify: Any = None,
    **kwargs: Any,
) -> requests.Response:
    return session.request(
        str(method or "GET").upper(),
        url,
        timeout=timeout,
        verify=resolve_tls_verify() if verify is None else verify,
        **kwargs,
    )


def summarize_http_detail(detail: Any, *, limit: int = 200) -> str:
    text = redact_text(detail)
    text = " ".join(str(text or "").strip().split())
    if len(text) <= limit:
        return text
    return text[:limit] + "..."
