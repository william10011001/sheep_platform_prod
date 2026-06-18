# -*- coding: utf-8 -*-
"""Symbol canonicalisation: map each venue's raw symbol to a canonical
"BASE/QUOTE" so the same coin lines up across exchanges.

For separator venues (BTC-USDT, BTC_USDT) the split is trivial. For no-separator
venues (BTCUSDT, btcusdt) we suffix-match a known quote list. Asset aliases are
normalised (XBT->BTC, XDG->DOGE, Bitfinex UST->USDT)."""
from __future__ import annotations
from typing import Optional

# longest-first so e.g. FDUSD matches before USD
QUOTES = ["USDT", "USDC", "FDUSD", "TUSD", "BUSD", "DAI", "USD", "EUR", "TRY",
          "BRL", "GBP", "AUD", "JPY", "KRW", "BTC", "ETH", "BNB", "TON", "SOL"]
QUOTES_SORTED = sorted(QUOTES, key=len, reverse=True)

ASSET_ALIAS = {"XBT": "BTC", "XXBT": "BTC", "XDG": "DOGE", "XXDG": "DOGE",
               "UST": "USDT", "XETH": "ETH", "ZUSD": "USD", "ZEUR": "EUR",
               "ZGBP": "GBP", "ZJPY": "JPY", "XLTC": "LTC", "XXRP": "XRP",
               "XXLM": "XLM", "XREP": "REP", "XZEC": "ZEC"}


def norm_asset(a: str) -> str:
    a = (a or "").upper()
    return ASSET_ALIAS.get(a, a)


def canon_from_parts(base: str, quote: str) -> str:
    return f"{norm_asset(base)}/{norm_asset(quote)}"


def split_sep(raw: str, sep: str) -> Optional[str]:
    if not raw or sep not in raw:
        return None
    parts = raw.split(sep)
    if len(parts) != 2:
        return None
    return canon_from_parts(parts[0], parts[1])


def split_nosep(raw: str) -> Optional[str]:
    if not raw:
        return None
    s = raw.upper()
    for q in QUOTES_SORTED:
        if s.endswith(q) and len(s) > len(q):
            return canon_from_parts(s[: -len(q)], q)
    return None


def load_bases(bases_arg: str = "", bases_file: str = "") -> set:
    """Collect base assets from a comma list and/or a file (one per line; '#'
    comments and full 'BTC/USDT' lines allowed — only the base is taken)."""
    out = set()
    if bases_file:
        with open(bases_file, encoding="utf-8") as f:
            for line in f:
                s = line.strip().upper()
                if s and not s.startswith("#"):
                    out.add(s.split("/")[0])
    for b in (bases_arg or "").split(","):
        b = b.strip().upper()
        if b:
            out.add(b)
    return out


def make_allow(bases: set, quotes):
    """Build a (canonical 'BASE/QUOTE') -> bool allowlist. Returns None (record
    everything) when no bases are given. quotes=None/['ALL'] means any quote."""
    if not bases:
        return None
    bset = set(bases)
    qlist = quotes if isinstance(quotes, (list, set, tuple)) else (quotes or "").split(",")
    qset = None if (not qlist or any(q.strip().upper() == "ALL" for q in qlist)) \
        else set(q.strip().upper() for q in qlist if q.strip())

    def allow(canonical: str) -> bool:
        if not canonical or "/" not in canonical:
            return False
        b, q = canonical.split("/", 1)
        if b not in bset:
            return False
        if qset is not None and q not in qset:
            return False
        return True
    return allow


def make_canon(kind: str):
    """kind: 'dash' (BTC-USDT), 'underscore' (BTC_USDT), 'slash' (BTC/USDT),
    'nosep' (BTCUSDT), 'nosep_lower' (btcusdt)."""
    if kind == "dash":
        return lambda r: split_sep(r, "-")
    if kind == "underscore":
        return lambda r: split_sep(r, "_")
    if kind == "slash":
        return lambda r: split_sep(r, "/")
    if kind in ("nosep", "nosep_lower"):
        return lambda r: split_nosep(r)
    raise ValueError(kind)
