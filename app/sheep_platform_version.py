import re
from typing import Tuple


_VERSION_RE = re.compile(r"^(\d+)\.(\d+)\.(\d+)(?:[\-\+].*)?$")


def parse_semver(v: str) -> Tuple[int, int, int]:
    s = str(v or "").strip()
    m = _VERSION_RE.fullmatch(s)
    if not m:
        return (0, 0, 0)
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def semver_lt(a: str, b: str) -> bool:
    return parse_semver(a) < parse_semver(b)


def semver_gte(a: str, b: str) -> bool:
    return parse_semver(a) >= parse_semver(b)
