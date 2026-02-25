from __future__ import annotations

import re
from decimal import Decimal, InvalidOperation
from typing import Any


_NUMERIC_RE = re.compile(r"^[+-]?(?:\d+\.?\d*|\.\d+)$")
_TOKEN_RE = re.compile(r"[a-z0-9]+")


def is_empty(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip() == ""
    return False


def normalize_text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def parse_bool(value: Any) -> bool | None:
    s = normalize_text(value).lower()
    if s in {"true", "1", "yes", "y"}:
        return True
    if s in {"false", "0", "no", "n"}:
        return False
    return None


def parse_decimal(value: Any) -> Decimal | None:
    s = normalize_text(value)
    if s == "" or not _NUMERIC_RE.match(s):
        return None
    try:
        return Decimal(s)
    except InvalidOperation:
        return None


def canonical_scalar(value: Any) -> str:
    """Canonical string used for exact set membership and key matching."""
    if is_empty(value):
        return ""
    b = parse_bool(value)
    if b is not None:
        return "true" if b else "false"
    d = parse_decimal(value)
    if d is not None:
        return format(d.normalize(), "f")
    return normalize_text(value)


def header_tokens(name: str) -> list[str]:
    raw = [t for t in _TOKEN_RE.findall(name.lower()) if t]
    tokens: list[str] = []
    for t in raw:
        tokens.append(_canon_header_token(t))
    return [t for t in tokens if t]


def _canon_header_token(token: str) -> str:
    aliases = {
        "crumb": "breadcrumb",
        "crumbs": "breadcrumbs",
        "tree": "path",
        "details": "desc",
        "highlights": "eyecatchers",
        "badges": "pills",
        "reviews": "rating",
        "score": "value",
        "qty": "quantity",
        "pack": "unit",
        "subline": "subheadline",
        "amt": "",  # low-signal suffix
        "code": "",  # low-signal suffix used in shuffled headers
        "is": "has",
    }
    if token in {"product"}:
        return ""  # neutral in many renamed columns
    return aliases.get(token, token)

