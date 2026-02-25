from __future__ import annotations

import math
from difflib import SequenceMatcher

from .normalization import canonical_scalar, header_tokens, is_empty, normalize_text, parse_bool, parse_decimal


def value_similarity(a: object, b: object) -> float:
    if is_empty(a) and is_empty(b):
        return 1.0
    if is_empty(a) or is_empty(b):
        return 0.0

    a_norm = normalize_text(a)
    b_norm = normalize_text(b)
    if a_norm == b_norm:
        return 1.0

    a_bool = parse_bool(a_norm)
    b_bool = parse_bool(b_norm)
    if a_bool is not None and b_bool is not None:
        return 1.0 if a_bool == b_bool else 0.0

    a_num = parse_decimal(a_norm)
    b_num = parse_decimal(b_norm)
    if a_num is not None and b_num is not None:
        if a_num == b_num:
            return 1.0
        a_f, b_f = float(a_num), float(b_num)
        denom = max(abs(a_f), abs(b_f), 1.0)
        return max(0.0, 1.0 - (abs(a_f - b_f) / denom))

    return normalized_levenshtein_similarity(a_norm, b_norm)


def normalized_levenshtein_similarity(a: str, b: str) -> float:
    if a == b:
        return 1.0
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    dist = levenshtein_distance(a, b)
    denom = max(len(a), len(b))
    if denom == 0:
        return 1.0
    return max(0.0, 1.0 - (dist / denom))


def levenshtein_distance(a: str, b: str) -> int:
    if a == b:
        return 0
    if len(a) < len(b):
        a, b = b, a
    if not b:
        return len(a)

    prev = list(range(len(b) + 1))
    for i, ca in enumerate(a, start=1):
        curr = [i]
        for j, cb in enumerate(b, start=1):
            ins = curr[j - 1] + 1
            delete = prev[j] + 1
            sub = prev[j - 1] + (0 if ca == cb else 1)
            curr.append(min(ins, delete, sub))
        prev = curr
    return prev[-1]


def header_similarity(a: str, b: str) -> float:
    a_norm = "".join(header_tokens(a))
    b_norm = "".join(header_tokens(b))
    if not a_norm and not b_norm:
        return 1.0

    seq_ratio = SequenceMatcher(None, a_norm, b_norm).ratio()
    a_set = set(header_tokens(a))
    b_set = set(header_tokens(b))
    if not a_set and not b_set:
        jacc = 1.0
    elif not a_set or not b_set:
        jacc = 0.0
    else:
        jacc = len(a_set & b_set) / len(a_set | b_set)
    return max(seq_ratio, jacc)


def exact_set_match(values_a: set[str], values_b: set[str]) -> bool:
    return values_a == values_b


def type_compatibility_score(ref_profile: dict, cand_profile: dict) -> float:
    r_num = ref_profile.get("numeric_ratio", 0.0)
    c_num = cand_profile.get("numeric_ratio", 0.0)
    r_bool = ref_profile.get("bool_ratio", 0.0)
    c_bool = cand_profile.get("bool_ratio", 0.0)

    if r_bool >= 0.9 and c_bool >= 0.9:
        return 1.0
    if (r_bool >= 0.9) != (c_bool >= 0.9):
        return 0.1
    if r_num >= 0.9 and c_num >= 0.9:
        return 1.0
    if (r_num >= 0.9) != (c_num >= 0.9):
        return 0.2
    return 0.8

