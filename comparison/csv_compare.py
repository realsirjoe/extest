from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .normalization import canonical_scalar, header_tokens, is_empty, normalize_text, parse_bool, parse_decimal
from .similarity import header_similarity, type_compatibility_score, value_similarity


@dataclass
class CSVTable:
    path: Path
    headers: list[str]
    rows: list[dict[str, str]]


def load_csv(path: str | Path) -> CSVTable:
    p = Path(path)
    with p.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {p}")
        rows = [dict(r) for r in reader]
        return CSVTable(path=p, headers=list(reader.fieldnames), rows=rows)


def compare_csv_files(
    reference_csv: str | Path,
    candidate_csv: str | Path,
    *,
    sample_size_mapping: int = 256,
    weights: dict[str, float] | None = None,
) -> dict[str, Any]:
    ref = load_csv(reference_csv)
    cand = load_csv(candidate_csv)

    weights = weights or {"columns": "equal"}  # placeholder/pluggable hook
    ref_profiles = profile_columns(ref)
    cand_profiles = profile_columns(cand)

    key_match = find_key_match(ref, cand, ref_profiles, cand_profiles)
    if not key_match["found_usable_match"]:
        return zero_result(ref, cand, ref_profiles, cand_profiles, key_match, weights)

    alignment = align_rows_by_key(ref, cand, key_match["reference_column"], key_match["candidate_column"])
    if alignment["matched_rows"] == 0:
        return zero_result(ref, cand, ref_profiles, cand_profiles, key_match, weights, alignment_override=alignment)

    alignment_summary = {k: v for k, v in alignment.items() if k != "pairs"}
    column_mapping = map_columns(
        ref,
        cand,
        ref_profiles,
        cand_profiles,
        alignment["pairs"],
        sample_size=sample_size_mapping,
    )
    scoring = score_columns(ref, cand, alignment["pairs"], column_mapping["mapping"])
    scoring["overall_score_with_coverage"] = (
        scoring.get("dataset_similarity_equal_weighted", 0.0) * alignment_summary.get("coverage_reference", 0.0)
    )

    return {
        "status": "ok" if alignment["complete"] else "partial_key_match",
        "config": {
            "reference_csv": str(ref.path),
            "candidate_csv": str(cand.path),
            "sample_size_mapping": sample_size_mapping,
            "column_weighting": weights,
            "missing_reference_column_score": 0.0,
            "extra_candidate_columns_penalize": False,
        },
        "reference_profile": {
            "row_count": len(ref.rows),
            "column_count": len(ref.headers),
            "unique_columns": [h for h, p in ref_profiles.items() if p["is_unique_non_empty"]],
        },
        "candidate_profile": {
            "row_count": len(cand.rows),
            "column_count": len(cand.headers),
        },
        "row_alignment": alignment_summary,
        "key_match": key_match,
        "column_mapping": column_mapping,
        "scores": scoring,
    }


def zero_result(
    ref: CSVTable,
    cand: CSVTable,
    ref_profiles: dict[str, dict[str, Any]],
    cand_profiles: dict[str, dict[str, Any]],
    key_match: dict[str, Any],
    weights: dict[str, Any],
    *,
    alignment_override: dict[str, Any] | None = None,
) -> dict[str, Any]:
    per_column = [
        {
            "reference_column": h,
            "candidate_column": None,
            "similarity": 0.0,
            "matched": False,
            "reason": "no_complete_key_match",
        }
        for h in ref.headers
    ]
    alignment = alignment_override or {
        "complete": False,
        "matched_rows": 0,
        "reference_rows": len(ref.rows),
        "candidate_rows": len(cand.rows),
        "coverage_reference": 0.0,
        "coverage_candidate": 0.0,
    }
    alignment_summary = {k: v for k, v in alignment.items() if k != "pairs"}
    return {
        "status": "no_complete_key_match",
        "config": {
            "reference_csv": str(ref.path),
            "candidate_csv": str(cand.path),
            "column_weighting": weights,
            "missing_reference_column_score": 0.0,
            "extra_candidate_columns_penalize": False,
        },
        "reference_profile": {
            "row_count": len(ref.rows),
            "column_count": len(ref.headers),
            "unique_columns": [h for h, p in ref_profiles.items() if p["is_unique_non_empty"]],
        },
        "candidate_profile": {
            "row_count": len(cand.rows),
            "column_count": len(cand.headers),
        },
        "row_alignment": alignment_summary,
        "key_match": key_match,
        "column_mapping": {
            "mapping": {},
            "candidate_unmatched": cand.headers,
            "reference_unmatched": ref.headers,
            "mapping_confidence_avg": 0.0,
            "pair_candidates_top": [],
        },
        "scores": {
            "dataset_similarity_equal_weighted": 0.0,
            "overall_score_with_coverage": 0.0,
            "mapped_reference_columns": 0,
            "reference_columns_total": len(ref.headers),
            "per_reference_column": per_column,
        },
    }


def profile_columns(table: CSVTable) -> dict[str, dict[str, Any]]:
    profiles: dict[str, dict[str, Any]] = {}
    row_count = len(table.rows)
    for h in table.headers:
        vals = [r.get(h, "") for r in table.rows]
        non_empty_vals = [v for v in vals if not is_empty(v)]
        canon_non_empty = [canonical_scalar(v) for v in non_empty_vals]
        non_empty_count = len(non_empty_vals)
        unique_non_empty_count = len(set(canon_non_empty))
        is_unique_non_empty = non_empty_count > 0 and unique_non_empty_count == non_empty_count

        sample = non_empty_vals[: min(500, non_empty_count)]
        if sample:
            numeric_hits = sum(1 for v in sample if parse_decimal(v) is not None)
            bool_hits = sum(1 for v in sample if parse_bool(v) is not None)
            avg_len = sum(len(normalize_text(v)) for v in sample) / len(sample)
            max_len = max(len(normalize_text(v)) for v in sample)
        else:
            numeric_hits = bool_hits = 0
            avg_len = max_len = 0.0

        profiles[h] = {
            "row_count": row_count,
            "non_empty_count": non_empty_count,
            "null_count": row_count - non_empty_count,
            "unique_non_empty_count": unique_non_empty_count,
            "is_unique_non_empty": is_unique_non_empty,
            "uniqueness_ratio_non_empty": (unique_non_empty_count / non_empty_count) if non_empty_count else 0.0,
            "numeric_ratio": (numeric_hits / len(sample)) if sample else 0.0,
            "bool_ratio": (bool_hits / len(sample)) if sample else 0.0,
            "avg_len_sample": avg_len,
            "max_len_sample": max_len,
            "header_tokens": header_tokens(h),
        }
    return profiles


def find_key_match(
    ref: CSVTable,
    cand: CSVTable,
    ref_profiles: dict[str, dict[str, Any]],
    cand_profiles: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []

    for ref_col in ref.headers:
        if not ref_profiles[ref_col]["is_unique_non_empty"]:
            continue
        ref_vals = [canonical_scalar(r.get(ref_col, "")) for r in ref.rows if not is_empty(r.get(ref_col, ""))]
        ref_set = set(ref_vals)
        if len(ref_set) != len(ref_vals):
            continue
        for cand_col in cand.headers:
            cand_vals = [canonical_scalar(r.get(cand_col, "")) for r in cand.rows if not is_empty(r.get(cand_col, ""))]
            cand_set = set(cand_vals)
            if len(cand_set) != len(cand_vals):
                continue
            intersection = len(ref_set & cand_set)
            complete = (
                len(ref.rows) == len(cand.rows)
                and len(cand_vals) == len(ref_vals)
                and cand_set == ref_set
            )
            cand_coverage = (intersection / len(cand_set)) if cand_set else 0.0
            ref_coverage = (intersection / len(ref_set)) if ref_set else 0.0
            if intersection == 0:
                continue
            header_score = header_similarity(ref_col, cand_col)
            # Prefer complete matches; otherwise prefer keys that explain all candidate rows.
            key_score = (1.0 if complete else 0.0) * 10.0 + (cand_coverage * 2.0) + ref_coverage + header_score
            candidates.append(
                {
                    "reference_column": ref_col,
                    "candidate_column": cand_col,
                    "complete_set_match": complete,
                    "intersection_count": intersection,
                    "candidate_key_coverage": round(cand_coverage, 6),
                    "reference_key_coverage": round(ref_coverage, 6),
                    "header_similarity": round(header_score, 6),
                    "reference_non_empty_count": len(ref_vals),
                    "candidate_non_empty_count": len(cand_vals),
                    "score": key_score,
                }
            )

    if not candidates:
        return {
            "found_usable_match": False,
            "found_complete_match": False,
            "reference_column": None,
            "candidate_column": None,
            "reason": "no_exact_or_partial_unique_key_match",
            "candidates": [],
        }

    candidates.sort(key=lambda x: (x["score"], x["reference_non_empty_count"]), reverse=True)
    best = candidates[0]
    usable = best["intersection_count"] > 0
    mode = "complete" if best["complete_set_match"] else "partial"
    return {
        "found_usable_match": usable,
        "found_complete_match": bool(best["complete_set_match"]),
        "match_mode": mode,
        "reference_column": best["reference_column"],
        "candidate_column": best["candidate_column"],
        "reason": "exact_unique_key_set_match" if best["complete_set_match"] else "partial_unique_key_overlap_match",
        "candidates": candidates[:10],
    }


def align_rows_by_key(ref: CSVTable, cand: CSVTable, ref_key: str, cand_key: str) -> dict[str, Any]:
    ref_index: dict[str, int] = {}
    duplicate_ref_keys = 0
    for idx, row in enumerate(ref.rows):
        key = canonical_scalar(row.get(ref_key, ""))
        if key == "":
            continue
        if key in ref_index:
            duplicate_ref_keys += 1
            continue
        ref_index[key] = idx

    pairs: list[list[int]] = []
    seen_ref_indices: set[int] = set()
    missing_candidate_keys = 0
    duplicate_candidate_matches = 0

    for cand_idx, row in enumerate(cand.rows):
        key = canonical_scalar(row.get(cand_key, ""))
        if key == "":
            missing_candidate_keys += 1
            continue
        ref_idx = ref_index.get(key)
        if ref_idx is None:
            missing_candidate_keys += 1
            continue
        if ref_idx in seen_ref_indices:
            duplicate_candidate_matches += 1
            continue
        seen_ref_indices.add(ref_idx)
        pairs.append([ref_idx, cand_idx])

    pairs.sort(key=lambda p: p[0])
    matched = len(pairs)
    complete = (
        duplicate_ref_keys == 0
        and duplicate_candidate_matches == 0
        and missing_candidate_keys == 0
        and matched == len(ref.rows) == len(cand.rows)
    )
    return {
        "complete": complete,
        "reference_key": ref_key,
        "candidate_key": cand_key,
        "matched_rows": matched,
        "reference_rows": len(ref.rows),
        "candidate_rows": len(cand.rows),
        "coverage_reference": (matched / len(ref.rows)) if ref.rows else 0.0,
        "coverage_candidate": (matched / len(cand.rows)) if cand.rows else 0.0,
        "duplicate_reference_keys": duplicate_ref_keys,
        "duplicate_candidate_matches": duplicate_candidate_matches,
        "missing_candidate_keys_or_unmatched": missing_candidate_keys,
        "pairs": pairs,
    }


def map_columns(
    ref: CSVTable,
    cand: CSVTable,
    ref_profiles: dict[str, dict[str, Any]],
    cand_profiles: dict[str, dict[str, Any]],
    alignment_pairs: list[list[int]],
    *,
    sample_size: int,
) -> dict[str, Any]:
    sample_pairs = alignment_pairs[: min(sample_size, len(alignment_pairs))]
    pair_scores: list[dict[str, Any]] = []

    for ref_col in ref.headers:
        for cand_col in cand.headers:
            h_score = header_similarity(ref_col, cand_col)
            t_score = type_compatibility_score(ref_profiles[ref_col], cand_profiles[cand_col])
            s_score = sample_column_similarity_fast(ref, cand, sample_pairs, ref_col, cand_col)
            combined = 0.35 * h_score + 0.10 * t_score + 0.55 * s_score
            pair_scores.append(
                {
                    "reference_column": ref_col,
                    "candidate_column": cand_col,
                    "header_similarity": round(h_score, 6),
                    "type_compatibility": round(t_score, 6),
                    "sample_similarity": round(s_score, 6),
                    "mapping_confidence": round(combined, 6),
                }
            )

    pair_scores.sort(key=lambda p: (p["mapping_confidence"], p["sample_similarity"], p["header_similarity"]), reverse=True)

    used_ref: set[str] = set()
    used_cand: set[str] = set()
    mapping: dict[str, dict[str, Any]] = {}
    mapping_confidences: list[float] = []
    for p in pair_scores:
        r = p["reference_column"]
        c = p["candidate_column"]
        if r in used_ref or c in used_cand:
            continue
        # Keep low-scoring pairs out of forced mapping for missing-column semantics.
        if p["mapping_confidence"] < 0.55 and p["sample_similarity"] < 0.85:
            continue
        mapping[r] = p
        used_ref.add(r)
        used_cand.add(c)
        mapping_confidences.append(p["mapping_confidence"])

    ref_unmatched = [h for h in ref.headers if h not in used_ref]
    cand_unmatched = [h for h in cand.headers if h not in used_cand]

    return {
        "mapping": mapping,
        "reference_unmatched": ref_unmatched,
        "candidate_unmatched": cand_unmatched,
        "mapping_confidence_avg": (sum(mapping_confidences) / len(mapping_confidences)) if mapping_confidences else 0.0,
        "pair_candidates_top": pair_scores[:50],
    }


def sample_column_similarity_fast(
    ref: CSVTable,
    cand: CSVTable,
    sample_pairs: list[list[int]],
    ref_col: str,
    cand_col: str,
) -> float:
    if not sample_pairs:
        return 0.0
    exact = 0.0
    both_empty = 0.0
    same_presence = 0.0
    for ref_idx, cand_idx in sample_pairs:
        rv = ref.rows[ref_idx].get(ref_col, "")
        cv = cand.rows[cand_idx].get(cand_col, "")
        r_empty = is_empty(rv)
        c_empty = is_empty(cv)
        if r_empty and c_empty:
            both_empty += 1.0
            same_presence += 1.0
            exact += 1.0
            continue
        if r_empty == c_empty:
            same_presence += 1.0
        if canonical_scalar(rv) == canonical_scalar(cv):
            exact += 1.0
    n = float(len(sample_pairs))
    # Exact match dominates; presence pattern helps with sparse text columns.
    return (0.85 * (exact / n)) + (0.15 * (same_presence / n))


def score_columns(
    ref: CSVTable,
    cand: CSVTable,
    alignment_pairs: list[list[int]],
    mapping: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    per_ref: list[dict[str, Any]] = []
    total = 0.0
    for ref_col in ref.headers:
        m = mapping.get(ref_col)
        if not m:
            per_ref.append(
                {
                    "reference_column": ref_col,
                    "candidate_column": None,
                    "similarity": 0.0,
                    "matched": False,
                    "mapping_confidence": 0.0,
                    "row_count_scored": 0,
                }
            )
            continue
        cand_col = m["candidate_column"]
        score = full_column_similarity(ref, cand, alignment_pairs, ref_col, cand_col)
        total += score
        per_ref.append(
            {
                "reference_column": ref_col,
                "candidate_column": cand_col,
                "similarity": score,
                "matched": True,
                "mapping_confidence": m["mapping_confidence"],
                "row_count_scored": len(alignment_pairs),
                "header_similarity": m["header_similarity"],
                "sample_similarity": m["sample_similarity"],
            }
        )

    dataset_score = (total / len(ref.headers)) if ref.headers else 0.0
    return {
        "dataset_similarity_equal_weighted": dataset_score,
        "mapped_reference_columns": sum(1 for x in per_ref if x["matched"]),
        "reference_columns_total": len(ref.headers),
        "per_reference_column": per_ref,
    }


def full_column_similarity(
    ref: CSVTable,
    cand: CSVTable,
    alignment_pairs: list[list[int]],
    ref_col: str,
    cand_col: str,
) -> float:
    if not alignment_pairs:
        return 0.0
    total = 0.0
    for ref_idx, cand_idx in alignment_pairs:
        total += value_similarity(ref.rows[ref_idx].get(ref_col, ""), cand.rows[cand_idx].get(cand_col, ""))
    return total / len(alignment_pairs)


def report_to_json(report: dict[str, Any], *, pretty: bool = True) -> str:
    if pretty:
        return json.dumps(report, ensure_ascii=False, indent=2)
    return json.dumps(report, ensure_ascii=False, separators=(",", ":"))
