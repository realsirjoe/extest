#!/usr/bin/env python3
from __future__ import annotations

import argparse
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from comparison.csv_compare import compare_csv_files, report_to_json  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare candidate CSV to reference CSV and emit similarity JSON")
    parser.add_argument(
        "--reference",
        default="outputs/dm_products_reference.csv",
        help="Reference CSV (ground truth)",
    )
    parser.add_argument(
        "--candidate",
        default="outputs/dm_products_candidate1.csv",
        help="Candidate CSV to evaluate",
    )
    parser.add_argument(
        "--output-json",
        default="",
        help="Optional path to write JSON report (stdout if omitted)",
    )
    parser.add_argument(
        "--sample-size-mapping",
        type=int,
        default=256,
        help="Aligned-row sample size used for column mapping confidence",
    )
    args = parser.parse_args()

    report = compare_csv_files(
        args.reference,
        args.candidate,
        sample_size_mapping=args.sample_size_mapping,
        weights={"columns": "equal"},
    )
    payload = report_to_json(report, pretty=True)

    if args.output_json:
        out = Path(args.output_json)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(payload + "\n", encoding="utf-8")
        print(f"Wrote JSON report: {out}")
        print(f"Status: {report.get('status')}")
        score = report.get("scores", {}).get("dataset_similarity_equal_weighted")
        overall = report.get("scores", {}).get("overall_score_with_coverage")
        row_alignment = report.get("row_alignment", {})
        cov_ref = row_alignment.get("coverage_reference")
        cov_cand = row_alignment.get("coverage_candidate")
        print(
            "Dataset similarity (equal weighted):",
            f"{score:.12f}" if isinstance(score, (int, float)) else score,
        )
        print(
            "Coverage (reference/candidate):",
            (
                f"{cov_ref:.12f} / {cov_cand:.12f}"
                if isinstance(cov_ref, (int, float)) and isinstance(cov_cand, (int, float))
                else f"{cov_ref} / {cov_cand}"
            ),
        )
        print(
            "Overall score with coverage:",
            f"{overall:.12f}" if isinstance(overall, (int, float)) else overall,
        )
    else:
        print(payload)


if __name__ == "__main__":
    main()
