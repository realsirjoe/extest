#!/usr/bin/env python3
"""Create a shuffled CSV variant of dm_products_reference.csv.

- Shuffles row order (deterministic via seed)
- Shuffles column order
- Slightly renames columns while preserving meaning
"""

from __future__ import annotations

import argparse
import csv
import random
from pathlib import Path


DEFAULT_INPUT = Path("outputs/dm_products_reference.csv")
DEFAULT_OUTPUT = Path("outputs/dm_products_candidate1.csv")
DEFAULT_SEED = 20260224


def slight_rename(col: str) -> str:
    """Apply a small semantic rename to make the schema look different."""
    out = col
    replacements = [
        ("breadcrumbs", "crumbs"),
        ("breadcrumb", "crumb"),
        ("category_path", "category_tree"),
        ("product_is_pharmacy", "is_pharmacy_product"),
        ("rating_count", "reviews_count"),
        ("rating_value", "rating_score"),
        ("price_eur", "price_eur_amt"),
        ("unit_price", "price_per_unit"),
        ("unit_quantity", "pack_qty"),
        ("currency", "currency_code"),
        ("title_subheadline", "title_subline"),
        ("has_", "is_"),
        ("desc_", "details_"),
        ("eyecatchers", "highlights"),
        ("pills", "badges"),
        ("gtin", "gtin_code"),
        ("dan", "dan_code"),
        ("name", "product_name"),
        ("brand", "brand_name"),
    ]
    for old, new in replacements:
        out = out.replace(old, new)
    return out


def build_unique_names(columns: list[str]) -> tuple[list[str], dict[str, str]]:
    rename_map: dict[str, str] = {}
    used: dict[str, int] = {}
    out_cols: list[str] = []
    for col in columns:
        candidate = slight_rename(col)
        if candidate in used:
            used[candidate] += 1
            candidate = f"{candidate}_{used[candidate]}"
        else:
            used[candidate] = 1
        rename_map[col] = candidate
        out_cols.append(candidate)
    return out_cols, rename_map


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument(
        "--sample-rows",
        type=int,
        default=0,
        help="If > 0, randomly sample this many rows before output (deterministic by seed)",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Input CSV not found: {args.input}")

    with args.input.open("r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if reader.fieldnames is None:
            raise SystemExit("Input CSV has no header")
        original_columns = list(reader.fieldnames)
        rows = list(reader)

    rng = random.Random(args.seed)
    shuffled_columns = list(original_columns)
    rng.shuffle(shuffled_columns)

    shuffled_rows = list(rows)
    rng.shuffle(shuffled_rows)
    if args.sample_rows and args.sample_rows > 0:
        shuffled_rows = shuffled_rows[: min(args.sample_rows, len(shuffled_rows))]

    renamed_columns, rename_map = build_unique_names(shuffled_columns)

    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open("w", encoding="utf-8-sig", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=renamed_columns)
        writer.writeheader()
        for row in shuffled_rows:
            out_row = {rename_map[col]: row.get(col, "") for col in shuffled_columns}
            writer.writerow(out_row)

    print(f"Input:  {args.input}")
    print(f"Output: {args.output}")
    print(f"Seed:   {args.seed}")
    print(f"Rows:   {len(shuffled_rows)}")
    print(f"Cols:   {len(shuffled_columns)}")
    print("Sample column mapping (first 10 in output order):")
    for col in shuffled_columns[:10]:
        print(f"  {col} -> {rename_map[col]}")


if __name__ == "__main__":
    main()
