# Comparison Module Spec (Draft)

## Goal

Build an isolated module that compares an extracted dataset against a known reference dataset and outputs similarity scores.

Initial target:

- Reference dataset: `outputs/dm_products_reference.csv`
- Candidate dataset: `outputs/dm_products_candidate1.csv`

The module should be reusable for later extraction runs where:

- row order may differ
- column order may differ
- column names may differ
- values may be slightly transformed

## High-Level Requirements

- Output similarity scores in `[0, 1]`
- `1.0` means identical
- `0.0` means completely missing / no match
- Support strings, numbers, booleans, and empty values
- Column-level similarity should be easy to compute as an average of row-level similarities
- Try to find likely column mapping first (fast path)
- Fall back to broader search if mapping is unclear
- Use indexing only on unique reference columns
- Keep implementation isolated and pluggable

## Core Concepts

### 1) Value Similarity

Given two cell values `a` and `b`, return `sim(a, b)` in `[0, 1]`.

Required edge cases:

- both empty -> `1.0`
- one missing / other present -> `0.0`
- both missing because column not present -> `0.0` at the "column missing" level (not value level)

Notes:

- "empty" should include `""`, whitespace-only, and null-like values after normalization.
- Missing column and empty cell are different cases.

### 2) Column Similarity

For aligned rows, column similarity is:

- average of `sim(value_ref, value_candidate)` across rows

This makes the score interpretable and composable.

### 3) Dataset Similarity

Can be computed later as:

- average of matched-column scores
- optionally weighted by importance / confidence / uniqueness

## Proposed Comparison Pipeline

### Phase A: Load + Normalize

- Read both CSVs
- Normalize headers (keep original + normalized forms)
- Normalize values for comparison:
  - trim whitespace
  - normalize null/empty values
  - preserve original strings for reporting
  - optionally parse numeric/boolean forms

### Phase B: Profile Reference Columns

For each reference column:

- row count
- non-empty count
- uniqueness ratio
- exact unique? (non-empty values unique across rows)
- type hints (numeric / mostly numeric / text / boolean)
- average length / max length (for similarity strategy)

Only exact-unique columns are eligible as index keys.

### Phase C: Candidate-to-Reference Column Mapping (Fast Path)

Try to map candidate columns to reference columns without full all-vs-all row comparison.

Signals to use:

- header-name similarity (e.g. `gtin_code` vs `gtin`)
- uniqueness profile similarity
- type compatibility
- sample-value similarity on a small sample
- exact match rate after normalization on sampled rows (if rows can be aligned using a provisional key)

Expected examples:

- `gtin_code` -> `gtin`
- `rating_score` -> `rating_value`
- `brand_name` -> `brand`

Output:

- candidate -> reference mapping with confidence `[0,1]`
- unresolved columns list

### Phase D: Row Alignment / Record Matching

Use one or more unique reference columns to align rows.

Preferred approach:

1. Detect unique reference columns (e.g. `gtin`)
2. Check candidate columns for strong similarity to one of those keys
3. Build hash index on reference unique key values
4. Map candidate rows to reference rows

Fallbacks:

- composite key candidates (future)
- approximate lookup on top-N likely key columns (future)
- positional alignment only for debugging (low confidence)

For current shuffled test data, `gtin_code` should map to `gtin` and enable exact row alignment.

### Phase E: Column Similarity Scoring

Once rows are aligned:

- score each mapped column by averaging value similarity
- unmatched candidate columns -> report as extra columns
- unmatched reference columns -> report as missing columns (score `0`)

### Phase F: Reporting

Return structured output (JSON-friendly):

- summary scores
- row alignment stats
- column mapping + confidence
- per-column similarity
- unmatched columns
- diagnostics / reasons / warnings

## Value Similarity Algorithm Options (Discussion)

We need similarity stable across short and long strings.

### Option A: Normalized Levenshtein Similarity (Recommended baseline)

Formula idea:

- `1 - edit_distance(a, b) / max(len(a), len(b))`

Pros:

- intuitive
- length-normalized
- works on IDs and long strings

Cons:

- can be slower on very long text if used everywhere

### Option B: Exact / Prefix Fast Paths + Levenshtein Fallback (Recommended)

Use cheap checks first:

- exact match -> `1`
- both empty -> `1`
- one empty -> `0`
- normalized numeric exact -> `1`

Then choose algorithm:

- short/medium strings -> normalized Levenshtein
- long strings -> token-based similarity or sequence matcher

This likely gives good speed/quality balance.

### Option C: Jaro-Winkler for Short IDs/Names

Pros:

- good for short strings / typos

Cons:

- less intuitive for long text
- may over-score shared prefixes

Best used only as an optional short-string metric.

### Option D: Token-Based Similarity for Long Text Fields

Examples: Jaccard / Dice on tokens or n-grams.

Pros:

- faster for long descriptions
- robust to word reordering

Cons:

- loses character-level detail

Good as a long-text mode.

## Proposed Similarity Strategy (v1)

Implement a pluggable `ValueSimilarity` function with strategy dispatch:

1. Normalize inputs
2. Edge cases (`empty`, missing)
3. Type-aware exact checks:
   - numeric exact after parse -> `1`
   - boolean exact -> `1`
4. String mode:
   - length <= 64 chars: normalized Levenshtein
   - length > 64 chars: token trigram Dice similarity (or `difflib.SequenceMatcher` baseline if we want simplicity first)

This keeps behavior stable across length while allowing optimization later.

## Performance Strategy

### Why not all-columns x all-columns by default?

Full comparison is expensive:

- many column pairs
- many rows
- expensive string similarity calls

So we should:

- map likely columns first
- align rows using unique keys
- compare only mapped pairs deeply

### Fallback behavior

If no reliable column/key mapping is found:

- run limited all-vs-all column scoring on samples
- choose top candidates
- then re-run full scoring on selected pairs

This keeps fallback possible without exploding cost.

## Isolation / Plug-and-Play Design

Keep this module independent from the server and extraction code.

Suggested layout:

- `comparison/`
  - `README.md` (usage)
  - `schema_profile.py`
  - `normalization.py`
  - `similarity.py`
  - `column_mapping.py`
  - `row_alignment.py`
  - `scorer.py`
  - `report_types.py`
  - `compare_csv.py` (CLI entry point)

Or for a smaller first pass:

- `scripts/compare_csvs.py`
- `scripts/comparison_lib/` for internals

## v1 Deliverable (Recommended)

First implementation should support:

- CSV-to-CSV comparison
- deterministic row alignment using one detected unique key
- column mapping using header + sample values
- per-column similarity scores
- summary JSON report

Stretch goals (later):

- composite key matching
- approximate row matching
- weighting by business-critical fields
- confusion analysis / mismatch examples
- SQLite inputs

## Open Questions (For Next Discussion)

1. Should dataset-level score weight all columns equally, or prioritize key/business columns (e.g. `gtin`, `name`, `price`)?
2. Do we want strict numeric tolerance support (e.g. `20.85` vs `20.8500` = exact) beyond string similarity?
3. For long text (`desc_*`), do we care more about exact characters or semantic overlap?
4. Should extra candidate columns reduce the final score, or just be reported?
5. Should row alignment require a single exact unique key in v1, or allow "best key among several" automatically?
6. Output format preference: JSON only, or JSON + human-readable markdown summary?

## Example Success Criteria (Current Test)

Comparing:

- `outputs/dm_products_reference.csv`
- `outputs/dm_products_candidate1.csv`

Expected:

- Detect row key mapping via `gtin_code` -> `gtin`
- Detect most column mappings despite renames
- Column similarities near `1.0` for all mapped columns
- Row alignment coverage near `100%`
