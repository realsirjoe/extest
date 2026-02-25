# Comparison Baseline Results (Python v1)

These are the current Python comparator outputs used as a translation baseline for the Go port.

Reference file:

- `outputs/dm_products_reference.csv`

Candidates tested:

## candidate1

- Candidate: `outputs/dm_products_candidate1.csv`
- Status: `ok`
- Key match: `gtin -> gtin_code` (`complete`)
- Dataset similarity (equal weighted): `1.0`
- Coverage (reference / candidate): `1.0 / 1.0`
- Overall score with coverage: `1.0`
- Mapped columns: `41 / 41`

## candidate2

- Candidate: `outputs/dm_products_candidate2.csv`
- Status: `ok`
- Key match: `gtin -> gtin_code` (`complete`)
- Dataset similarity (equal weighted): `0.9999999837592288`
- Coverage (reference / candidate): `1.0 / 1.0`
- Overall score with coverage: `0.9999999837592288`
- Mapped columns: `41 / 41`

## candidate3

- Candidate: `outputs/dm_products_candidate3.csv`
- Status: `partial_key_match`
- Key match: `gtin -> gtin_code` (`partial`)
- Dataset similarity (equal weighted): `1.0`
- Coverage (reference / candidate): `0.0031295965949989044 / 1.0`
- Overall score with coverage: `0.0031295965949989044`
- Mapped columns: `41 / 41`

## Translation Goal

The Go implementation should produce the same values (or numerically equivalent values within tiny floating point tolerance) for these three comparisons.
