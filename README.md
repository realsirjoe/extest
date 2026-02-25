# extest

`extest` is a local testbed for evaluating web extraction/crawling systems against known data.

It provides three separable parts:

1. Reference data generation (ground truth)
2. Test storefront servers (easy / medium / hard challenge variants)
3. A comparison module that scores extraction output against the reference

The goal is to test extraction quality without hitting real websites and without manually grading results.

## Project Status

- Implemented: reference data tooling (Go), comparison module (Go), `easy` storefront server (Go), internal candidate shuffler utility (Go)
- Planned / documented: `medium` and `hard` challenge servers (see `SPEC.md`)

## High-Level Architecture

```text
                 raw JSONL (local only)
                outputs/sample_products_all.jl
                           |
                           v
              +---------------------------+
              | cmd/process-products      |
              | build reference artifacts |
              +---------------------------+
                 |            |          |
                 |            |          |
                 v            v          v
      sample_products_reference.csv   sample_products_cleaned.sqlite   sample_products_profile.md
                 |                     |
                 |                     v
                 |          +---------------------------+
                 |          | test server(s)            |
                 |          | cmd/easy-server (+ future |
                 |          | medium / hard variants)   |
                 |          +---------------------------+
                 |                     |
                 |                     v
                 |          your extractor / crawler
                 |         (outside this repository)
                 |                     |
                 |                     v
                 |          candidate output CSV (yours)
                 |                     |
                 +---------------------+------------------+
                                       |
                                       v
                          +---------------------------+
                          | cmd/compare-csv           |
                          | similarity + coverage     |
                          +---------------------------+
                                       |
                                       v
                             JSON report + summary

  Internal dev utility (optional):
    cmd/shuffle-csv -> generates synthetic candidate CSVs for comparator testing
```

## Components

### 1) Reference Data Tooling (`cmd/process-products`)

Builds the reference dataset (ground truth) from a local JSON Lines source file.

Outputs:

- `outputs/sample_products_reference.csv` (reference CSV)
- `outputs/sample_products_cleaned.sqlite` (SQLite DB used by the test server)
- `outputs/sample_products_profile.md` (profiling/QA summary)

Default input:

- `outputs/sample_products_all.jl` (ignored locally, not committed)

Example:

```bash
go run ./cmd/process-products
```

Useful flags:

- `--input`
- `--out-dir`
- `--csv`
- `--sqlite`
- `--profile`
- `--limit`

### 2) Test Storefront Servers (`cmd/easy-server`)

The server reads the generated SQLite DB and exposes:

- storefront pages (`/`, `/product/{id}`)
- product APIs (`/api/product/{id}`)
- similar-products API (`/api/product/{id}/similar`)
- home page feed API (`/api/home`)

Current implementation:

- `easy` variant is implemented (`cmd/easy-server`)
- `medium` / `hard` variants are planned (see `SPEC.md`)

Example:

```bash
go run ./cmd/easy-server -path outputs/sample_products_cleaned.sqlite -id gtin -addr 127.0.0.1:8080
```

Then open:

- `http://127.0.0.1:8080/`
- `http://127.0.0.1:8080/product/<some-id>`

The intended usage is:

1. Run one of the local test servers
2. Run your extractor/crawler against it
3. Export your extracted result to a candidate CSV
4. Compare that CSV against the reference with `cmd/compare-csv`

### 3) Comparison Module (`cmd/compare-csv`)

Compares a candidate CSV (typically produced by your extractor) against a reference CSV and produces:

- value similarity (0..1)
- row coverage (reference/candidate)
- combined score (`similarity * coverage_reference`)
- discovered key match and column mapping details
- per-column scores and diagnostics

Key behavior:

- Key discovery is dynamic (not hardcoded to a single column name)
- Row alignment is key-based
- Supports full and partial key matches
- Missing reference columns score `0`
- Extra candidate columns are reported but not penalized (current default)

Example:

```bash
go run ./cmd/compare-csv \
  --reference outputs/sample_products_reference.csv \
  --candidate path/to/your_extractor_output.csv \
  --output-json outputs/report_extraction_run.json
```

CLI summary includes:

- dataset similarity (equal weighted)
- coverage (reference/candidate)
- overall score with coverage

See also:

- `COMPARISON_MODULE_SPEC.md`
- `COMPARISON_BASELINE_RESULTS.md`

### 4) Internal Candidate Generator (`cmd/shuffle-csv`) [Optional]

Creates transformed candidate CSVs from the reference CSV to simulate extractor output:

- row order shuffled
- column order shuffled
- column names slightly renamed
- optional row sampling (subset candidates)

This is primarily an internal/developer tool for testing the comparator itself (mapping, alignment, subset coverage, mutation behavior). It is not the primary project workflow.

Example:

```bash
go run ./cmd/shuffle-csv \
  --input outputs/sample_products_reference.csv \
  --output outputs/sample_products_candidate1.csv \
  --seed 20260224
```

Subset example:

```bash
go run ./cmd/shuffle-csv \
  --input outputs/sample_products_reference.csv \
  --output outputs/sample_products_candidate3.csv \
  --sample-rows 100
```

## Difficulty Variants (Roadmap)

See `SPEC.md` for the draft challenge design.

- `easy`: JSON-backed storefront, easier discovery
- `medium`: HTML-first storefront with stable selectors
- `hard`: reduced discoverability (e.g. closed sitemap, pagination, anti-bot constraints)

The comparison module is designed to stay reusable across all variants because it only depends on reference/candidate tabular data.

## Quick Start

If you already have a local source JSONL file at `outputs/sample_products_all.jl`:

```bash
make build-all
bin/process-products
bin/easy-server -path outputs/sample_products_cleaned.sqlite -id gtin -addr 127.0.0.1:8080
```

Then run your extractor against the local server and compare its CSV output:

```bash
bin/compare-csv \
  --reference outputs/sample_products_reference.csv \
  --candidate path/to/your_extractor_output.csv \
  --output-json outputs/report_extraction_run.json
```

Optional (internal comparator testing only):

```bash
bin/shuffle-csv --input outputs/sample_products_reference.csv --output outputs/sample_products_candidate1.csv
bin/compare-csv --reference outputs/sample_products_reference.csv --candidate outputs/sample_products_candidate1.csv
```

Or run the server directly with the provided Make target:

```bash
make serve-easy
```

## Testing

Comparator tests:

```bash
make test
```

Notes:

- `testdata/` is kept in git, but local CSV fixtures are intentionally ignored (`testdata/*.csv`)
- `outputs/` is local-only and ignored

## Repository Layout

```text
cmd/
  compare-csv/      CSV comparison/scoring CLI
  easy-server/      easy storefront + APIs
  process-products/ reference data builder (JSONL -> CSV/SQLite/profile)
  shuffle-csv/      internal candidate generator for comparator tests

testdata/           local test fixtures directory (tracked via .gitkeep)
outputs/            local generated artifacts (ignored)

SPEC.md                     challenge-variant roadmap
COMPARISON_MODULE_SPEC.md   comparator design notes
COMPARISON_BASELINE_RESULTS.md  parity/baseline expectations
```

## Public Repo Data Policy

This repo is structured so generated/local data can stay on your machine:

- `outputs/` is ignored
- `testdata/*.csv` is ignored
- only `testdata/.gitkeep` is committed to preserve the directory

This keeps the repository small and avoids uploading bulky or source-derived fixture data by default.
