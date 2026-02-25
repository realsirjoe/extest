# Easy Server Spec (Current Intent)

This document describes the intended behavior of `cmd/easy-server`.

## Available Servers (Context)

- `easy-server` (implemented): easiest; public JSON APIs + open sitemaps.
- `medium-server-1` (implemented): medium; no public JSON APIs, inline JSON embedded in HTML.
- `hard-server` (planned): hardest; additional discovery and anti-extraction constraints.

## Goal

`easy-server` is the easiest extraction target in the test suite.

It should make structured data access straightforward so extractor/crawler implementations can be validated quickly before moving to harder variants.

## Current Design Rules

1. Public data APIs are available
- `GET /api/home`
- `GET /api/product/{id}`
- `GET /api/product/{id}/similar`

2. HTML pages may render client-side from those APIs
- Homepage (`/`) is API-driven in the browser using `/api/home`
- Product page (`/product/{id}`) is API-driven in the browser using:
  - `/api/product/{id}`
  - `/api/product/{id}/similar`

3. APIs are intentionally discoverable
- Route shapes are part of the challenge design for `easy`
- The page may expose API usage via network requests (and optionally visible links/buttons during development)

4. Sitemaps are available
- `GET /sitemap.xml` (sitemap index)
- `GET /sitemaps/products-{n}.xml` (paged product sitemaps)
- Sitemap page count is computed dynamically from the database row count

## Why This Is “Easy”

- Structured data is available through explicit JSON endpoints
- Route discovery is simple
- Sitemap discovery is open
- Product and home pages can be validated against visible API payloads

## Intended Usage

1. Run `easy-server`
2. Point your extractor/crawler at the local site
3. Produce a candidate CSV from extracted results
4. Compare it with `cmd/compare-csv` against the reference CSV

## Planned Relationship to Other Variants

- `easy`: public APIs + open sitemaps + low friction
- `medium`: no public APIs, data embedded inline in HTML
- `hard`: additional discovery and anti-extraction constraints (planned)
