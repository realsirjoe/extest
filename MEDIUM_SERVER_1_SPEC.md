# Medium Server Spec (Current Intent)

This document describes the intended behavior of `cmd/medium-server-1`.

## Available Servers (Context)

- `easy-server` (implemented): easiest; public JSON APIs + open sitemaps.
- `medium-server-1` (implemented): medium; no public JSON APIs, inline JSON embedded in HTML.
- `hard-server` (planned): hardest; additional discovery and anti-extraction constraints.

## Goal

`medium-server-1` should be harder to extract than `easy-server` by removing direct public JSON API access while still serving the same underlying product data.

## Current Design Rules

1. Public data APIs are hidden
- No public `/api/home`
- No public `/api/product/{id}`
- No public `/api/product/{id}/similar`

2. Data may still exist as JSON, but only inline in HTML
- The backend may fetch/build structured data and embed it into the HTML response
- This data is considered part of the page source (HTML inline script data), not a standalone API surface

3. Frontend pages render from backend-provided inline data
- Homepage (`/`) renders HTML + inline data payload
- Product page (`/product/{id}`) renders HTML + inline product/similar payloads

4. Route shapes for hidden APIs should not be leaked
- Do not include fields like `api_path` in page payloads
- Avoid UI copy that references hidden endpoints

## Why This Sits Between Easy and Hard

- Easier than a fully HTML-only server because structured data can still be present in page source
- Harder than `easy-server` because there are no direct JSON endpoints to call

## Planned Hardening (Future)

- Reduce or obfuscate inline structured payloads
- Move more data into rendered HTML-only output
- Remove predictable field names
- Introduce pagination/discovery friction and other anti-extraction constraints
