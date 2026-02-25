# Extest Specification (Draft)

Extest is a testing library for evaluating the creation of web extraction tools. It provides a way to test fast and avoids putting pressure on real online resources. It is effectively a tool that can spin up different instances of websites with boilerplate data that is given to it. Different instances might have different constraints (for example: some sites might deploy Cloudflare, others might not have sitemaps, others might need JavaScript, others might be server‑side rendered). A good way to start is by creating three versions: easy, medium, and hard to extract. Since the data is known, we can compare if the extraction was successful. Data may be transformed on the page and an effective crawler would reorder the data correctly to its structured source form.

## Current Availability

- `easy-server` (implemented): easiest variant; public JSON APIs and open sitemaps.
- `medium-server` (implemented): medium variant; no public JSON APIs, structured data embedded inline in HTML.
- `hard-server` (planned): hardest variant; additional discovery friction and anti-extraction constraints.

## Test Site Versions

### Easy
- Storefront with JSON data returned for products behind the scenes.
- Open sitemap.

### Medium
- Storefront with products in HTML.
- Deterministic classes or IDs.
- Open sitemap.

### Hard
- Closed sitemap.
- Requires pagination of search results.
- May include TLS fingerprinting or other bot defenses.
