# UKSC Scraper Issues & Technical Notes

## 1. Next.js Client-Side Pagination (resolved: sitemap)

* **Problem:** The cases listing paginates through Next.js Server Actions (background `POST`), not URL parameters. A plain `GET` to `?p=2` returns page 1 every time — verified across `www`/non-`www`, `+`/`%20` encoding, browser headers, and cache-busting params. All return identical bytes.
* **Resolution:** The listing is not used at all. `https://supremecourt.uk/sitemap.xml` lists every case page (~1,562) in one request, with a per-case `lastmod`. `scrapy-playwright` was considered and rejected: it would still need one detail fetch per case afterwards, and adds a browser dependency for pages that are already fully server-rendered.
* **Consequence:** There is no way to filter by date or status before fetching. Cases are enumerated in full and filtered after the case page is parsed. `lastmod` is the incremental re-crawl signal.

---

## 2. Structural Variations Across Detail Pages

* **Problem:** Case pages vary by age. Newer cases host judgment HTML on the UKSC site (`#judgment-details-link`); older cases offer only a PDF, or link off-site to BAILII.
* **Status:** `#judgment-pdf`, `#judgment-details-link` and `#judgment-on-bailii-link` accurately differentiate these. A case with neither PDF nor HTML link has no published judgment yet and is skipped (`cases/without_judgment`).

---

## 3. Duplicate element IDs

The site reuses `id` attributes, so selectors must be scoped by tag or href:

* `case-id` — appears twice; take the first.
* `judgment-date` — on both the `<h3>` label and the `<p>` value; scope to `//p`.
* `press-summary-link` — on both the on-site link and the National Archives mirror; disambiguate by the `/cases/press-summary/` href prefix.
* `judgment-details` — on the judgment page this exists as **both** `<h2>` and `<h3>`. `html_content` uses the `<h3>`: its sibling `<p>` run is the judgment text alone, whereas the `<h2>` variant also picks up five leading hearing-metadata paragraphs. The case page only has the `<h2>`, but `html_content` never applies there.
