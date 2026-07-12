# Extraction libraries adoption — design

Date: 2026-07-13
Status: approved (scope: full set; rollout: adopt going forward, no flag)

## Goal

Replace hand-rolled HTML text/data handling in cc-enrich-worker with battle-tested libraries
where they are strictly better, improving embed-text quality (NACE classification input),
non-UTF-8 correctness, profile recall on older sites, and phone-contact data quality.
No output schema changes. Every new path degrades to current behavior on failure and never
fails a page.

## Components (independent, staged in this order, one commit each)

### 1. Charset transcoding — `golang.org/x/net/html/charset` (already a dependency)

- New `parse.DecodeHTML(body []byte, contentType string) (decoded []byte, encodingName string)`.
  Uses `charset.DetermineEncoding` (sniffs BOM, Content-Type param, `<meta charset>`, and the
  `http-equiv` form the current head-meta parser misses). UTF-8 → return input unchanged
  (zero copy). Otherwise transcode to UTF-8; on transcode error return input unchanged.
- Call sites: `worker.processPage` and the `ProcessIndustryStream` fetch worker decode once per
  page. Decoded bytes feed: visible-text parse, emails, JSON-LD profile, LEI/VAT, trackers,
  head-meta. Raw bytes still feed `tech.DetectTech` (upstream wappalyzergo parity).
- `HeadMeta.Charset` is backfilled with the detected encoding name when the head declares none.
- Content-Type comes from the WARC HTTP response headers (already available in `processPage`;
  the industry stream currently discards headers — stop discarding).

### 2. Embed text — `github.com/markusmobius/go-trafilatura`

- New `parse.MainText(body []byte, pageURL string) string`: trafilatura main-content extraction
  (drops nav/cookie banners/footer boilerplate). On error or empty result, fall back to the
  existing `ParseHTML` walk text — never return less than today.
- Used ONLY for the text sent to the embedder (`processPage` runEmbed branch and the industry
  stream). `ParseHTML` stays for emails/socials.
- Benchmark added (walk vs trafilatura); runs once per domain (primary page), overlapped with
  GPU embedding — acceptable at single-digit ms/page.
- Rollout: adopt going forward. Already-completed parts keep their vectors (verify-and-skip);
  mixed extraction styles within a crawl are accepted and distinguishable by `source_run_id`.
- Alternative considered: `go-shiori/go-readability` (smaller deps, weaker extraction) — rejected;
  batch worker, binary size irrelevant.

### 3. Microdata org profiles — `github.com/iand/microdata`

- New `extract.ExtractProfileMicrodata(body []byte, pageURL string) (model.CompanyProfile, []model.Identifier)`
  mapping schema.org Organization/LocalBusiness items (reusing `isOrgType` on the itemtype URL)
  into the same structs as the JSON-LD path. `Source: "microdata"` on metadata rows and identifiers.
- Merge rule: JSON-LD wins; microdata fills an empty profile. Identifiers are unioned (dedup by value).
- Perf guard: skip entirely unless the raw body contains `itemtype`.

### 4. Phone normalization — `github.com/nyaruka/phonenumbers`

- At the contact-row boundary: parse `profile.Phone` with a region hint from the domain's ccTLD
  (2-letter TLD uppercased; generic TLDs → no hint, so only `+`-prefixed numbers parse).
  Valid → store E.164 (dedupes formatting variants). Invalid/unparseable → keep raw, trimmed.

## Testing

- TDD (red-green) per component; real-input fixtures: ISO-8859-1 page ("Müller"), microdata org
  page, DE national-format phone on a .de domain.
- Full `go test -race ./...` plus the existing wappalyzer-parity tests must stay green.

## Out of scope

- Embed client HTTP/context fixes, ShardStreamer commit crash edges (tracked from the 2026-07-12
  review, separate work).
- Re-embedding completed parts.
