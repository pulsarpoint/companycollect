# bundesanzeiger (bundesAPI `deutschland`) — Field Catalog

> Free, **per-company**, unofficial. The tool (`Bundesanzeiger().get_reports("<name>")`) queries the
> **public Bundesanzeiger search**, solving its captcha with a bundled ML model, and returns a dict of
> report title → content. **Captcha/rate-limited — for targeted enrichment, NOT bulk.** No financial
> values are copied here.

## Source Summary

- Country: Germany
- Source type: community_tool (unofficial)
- Organization: bundesAPI community
- URL: https://github.com/bundesAPI/deutschland
- License: tool **Apache-2.0**; retrieved data is Bundesanzeiger content subject to portal terms
- Access: public, free; effectively rate-limited (captcha per query)
- Freshness: live
- Record shape: `dict { report_title: report_content }`
- Primary keys: company name (query) + report_title

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| `<report_title>` | dict key | Report title (encodes period) | string | filing | `Jahresabschluss zum Geschäftsjahr vom 01.01.2020 bis zum 31.12.2020` | Filter to Jahresabschluss/Bilanz |
| `<report_title>.content` | dict value | Report full-text/HTML | string | document | — | **Not XBRL** — extract numbers |
| query.company_name | get_reports() arg | Query name | string | identifier | `Deutsche Bahn AG` | Name-based; disambiguate |
| report_type | derived | Report kind | string | filing | `Jahresabschluss` | Derived from title |
| fiscal_period | parsed | Period from title | string | date | `2020-01-01..2020-12-31` | Parsed from German text |

## Interpretation Notes

- **Output is documents, not figures.** `get_reports()` returns report **text/HTML**, not structured
  numbers. To populate `financial_statements[]` you must run an HTML/table extractor (and ideally an
  XBRL fetch where available) over the content to derive `total_assets`, `equity`, `revenue`,
  `net_income`, etc. This is why it sits *behind* the financial schema rather than satisfying it directly.
- **Per-company and captcha-gated.** It queries the public search and solves the captcha with a bundled
  ML model — so it is inherently **not a bulk source**. Use it to enrich a bounded set of target
  companies, throttled, respecting portal terms; do not loop it over the whole population.
- **Matching by name.** The basic API takes a company **name**, not a register number — common names are
  ambiguous. Disambiguate against the registry spine (seat/court) before attaching results.
- **Legal posture.** The tool is open source; the *data* remains Bundesanzeiger content. Treat usage as
  automation the portal terms may restrict, and keep volume low.
- No `sample_record.json`: output carries live Bundesanzeiger report content; none retrieved, and a real
  capture could include third-party report text — omitted deliberately.
