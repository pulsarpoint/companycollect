# Company data sources for Mauritius

## Status

- Official bulk data: partial (open sectoral ICT-companies CSV; no full register)
- Official API: open-data CKAN API works (no register dataset); CBRD search has no open API
- Open data portal: found (data.govmu.org, CKAN)
- License: CC-BY-SA-4.0 (ICT dataset); restricted (CBRD); public disclosure (SEM)
- Recommended ingestion path: bulk (open ICT CSV) + browser/manual for the full register

## Best source

The authoritative register is the **CBRD CBRIS Online Search**
(`onlinesearch.mns.global`), keyed on the **BRN (Business Registration Number)** — but its
search is **Cloudflare Turnstile-gated** (CAPTCHA) and document purchase is paid, so it is
not openly downloadable. The genuinely **open** company data is a **sectoral directory**:
`data.govmu.org` (national CKAN portal) publishes **"List of ICT Companies in Mauritius"**
(CSV, **CC-BY-SA-4.0**, 1,060 rows: name, address, district, sectors) — open but ICT-only,
with no identifiers. **SEM** covers listed companies (browser-public).

## Next action

Use the open ICT-companies CSV directly (CC-BY-SA-4.0) as a sectoral seed list. For the full
register (BRN, status, directors), use CBRD CBRIS via the browser (Turnstile; documents
paid) — do not bypass Turnstile. Use SEM for listed companies. Monitor data.govmu.org for
any future register dataset. Redact directors/shareholders (personal data).
