# Mauritius Schema Notes

## Identifiers

- **BRN (Business Registration Number)** — the CBRD/CBRIS company/business identifier; the
  basis of the entity's identity with the MRA (tax). The key for the full register. **Not**
  present in the open ICT directory.
- (Listed) SEM issuer identity is by company name / segment (Official Market, DEM).

## data.govmu.org — List of ICT Companies (open CSV) — observed fields

| Field | Meaning |
|---|---|
| Title | Company name (e.g. "A CHAMROO LTD") |
| Address | Street address (may be "Not Available") |
| District | District / region (e.g. Plaine Wilhems, Port Louis) |
| Sectors | ICT sector(s); newline-separated within the cell |
| Other Related Sectors | Additional related sectors / free text |

- 1,060 rows; encoding **Windows-1252 (cp1252)**, not UTF-8. Sectoral (ICT) directory; no
  identifiers, status, or dates. No personal data.

## CBRD CBRIS — fields (from public knowledge; search Turnstile-gated)

- company_name, **BRN**, company_type (e.g. private company limited by shares, sole trader,
  société, foundation), company_status (live / removed / defunct), incorporation/registration
  date, registered office address, directors, shareholders. Directors/shareholders are
  personal data — redact. Documents (constitution, annual return, financials) are paid.

## SEM — fields

- issuer_name, market_segment (Official Market / DEM), published_accounts (PDF), company
  announcements. Listed companies only; browser-public.

## Formats, language, encoding

- Language: English (Mauritius official business language). ICT CSV is cp1252.
- Dates: Gregorian (formats to be confirmed per source on capture).
- Currency: Mauritian Rupee (MUR) for any financials (SEM published accounts).

## Mapping to internal model

- company_id ← BRN (CBRD); for the open ICT directory there is no id (name+address only)
- registration_number ← BRN
- tax_id ← BRN-based MRA tax identity (not in open data)
- legal_name ← Title (ICT CSV) / company_name (CBRD)
- status ← CBRD company_status (not in open data)
- incorporation_date ← CBRD registration date (not in open data)
- registered_address ← Address/District (ICT) / registered office (CBRD)
- activity_code ← Sectors (ICT directory; free-text, not a code list)
- officers/owners ← CBRD directors/shareholders (**redact**)
- source_url, source_name, source_retrieved_at preserved per record
