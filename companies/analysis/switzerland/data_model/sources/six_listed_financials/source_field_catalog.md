# SIX Swiss Exchange — Listed-Company Financials Field Catalog

> **DOCUMENTED-ONLY / LISTED ISSUERS ONLY.** The only broadly-open route to Swiss
> financials — but it covers only the few hundred **listed** companies. Private
> companies have **no public filing obligation**. Cataloged from public docs.

## Source Summary

- Country: Switzerland
- Source type: stock_exchange
- Organization: SIX Group
- URL: SIX official publications / issuer IR pages
- License: issuer publications (open to view; redistribution per SIX/issuer terms)
- Access: public
- Freshness: annual (IFA) + interim (IFI)
- Record shape: issuer reports (XBRL / PDF)
- Primary keys: `isin`
- Join keys: `uid`, `isin`

## Fields (documented)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| balance_sheet | balance sheet | Balance sheet | object | financial | listed only; XBRL/PDF |
| income_statement | income statement | Income statement | object | financial | listed only |
| fiscal_year | reporting period | Year/period | integer | date | annual + interim |
| isin | ISIN | Security id | string | identifier | link to UID via name |

## Interpretation Notes

- Use for **listed companies only**. Format varies by issuer (some XBRL, mostly
  PDF). Link the ISIN/issuer to the Zefix **UID** via name/register.
- For the ~99% private universe there is **no open financial source** — this is a
  structural gap for Switzerland, not a sourcing failure.
