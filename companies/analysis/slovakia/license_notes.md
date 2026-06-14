# Slovakia — License Notes

## RPO — Register of Legal Entities (Statistics Office)

- **License: CC-BY 4.0** (Creative Commons Attribution 4.0 International), declared
  by Štatistický úrad SR under Act No. 272/2015 Coll. on the register of legal
  entities. The license text is returned **inline** in each API response
  (`license` field).
- **Attribution required**: credit "Štatistický úrad Slovenskej republiky (RPO)"
  and retain the source + retrieval date.
- **PERSONAL DATA (GDPR)**: `statutoryBodies` (directors/Konateľ), `stakeholders`
  (shareholders/Spoločník), and `deposits` (per-person capital contributions)
  contain **person names and addresses**. Even though public, processing is
  subject to GDPR — redact/minimise person-level fields in published outputs and
  have a lawful basis before persisting officer identities.
- Company-level fields (name, IČO, legal form, activities, share-capital totals)
  are not personal data for legal persons.

## RÚZ — Register of Financial Statements

- **License: CC0** (Public Domain Dedication) — "Všetky dáta v rámci RÚZ Open API
  sú zverejňované pod licenciou CC0". No attribution legally required (but good
  practice to credit the source).
- Financial statements of accounting units are public by law. Free Open API;
  pagination capped at `max-zaznamov` ≤ 10,000 per page. Be polite (sequential
  crawl, incremental via `zmenene-od`).
- Attachments (`prilohy`) are PDFs; some carry the same data as the structured
  tables. Respect the per-attachment access flag (`pristupnostDat`).

## ORSR (commercial register web portal)

- License unclear; data is already available via the RPO API under CC-BY 4.0, so
  prefer RPO. Do not scrape ORSR aggressively.

## FinStat / aggregators

- **Commercial / restricted** (paid API). Redistribution governed by their terms;
  they add little authoritative over the free RPO + RÚZ. Planning-only.

## Summary

- **Open & usable**: RPO (CC-BY 4.0, attribute) and RÚZ (CC0). Both official, free.
- **Personal-data caution**: RPO officers/shareholders/deposits (GDPR) — redact.
- **Restricted**: FinStat (paid). ORSR (web only; prefer RPO).
