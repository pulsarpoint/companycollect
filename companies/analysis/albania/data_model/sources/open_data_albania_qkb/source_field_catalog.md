# Open Corporates Albania (QKB open data) Field Catalog

## Source Summary

- Country: Albania
- Source type: official_registry (open mirror)
- Organization: Open Data Albania / AIS (republishing QKB)
- URL: https://opencorporates.al/sq/company/
- License: Open Data Albania (open, attribution)
- Access: public, no key
- Freshness: periodic (mirrors QKB)
- Record shape: company lists + per-company pages keyed by NIPT
- Primary keys: nipt
- Join keys: nipt

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| company.nipt | NIPT/NUIS | Unique id | string | identifier | L67508702G | = tax id = VAT id |
| company.emri | Emri | Name | string | legal_name | NEXUS GROUP | |
| company.forma_ligjore | Forma ligjore | Legal form | string | legal_form | Sh.p.k. | Ltd/JSC/sole trader |
| company.administrator | Administrator | Director | string | person | | **PERSONAL DATA — redact** |
| company.ortake | Ortakë | Owners | array | ownership | | **PERSONAL DATA** |
| company.kapitali | Kapitali | Capital (ALL) | decimal | financial | | |
| company.objekti | Objekti | Activity | string | activity | | free text |
| company.statusi | Statusi | Status | string | status | Aktiv | |
| company.former_names | ish ... | Former names | array | legal_name | | |

## Interpretation Notes

- **Verified from real data**: the company list yielded **4,459 NIPTs** + names
  (NEXUS GROUP `L67508702G`, MALESIA TRAVEL `L61307015S`, MEDIAL `L61306025U`).
- **NIPT/NUIS** (letter+8digits+letter) is the company id, the **tax id**, and the
  **VAT id** (Albania has VAT; the NIPT serves as the VAT number).
- **Legal forms**: Sh.p.k. (Ltd), Sh.a. (JSC), Person Fizik (sole trader), Degë
  (branch). **Status**: Aktiv/Pasiv/Çregjistruar.
- **Personal data**: administrator + owners (ortakë) are personal data (Albanian
  Law 9887 / GDPR-aligned) — **redact**. The committed sample redacts the admin.
- Capital/financials in **ALL (Lek)**. Currency dates `DD.MM.YYYY`.
