# Russia Company Profile — Source Mapping

> Keyed on **OGRN** (13-digit company id) + **INN** (10-digit tax id). Russia has
> VAT (НДС) but **no separate VAT number** — the INN is the tax id. Open identity +
> financials come from **GIR BO**; the open SME list from **RSMP**; the full
> authoritative register (EGRUL — directors/founders/capital/history) is **paid**.
> Directors/founders and ИП data are personal data (152-ФЗ).

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.ogrn | gir_bo | content[].ogrn | ogrn | live | open | Company id. |
| registration.inn | gir_bo | content[].inn | inn | live | open | Tax id. |
| registration.kpp | gir_bo | bfo[].organizationInfo.kpp | — | live | open | Reason code. |
| tax_identifiers.tax_id | gir_bo | content[].inn | — | live | open | = INN. |
| tax_identifiers.vat_id | — | — | — | — | not available | No separate VAT number. |
| legal_identity.short_name / full_name | gir_bo | shortName / fullName | — | live | open | |
| legal_identity.okopf_legal_form | gir_bo | okopf.name | — | live | open | OKOPF. |
| legal_identity.okfs_ownership | gir_bo | okfs.name | — | live | open | OKFS. |
| status.status_code | gir_bo | statusCode | — | live | open | |
| activity.okved_code | gir_bo | okved2 | — | live | open | ~NACE. |
| registered_location.* | gir_bo | region/city/street | — | live | open | |
| sme.category / headcount | rsmp_sme_register | КатСубМСП / ССЧР | inn/ogrn | monthly | open bulk | SME register. |
| financial_statements[] | gir_bo | bfo[] + statement forms | inn/ogrn | annual | open | Balance + income, RUB. |
| officers[] | egrul | directors_founders | ogrn/inn | live | paid / per-company | PLANNING-ONLY; personal data (152-ФЗ). |
| tax_enrichment.* | fns_opendata_taxinfo | sshr / taxregime | inn | annual | open | Per-INN enrichment. |

## Source precedence

1. **gir_bo** — open identity + financials (authoritative for filed accounts).
2. **rsmp_sme_register** — open SME company list (category, headcount, OKVED).
3. **fns_opendata_taxinfo** — per-INN open enrichment (headcount, taxes, regime).
4. **egrul** — authoritative directors/founders/capital/history; paid (per-company
   free). Planning-only.

Conflict rules:
- **Identity/financials**: GIR BO is authoritative for filers; banks are absent
  (use the Central Bank). **EGRUL** is authoritative for the full register but paid.
- **No VAT number** — never synthesize; the INN is the tax id.

## Join keys

- **OGRN** (13-digit) and **INN** (10-digit) across all sources. The INN is the tax
  id; there is no separate VAT number. KPP is the registration-reason code.

## Missing / restricted data

- **Directors / founders / capital / full history** — EGRUL (paid bulk / free
  per-company); personal data (152-ФЗ).
- **Bank financials** — not in GIR BO (Central Bank instead).
- **A separate VAT number** — Russia uses the INN.
