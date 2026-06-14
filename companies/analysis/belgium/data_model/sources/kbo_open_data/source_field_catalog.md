# KBO/BCE Open Data — Field Catalog

> **OPEN** free bulk CSV company master (full + daily update). Behind a **free registration + terms**
> (portal/SFTP) — not payment. Fields documented from the standard KBO open-data CSV set; no per-company
> record was downloadable here (registration required) → no sample.

## Source Summary

- Country: Belgium
- Source type: official_registry_bulk (the open spine)
- Organization: FOD Economie / SPF Économie (KBO / BCE)
- URL: https://economie.fgov.be/.../cbe-open-data ; portal https://kbopub.economie.fgov.be/kbo-open-data/login ; SFTP on request
- License: **Licence-BCE-Open-Data** (reuse allowed; **no direct marketing** with personal data)
- Access: public + **free registration**
- Freshness: daily (full + update files; kept 31 days)
- Record shape: **multi-file CSV** set joined on `EntityNumber` / `EnterpriseNumber`
- Primary keys: `EnterpriseNumber`
- Join keys: `EnterpriseNumber`, `EstablishmentNumber`, `EntityNumber`

## Fields (by file)

| File:Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| enterprise:EnterpriseNumber | EnterpriseNumber | Enterprise number (10) | string | identifier | **PK**; VAT root |
| enterprise:Status | Status | Status code | string | status | code.csv; AC=active |
| enterprise:JuridicalSituation | JuridicalSituation | Legal situation | string | status | code.csv |
| enterprise:TypeOfEnterprise | TypeOfEnterprise | natural person / legal entity | string | metadata | 1=person (PII), 2=entity |
| enterprise:JuridicalForm | JuridicalForm | Legal form | string | legal_form | code.csv (NV/SA, BV/SRL, VZW…) |
| enterprise:StartDate | StartDate | Incorporation date | date | date | dd-mm-yyyy |
| establishment:EstablishmentNumber | EstablishmentNumber | Establishment unit (10, starts 2) | string | identifier | join via EnterpriseNumber |
| denomination:Denomination | Denomination | Name | string | legal_name | multi-row (Language+Type) |
| denomination:TypeOfDenomination | TypeOfDenomination | social/abbr/commercial | string | metadata | 001=social name |
| denomination:Language | Language | NL/FR/DE | string | metadata | code.csv |
| address:* | Zipcode/Municipality/Street/HouseNumber | Address | object | address | NL/FR columns |
| activity:NaceCode | NaceCode | NACE-BEL code | string | activity | + NaceVersion |
| activity:Classification | Classification | MAIN/SECO/ANCI | string | activity | MAIN = primary |
| contact:Value | Value (TEL/EMAIL/WEB) | Contact | string | metadata | WEB → website |
| meta:* | SnapshotDate/ExtractType/Version | Snapshot metadata | object | metadata | full vs update |

## Interpretation Notes

- **The open spine** — a free bulk CSV covering all active enterprises + establishment units. Everything
  keys on the **EnterpriseNumber** (= VAT root, `BE` + 10 digits) — the same key the NBB financials use,
  so the company↔financials join is **clean** (no fuzzy matching).
- **Multi-file model**: `denomination`/`address`/`activity`/`contact` attach to an **EntityNumber** that is
  *either* an EnterpriseNumber or an EstablishmentNumber — resolve accordingly. Codes (Status, JuridicalForm,
  NACE…) resolve via **code.csv** (multilingual labels).
- **Multilingual**: names/addresses carry NL/FR/DE — pick one language consistently for the canonical name.
- **Activity is clean** (NACE-BEL, with MAIN/SECO/ANCI) — a real advantage vs DE/AT.
- **PII**: `TypeOfEnterprise=1` (natural persons / sole traders) — the license **forbids direct-marketing
  reuse** of personal data; apply GDPR.
- No `sample_record.json` (registration-gated; not downloaded). Structure is well documented (cookbook).
