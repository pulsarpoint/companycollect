# Търговски регистър (Commercial Register) — Field Catalog

> The authoritative Bulgarian company register (Registry Agency). **Open-ish**: free public search (single
> lookups), an official **web service** (registration for integration), and **CC-BY daily publications** on
> data.egov.bg; a **full bulk** needs a data-sharing agreement. Fields from the documented public search /
> publications; no per-company open record was downloadable here → no sample.

## Source Summary

- Country: Bulgaria
- Source type: official_registry (authoritative spine)
- Organization: Агенция по вписванията (Registry Agency, Ministry of Justice)
- URL: https://portal.registryagency.bg/CR/en ; open data via data.egov.bg
- License: free public search; **CC-BY** daily publications; full bulk by agreement
- Access: public (search) / registered (web service) / agreement (bulk)
- Freshness: authoritative / daily publications
- Record shape: per-company entry; daily publication stream
- Primary keys: `eik`
- Join keys: `eik`

## Fields

| Path | Source field (BG) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| eik | ЕИК | Unified Identification Code (9/13) | string | identifier | **PK**; VAT root |
| naimenovanie | наименование/фирма | Legal name | string | legal_name | Cyrillic + Latin translit |
| pravna_forma | правна форма | Legal form | string | legal_form | ЕООД/ООД/АД/ЕТ |
| status | статус | вписано/заличено/ликвидация | string | status | |
| sedalishte_adres | седалище и адрес | Registered seat/address | string | address | parse oblast |
| predmet_na_deynost | предмет на дейност | Object of activity | string | activity | **free text, no КИД** |
| kapital | капитал | Registered capital | decimal | financial | register capital (BGN→EUR 2026) |
| upraviteli | управители/съвет | Managers/board | array | person | **PII** |
| sobstvenitsi | съдружници/собственик | Partners/sole owner | array | ownership | **PII**; share ownership |
| data_na_vpisvane | дата на вписване | Registration date | date | date | incorporation |
| deystviya | вписвания/обявявания | Registered acts | array | filing | the CC-BY publication stream |

## Interpretation Notes

- **The authoritative spine.** Everything keys on the **ЕИК** (= VAT root, `BG` + EIK). Unlike Belgium's
  KBO open data, the Bulgarian register **does** carry **managers/board and capital partners** (PII) — an
  open ownership/officers signal (share ownership; distinct from the beneficial-ownership register).
- **Open access model**: single-company **public search** is free; **bulk** for a commercial database needs
  a **data-sharing agreement**; the **CC-BY daily publications** (data.egov.bg) are a **change stream** —
  accumulate them (keyed on EIK) to build/maintain a master.
- **No coded activity**: `предмет на дейност` is **free text** (КИД/NACE not reliably coded) — derive if
  needed. Language is **Cyrillic** (keep Latin transliteration for matching).
- **PII**: managers + partners are natural persons — GDPR; honor the register's reuse terms.
- No `sample_record.json` (data.egov.bg WAF-blocked; web service registration). Field structure is well
  documented from the public search.
