# Sudski registar (Court Register) — Field Catalog

> The authoritative Croatian company register. **Open** REST/OpenAPI under the **Otvorena dozvola**, behind
> a **free registration** (Client ID/Secret + `Ocp-Apim-Subscription-Key`). Fields from the documented
> OpenAPI; no per-company record was downloadable here (key required) → no sample.

## Source Summary

- Country: Croatia
- Source type: official_registry_api (the open spine)
- Organization: Ministarstvo pravosuđa (Ministry of Justice)
- URL: https://sudreg-data.gov.hr ; docs https://sudreg-podaci.pravosudje.hr/docs/services ; web search https://sudreg.pravosudje.hr
- License: **Otvorena dozvola** (Croatian Open Licence)
- Access: public + **free registration** (subscription key)
- Freshness: authoritative / continuous
- Record shape: JSON per subjekt (query by `tipIdentifikatora` oib|mbs + `identifikator`)
- Primary keys: `mbs`
- Join keys: `oib`, `mbs`

## Fields

| Path | Source field (HR) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| mbs | mbs | Court register number | string | identifier | **PK** |
| oib | oib | Tax id (11) | string | identifier | **VAT root** (HR+OIB); universal join |
| tvrtka/naziv | tvrtka | Legal name | string | legal_name | |
| pravni_oblik | pravni_oblik | Legal form | string | legal_form | d.o.o./j.d.o.o./d.d./obrt |
| status | status | aktivan/likvidacija/brisan | string | status | |
| nadlezni_sud | nadlezni_sud | Competent court | string | metadata | |
| sjediste | sjediste | Registered seat | string | geography | → municipality |
| adresa | adresa | Address | object | address | |
| temeljni_kapital | temeljni_kapital | Share capital | decimal | financial | register capital; EUR since 2023 |
| predmet_poslovanja | predmet_poslovanja | Activities | array | activity | NKD where coded |
| osobe | osobe | Persons (members/board) | array | person | **PII**; open officers + owners |
| datum_osnivanja | datum_osnivanja | Registration date | date | date | incorporation |

## Interpretation Notes

- **The open spine.** Everything keys on the **OIB** (= VAT root, `HR` + OIB) and/or **MBS** (court register
  number) — the same OIB key the FINA RGFI financials and the RSV beneficial-ownership register use, so the
  company↔financials join is **clean** (no fuzzy matching).
- **Officers AND owners are open** here: `osobe` carries both **members/partners (owners)** and **management
  board/directors** — richer than e.g. Belgium's KBO open data (which omits directors). PII → GDPR.
- **Activity**: `predmet_poslovanja` carries **NKD** (Croatian NACE) codes where present (partly free text).
- **Access**: open-licensed but **free registration** for the subscription key (Oracle APIM gateway); the
  free **web search** (sudreg.pravosudje.hr) gives the same data for single lookups.
- **Currency**: `temeljni_kapital` is **EUR since 2023-01-01** (HRK before).
- No `sample_record.json` (subscription key required; not downloaded). Structure from the OpenAPI docs.
