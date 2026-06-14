# CEIDG (sole proprietors) — Field Catalog

> **OPEN** register of **individual entrepreneurs** (jednoosobowa działalność gospodarcza) — the
> population the KRS does NOT cover. API v3 needs a **free token**. Cataloged from public API docs (not
> exercised: token required). Records are natural persons → **PII/GDPR**.

## Source Summary

- Country: Poland
- Source type: official_registry_api (sole traders)
- Organization: Ministerstwo Rozwoju i Technologii
- URL: https://dane.biznes.gov.pl/api/ceidg/v3
- License: open; **free token** required
- Access: public with free token
- Freshness: daily
- Record shape: paginated `firma[]`
- Primary keys: CEIDG `id`, `nip`
- Join keys: `nip`, `regon`

## Fields

| Path | Source field (PL) | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| firma[].nazwa | nazwa | Business name | string | legal_name | embeds owner name |
| firma[].wlasciciel.imie/nazwisko | imię/nazwisko | Owner name | string | person | **PII** |
| firma[].wlasciciel.nip | nip | Tax id | string | identifier | VAT=PL+NIP |
| firma[].wlasciciel.regon | regon | REGON | string | identifier | |
| firma[].status | status | AKTYWNY/ZAWIESZONY/WYKREŚLONY | string | status | suspension common |
| firma[].dataRozpoczecia | dataRozpoczęcia | Start date | date | date | |
| firma[].adresDzialalnosci | adres działalności | Business address | object | address | may be restricted |
| firma[].pkd | pkd | PKD codes | array | activity | |
| firma[].dataZawieszenia | dataZawieszenia/Wznowienia | Suspension dates | date | date | |
| firma[].id | id | CEIDG id | string | identifier | stable key |

## Interpretation Notes

- **Completeness layer**: KRS covers companies; CEIDG covers the **millions of sole proprietors**. Combine
  both for full coverage. Sole traders have **no KRS number** — key on NIP/REGON/CEIDG id.
- **PII**: a sole trader is a natural person — name (and often address) are personal data → GDPR lawful
  basis + retention; honor any address-hiding flags.
- **Status nuance**: `ZAWIESZONY` (suspended) is common and distinct from active/closed.
- **Token**: API v3 requires a free token (`CEIDG_TOKEN`); no sample retrieved here (auth not bypassed).
