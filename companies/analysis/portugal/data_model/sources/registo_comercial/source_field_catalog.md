# Registo Comercial — certidão permanente (IRN) Field Catalog

> **Paid.** Per-company register data is accessed via the paid certidão permanente. Field model documented from
> the register; no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Portugal
- Source type: official_registry
- Organization: Instituto dos Registos e do Notariado (IRN) / Ministério da Justiça
- URL: https://eportugal.gov.pt/servicos/aceder-a-certidao-permanente-de-registo-comercial
- License: public register (paid access; reuse terms unclear)
- Access: paid (certidão permanente ~EUR 25/year; or access codes)
- Freshness: real-time
- Record shape: company certificate (HTML/PDF)
- Primary keys: `nipc`
- Join keys: `nipc`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| nipc | NIPC | 9-digit company id (=NIF) | string | identifier | (paid) | join key; VAT = PT+NIPC |
| firma | firma / denominação | Legal name | string | legal_name | (paid) | |
| natureza_juridica | natureza jurídica | Legal form | string | legal_form | (paid) | Lda./S.A./Unipessoal |
| estado | estado | Status | string | status | (paid) | ativa/dissolvida/insolvente |
| sede | sede | Registered office | string | address | (paid) | |
| cae | CAE | Activity code | string | activity | (paid) | CAE Rev.3 |
| objeto | objeto | Corporate purpose | string | activity | (paid) | free text |
| capital_social | capital social | Share capital | decimal | financial | (paid) | EUR |
| socios[] | sócios | Shareholders | array | ownership | (paid) | **PII** |
| gerencia[] | gerência / administração | Officers | array | person | (paid) | **PII** |

## Interpretation Notes

- **Authoritative register, paid.** The Registo Comercial holds the full per-company identity (NIPC, firma,
  natureza jurídica, estado, sede, CAE, objeto, **capital social**, **sócios**, **gerência**), but per-company
  data is accessed via the **paid certidão permanente** (~EUR 25/year subscription) or per-document access codes.
  There is **no open bulk/API**.
- **Identifiers.** **NIPC** (9 digits) is the company id and **equals the NIF** (tax number); **VAT** = `PT` +
  NIPC. **CAE Rev.3** = activity code.
- **Registered shareholders (sócios) are in the register** (paid) — distinct from beneficial owners (restricted
  RCBE). **GDPR** applies to sócios and gerência. **License** terms for reuse are not clearly stated.
