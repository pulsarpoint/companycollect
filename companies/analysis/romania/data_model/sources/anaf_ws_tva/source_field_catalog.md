# ANAF VAT / fiscal-info web service (ws/tva) Field Catalog

> **Documented, not verified live this run.** Every version path tested (v5–v9)
> returned HTTP 404 — the service appears to have been version-bumped/moved.
> Fields below come from the official ANAF documentation (doc_WS_V5.txt). No
> `sample_record.json`. Confirm the current endpoint before implementation.

## Source Summary

- Country: Romania
- Source type: official_tax
- Organization: ANAF
- URL: `POST https://webservicesp.anaf.ro/PlatitorTvaRest/api/v5/ws/tva` (documented; current path unconfirmed)
- License: public information
- Access: public (no auth/payment)
- Freshness: real-time fiscal status
- Record shape: JSON; body `[{"cui":N,"data":"YYYY-MM-DD"}]`, up to 500 CUIs/request
- Primary keys: `cui`
- Join keys: `cui`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| …cui | cui | Fiscal code | integer | identifier | join to OD_FIRME.CUI |
| …denumire | denumire | Name | string | legal_name | |
| …adresa | adresa | Fiscal address | string | address | |
| …scpTVA | scpTVA | VAT-registered? | boolean | status | confirms RO+CUI active |
| …statusInactivi | statusInactivi | Fiscally inactive? | boolean | status | |
| …statusTvaIncasare | statusTvaIncasare | Cash-VAT status | boolean | status | |
| …nrRegCom | nrRegCom | Trade-register number | string | identifier | ↔ COD_INMATRICULARE |
| …iban | iban | IBAN | string | raw_extension | often blank |

## Interpretation Notes

- This is an **enrichment** service for **current fiscal/VAT status** (is the
  company VAT-registered, fiscally inactive, on cash-VAT, split-VAT). The ONRC
  register CSV already supplies the company master data, so this is optional.
- Limits: **max 500 CUIs per request, max 1 request/second**.
- Because the live endpoint 404'd here, mark as **useful_secondary_source /
  documented**; the Go implementation must first resolve the current version path
  from the ANAF web-services page before relying on it.
