# AADE RgWsPublic — tax registry company basic data web service Field Catalog

> **Planning-only / credentialed.** The SOAP service requires registered TaxisNet web-service credentials. Field
> model documented from the public service description; no records/values copied. No `sample_record.json`.

## Source Summary

- Country: Greece
- Source type: official_tax
- Organization: Ανεξάρτητη Αρχή Δημοσίων Εσόδων (AADE)
- URL: https://www1.aade.gr/webtax2/wsgsis/RgWsPublic/RgWsPublicPort (SOAP)
- License: restricted (registered TaxisNet credentials required)
- Access: restricted
- Freshness: real-time
- Record shape: SOAP response per ΑΦΜ (RgWsPublic2)
- Primary keys: `afm`
- Join keys: `afm`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| afm | afm | Tax id (9-digit) | string | identifier | (credentialed) | join key |
| onomasia | onomasia | Name | string | legal_name | (credentialed) | tax-side |
| postalAddress | postalAddress/… | Registered address | object | address | (credentialed) | |
| firmActivities[].kad | kad / firmActDescr | ΚΑΔ activities (primary flagged) | array | activity | (credentialed) | primary + secondary |
| doy | doyDescr | ΔΟΥ tax office | string | metadata | (credentialed) | |
| deactivationFlag | deactivationFlag | Active/ceased | string | status | (credentialed) | tax-side status |

## Interpretation Notes

- **Tax-side complement to GEMI, by ΑΦΜ.** RgWsPublic/RgWsPublic2 returns basic firm data (name, address, ΚΑΔ
  with the **primary activity flagged**, ΔΟΥ tax office, active/ceased status) for a given ΑΦΜ. Verified
  reachable (HTTP 200) but **requires registered TaxisNet web-service credentials** → planning-only, not open
  bulk; per-ΑΦΜ lookup.
- **Distinctive value:** the **primary ΚΑΔ** flag and tax-side status, which GEMI's portal does not cleanly
  expose. Join on ΑΦΜ.
