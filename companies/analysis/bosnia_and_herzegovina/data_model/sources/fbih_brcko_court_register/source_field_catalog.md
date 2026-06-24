# Registar poslovnih subjekata — FBiH & Brčko (bizreg.pravosudje.ba) Field Catalog

## Source Summary

- Country: Bosnia and Herzegovina (Federation of BiH + Brčko District)
- Source type: official_registry
- Organization: High Judicial and Prosecutorial Council (VSTV/HJPC) — entity courts
- URL: https://bizreg.pravosudje.ba/
- License: public per-company; no open bulk
- Access: public per-company search (Oracle APEX, app 183)
- Freshness: live register
- Record shape: Oracle APEX HTML per-company search results
- Primary keys: JIB
- Join keys: JIB

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| naziv | Naziv / Firma | Business name | string | legal_name |  | from APEX results |
| jib | JIB | 13-digit unique id | string | identifier |  | join key; FBiH starts 42 |
| mbs | MBS / broj uloška | Court registration number | string | identifier |  |  |
| sjediste | Sjedište | Registered seat | string | address |  |  |
| djelatnost | Djelatnost | Activity (KD BiH ~NACE) | string | activity |  |  |
| status | Status | Registration status | string | status |  |  |
| sud | Nadležni sud | Competent court | string | metadata |  | FBiH canton / Brčko |

## Interpretation Notes

- This is the central portal for the **Federation of BiH** cantonal court
  registers and the **Brčko District** register — the complement to the RS system
  (`bizreg.esrpska.com`). Together they cover all of BiH.
- It is an **Oracle APEX** application (app 183, "Registar poslovnih subjekata").
  Search is per company by **Naziv / JIB / MBS**. Result/detail pages are
  **session-bound APEX URLs**; **no JSON API or bulk export** was found.
- Field paths above are **logical** (the labels shown by the portal), not a
  stable machine schema — hence `source_confidence: medium`. A production
  implementation must confirm the exact APEX request/response shape, which is why
  the handoff is marked `insufficient_transport_info`.
- **Join key** is JIB (same 13-digit id as RS), so FBiH/Brčko records merge with
  RS records and financial/tax sources on JIB.
- Officers/founders shown in detail pages are **personal data** when natural
  persons — redact.
