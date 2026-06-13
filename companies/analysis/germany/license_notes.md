# Germany — License & Terms Notes

> Public ≠ freely reusable. Confirm before redistribution or commercial use.

## OffeneRegister.de (the downloaded bulk)
- **Stated license:** Creative Commons Attribution 4.0 (CC-BY 4.0), per https://offeneregister.de/daten/.
  Attribution to **OpenCorporates** required ("from OpenCorporates", hyperlinked preferred).
- **Conflict to resolve:** The OpenSanctions mirror of the same dataset
  (https://www.opensanctions.org/datasets/de_offeneregister/) labels it
  **CC-BY-NC 4.0** (NonCommercial; commercial use needs a separate OpenSanctions data license).
- **Action:** Before any commercial use/redistribution, confirm directly with OffeneRegister /
  Open Knowledge Foundation Deutschland and OpenCorporates which terms apply. Treat as
  **NonCommercial until confirmed** to be safe.

## OpenSanctions de_offeneregister (FTM mirror)
- **License:** CC-BY-NC 4.0. Commercial use explicitly requires a separate OpenSanctions license.
- Attribution to OpenSanctions + OffeneRegister/OpenCorporates.

## Handelsregister (handelsregister.de)
- Viewing free since 2022-08-01. **No bulk/API.** Terms of use **forbid automated mass retrieval**;
  community guidance is ≤60 lookups/hour, and mass automated querying may breach
  **§§303a/303b StGB** (data tampering / computer sabotage). Do **not** bulk-scrape.

## Unternehmensregister
- Basic data free; full documents (financial statements, shareholder lists) carry a per-document
  fee (~€1). No bulk redistribution rights implied; documents are individually licensed/paid.

## BRIS / EU e-Justice
- Official EU service, single-company lookups. No bulk export; reuse governed by the portal terms.

## GovData.de
- License is **per dataset** (often DL-DE/CC variants). Check each dataset's license field
  individually before reuse.

## Financial statements (Unternehmensregister / Bundesanzeiger)
- Since DiRUG (2022-08-01), disclosed annual financial statements are **free to view** without fee or
  registration (FY≥2022 on Unternehmensregister; FY<2022 on Bundesanzeiger).
- **Free viewing does NOT grant reuse/redistribution rights.** The statements are official disclosures
  under §325 HGB; portal terms restrict automated mass retrieval. Confirm reuse rights before storing/
  redistributing financial data extracted from the portals.
- No bulk download and no free retrieval API; the official "Massendatenschnittstelle" is for
  *submitting* statements, not bulk retrieval.

## bundesanzeiger Python tool (bundesAPI/deutschland)
- The **tool** is open source (Apache-2.0). The **data** it retrieves remains Bundesanzeiger content
  subject to portal terms — per-company use only; do not mass-scrape. It solves the search captcha via
  a bundled ML model, so treat usage as automation the portal terms may restrict.

## Commercial APIs
- Proprietary, paid, per-vendor contract. Redistribution typically prohibited without a license.
- Financial-data vendors (OpenRegister, North Data, Implisense, Creditreform, Dun & Bradstreet) license
  derived/structured financials under their own terms — redistribution and storage limits per contract.

## Summary recommendation
- Safe to **explore/analyze internally** with the OffeneRegister bulk now.
- **Do not redistribute or use commercially** until the CC-BY vs CC-BY-NC ambiguity is resolved.
- Never bulk-scrape the official portal.
