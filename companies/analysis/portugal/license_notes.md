# Portugal — License & Terms Notes

> Portugal's per-company register data and financials are paid/restricted; the open data portals carry only
> statistical aggregates (CC-BY-SA / CC-BY); the free company-acts publication is reCAPTCHA-gated.

## Registo Comercial (certidão permanente)
- The commercial register is **public** but per-company data (NIPC, firma, sede, CAE, capital, sócios, gerência)
  is accessed via the **paid certidão permanente** (~EUR 25/year subscription) or per-document access codes.
- No open-data **licence** for reuse/redistribution of the register data is stated — **confirm terms** before
  redistribution. There is **no open bulk/API**.

## publicacoes.mj.pt (company acts)
- Company acts (constituições, alterações, dissoluções) are **published for free** and searchable by NIPC/firma.
  However the search is **reCAPTCHA-protected** — automated/bulk scraping is an access-control bypass; **do not**
  do it. Reuse terms of the published acts are not clearly stated.

## IES (Informação Empresarial Simplificada)
- Annual accounts (balanço + demonstração de resultados + anexo) are filed to **AT / INE / Banco de Portugal**
  (and the commercial register via the deposit of accounts). They are **not openly published** per company.
  Banco de Portugal publishes only **aggregate sector statistics**. No bulk redistribution; figures via the paid
  register or a vendor.

## dados.justica.gov.pt / dados.gov.pt
- Open **statistical** datasets (counts of incorporations/extinctions, registrations, insolvencies) under
  **CC-BY-SA** (most) or **CC-BY**. Reusable with attribution + share-alike, but **not** per-company data.

## RCBE (beneficial ownership)
- Restricted after the 2022 CJEU ruling (legitimate interest / obliged entities). **Not** open. Personal data
  (GDPR).

## AT / VIES
- Validates a supplied PT VAT/NIF (`PT` + NIPC). Validation/enrichment only; not redistributable as a list.

## Commercial aggregators
- Racius, Informa D&B/einforma, Iberinform resell the register + parsed IES financials under **commercial,
  per-vendor contracts** (Racius offers free basic search).

## Personal data / GDPR
- Sócios (shareholders), gerência/administração (officers) and beneficial owners are **personal data** — apply a
  GDPR lawful basis + retention before persisting; no direct-marketing reuse.

## Summary recommendation
- **Free (manual)**: publicacoes.mj.pt company acts + Racius basic search.
- **Paid**: certidão permanente (register); IES financials at scale via a vendor.
- **Blocked for automation**: publicacoes.mj.pt search (reCAPTCHA) — do not bypass.
- **Restricted**: RCBE beneficial ownership.
- **Open (CC-BY-SA, statistical only)**: dados.justica.gov.pt aggregate datasets (attribute + share-alike).
- Confirm register reuse terms; treat officer/shareholder/owner data under GDPR.
