# Croatia — License & Terms Notes

> Croatia's core company + financial data is **open-licensed** (Otvorena dozvola) but behind a **free
> registration/account** (not payment). Record attribution.

## Sudski registar (Court Register) — Otvorena dozvola
- The Open Data API is published under the **Otvorena dozvola (Croatian Open Licence)** — free reuse,
  including commercial, **with attribution** to the Sudski registar / Ministarstvo pravosuđa.
- Access needs a **free registration** → Client ID/Secret + a **subscription key** (`Ocp-Apim-Subscription-Key`).
  Not anonymous; not paid. Respect any per-subscription rate limits.

## FINA RGFI (javna objava) — Otvorena dozvola
- The public-disclosure annual financial statements are available **free of charge** after **registration**,
  and the **balance sheet + income statement** are published in **machine-readable CSV** under the **Otvorena
  dozvola** (data.gov.hr dataset). Attribute FINA.
- **Coverage caveat**: the open CSV is highlighted for **micro/small** enterprises; **fuller / large-company
  data** may require FINA's **paid** RGFI products (cjenik). Confirm before relying on full coverage.

## data.gov.hr
- License is **per dataset** (the Sudski registar + RGFI datasets are **Otvorena dozvola**). Check each
  dataset's license field.

## Registar stvarnih vlasnika (RSV) — restricted
- Beneficial-ownership register (FINA). **Access conditions apply** (legitimate interest post-CJEU). Not
  open bulk.

## DZS
- Croatian Bureau of Statistics content is reusable with attribution; aggregate only.

## Commercial aggregators
- Proprietary, paid, per-vendor contract. They resell Sudski registar + FINA RGFI.

## Personal data / GDPR
- Register data includes natural persons (sole traders, board members, beneficial owners). Apply a GDPR
  lawful basis + retention policy before persisting personal data; honor the Otvorena dozvola terms.

## Summary recommendation
- **Free to ingest (with attribution)**: the **Sudski registar API** + the **FINA RGFI CSV**, both under the
  Otvorena dozvola — provision the **free registration** credentials (sudreg subscription key; FINA login).
- **Paid/restricted**: fuller FINA RGFI products; RSV beneficial ownership; commercial aggregators.
