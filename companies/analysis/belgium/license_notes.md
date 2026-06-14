# Belgium — License & Terms Notes

> Belgium's core company + financial data is **open and free**, but behind a **free registration/account**
> (not payment). Record attribution per source.

## KBO/BCE Open Data (FOD/SPF Economie) — open, free registration
- Free **bulk CSV** reuse under the **Licence-BCE-Open-Data** (Conditions d'utilisation). Reuse permitted,
  including commercially, **but personal data may NOT be reused for direct marketing**. Attribution to the
  Crossroads Bank for Enterprises (KBO/BCE) expected.
- Access needs a **free registration + terms acceptance** (portal login or SFTP). Not anonymous; not paid.

## NBB Central Balance Sheet Office — free (Authentic Data); Improved paid
- Annual accounts are made **available free of charge** to the public. The **Authentic Data** (as-filed
  XBRL/PDF/CSV) is **free**; the **Improved Data** (NBB-rectified) is a **paid** service. Web-service access
  needs a **free developer account** (CLIENT_ID). Terms of use apply; attribute the NBB/BNB.
- CONSULT (consult.cbso.nbb.be) is free per-entity.

## KBO Public Search
- Free **web** search. The **Public Search Web Service** API is **paid** (~€50 / 2000 requests).

## Free third-party REST mirrors (cbeapi.be, ...)
- Convenience JSON over the KBO open data; **free tier with an API key**. The underlying data remains KBO
  open data under the Licence-BCE-Open-Data — same attribution + no-direct-marketing condition.

## UBO register (beneficial ownership)
- **Restricted** (MyMinfin; legitimate interest / authorities / obliged entities; fee). **Not** open.

## Moniteur Belge / Belgisch Staatsblad
- Official gazette; free public consultation. Reuse of published acts permitted; check the e-justice terms.

## Commercial aggregators
- Proprietary, paid, per-vendor contract. They resell KBO + NBB data with added scores/ratios.

## Personal data / GDPR
- KBO data includes natural persons (e.g. self-employed, directors); the license **forbids direct-marketing
  reuse**. UBO beneficial owners are natural persons (restricted). Apply a GDPR lawful basis + retention
  policy before persisting personal data.

## Summary recommendation
- **Free to ingest (with attribution + no direct marketing)**: KBO Open Data (registration) + NBB CBSO
  Authentic Data (free account). The open company master + structured financials cover the bulk of needs.
- **Paid/restricted**: KBO Public Search web service, NBB Improved Data, UBO, commercial aggregators.
