# APR Automated Data Delivery Web-Service Field Catalog

> **PLANNING-ONLY / PAID.** Cataloged from public APR service descriptions only.
> Free for state bodies; paid (contract) for banks/businesses. No raw records or
> values retrieved. Fields below are the concepts the **open data omits**.

## Source Summary

- Country: Serbia
- Source type: official_registry
- Organization: Agencija za privredne registre (APR)
- URL: https://www.apr.gov.rs/usluge/epodaci-na-zahtev-korisnika/automatizovano-izdavanje-podataka-(veb-servis).2413.html
- License: contract / terms of use
- Access: restricted (authentication + payment; contract; contact apr-podaci@apr.gov.rs)
- Freshness: real-time / continuous
- Record shape: planning-only
- Primary keys: `maticni_broj`
- Join keys: `maticni_broj`, `pib`

## Fields (concepts from public docs)

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| pib | PIB | Tax id (9-digit) | string | identifier | planning-only; the key open gap |
| preduzetnici[] | preduzetnici | Sole traders | array | identifier | planning-only; not in open data |
| zakonski_zastupnici[] | SP3: Zakonski zastupnici | Legal/statutory representatives | array | person/relationship | planning-only; paid; personal data |
| ostali_zastupnici[] | SP4: Ostali zastupnici / prokuristi / odbori | Other representatives, procurists and boards | array | person/relationship | planning-only; paid; personal data |

## Interpretation Notes

- This paid status-register web service is the route to **PIB**, **sole traders**
  and **representatives**. Beneficial ownership is deliberately excluded from
  this catalog because it is a separate APR CEV source and contract. APR
  explicitly says financial-statement-register data is not
  available through this status-register web service.
- Manual inspection of one public company record confirmed the representative
  concepts `name`, `function_title`, masked `JMBG` availability and
  `represents_independently`. These are semantic targets only; the SP3/SP4
  transport field names remain unknown.
- Keep entirely **planning-only**; do not synthesise values. Free for state
  bodies, otherwise per contract.
