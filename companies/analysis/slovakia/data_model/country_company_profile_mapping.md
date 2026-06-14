# Slovakia Company Profile — Mapping Report

Slovakia is **best-in-class fully-open**: two official free APIs joined on **IČO**.
**RPO** (Statistics Office, CC-BY 4.0) provides identity, officers, shareholders,
share capital, activities, predecessors, and history; **RÚZ** (Register of
Financial Statements, CC0) provides the accounting-unit master and **full
structured financial statements**. Both official, both free.

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.ico | ruz_accounting_units | ico | IČO | RÚZ/RPO agree | company id + join key |
| registration.rpo_entity_id | rpo | id | IČO | RPO | for entity lookups |
| tax_identifiers.dic | ruz_accounting_units | dic | IČO | RÚZ | tax id |
| tax_identifiers.vat_id | ruz_accounting_units | "SK"+dic | IČO | derived | if VAT-registered |
| legal_identity.legal_name | rpo | fullNames[current] | IČO | RPO > RÚZ nazovUJ | history available |
| legal_identity.legal_form | rpo | legalForms[current] | IČO | RPO | code 112 = s.r.o. |
| legal_identity.legal_form_code | ruz_accounting_units | pravnaForma | IČO | RÚZ | classifier |
| status.dissolution_date | ruz_accounting_units | datumZrusenia | IČO | RÚZ | best status signal |
| activity.sk_nace | ruz_accounting_units | skNace | IČO | RÚZ | coded |
| activity.main_activity | rpo | statisticalCodes.mainActivity | IČO | RPO | coded label |
| activity.business_activities[] | rpo | activities[] | IČO | RPO | free-text scope |
| incorporation.incorporation_date | rpo | establishment | IČO | RPO ≈ RÚZ datumZalozenia | |
| registered_location.* | rpo / ruz_accounting_units | addresses / ulica+mesto+psc, kraj, okres | IČO | RPO street; RÚZ region/district | |
| share_capital | rpo | equities[current] | IČO | RPO | EUR; registered |
| officers[] | rpo | statutoryBodies[] | IČO | RPO | OPEN but PII — redact |
| owners[] | rpo | stakeholders[] + deposits[] | IČO | RPO | OPEN but PII — redact |
| financial_statements[] | ruz_financial_reports | uctovny-vykaz tables via sablona | IČO (via idUJ) | RÚZ | structured; EUR; multi-year |
| predecessors[] | rpo | predecessors[] | IČO | RPO | mergers |

## Source Precedence

1. **RPO** — authoritative for identity, legal form, activities, **officers**,
   **shareholders**, share capital, predecessors, history. CC-BY 4.0.
2. **RÚZ accounting units** — authoritative for IČO↔DIČ, address, SK NACE, dates,
   region/district, and the link to financial statements. CC0.
3. **RÚZ financial reports** — authoritative for **financials** (structured
   tables decoded via templates). CC0.
4. **FinStat** — paid aggregator; only a proprietary rating adds value →
   planning-only. **ORSR** — web register superseded by RPO.

On name conflict, prefer **RPO** `fullNames` (with history) over RÚZ `nazovUJ`.

## Join Keys

- **IČO** (8-digit) joins RPO ↔ RÚZ. Within RÚZ, the accounting-unit `id` links to
  statements (`idUctovnychZavierok`) → reports (`idUctovnychVykazov`); a report's
  `idUJ` resolves back to the IČO.

## Missing / Restricted

- **Single status flag** — none; derive active from `datumZrusenia` / legal facts.
- **Officers / shareholders / deposits** — OPEN (RPO) but **personal data**;
  redact.
- **Financials decoding** — needs the template (`sablona`); some large filers
  expose only PDF (empty `obsah`).
- **Credit rating** — only via paid FinStat (planning-only).
