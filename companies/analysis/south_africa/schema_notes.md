# South Africa — Schema Notes

## Identifiers

- **Company registration number** (CIPC) — format `YYYY/NNNNNN/NN`
  (`2015/123456/07`): 4-digit registration year + 6-digit serial + 2-digit **type
  suffix**:
  - `/07` private company ((Pty) Ltd)
  - `/06` public company (Ltd)
  - `/08` non-profit company (NPC)
  - `/23` external company (foreign)
  - `/24` co-operative · `/10` incorporated ((Inc) / personal liability) · etc.
  The authoritative id, but only in the **paid** CIPC registry.
- **Income tax number** (SARS) — 10-digit. **VAT number** — 10-digit starting with
  `4`. South Africa has **VAT**; neither is openly published.
- **CSD number** — the Central Supplier Database registration id (gated).
- The open **OCDS** data keys suppliers on **name** (`legalName`) only.

## eTenders OCDS release (verified)

| Path | Meaning |
|---|---|
| ocid | Open Contracting ID (release key) |
| id / date / tag | Release id, date, tag (planning/tender/award/contract/compiled) |
| tender.title / tender.value / tender.procurementMethod | The tender |
| planning | Planning info (budget, etc.) |
| parties[] | Each party: `name`, `roles` (supplier / buyer / procuringEntity / tenderer), `identifier` = `{legalName}` |
| buyer.name | Procuring entity (gov dept / municipality / SOE) |
| awards[].suppliers[].name | Awarded supplier **company name** |
| awards[].value.amount / .currency | Award value (ZAR) |
| awards[].date | Award date |
| contracts[] | Contract details |

- Currency **ZAR**. JSON via the OCDS API (no key); paginated by
  `PageNumber`/`PageSize` + `dateFrom`/`dateTo`.
- **No registration number** on the supplier in the sampled releases.

## CIPC registry (paid) — fields

registration_number, enterprise_name, company_status (In Business / Deregistered /
In Liquidation / Final Deregistration / …), company_type, registration_date,
directors (names + ID), registered_address, postal_address, and annual financial
statements (AFS, iXBRL).

## Financials

Private companies: **not public** (CIPC AFS/iXBRL, paid). Listed companies:
**JSE/SENS** results / annual reports (PDF/HTML), ZAR.

## Dates, money, encoding

- Dates: ISO `YYYY-MM-DD` / datetime (OCDS).
- Money: **ZAR**.
- Encoding: UTF-8 JSON (OCDS).

## Internal model mapping

```text
company_id          <- registration_number (CIPC; paid) — NOT in OCDS
registration_number <- YYYY/NNNNNN/NN (CIPC; paid)
tax_id              <- SARS income tax number (not open)
vat_id              <- SARS VAT number (10-digit, starts 4; not open)
legal_name          <- OCDS awards[].suppliers[].name (open) / CIPC enterprise_name (paid)
status              <- OCDS: awarded supplier (procurement) ; CIPC company_status (paid)
company_type        <- CIPC company_type (paid; inferable from reg-number suffix)
incorporation_date  <- CIPC registration_date (paid)
registered_address  <- CIPC (paid) — not in OCDS
activity            <- OCDS tender title / classification (procurement context)
financials          <- JSE/SENS (listed) ; CIPC AFS (paid)
officers            <- CIPC directors (paid; personal data, POPIA)
procurement         <- OCDS awards/contracts (open; ZAR, buyer, value)
```
