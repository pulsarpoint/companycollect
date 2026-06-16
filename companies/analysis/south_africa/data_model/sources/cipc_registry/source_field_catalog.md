# CIPC Company Register Field Catalog

> **PLANNING-ONLY / PAID.** The authoritative South African company register
> (CIPC). Company search, disclosures, directors, and annual financial statements
> are sold **per-transaction** via CIPC eServices (customer code + fees). No open
> bulk/API; BizPortal is mainly a registration service. Cataloged from public
> documentation only — no records fetched. Director data is personal data (POPIA).

## Source Summary

- Country: South Africa
- Source type: official_registry
- Organization: CIPC
- URL: https://eservices.cipc.co.za/
- License: restricted (paid)
- Access: paid per-transaction (customer code + fees)
- Freshness: live register
- Record shape: per-company disclosure + AFS
- Primary keys: `registration_number`
- Join keys: `registration_number`, `enterprise_name`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| company.registration_number | Registration number | YYYY/NNNNNN/NN | string | identifier | authoritative id |
| company.enterprise_name | Enterprise name | Company name | string | legal_name | |
| company.company_status | Company status | Status | string | status | In Business / Deregistered / In Liquidation |
| company.company_type | Company type | Legal form | string | legal_form | Private/Public/NPC/External/Co-op |
| company.registration_date | Registration date | Incorporation date | date | date | |
| company.registered_address | Registered address | Address | string | address | |
| company.directors | Directors | Directors (name + ID) | array | person | **PERSONAL DATA (POPIA)** |
| company.annual_financial_statements | AFS (iXBRL) | Financial statements | array | financial | paid; ZAR |

## Interpretation Notes

- The **registration number** (`YYYY/NNNNNN/NN`) is the authoritative company id;
  the `/NN` suffix encodes type (`/07` private (Pty) Ltd, `/06` public, `/08` NPC,
  `/23` external, `/24` co-op, `/10` personal-liability, …).
- This is the **only** source of company **status, type, registration date,
  directors, registered address, and AFS** — none openly available.
- **Directors** are personal data (POPIA) — redact in any output.
- **Financials** (AFS, iXBRL) are filed with CIPC but **paid**; for listed
  companies use JSE/SENS instead.
- **Access**: per-transaction fees via CIPC eServices. No open bulk/API. No raw
  sample record (paid source). The open OCDS data has **no registration number**,
  so linking is name-based.
