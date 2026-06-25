# Ghana — combined profile mapping

## Join keys & precedence

- **Primary join key: company registration number** (ORC). **TIN** (GRA) links tax;
  for listed companies the **GSE ticker** is an additional key (join to the ORC by
  company name).
- **Precedence**: **ORC** (eServices, paid) is authoritative for corporate identity,
  status, capital, directors, shareholders, and annual returns — but not open (and
  firewalled here). **GSE** is authoritative for the **listed** subset (open).

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.registration_number | orc_eservices | registration_number | reg_no | authoritative | eServices/paid |
| tax_identifiers.tin | orc_eservices | tin | reg_no | authoritative | GRA |
| tax_identifiers.vat_id | n/a | — | — | n/a | VAT tied to the TIN |
| legal_identity.legal_name | orc_eservices | company_name | reg_no | authoritative | GSE name as alt (listed) |
| legal_identity.company_type | orc_eservices | company_type | reg_no | authoritative | Ltd/PLC/by guarantee/external |
| status.status_text | orc_eservices | status | reg_no | authoritative | Active/Dissolved/Struck off |
| status.incorporation_date | orc_eservices | incorporation_date | reg_no | authoritative |  |
| activity.nature_of_business | orc_eservices | nature_of_business | reg_no | authoritative | ORC |
| activity.gse_sector | gse_listed | sector | ticker | authoritative (listed) | open |
| registered_location.registered_address | orc_eservices | registered_address | reg_no | authoritative | paid |
| capital.stated_capital | orc_eservices | stated_capital | reg_no | authoritative (paid) | GHS |
| owners[] | orc_eservices | shareholders | reg_no | authoritative (paid) | REDACT (Act 843) |
| officers[] | orc_eservices | directors | reg_no | authoritative (paid) | REDACT |
| listing.* | gse_listed | ticker/sector | ticker | authoritative (listed) | OPEN |
| financial_statements[] | gse_listed / orc_eservices | financials / annual returns | ticker/reg_no | authoritative | GSE open (listed); ORC paid |

## Freshness

- GSE: **event-driven/quarterly** (open). ORC: **live** (eServices/paid; firewalled here).

## Missing-data notes

- **No open bulk corporate register; no open private financials** — ORC paid/gated;
  only the GSE (listed) is open.
- **data.gov.gh firewalled** — no company dataset confirmed.
- **No separate VAT number** (VAT tied to the TIN).
- **Directors/shareholders** redacted as personal data (Act 843; may include Ghana
  Card PIN).
- **No ORC per-company values captured** (login/paywall not bypassed; hosts
  firewalled); listed data from the GSE.
