# Malaysia — combined profile mapping

## Join keys & precedence

- **Primary join key: the SSM company registration number** — the new **12-digit**
  format (since 2019), with the old **NNNNNNN-A** as a cross-reference. **TIN**
  (LHDN) links tax; for listed companies the **Bursa stock code** is an additional
  key (announcements carry the SSM number to join back).
- **Precedence**: **SSM** (via e-Info / MyData-SSM) is authoritative for identity,
  status, activity, capital, directors, shareholders, and financials — but **paid**.
  **Bursa** covers the listed subset (public; WAF-blocked here).

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.registration_number_new | ssm_einfo | registration_number_new | reg_no | authoritative | paid; 12-digit |
| registration.registration_number_old | ssm_einfo | registration_number_old | reg_no | authoritative | legacy cross-ref |
| tax_identifiers.tin | lhdn | TIN | reg_no | authoritative | tax id |
| tax_identifiers.sst_number | lhdn | SST no | reg_no | authoritative | no VAT/GST |
| legal_identity.legal_name | ssm_einfo | company_name | reg_no | authoritative | Bursa name as alt |
| legal_identity.company_type | ssm_einfo | company_type | reg_no | authoritative | Sdn. Bhd./Bhd./PLT |
| status.status_text | ssm_einfo | status | reg_no | authoritative | Existing/Dissolved/... |
| status.incorporation_date | ssm_einfo | incorporation_date | reg_no | authoritative |  |
| activity.msic_code | ssm_einfo | nature_of_business | reg_no | authoritative | MSIC |
| registered_location.* | ssm_einfo | registered_address | reg_no | authoritative |  |
| capital.* | ssm_einfo | paid_up_capital | reg_no | authoritative (paid) | MYR |
| owners[] | ssm_einfo | shareholders | reg_no | authoritative (paid) | REDACT natural persons |
| officers[] | ssm_einfo | directors | reg_no | authoritative (paid) | REDACT (incl. NRIC) |
| listing.* | bursa_listed | stock_code/sector | stock_code/reg_no | authoritative (listed) | WAF here |
| financial_statements[] | ssm_einfo | Financial Comparison/Historical | reg_no | authoritative (paid) | MYR; Bursa for listed |

## Freshness

- SSM (e-Info/MyData): **live** (paid). Bursa: **quarterly** (WAF here).

## Missing-data notes

- **No open bulk register; no open financials** — SSM is paid; only a free e-Search
  (existence) is open.
- **data.gov.my has no company register** (DOSM statistics).
- **No separate VAT number** (SST; no VAT/GST since 2018).
- **Directors/shareholders** redacted as personal data (PDPA 2010).
- **No per-company values captured** (paywall/WAF not bypassed).
