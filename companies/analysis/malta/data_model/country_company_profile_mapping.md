# Malta Company Profile — Mapping Report

Join on the **registration number** across sources. Free MBR search gives identity + status; officers,
shareholders and financials are in the register but **paid** (documents / API); registry portals are WAF-blocked
for automation; VAT and beneficial ownership are separate/restricted.

| Profile path | Source | Source path | Join key | Freshness | Access/License | Precedence | Notes |
|---|---|---|---|---|---|---|---|
| registration.registration_number | mbr_register | registration_number | self | real-time | public (manual) | authoritative | prefix = entity class |
| tax_identifiers.vat_id | vies_vat | vatNumber | (name match) | real-time | public / validation | external | MT+8; not in register |
| legal_identity.legal_name | mbr_register | name | registration_number | real-time | public (manual) | authoritative | |
| legal_identity.company_type | mbr_register | company_type | registration_number | real-time | public (manual) | authoritative | Ltd/plc/partnership |
| status.value | mbr_register | status | registration_number | real-time | public (manual) | authoritative | Active/Struck off/Liquidated |
| activity.activity_code | — | not_available | — | — | — | none | no public NACE in free register |
| incorporation.incorporation_date | mbr_register | registration_date | registration_number | real-time | public (manual) | authoritative | |
| registered_location.* | mbr_register | registered_address | registration_number | real-time | public (manual) | authoritative | parse locality |
| officers[] | mbr_register / mbr_api / commercial_aggregators | officers[] | registration_number | real-time | **paid** | planning-only | **PII (GDPR)** |
| shareholders[] | mbr_register / mbr_api / commercial_aggregators | shareholders[] | registration_number | real-time | **paid** | planning-only | registered owners; **PII** |
| financial_statements[] | mbr_annual_accounts | balance_sheet / P&L (PDF) | registration_number | annual | **paid** | planning-only | OCR; EUR; IFRS/GAPSME |
| financial_statements[] (alt) | mbr_api / commercial_aggregators | financialInformation / company.financials[] | registration_number | real-time/vendor | **paid** | planning-only | structured + bulk |
| beneficial_owners[] | rbe_register | beneficial_owners[] | registration_number | continuous | **restricted** | planning-only | **PII (GDPR)** |

## Precedence Rules

1. **MBR is authoritative** for identity, type, status, incorporation, registered office (free), and for the
   deeper register data — officers, shareholders, financial information — but those are **paid** (documents or
   the paid API), and the registry portals are **WAF-blocked** for automation (do not bypass).
2. **Single key:** registration number (prefix = entity class). **VAT** = `MT` + 8 digits, separate (VIES; no
   open crosswalk).
3. **Financials**: from the **paid** annual accounts (PDF, OCR) or the **paid MBR API** / a **commercial
   provider** (structured, bulk).
4. **Beneficial ownership (UBO)** is **restricted** (legitimate interest, post-CJEU) — planning-only; distinct
   from registered shareholders.
5. **data.gov.mt** is not the register (WAF-blocked, statistics/other).

## Missing-Data Notes

- **No open bulk** company/financials export; **registry portals WAF-blocked** (no free API).
- **Officers/shareholders/financials are paid** (documents or the paid MBR API).
- **No NACE/activity code** in the free register data (`activity_code` = not_available); **VAT/employee count**
  not in the register.
- **Beneficial ownership (UBO)** restricted (legitimate interest).
- **GDPR**: officers, shareholders and beneficial owners are personal data.
- **License**: MBR reuse/redistribution terms unclear — confirm before redistribution.
