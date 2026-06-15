# Australia Company Profile — Mapping Report

Australia has an **open identity backbone** (ABR Bulk Extract, CC-BY 3.0 AU) but
**company-register detail and financials are paid (ASIC) or listed-only (ASX)**.
Everything keys on the **ABN** (= tax id); companies also carry an **ACN**.
Australia has **no separate VAT number** — GST registration is the flag.

## Mapping Table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.abn | abr_bulk_extract | ABR/ABN | ABN | ABR | id + tax id |
| registration.acn | abr_bulk_extract | ABR/ASICNumber | ACN | ABR | companies; join to ASIC |
| tax_identifiers.tax_id | abr_bulk_extract | ABR/ABN | ABN | derived | = ABN |
| tax_identifiers.gst_registered | abr_bulk_extract | ABR/GST/@status | ABN | ABR | no separate VAT |
| legal_identity.legal_name | abr_bulk_extract | NonIndividualNameText | — | ABR | |
| legal_identity.entity_type | abr_bulk_extract | EntityTypeText | — | ABR | PUB/PRV/… |
| legal_identity.trading_business_names | abr_bulk_extract | OtherEntity | — | ABR | TRD/BN |
| status.abn_status | abr_bulk_extract | ABR/ABN/@status | — | ABR | ACT/CAN |
| status.asic_company_status | asic_company_register | status | ACN | PLANNING-ONLY | paid; precise status |
| registered_location.state/postcode | abr_bulk_extract | BusinessAddress State/Postcode | — | ABR | no street |
| registered_location.registered_office_address | asic_company_register | registered office | ACN | PLANNING-ONLY | paid; full address |
| incorporation.incorporation_date | asic_company_register | registration date | ACN | PLANNING-ONLY | paid; not in ABR |
| officers[] | asic_company_register | officeholders | ACN | PLANNING-ONLY | paid; PII |
| financial_statements[] | asx_listed / asic_financial_reports | financial report | ACN/ABN (via ticker) | PLANNING-ONLY | listed (free) or paid |
| (enrichment) | abn_lookup_api | per-ABN lookup | ABN | free GUID | same fields, real-time |

## Source Precedence

1. **ABR Bulk Extract** — authoritative open identity (ABN/ACN, name, entity type,
   trading names, state/postcode, GST). CC-BY 3.0 AU, weekly.
2. **ABN Lookup API** — same data, per-ABN, real-time (free GUID).
3. **ASIC company register** — full address, incorporation date, precise status,
   officers → **paid** (planning-only).
4. **ASX** (listed) / **ASIC financial reports** (paid) — financials → planning-only.

## Join Keys

- **ABN** (11-digit) is the universal key + tax id; **ACN** (= ABR `ASICNumber`)
  joins companies to ASIC. `vat_id` is **not available** (GST flag instead).

## Missing / Restricted

- **Street address, incorporation date, ANZSIC, officers** — not in the open ABR;
  **paid ASIC**.
- **Financials** — listed-only (free ASX) or paid (ASIC); most small proprietary
  companies don't lodge.
- **Personal data** — sole-trader/individual names (ABR), officeholders (ASIC).
