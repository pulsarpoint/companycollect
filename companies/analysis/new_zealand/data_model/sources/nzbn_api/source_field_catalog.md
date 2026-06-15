# NZBN API (New Zealand Business Number) Field Catalog

> **PLANNING-ONLY for field values.** The NZBN API gateway returns HTTP **401
> "missing subscription key"** without a free subscription key. The schema below
> is from the public NZBN API v5 documentation — no records were fetched without a
> key. The returned public data is free to reuse (Crown copyright).

## Source Summary

- Country: New Zealand
- Source type: official_registry
- Organization: Companies Office / MBIE (api.business.govt.nz)
- URL: https://api.business.govt.nz/gateway/nzbn/v5/entities
- License: Crown copyright; publicly available NZBN data, reusable
- Access: public with a free subscription key (OAuth / Ocp-Apim-Subscription-Key)
- Freshness: live register
- Record shape: JSON entity object keyed by `nzbn`
- Primary keys: `nzbn`
- Join keys: `nzbn`, `sourceRegisterUniqueIdentifier` (company number)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| nzbn | nzbn | 13-digit NZBN (GLN) | string | identifier | key; starts 9429 |
| entityName | entityName | Entity name | string | legal_name | |
| entityTypeCode/Description | entityTypeCode | Entity type | string | legal_form | LTD/sole trader/partnership/trust/gov |
| entityStatusCode/Description | entityStatusCode | Status | string | status | Registered/Removed/In liquidation |
| registrationDate | registrationDate | Registration date | date | date | ISO |
| sourceRegister | sourceRegister | Originating register | string | metadata | COMPANIES / IR / … |
| sourceRegisterUniqueIdentifier | sourceRegisterUniqueIdentifier | Id in source register | string | identifier | = company number for companies |
| addresses.addressList[] | addresses | Reg/service/postal addresses | array | address | addressType + address1-4 + postCode |
| tradingNames[] | tradingNames | Trading names | array | legal_name | start/end dates |
| emailAddresses[] / phoneNumbers[] | contacts | Contact details | array | raw_extension | may be personal data — redact |
| websites[] | websites | Websites | array | raw_extension | |
| industryClassifications[] | industryClassifications | ANZSIC code + desc | array | activity | ANZSIC 2006 |
| companyDetails | companyDetails | Company block | object | metadata | company number, NZSX, insolvency flags |

## Interpretation Notes

- **NZBN** (13-digit GS1 GLN, starts `9429`) is the universal identifier for every
  NZ business entity and the join key across all NZ registers. Keep as a string.
- **Company number**: for companies (`sourceRegister=COMPANIES`) the Companies
  Register company number is exposed as `sourceRegisterUniqueIdentifier` and inside
  `companyDetails` — the join key to the Companies Register.
- **No tax id**: the **IRD number** and **GST number** are not in the public tier.
  NZ has **GST, not VAT** — no VAT number exists.
- **Restricted elements**: `gstNumbers` and `roles` (directors/office holders) are
  not in the publicly available tier; they need elevated authorisation and are
  personal data (Privacy Act 2020).
- **Industry**: ANZSIC 2006 classification.
- No raw sample record (key-gated source); the combined example is schematic.
