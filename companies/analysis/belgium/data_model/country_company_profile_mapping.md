# Belgium Company Profile — Source Mapping

How each section of `country_company_profile.schema.json` is populated. **Belgium's defining trait: a
single clean key (EnterpriseNumber) joining an open company master AND open structured financials** — among
the cleanest open setups analysed.

## Identity / legal / activity / location (open — KBO)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| registration.enterprise_number | kbo_open_data | enterprise.csv:EnterpriseNumber | **PK** | daily | open + free reg | = VAT root |
| registration.vat_id | derived | 'BE'+digits | — | — | — | |
| registration.type_of_enterprise | kbo_open_data | enterprise.csv:TypeOfEnterprise | — | daily | open | 1=person (PII), 2=entity |
| legal_identity.name | kbo_open_data | denomination.csv (Type=001) | — | daily | open | one language |
| legal_identity.legal_form | kbo_open_data | enterprise.csv:JuridicalForm | — | daily | open | code.csv |
| status.* | kbo_open_data | Status + JuridicalSituation | — | daily | open | code.csv; + Moniteur acts |
| activity.nace_main | kbo_open_data | activity.csv:NaceCode (MAIN) | — | daily | open | **clean NACE-BEL** |
| registered_location.* | kbo_open_data | address.csv | — | daily | open | NL/FR; region from zipcode |
| contact.* | kbo_open_data | contact.csv (WEB/EMAIL/TEL) | — | daily | open | website discovery |
| establishments[] | kbo_open_data | establishment.csv | EnterpriseNumber | daily | open | vestigingseenheden |
| acts[] | moniteur_belge | gazette publications | EnterpriseNumber | daily | free public | lifecycle events |

## Financial statements (open — NBB)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| financial_statements[] | nbb_cbso_financials | balans + resultatenrekening (XBRL) | **EnterpriseNumber** | annual | **open** (free account) | clean join; XBRL since 2007 |

### Financial precedence
- **Single open source**: `nbb_cbso_financials` (free Authentic Data XBRL). No paid tier needed for the
  as-filed data (Improved Data is paid; skip). Dedupe on `EnterpriseNumber + fiscal_year + schema`;
  `revenue`/`operating_result`/`net_income` **null** for micro/abbreviated schemas; scale nothing (EUR).

## Restricted / planning-only

| Profile path | Source | Join key | Access | Notes |
|---|---|---|---|---|
| beneficial_owners[] | ubo_register | EnterpriseNumber | **restricted** (legit. interest/fee) | planning-only; sensitive PII; not open |

## Join & precedence summary

- **Single clean key**: the **EnterpriseNumber** keys the KBO master, the NBB financials, the gazette acts,
  and UBO — so the whole profile assembles with **no fuzzy matching** (best-in-class, like Poland/France).
- **Authority**: KBO authoritative for identity/status/activity/establishments; NBB authoritative for
  financials; Moniteur for dated acts; UBO (restricted) for ownership.
- **Build order**: KBO bulk (spine) → NBB financials (join on EnterpriseNumber) → Moniteur acts → (UBO only
  with lawful access). Freshness: KBO daily, NBB annual, Moniteur daily.
- **Normalization**: KBO multi-file join + code.csv resolution; NBB Belgian-GAAP XBRL schema variants +
  yearly taxonomy versions; pick one denomination language; region from zipcode.

## Missing / restricted data — minimal

- Almost nothing is missing: identity, **financials**, activity, establishments, contact, and acts are all
  **open** and clean-joined.
- **Beneficial ownership (UBO)**: restricted (planning-only).
- **PII**: KBO natural persons (TypeOfEnterprise=1) — license forbids direct-marketing reuse; UBO sensitive.
- **Access**: both core sources need a **free registration/account** (not payment).
- **Financials nullability**: micro/abbreviated schemas omit the income statement (no revenue).
