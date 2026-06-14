# Bulgaria Company Profile — Source Mapping

How each section of `country_company_profile.schema.json` is populated. **Bulgaria's defining trait: a
single clean key (EIK) joins an open-ish register to document-based financials** — registry openness is good
(CC-BY), but financials are public PDFs, not structured open.

## Identity / legal / activity / location / ownership (register)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| registration.eik | commercial_register / data_egov_bg | eik | **PK** | daily | free search / CC-BY | = VAT root |
| registration.vat_id | derived | 'BG'+eik | — | — | — | |
| legal_identity.name | commercial_register | наименование | — | daily | open-ish | Cyrillic (+Latin) |
| legal_identity.legal_form | commercial_register | правна форма | — | daily | open-ish | ЕООД/ООД/АД |
| status.* | commercial_register | статус | — | daily | open-ish | + acts |
| activity.object_of_activity | commercial_register | предмет на дейност | — | daily | open-ish | **free text, no КИД** |
| registered_location.* | commercial_register | седалище и адрес | — | daily | open-ish | parse oblast |
| capital.* | commercial_register | капитал | — | daily | open-ish | register capital (BGN→EUR) |
| officers[] | commercial_register | управители/съвет | eik | daily | open-ish · **PII** | **directors are open** |
| owners[] | commercial_register | съдружници/собственик | eik | daily | open-ish · **PII** | **share owners are open** |
| acts[] | data_egov_bg | publications | eik | daily | **CC-BY** | change stream |
| beneficial_owners[] | beneficial_ownership | declarations | eik | continuous | **restricted** | planning-only; sensitive PII |

## Financial statements (document-based)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| financial_statements[] (official) | gfo_financial_statements | баланс + ОПР (PDF) | eik | annual | public PDF | **parse/OCR**; no XBRL |
| financial_statements[] (structured) | companybook_bg | parsed financials | eik | annual | **paid** | structured 2022+ (convenience) |

### Financial precedence
1. **companybook_bg / APIS** (paid) — structured parsed financials at scale (avoids Cyrillic OCR).
2. **gfo_financial_statements** — official public PDFs; parse/OCR. No open structured option.
   Dedupe on `eik + fiscal_year`; revenue/employees nullable for micro/small; currency BGN (EUR from 2026).

## Join & precedence summary

- **Single clean key**: the **EIK** (= VAT root) joins the register, the CC-BY publications, the ГФО, and
  (restricted) beneficial ownership — **no fuzzy matching**.
- **Authority**: Commercial Register authoritative (free search / CC-BY publications / registered web service
  / agreement bulk); ГФО for financials (PDF); commercial providers for structured financials.
- **Build order**: accumulate the **CC-BY publications** (data_egov_bg) into an EIK-keyed master (or use the
  registered web service per EIK) → attach ГФО (parse PDFs, triggered by 'обявяване на ГФО' acts) or a
  provider → (beneficial ownership only with lawful access).
- **Normalization**: Cyrillic (+Latin translit); free-text activity (no КИД); BGN→EUR (2026); PDF OCR for financials.

## Missing / restricted data

- **Open structured financials**: none — ГФО are PDFs (parse) or a paid provider; no XBRL.
- **Coded activity (КИД/NACE)**: not in the register (предмет на дейност free text) — derive.
- **Full anonymous registry bulk**: needs a **data-sharing agreement**; the open path is the CC-BY change
  stream (accumulate).
- **Beneficial ownership**: restricted (planning-only). But **directors + share owners are OPEN** in the register.
- **PII**: managers, share owners, beneficial owners — GDPR.
