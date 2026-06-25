# Qatar Schema Notes

## Identifiers

- **CR number** — Commercial Registration number issued by **MoCI**; the primary onshore
  company identifier. Authoritative but not openly downloadable.
- **QFC Number** — identifier for **QFC-licensed firms** (financial centre); from the QFC
  Public Register. Distinct from the onshore CR number.
- **Establishment / Tax card number** — Qatar issues an establishment/tax identification via
  the General Tax Authority (Dhareeba); not an open register.
- **QSE ticker symbol** + **ISIN** (`QA…`) — for listed companies (Qatar Stock Exchange).

## QFC Public Register — observed fields (from page table headers)

- Firm Name
- QFC Number
- Senior Executive Function
- Approved Individual: Full Name, Address  (**personal data**)
- Date Of Registration
- QFCA Licensed (status)
- Registered Insolvency Practitioner / Registered Official Liquidator (separate registers)

Record shape: ASP.NET GridView, populated by **search postback** (empty on plain GET).

## MoCI Commercial Registration — fields (from public knowledge)

- CR number, establishment/trade name (Arabic + English), legal form, activities, status,
  capital, owners/partners, manager. Lookup-only; no per-company values captured.

## QSE listed — fields

- Company Name, Symbol, ISIN, Sector, Financial Statements, Disclosures. Browser-public
  Liferay portal; portlet AJAX, no clean open JSON API. Listed only.

## Formats, language, encoding

- Languages: Arabic and English (bilingual). UTF-8.
- Dates: Gregorian (QFC register uses Gregorian "Date Of Registration").
- Currency: Qatari Riyal (QAR) for capital and financials.

## Mapping to internal model

- company_id ← CR number (onshore) / QFC Number (financial centre) / symbol (listed)
- registration_number ← CR number / QFC Number
- legal_name ← Firm Name / establishment name / listed company name
- status ← QFCA Licensed / MoCI status
- incorporation_date ← Date Of Registration (QFC) / CR issue date (MoCI)
- financials ← QSE financial statements (listed only, QAR)
- officers/owners ← approved individuals (QFC) / owners-partners-manager (MoCI) — **redact**
- source_url, source_name, source_retrieved_at preserved per record
