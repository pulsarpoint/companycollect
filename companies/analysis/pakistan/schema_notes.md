# Pakistan Schema Notes

## Identifiers

- **CUIN (Company Universal Identification Number)** — SECP company identifier (registrar
  key); also the SECP registration number. Not reachable here (SECP firewalled).
- **NTN (National Tax Number)** — FBR tax identifier (ATL key).
- **PSX symbol** — listed-security/company ticker (listed companies only).

## PSX Data Portal — observed fields

### /symbols (JSON array)

| Field | Meaning |
|---|---|
| symbol | PSX ticker (e.g. OGDC, HBL, LUCK, ENGRO) |
| name | Listed company / security name |
| sectorName | PSX sector (e.g. COMMERCIAL BANKS, CEMENT, FERTILIZER) |
| isETF | Whether the symbol is an ETF |
| isDebt | Whether the symbol is a debt instrument (TFC/bond) |

1,068 symbols; 744 are non-debt/non-ETF equities.

### /company/{symbol} (HTML page)

Browser-public per-company page with sector, **registered address**, free float, shares
outstanding, and related disclosures (HTML; parse as needed).

## SECP eServices — fields (from public knowledge; firewalled here)

- CUIN, company name, company kind (e.g. private limited, public limited, SMC, LLP), status
  (active/dormant/dissolved), incorporation date, registered office address, directors.
  Directors are personal data — redact.

## FBR ATL — fields (per-NTN verification)

- NTN, registration number, name, ATL status (active/inactive), category. Covers companies
  and individuals (individuals are personal data).

## Formats, language, encoding

- Language: English (Pakistan business/registry language); company names in English. UTF-8.
- Dates: Gregorian (formats per source on capture).
- Currency: Pakistani Rupee (PKR) for any financials.

## Mapping to internal model

- company_id ← CUIN (SECP) / PSX symbol (listed) / NTN (tax)
- registration_number ← CUIN
- tax_id ← NTN
- legal_name ← PSX name / SECP company name
- status ← SECP status / FBR ATL status
- incorporation_date ← SECP incorporation date (firewalled here)
- registered_address ← PSX company page / SECP registered office
- activity_code ← PSX sectorName (listed); SECP company kind
- officers ← SECP directors (**redact**)
- source_url, source_name, source_retrieved_at preserved per record
