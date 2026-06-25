# Kazakhstan Schema Notes

## Identifiers

- **BIN (Business Identification Number / БИН)** — 12-digit; the universal company identifier
  across `gbd_ul`, KGD, and government services. (Individuals use the **IIN / ИИН**, 12-digit.)
- **ISIN** — for KASE-listed securities (`KZxxxxxxxxxx`).

## data.egov.kz gbd_ul — fields (from the dataset description; API key-gated)

| Field (RU) | Meaning |
|---|---|
| БИН (bin) | 12-digit Business Identification Number — primary key |
| Наименование (name) | Legal-entity name (Russian; Kazakh variant where present) |
| Дата регистрации (registration_date) | Date of state registration |
| Юридический адрес (legal_address) | Registered (legal) address at registration |
| Вид деятельности / ОКЭД (oked_activity) | Economic activity (OKED — KZ economic-activity classifier) |
| ФИО руководителя (director_full_name) | Director's full name (**personal data — redact**) |

Covers legal entities, branches (филиалы), and representative offices (представительства).
Accessed via `https://data.egov.kz/api/v4/gbd_ul/<version>?apiKey=<free key>`.

## KGD — fields (taxpayer search / lists)

- BIN/IIN, taxpayer_name, VAT registration (registered/not), taxpayer_status (active /
  inactive / pseudo-enterprise / debtor). Browser-public per-BIN search and downloadable lists.

## KASE — fields (listed; SPA)

- issuer_name, ticker, ISIN (`KZxxxxxxxxxx`). Listed securities only.

## Formats, language, encoding

- Languages: Russian (primary) + Kazakh. UTF-8.
- Dates: Gregorian (format per API on capture).
- Currency: Kazakhstani Tenge (KZT) for any financials.

## Mapping to internal model

- company_id ← BIN
- registration_number ← BIN
- tax_id ← BIN (the BIN serves as the tax id; VAT status via KGD)
- legal_name ← gbd_ul name (RU) / KGD taxpayer_name
- status ← KGD taxpayer_status (gbd_ul is registration data; active/liquidated may need KGD)
- incorporation_date ← gbd_ul registration_date
- registered_address ← gbd_ul legal_address
- activity_code ← gbd_ul OKED
- officers ← gbd_ul director_full_name (**redact**)
- source_url, source_name, source_retrieved_at preserved per record
