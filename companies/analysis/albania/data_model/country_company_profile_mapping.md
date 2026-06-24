# Albania Company Profile — Source Mapping

> Keyed on the **NIPT/NUIS** (letter+8digits+letter) = company id = tax id = VAT id
> (Albania has VAT; the NIPT serves as the VAT number). The QKB register is open via
> **Open Data Albania**; QKB provides the official per-company extract + financials
> (bilanci, ALL). Administrator/owners are personal data (Law 9887 / GDPR).

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| registration.nipt | open_data_albania_qkb | company.nipt | nipt | periodic | open | Company id. |
| tax_identifiers.tax_id / vat_id | open_data_albania_qkb | company.nipt | — | periodic | open | = NIPT. |
| legal_identity.legal_name | open_data_albania_qkb | company.emri | — | periodic | open | |
| legal_identity.legal_form | open_data_albania_qkb | company.forma_ligjore | — | periodic | open | Sh.p.k./Sh.a./Person Fizik. |
| legal_identity.former_names | open_data_albania_qkb | company.former_names | — | periodic | open | "ish". |
| status.status | open_data_albania_qkb | company.statusi | — | periodic | open | Aktiv/Pasiv/Çregjistruar. |
| incorporation.registration_date | qkb_registry | ekstrakt.data_regjistrimit | nipt | live | official | QKB extract. |
| activity.activity_text | open_data_albania_qkb | company.objekti | — | periodic | open | free text. |
| capital.registered_capital_all | open_data_albania_qkb | company.kapitali | — | periodic | open | ALL. |
| officers[] | open_data_albania_qkb | company.administrator/ortake | nipt | periodic | open | PERSONAL DATA — redact. |
| financial_statements[] | qkb_registry | ekstrakt.bilanci | nipt | annual | official | ALL; per-company. |

## Source precedence

1. **open_data_albania_qkb** — open QKB register data (identity, status, activity,
   owners). Primary open source (verified 4,459 companies).
2. **qkb_registry** — official per-company extract + financial statements
   (authoritative; per-company, no open bulk).

## Join keys

- **NIPT/NUIS** across both sources; it is the company id, the tax id, and the VAT
  id.

## Missing / restricted data

- **Clean open bulk** financials — none (bilanci filed with QKB per-company).
- **Administrator / owners** — open but personal data (Law 9887 / GDPR), redact.
- No separate VAT number — the NIPT is the VAT id.
