# Hong Kong Company Profile — Mapping

Hong Kong has **two identifiers** and a split between an **open incremental feed** and an
**authoritative paid full register**. The **Companies Registry open data** (RNC063 weekly
CSVs on data.gov.hk) is the open layer, keyed on the **BR Number** with names + dates and
**no personal data**. The **ICRIS e-Search** full register (keyed on the **CR Company
Number**) adds type, status, registered office, directors, and charges — but is
**interactive / pay-per-use** (planning-only). **HKEX** covers listed stocks (browser-public;
static xlsx is a template).

## Identifiers

- **BR Number** — IRD Business Registration number, 8-digit; the **open-feed** key; de-facto
  business/tax id.
- **CR Company Number** — Companies Registry key (ICRIS, paid).
- **Stock Code** / **ISIN** — HKEX listed-security key (listed only).

## Mapping table

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.br_number | cr_open_data_newly_registered | BR Number | yes | open feed | IRD; primary open key |
| registration.cr_company_number | icris_esearch | cr_company_number | yes | ICRIS (paid) | registry key |
| legal_identity.legal_name_en | cr_open_data_newly_registered | Current Company Name in English | no | open feed > ICRIS | |
| legal_identity.legal_name_zh | cr_open_data_newly_registered | Current Company Name in Chinese | no | open feed | often empty |
| legal_identity.company_type | icris_esearch | company_type | no | ICRIS (paid) | planning-only |
| status.status_text | cr_open_data_newly_registered | (derived) | no | open feed / ICRIS | event vs ICRIS status |
| status.incorporation_date | cr_open_data_newly_registered | Date of Incorporation / Date of Registration | no | open feed | DD-MM-YYYY→ISO |
| status.name_change_date | cr_open_data_newly_registered | Date of Change of name | no | open feed | |
| registered_location.registered_office_address | icris_esearch | registered_office_address | no | ICRIS (paid) | planning-only |
| officers[] | icris_esearch | directors / company_secretary | no | ICRIS (paid) | **PERSONAL DATA — REDACT** |
| listing.stock_code | hkex_securities | stock_code | no | HKEX | listed only; template |
| listing.isin | hkex_securities | isin | no | HKEX | listed only |
| source_provenance[] | all | n/a | n/a | n/a | per-section provenance |

## Precedence and joins

- **Open identity (name, BR Number, incorporation/registration dates)**: from the **CR open
  data** — authoritative and free. **Full particulars (CR Company Number, type, status,
  registered office, directors, charges)**: from **ICRIS** (paid, planning-only).
- **Join**: the open feed (BR Number) and ICRIS (CR Company Number) join by **company name /
  BR Number**; HKEX joins to the register by **name** (HKEX publishes neither HK identifier).
- **Dates**: open feed `DD-MM-YYYY` → ISO 8601. **Currency** HKD. **Language** English +
  Traditional Chinese.

## Missing / restricted

- The **open feed is incremental** (new/changed entries) and **company-level only** — no
  registered address, status detail, or officers. Those require **ICRIS (paid)**.
- **Directors / company secretary** are personal data under the **PDPO** — redact; only
  available via ICRIS.
- **HKEX** populated securities list is not cleanly available via the static xlsx (template;
  populated server-side).
- No VAT id in HK — the **BR Number** is the business id.
