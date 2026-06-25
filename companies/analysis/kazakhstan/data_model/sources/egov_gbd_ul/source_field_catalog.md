# data.egov.kz — State Database of Legal Entities (gbd_ul) Field Catalog

## Source Summary

- Country: Kazakhstan
- Source type: official_registry
- Organization: Open Data Portal of the Republic of Kazakhstan (data.egov.kz)
- URL: https://data.egov.kz/datasets/view?index=gbd_ul
- License: data.egov.kz open-data terms (free API key required)
- Access: **public open API; FREE API key required** (registration)
- Freshness: periodic
- Record shape: JSON/XML/CSV array of legal-entity objects (API-key-gated)
- Primary keys: bin
- Join keys: bin

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| bin | БИН | 12-digit Business Identification Number | string | identifier |  | primary + join key |
| name | Наименование | Entity name (RU/KZ) | string | legal_name |  | incl. branches/rep offices |
| registration_date | Дата регистрации | Registration date | date | date |  | Gregorian |
| legal_address | Юридический адрес | Legal address | string | address |  | at registration |
| oked_activity | Вид деятельности / ОКЭД | Economic activity | string | activity |  | OKED classifier |
| director_full_name | ФИО руководителя | Director full name | string | person |  | **PERSONAL DATA — redact** |

## Interpretation Notes

- `gbd_ul` (ГБД ЮЛ — **State Database of Legal Entities**) is the **authoritative open
  register** of Kazakhstani legal entities, branches, and representative offices. Per the
  dataset description it carries **BIN, name, registration date, legal address, activity
  (OKED), and director's full name**.
- **Access**: via the data.egov.kz API
  (`https://data.egov.kz/api/v4/gbd_ul/<version>?apiKey=<key>`). **Verified**: the API returns
  **HTTP 403 `{"error":"API key is required"}`** without a key — a **free API key**
  (registration on data.egov.kz) is required. No key-less bulk mirror was found, and **no
  values were captured** here. Confirm the current `<version>` string on the dataset page.
- **Identifier**: the **BIN (12-digit Business Identification Number)** is the primary key and
  the universal join key (to KGD). **OKED** is the KZ economic-activity classifier (analogue
  of NACE/ISIC). **Language**: Russian (primary) + Kazakh.
- **Personal data**: the **director's full name** is a natural person under Kazakhstan's Law
  on Personal Data — redact.
- No `sample_record.json`: data is API-key-gated and was not captured.
