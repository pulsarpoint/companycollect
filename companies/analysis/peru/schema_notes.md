# Peru schema notes

No full data sample was parsed in discovery because the main file is about 370 MB.
Expected fields from SUNAT Padron Reducido RUC documentation/page context:

- `RUC` - stable taxpayer/company identifier.
- legal or taxpayer name.
- taxpayer status.
- domicile/address condition.
- ubigeo and fiscal address fields.

## Mapping

- `registration_number` / `tax_id`: RUC
- `legal_name`: taxpayer/business name
- `lifecycle_status`: SUNAT taxpayer status
- `registered_address`: fiscal address fields

Parser must confirm delimiter, encoding, and exact headers after downloading.
