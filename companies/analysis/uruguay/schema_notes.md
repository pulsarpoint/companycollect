# Uruguay schema notes

## Fields observed

- `Estado de la empresa` - approval/status.
- `RUT` - identifier and tax id.
- `Denominacion Social`, `Nombre comercial` - legal/trade names.
- `Tamano de la empresa` - size band.
- `Tipos de actividad de la empresa`, `Descripcion de la Actividad`.
- `Codigo CIIU principal`, `Descripcion Codigo CIIU principal`.
- establishment address, locality, department, postal code, coordinates.
- public email, website, phone.
- `Fecha de Registro`, `Fecha de vencimiento`.

## Mapping

- `registration_number` / `tax_id`: RUT
- `legal_name`: `Denominacion Social`
- `trade_name`: `Nombre comercial`
- `lifecycle_status`: `Estado de la empresa`
- `activity_code`: CIIU principal
- `registered_address`: establishment address fields
- contacts/websites: public email, phone, website if retained
