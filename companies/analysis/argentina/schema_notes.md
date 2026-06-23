# Argentina schema notes

## Fields observed

- `cuit` - tax/company id; primary key.
- `razon_social` - legal name.
- `fecha_hora_contrato_social` - incorporation/contract timestamp.
- `tipo_societario` - legal form.
- `fecha_hora_actualizacion` - update timestamp.
- `numero_inscripcion` - registration number when present.
- `dom_fiscal_*`, `dom_legal_*` - fiscal/legal address components.
- `actividad_codigo`, `actividad_descripcion` - activity fields.

## Mapping

- `registration_number`: CUIT, with `numero_inscripcion` retained as extra
- `tax_id`: CUIT
- `legal_name`: `razon_social`
- `company_type`: `tipo_societario`
- `incorporation_date`: `fecha_hora_contrato_social`
- `registered_address`: legal address components
