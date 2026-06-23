# Chile schema notes

## RES fields observed

- `ID` - RES internal event id.
- `RUT` - Chile company/tax identifier; primary join key.
- `Razon Social` - legal name.
- `Fecha de actuacion`, `Fecha de registro`, `Fecha de aprobacion x SII` - event dates.
- `Codigo de sociedad` - legal form code, e.g. `SpA`.
- `Tipo de actuacion` - event type, e.g. constitucion.
- `Capital` - registered capital.
- `Comuna Social`, `Region Social` - registered location.

## Mapping

- `registration_number` / `tax_id`: `RUT`
- `legal_name`: `Razon Social`
- `company_type`: `Codigo de sociedad`
- `incorporation_date`: first suitable date from event/registration fields
- `registered_address`: commune + region only from RES; enrich from SII addresses
