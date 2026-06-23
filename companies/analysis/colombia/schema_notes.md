# Colombia schema notes

## Fields observed

- `codigo_camara`, `camara_comercio` - chamber identifiers.
- `matricula` - chamber registration number.
- `razon_social` - legal name / merchant name.
- `nit`, `digito_verificacion` - Colombian tax id and check digit.
- `cod_ciiu_act_econ_pri`, `cod_ciiu_act_econ_sec`, `ciiu3`, `ciiu4` - activities.
- `fecha_matricula`, `fecha_renovacion`, `fecha_cancelacion` - dates in `YYYYMMDD`.
- `organizacion_juridica`, `tipo_sociedad`, `categoria_matricula`.
- `estado_matricula` - active/cancelled status.
- `representante_legal` fields - personal data.

## Mapping

- `registration_number`: `codigo_camara` + `matricula`
- `tax_id`: `nit` + optional check digit
- `legal_name`: `razon_social`
- `lifecycle_status`: `estado_matricula`
- `activity_code`: CIIU fields
