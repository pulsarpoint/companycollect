# Argentina - Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| Registro Nacional de Sociedades | Ministry of Justice | official registry | public bulk | CSV, ZIP | CC BY 4.0 | **recommended** |
| IGJ entities | Ministry of Justice / IGJ | Buenos Aires registry details | public bulk | CSV, ZIP | CC BY 4.0 | useful secondary |
| Registro MiPyME | Ministry of Production | SME certificate/register | public CSV | CSV | open-data terms | useful secondary |

## Roles

- `registro_nacional_sociedades` - national company registry spine keyed on CUIT.
- `igj_entities` - richer Buenos Aires/IGJ entity, address, balance, authority, and assembly files.
- `registro_mipyme` - partial SME population; not a full company register.

## Join keys

Use **CUIT** as the core identifier.
