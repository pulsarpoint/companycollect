# Colombia - Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| datos.gov.co `c82u-588k` RUES-style extract | datos.gov.co / chambers | company registry extract | public API | JSON, CSV | CC BY-SA 4.0 | **recommended** |
| RUES portal | Confecamaras | official registry search | public web | HTML | terms unclear | useful verification |
| SuperSociedades financial statements | SuperSociedades / datos.gov.co | financial statements | public API | JSON, CSV | open-data license per dataset | useful secondary source |

## Roles

- `datos_gov_co_c82u_588k` - registry spine for companies and natural-person merchants.
- `rues_portal` - verification portal.
- `supersociedades_financials` - financial enrichment, not part of first registry load.

## Join keys

Use **NIT + check digit** where present. Also preserve chamber code and `matricula`.
