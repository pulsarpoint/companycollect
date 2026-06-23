# Chile - Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| Registro de Empresas y Sociedades | Ministry / datos.gob.cl | official registry | public bulk | CSV via CKAN | open data, confirm terms | **recommended** |
| SII Personas Juridicas y Empresas | Servicio de Impuestos Internos | tax/business enrichment | public bulk | ZIP/TXT | public tax statistics/reuse terms unclear | **recommended enrichment** |
| SII Composicion de Sociedades | Servicio de Impuestos Internos | ownership/composition | public bulk | ZIP/TXT | public tax statistics/reuse terms unclear | useful secondary source |

## Roles

- `res_constitutions` - authoritative incorporation/event stream by year. Keyed on `RUT` and internal `ID`.
- `sii_personas_juridicas` - legal-entity tax-side names, activities, addresses, company-size bands, and historical files.
- `sii_composicion_sociedades` - company composition/relationship enrichment; review personal-data fields before ingestion.

## Join keys

The main key is **RUT**. RES has `RUT`; SII files also use RUT/RUT-like identifiers.
