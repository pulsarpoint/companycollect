# Spain Company Profile — Source Mapping

How each section of `country_company_profile.schema.json` is populated, with join keys, freshness,
license/access, and precedence. **Spain's defining traits:** identity/events are open (BORME +
OpenMercantil) but the **CIF is sparse** (~18%), so the open join key is the **Hoja registral**; and
**financials are split** between open-but-listed (CNMV) and paid-but-general (Registro Mercantil).

## Open spine (identity + events)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| registration.cif | openmercantil / borme_boe_api | cif | **PK when present** | daily | CC-BY / BOE open | Best key; ~18% populated |
| registration.hoja_registral | borme_boe_api | Datos registrales → H | **open PK fallback** | daily | BOE open | Stable register key (province+number) |
| registration.tomo/folio | borme_boe_api | Datos registrales → T/F | — | daily | BOE open | Pinpoints entry |
| registration.openmercantil_slug | openmercantil | slug | fallback id | daily | CC-BY | When cif empty |
| registration.province | openmercantil / borme_boe_api | province / item.titulo | name+province join | daily | CC-BY / BOE open | |
| legal_identity.name | openmercantil / borme_boe_api | name / p.articulo | — | daily | CC-BY / BOE open | Prefer BORME (authoritative gazette) |
| legal_identity.company_type | derived | name suffix / cif letter | — | — | — | SL/SA/SLU/SCP |
| status.derived / dissolution_date | borme_boe_api | Disolución/Extinción | — | daily | BOE open | Also name suffix 'EN LIQUIDACION' |
| registered_location.registered_address | openmercantil(full CSV) / borme_boe_api | Address / Constitución domicilio | — | daily | CC-BY / BOE open | Free text |
| share_capital | borme_boe_api | Constitución/Ampliación → Capital | — | daily | BOE open | **register capital, NOT accounts** |
| officers[] | borme_boe_api | Nombramientos/Ceses | hoja/cif | daily | BOE open · **PII** | Parse prose; GDPR |
| ownership.sole_shareholder | borme_boe_api | Declaración de unipersonalidad | — | daily | BOE open · **PII** | Only open ownership signal |
| acts[] | borme_boe_api | p.articulo + p.parrafo | hoja/cif | daily | BOE open | Raw event history |

## Financial statements (multi-source)

| Profile path | Source | Source path | Join key | Freshness | License/access | Precedence / notes |
|---|---|---|---|---|---|---|
| financial_statements[] (listed) | cnmv_financials | CNMV/IPP XBRL | NIF/CIF | annual+interim | **open** | **Preferred when issuer is listed** (free, open) |
| financial_statements[] (general) | registro_mercantil_cuentas_anuales | PGC XBRL deposit | NIF/CIF (else name+prov) | annual | **paid ~€9–20** | Only path for non-listed; planning-only |

### Financial source precedence
1. **cnmv_financials** — if the company is a listed issuer: free, open XBRL. Use it.
2. **registro_mercantil_cuentas_anuales** — for everyone else: XBRL but **paid per company**, no bulk.
   At scale, substitute a **commercial aggregator** (eInforma/Informa D&B, Axesor) that resells these.

Dedupe financial records on `cif/name + fiscal_year + accounts_type`. Prefer **individual** accounts for
the entity's own figures; keep **consolidado** as the group view. `revenue`/`net_result`/`employees` are
**nullable** for small PGC models (abreviado/PYME/microempresa) and for reduced CNMV filings.

## Join & precedence summary

- **Identity join**: `cif` when present, else **`hoja_registral`** (province+number), else
  `name`+`province`. The Hoja is the reliable open key because BORME act text usually omits the CIF.
- **Identity ↔ financials join**: by **NIF/CIF**. Because open CIF coverage is low (~18%), expect a
  matching step (CIF enrichment, or name+province fuzzy match) before financials can attach. This is the
  central engineering risk for Spain.
- **Authority**: BORME (BOE) is the authoritative gazette; OpenMercantil is a convenient CC-BY
  reconstruction — prefer BORME for authoritative fields, OpenMercantil for fast bulk coverage.
- **Freshness**: identity/events daily; financials annual.

## Missing data (kept as notes, not invented fields)

- **activity / CNAE code**: not a clean field in open BORME/OpenMercantil; only inside the Constitución
  "objeto social" free text → derive/enrich (DIRCE has CNAE but only in aggregate). Modeled as enrichment,
  not a spine field.
- **Full ownership / cap table**: only the **sole-shareholder** case is open; beneficial ownership
  (Registro Central de Titularidades Reales) is restricted.
- **Authoritative current address / objeto social**: cleanly only via the paid Registro Mercantil.
- **tax_id coverage**: CIF sparse in open data (enrichment needed).
