# Mexico Company Profile — Source Mapping

> **Fragmented, no single open key.** The open layer is **INEGI DENUE**
> (establishment directory; keys `id`/`clee`) with **no RFC and no folio
> mercantil**. The legal registry (RPC, **folio mercantil electrónico**) and the
> tax id (**RFC**, SAT) are not in DENUE, so cross-source joins are **name-based**.
> RFC = tax id = VAT id (Mexico has IVA, no separate VAT number). Private-company
> financials are not public (listed only via BMV/CNBV).

## Field mapping

| Profile path | Source | Source path | Join key | Freshness | License/Access | Precedence / Notes |
|---|---|---|---|---|---|---|
| identity.denue_id / clee | inegi_denue | id / clee | denue_id/clee | ~2×/yr | libre uso/open | Open-layer primary keys. |
| identity.folio_mercantil_electronico | rpc_psm_registry | Folio Mercantil Electrónico | folio | live | paid | Authoritative legal id; not in DENUE. |
| identity.rfc | sat_69b | RFC | rfc | periodic | public | Tax id; not in DENUE (name join). |
| tax_identifiers.tax_id / vat_id | sat_69b | RFC | — | periodic | public | Same value; no separate VAT id. |
| legal_identity.legal_name | inegi_denue | raz_social | — | ~2×/yr | open | Else registry denominación. |
| legal_identity.trade_name | inegi_denue | nom_estab | — | ~2×/yr | open | Always present. |
| legal_identity.legal_form | rpc_psm_registry | tipo_societario | folio | live | paid/inferred | Or infer from name suffix (S.A. de C.V. …). |
| status.in_directory | inegi_denue | (presence) | — | ~2×/yr | open | Operating establishment. |
| status.risk_69b | sat_69b | Situación del contribuyente | rfc/name | periodic | public | Shell-company risk overlay. |
| activity.scian_code / scian_name | inegi_denue | codigo_act / nombre_act | — | ~2×/yr | open | SCIAN. |
| size.employee_band | inegi_denue | per_ocu | — | ~2×/yr | open | Band, not exact. |
| locations[] | inegi_denue | address cols + lat/long | denue_id | ~2×/yr | open | Per establishment; geolocated. |
| registry_details.* | rpc_psm_registry | fecha_constitucion / capital_social / objeto_social | folio | live | paid | PLANNING-ONLY. |
| financial_statements[] | bmv_cnbv_listed | report.* | ticker/name | quarterly | exchange terms | PLANNING-ONLY; listed-only; MXN. |

## Source precedence

1. **inegi_denue** — the open identity/activity/location layer (establishment-
   level). Primary open source.
2. **sat_69b** — RFC risk overlay (and a source of RFC + legal name with the
   corporate-form suffix).
3. **rpc_psm_registry** — authoritative legal identity (folio, legal form,
   incorporation, capital, objeto social); fee-based, planning-only.
4. **bmv_cnbv_listed** — listed-company financials only.

Conflict rules:
- **Legal name**: prefer registry denominación when available; else DENUE
  `raz_social`; else `nom_estab` (trade name).
- **Legal form**: registry `tipo_societario` is authoritative; otherwise infer from
  the name suffix.
- **No deduplication key across sources** — DENUE is establishment-level and lacks
  RFC/folio; treat name-based links as approximate.

## Join keys

- **DENUE**: `id` / `clee` (establishment).
- **Legal registry**: `folio mercantil electrónico`.
- **Tax**: `RFC` (12-char companies) = tax id = VAT id.
- **No shared open key** — DENUE↔SAT↔RPC joins are by **name** (fuzzy).

## Missing / restricted data

- **A single open legal-entity register** with folio + RFC — none.
- **Private-company financials** — not public (listed only).
- **RFC / folio inside DENUE** — absent (no clean cross-source join).
- **Officers, incorporation date, capital, objeto social** — fee-based registry.
- **No separate VAT id** — the RFC is the tax/VAT identifier.
