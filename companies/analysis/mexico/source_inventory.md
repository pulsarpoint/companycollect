# Mexico — Source Inventory

| Source | Org | Type | Access | Formats | License | Status |
|---|---|---|---|---|---|---|
| INEGI DENUE | INEGI | statistical business directory | public, no token (bulk) | CSV, ZIP, JSON | INEGI libre uso | **recommended** |
| SAT Listado 69-B | SAT | tax risk list | public | CSV | public (CFF 69-B) | useful_secondary_source |
| RPC / PSM | Secretaría de Economía | official registry | search / per-document (paid) | HTML, PDF | restricted | blocked_by_payment |
| BMV / CNBV | BMV / CNBV | financial disclosure | public (listed) | XBRL, XLSX | exchange terms | useful_secondary_source |
| datos.gob.mx | Gobierno de México | open data portal | public | CSV, JSON | Libre Uso MX | useful_secondary_source |

## Roles

- **inegi_denue** — the best **open bulk** business listing: trade name, legal name
  (`raz_social`), SCIAN activity, employee band, address, geolocation. Per-state
  CSV, no token. Establishment-level; **no RFC/folio**. Verified live (71,871 units
  for Aguascalientes).
- **sat_69b** — open **RFC risk list** (presumed non-existent operations).
  Verified (14,247 rows).
- **rpc_psm_registry** — the **legal** registry (folio mercantil); search/per-doc,
  fee-based. Authoritative identity, not openly bulk.
- **bmv_cnbv_listed** — **listed-company financials** (the only open financial
  route; issuers only).
- **datos_gob_mx** — portal; legacy CKAN API revamped; not the register host.

## Join keys

- **No single open key.** DENUE: `id`/`clee` (establishment). Legal registry: folio
  mercantil. SAT: **RFC** (12-char companies; also the VAT/tax id). Cross-source
  joins are by **name** unless RFC/folio is sourced separately.
