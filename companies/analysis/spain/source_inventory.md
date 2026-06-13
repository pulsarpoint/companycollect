# Spain — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **BORME via BOE open-data API** | Official gazette API | Free public, no auth | JSON/XML/PDF | BOE reuse (open) | **recommended** ✅ samples |
| **OpenMercantil** (BORME-derived master) | Open reconstructed master | Free public | CSV/Parquet/API | CC BY 4.0 | **recommended** ✅ samples |
| Registro Mercantil / CORPME | Official registry | Paid docs | HTML/PDF/XML | Per-doc fee | blocked by payment (authoritative) |
| **Depósito de Cuentas Anuales** (Reg. Mercantil) | Official financials | Paid (~€9–20/company) | **XBRL**/XML/PDF | Per-doc fee | blocked by payment (**financials, non-listed**) |
| **CNMV** (listed financials) | Official financials | Free public | **XBRL**/PDF | Open | **recommended** (**financials, listed**) |
| datos.gob.es | Open data portal | Free public | various (apidata) | Per dataset | useful secondary (discovery) |
| INE — DIRCE | Statistical register | Free public | XLS/PX/JSON | INE reuse | useful secondary (**aggregate only**) |
| Registro Central de Titularidades Reales | Beneficial ownership | Restricted/paid | per query | Restricted | blocked by authentication |
| Commercial aggregators (eInforma, Axesor, Iberinform) | Commercial API | Paid | JSON/PDF/XBRL | Commercial | blocked by payment (financials at scale) |

## Open access points

- BORME summary API: `https://www.boe.es/datosabiertos/api/borme/sumario/{YYYYMMDD}` (Accept: application/json|xml)
- BORME per-province act XML: `https://www.boe.es/diario_borme/xml.php?id=BORME-A-{YYYY}-{N}-{PP}`
- OpenMercantil: `https://openmercantil.es/` — bulk `/descargas`, API `/api`, per-company `/export?...`
- CNMV XBRL: `https://www.cnmv.es/ipps/` and CNMV datasets on `https://datos.gob.es/`
- INE DIRCE Tempus API: `https://servicios.ine.es/wstempus/js/ES/...`

## Paid / authoritative (no open bulk)

- Registro Mercantil (CORPME): https://www.registradores.org/ — per-document fees, incl. cuentas anuales XBRL
- Commercial: https://www.einforma.com/ (Informa D&B), Axesor, Iberinform

## Key facts

- **Open identity/events**: BORME (official) + OpenMercantil (CC-BY master, ~2.8M companies).
- **Open financials**: CNMV only (listed issuers).
- **Non-listed financials**: XBRL, but **paid per-company** at the Registro Mercantil (~€9–20), no bulk.
- **DIRCE is aggregate-only** — not a per-company source.

See `source_inventory.json` for the machine-readable version.
