# Italy — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **Registro Imprese (InfoCamere)** | Official registry | Paid API/Telemaco | JSON/XML/PDF/XBRL | Contractual | blocked by payment (authoritative) |
| **Registro Imprese — Bilanci XBRL** | Official financials | Paid per document | **XBRL**/PDF/XLS/CSV | Contractual | blocked by payment (**financials**) |
| InfoCamere/CCIAA regional open data | Open data portal | Free public | CSV/RDF/OData | CC-BY 4.0 | useful secondary (**aggregate**) ✅ downloaded |
| **Startup & PMI innovative** | Open registry subset | Free public | XLS/JSON | IODL 2.0 / CC-BY | **recommended** (open per-company **subset**) |
| Commercial aggregators (Cerved, AIDA/BvD, Atoka, CRIBIS) | Commercial API | Paid | JSON/XBRL | Commercial | blocked by payment (**financials at scale**) |
| ANAC procurement | Open procurement | Free public | CSV/JSON/TTL | Open | useful secondary (supplier CF/PIVA) |
| ISTAT — ASIA | Statistical register | Free public | SDMX/CSV | CC-BY | useful secondary (**aggregate**) |
| GLEIF LEI | Open identifier registry | Free public | JSON/CSV | CC0 | useful secondary (LEI subset) |
| dati.gov.it | Open data portal | Free public | various | Per dataset | useful secondary (discovery) |

## Access points

- Registro Imprese API (paid): https://accessoallebanchedati.registroimprese.it/abdo/api — Ricerca
  Anagrafica, Visure, Visura Amministratori, **Bilancio XBRL**, Protesti; Telemaco: https://www.registroimprese.it/area-utente
- InfoCamere open data (aggregate, CC-BY): https://opendata.marche.camcom.it/ (e.g. `data/imprese-attive-ateco.csv`)
- Startup/PMI innovative (open subset): https://startup.registroimprese.it/ ; MIMIT https://www.mimit.gov.it/it/open-data
- ANAC open procurement: https://dati.anticorruzione.it/opendata
- GLEIF golden copy / API: https://www.gleif.org/ ; https://api.gleif.org/api/v1/
- ISTAT (SDMX): https://esploradati.istat.it/ ; national catalog: https://www.dati.gov.it/

## Key facts

- **No open per-company master**; the Registro Imprese (authoritative) is **paid**.
- **Financials = XBRL but paid** (Telemaco / Bilancio XBRL API); aggregators (Cerved/AIDA/Atoka) for scale.
- **Open per-company** = innovative startups/PMI subset + ANAC suppliers + LEI holders.
- **Open free at population scale** = only **aggregate statistics** (InfoCamere/ISTAT).

See `source_inventory.json` for the machine-readable version.
