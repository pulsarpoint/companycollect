# Estonia — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **e-Business Register — company data** | Official registry | Free | **CSV/JSON/XML/Parquet** | CC-BY 4.0 | **recommended** (open spine) |
| **e-Business Register — annual report financials** | Official financials | Free | **CSV** | CC-BY 4.0 | **recommended** (**structured financials**) |
| **e-Business Register — beneficial owners (kasusaajad)** | BO register | Free | JSON/XML | CC-BY 4.0 | **recommended** (open BO) |
| **e-Business Register — shareholders (osanikud)** | Official registry | Free | JSON/XML | CC-BY 4.0 | **recommended** (open owners) |
| e-Business Register — persons/officers + other | Official registry | Free | JSON/XML | CC-BY 4.0 | useful secondary (officers, pledges, rulings) |
| e-Business Register XML/REST API | Official registry | Free | XML/JSON | CC-BY 4.0 | useful secondary (real-time lookups) |
| EMTA — VAT (KMKR) / tax debt / VIES | Official tax | Free | HTML/SOAP | Validation/open | useful secondary |
| avaandmed.eesti.ee | Open data portal | Free | CSV/JSON/XML | Per dataset | useful secondary (discovery) |

## Access points

- Download page: https://avaandmed.ariregister.rik.ee/en/downloading-open-data
- Basic data CSV: `.../avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip`
- General data JSON: `.../avaandmed/ettevotja_rekvisiidid__yldandmed.json.zip`
- Beneficial owners: `.../avaandmed/ettevotja_rekvisiidid__kasusaajad.json.zip`
- Shareholders: `.../avaandmed/ettevotja_rekvisiidid__osanikud.json.zip`
- Officers/persons: `.../avaandmed/ettevotja_rekvisiidid__kaardile_kantud_isikud.json.zip`
- Financials: `.../1.aruannete_yldandmed_*.zip`, `.../4.{2019..2025}_aruannete_elemendid_*.zip`, `.../2.EMTAK_myygitulu_*.zip`, `.../3.myygitulu_geograafiline_*.zip`
- API: https://www.rik.ee/en/e-business-register/business-register-queries
- National portal: https://avaandmed.eesti.ee/

## Key facts

- **Single join key**: **registrikood** (ariregistri_kood, 8-digit). **KMKR** (VAT) = `EE` + 9 digits (in basic data).
- **Fully open** under **CC-BY 4.0** (free since 1 Oct 2022): company data + **structured financial statements**
  + **beneficial owners** + **shareholders** + officers.
- **Financials are STRUCTURED** (not PDF): report metadata + balance-sheet/P&L line items (XBRL-like element
  names + values), years 2019–2025, + revenue by activity. Join `report_id` → registrikood. EUR.
- **Verified live**: downloaded basic CSV (373,025 companies) + report metadata + 2024 financial elements +
  EMTAK revenue; BO + shareholders reachable (HTTP 200).
- **GDPR**: beneficial owners / shareholders / officers are personal data.

See `source_inventory.json` for the machine-readable version.
