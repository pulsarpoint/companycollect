# Belgium — Source Inventory

| Source | Type | Access | Format | License | Status |
|---|---|---|---|---|---|
| **KBO/BCE Open Data** | Official registry bulk | Free + registration | CSV/ZIP | Licence-BCE-Open-Data | **recommended** (open **company master**) |
| **NBB Central Balance Sheet Office** | Official financials | Free + account | **XBRL**/JSON/CSV/PDF | Free (Authentic Data) | **recommended** (open **structured financials**) |
| KBO Public Search (web/web service) | Official registry search | Free web / paid API | HTML / SOAP | Free web; API paid | useful secondary (lookup) |
| Free CBE REST mirrors (cbeapi.be, ...) | Third-party API | Free key | JSON | KBO open data | useful secondary (lookup) |
| UBO register | Beneficial ownership | Restricted/fee | PDF | Restricted | blocked by authentication |
| data.gov.be | Open data portal | Free | various | Per dataset | useful secondary (discovery) |
| Moniteur Belge / Belgisch Staatsblad | Official gazette | Free | HTML/PDF | Free | useful secondary (acts/events) |
| Commercial aggregators (Companyweb, BvD/Bel-First, Graydon) | Commercial API | Paid | JSON/PDF | Commercial | useful secondary (resell open data) |

## Access points

- KBO Open Data: https://kbopub.economie.fgov.be/kbo-open-data/login (free registration; SFTP on request) —
  docs https://economie.fgov.be/en/themes/enterprises/crossroads-bank-enterprises/services-everyone/cbe-open-data
- NBB CBSO: CONSULT https://consult.cbso.nbb.be/ (free per-entity PDF/XBRL/CSV); web services https://developer.cbso.nbb.be/ (free account)
- KBO Public Search (web): https://kbopub.economie.fgov.be/kbopub/zoeknummerform.html
- UBO register (restricted): https://finances.belgium.be/ (MyMinfin)
- National catalog: https://data.gov.be/ ; gazette: https://www.ejustice.just.fgov.be/

## Key facts

- **Open company master** (KBO Open Data bulk CSV) + **open structured XBRL financials** (NBB CBSO) — both
  free, behind a **free registration/account** (not payment). Top-tier open.
- **Single key**: Ondernemingsnummer / Numéro d'entreprise (10 digits) = VAT root (`BE` + number);
  vestigingseenheidsnummer for establishments.
- NBB "Improved Data" is paid; the free **Authentic Data** is the as-filed version. UBO is restricted.

See `source_inventory.json` for the machine-readable version.
