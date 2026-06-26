# Uzbekistan Source Inventory

| Source | Slug | Org | Access | Format | Status | Role |
|---|---|---|---|---|---|---|
| EGRPO register (data.egov.uz / stat.uz) | egrpo_register | Statistics Agency / open-data portal | firewalled from environment | unknown | unavailable | Authoritative register (STIR/INN, name, status, OKED, director) |
| State Tax Committee (soliq.uz) | soliq_taxpayer | State Tax Committee | firewalled from environment | HTML | unavailable | Taxpayer search / VAT status by STIR/INN |
| Republican Stock Exchange 'Toshkent' (UZSE) | uzse_listed | UZSE | browser-public (SPA) | HTML/JSON | useful_secondary_source | Listed issuers (ISIN) |
| Statistics Agency (stat.uz) | stat_uz | Statistics Agency | public | HTML | useful_secondary_source | EGRPO custodian / entry point |

## Notes

- The authoritative **EGRPO** register (Unified State Register of Enterprises and
  Organizations) is hosted on **data.egov.uz** (Statistics Agency) — **firewalled from this
  environment** (HTTPS timeout / HTTP refused). Likewise **soliq.uz** (tax) **timed out**.
  Both are documented from public knowledge; nothing captured. The firewall is environmental,
  not a real-world block — re-check from an unblocked network.
- **stat.uz** is reachable and is the **EGRPO custodian/entry point**, but links out to the
  firewalled portal.
- **UZSE** (`uzse.uz`) is reachable but a **JS SPA**; the issuers list is client-side and the
  REST API route was not located (uzse.uz/api/... returns JSON 404 for guessed paths).
- **Identifier**: **STIR/INN** (9-digit taxpayer id) is the universal key (EGRPO + tax);
  EGRPO code; **OKED** activity; ISIN for listed.
- Director (EGRPO) and individual taxpayers (soliq) are personal data — redact.
