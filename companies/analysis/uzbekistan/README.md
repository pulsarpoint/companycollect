# Company data sources for Uzbekistan

## Status

- Official bulk data: known to exist (EGRPO on data.egov.uz) but FIREWALLED from this environment
- Official API: not reachable (open-data portal + tax committee firewalled)
- Open data portal: exists (data.egov.uz) but unreachable here
- License: unknown (portal not reachable); UZSE public disclosure
- Recommended ingestion path: bulk/API from an unblocked network (EGRPO); manual/SPA for UZSE

## Best source

The authoritative Uzbek company register is the **EGRPO — Unified State Register of
Enterprises and Organizations** (ЕГРПО), maintained by the **Statistics Agency** (stat.uz)
and published via the national open-data portal **data.egov.uz** — keyed on the **STIR/INN**
(9-digit taxpayer id) and the EGRPO code, with name, legal form, status, registration date,
address, **OKED** activity, and director. **From this environment both `data.egov.uz` and
`data.gov.uz` are firewalled** (HTTPS timeout / HTTP refused), and the **State Tax Committee**
(`soliq.uz`) also timed out. **stat.uz** is reachable but links out to the firewalled portal.
The **Republican Stock Exchange 'Toshkent'** (`uzse.uz`) is reachable but a JS SPA (issuers
list client-side; API path not found).

## Next action

Re-run the EGRPO pull from an **unblocked network** (data.egov.uz open-data API / dataset),
keyed on **STIR/INN**; use soliq.uz for tax/VAT status and UZSE for listed companies. Confirm
the dataset/API and license on data.egov.uz when reachable. Redact the director's name
(personal data). Note the firewall is environmental, not a real-world access block.
