# Company data sources for Qatar

## Status

- Official bulk data: not found
- Official API: not found (data.gov.qa API works but carries no company register)
- Open data portal: found but not company data (statistical only)
- License: restricted / unknown
- Recommended ingestion path: manual review / browser-public lookup; no open bulk or API

## Best source

The **QFC Public Register** (eservices.qfc.qa) is the most structured open source, but
it only covers **Qatar Financial Centre-licensed firms** (the financial centre), not the
onshore economy. It is browser-public and searchable, but ASP.NET postback-driven (no
clean GET/bulk/API). For the **onshore** company population the registry is the
**Ministry of Commerce and Industry (MoCI) Commercial Registration**, keyed on the
**CR number** — but no open bulk file or API was found (per-CR lookup, often Arabic,
commonly behind the national portal). **Qatar Stock Exchange (QSE)** covers listed
companies (browser-public, financials/disclosures, no clean open API).

## Next action

Treat Qatar as a browser-public / lookup country. For QFC entities, implement against the
QFC Public Register search (postback) behind a browser/clearance context. For onshore
companies, confirm whether MoCI offers an official data-sharing channel or CR-lookup
service. For listed companies, use QSE. Do not bypass authentication, postback, or WAF.
Redact approved-individual / owner names (personal data).
