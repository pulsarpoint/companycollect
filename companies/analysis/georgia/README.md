# Company data sources for Georgia

## Status

- Official bulk data: not found (no open bulk/API reachable)
- Official API: not found (api.napr.gov.ge "Access Denied"; data.gov.ge firewalled)
- Open data portal: exists (data.gov.ge) but unreachable from this environment
- License: restricted (NAPR) / public disclosure (reportal, GSE)
- Recommended ingestion path: browser-public lookup; no open bulk or free API

## Best source

Despite Georgia's open-registry reputation, the authoritative **NAPR e-registry**
(`enreg.reestri.gov.ge`) gates its public search behind a **CAPTCHA** (free company
extracts are available only after solving it), and `api.napr.gov.ge` returns **Access
Denied**. The most useful **browser-public** source is the **SARAS Reporting Portal**
(`reportal.ge`) — the official portal where Georgian reporting entities file annual
**financial statements + management reports**, freely viewable by identification code or
name (automation is anti-forgery-token-gated). **GSE** covers listed securities (ISINs).
**data.gov.ge** was firewalled from this environment.

## Next action

For company identity/status, use NAPR e-registry extracts (CAPTCHA-gated; key = 9-digit
identification code). For financials, use reportal.ge (browser/token). For listed companies,
use GSE. Re-check data.gov.ge from another network for a possible open dataset. Do not
bypass the CAPTCHA, anti-forgery token, or any access control. Redact directors/partners.
