# Company data sources for Armenia

## Status

- Official bulk data: not found (State Register bot-protected; no open bulk)
- Official API: not found (no documented open register API; data.gov.am NXDOMAIN)
- Open data portal: civic portal exists (data.opendata.am) but carries no register
- License: restricted (State Register / SRC); public disclosure (AMX)
- Recommended ingestion path: browser-public lookup; no open bulk or free API

## Best source

Despite Armenia's open-data reputation, the authoritative **State Register of Legal Entities**
(`e-register.am` / `e-register.moj.am`) protects its free company search behind **Radware Bot
Manager** (requests redirect to `validate.perfdrive.com`), so it is not automatable. The most
useful **browser-public** source is the **State Revenue Committee (SRC)** taxpayer search
(`src.am`), which looks up a taxpayer name/status by **TIN (ՀՎՀՀ / HVHH)** — per-TIN, not
bulk. **AMX** (Armenia Securities Exchange) covers listed securities but is a JS SPA with no
clean public API. **Open Data Armenia** (`data.opendata.am`, CKAN) has **no company
register** (research/sectoral datasets only).

## Next action

For company identity/status, use the State Register search (bot-protected — from an
appropriate context, do not bypass the bot manager). For tax identity, use the SRC taxpayer
search by TIN. For listed companies, use AMX via the browser. Re-check `data.gov.am` (did not
resolve here). The universal key is the **TIN (ՀՎՀՀ, 8-digit)**. Redact directors/founders.
