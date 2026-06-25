# Pakistan Source Inventory

| Source | Slug | Org | Access | Format | Status | Role |
|---|---|---|---|---|---|---|
| PSX Data Portal | psx_dataportal | Pakistan Stock Exchange | open API | JSON/HTML | recommended | Listed companies: symbol, name, sector, address |
| SECP eServices | secp_eservices | SECP | WAF/firewalled; login for filings | HTML | blocked_by_authentication | Authoritative registrar (CUIN, status, directors) |
| FBR ATL / NTN | fbr_atl | FBR | per-NTN online verification | HTML | useful_secondary_source | Tax status (NTN); no open bulk located |
| opendata.com.pk | opendata_com_pk | Open Data Pakistan (3rd-party) | public | unknown | not_company_data | Non-official; no register dataset |

## Notes

- **PSX data portal** is the genuinely open source: `dps.psx.com.pk/symbols` (JSON, 1,068
  symbols / 744 equities) + per-company HTML pages. Listed companies only.
- **SECP** (the authoritative registrar; key = **CUIN**) is **WAF-blocked / firewalled** from
  this environment (403/timeout) — not reachable.
- **FBR ATL** (key = **NTN**) is **per-NTN online verification**; no open bulk file located.
- **opendata.com.pk** is third-party, not an official register.
- **Three identifiers**: CUIN (SECP), NTN (FBR), PSX symbol (listed).
- Directors (SECP) and individuals (FBR ATL) are personal data — redact.
