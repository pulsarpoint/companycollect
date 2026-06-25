# Company data sources for Pakistan

## Status

- Official bulk data: not found (SECP registrar firewalled; no open register bulk)
- Official API: found — **PSX data portal** open JSON (listed companies only)
- Open data portal: third-party only (opendata.com.pk; no authoritative register)
- License: restricted (SECP/FBR); PSX terms unconfirmed
- Recommended ingestion path: **API** for listed (PSX); manual/blocked for the full register

## Best source

The **Pakistan Stock Exchange data portal** (`dps.psx.com.pk`) is the standout open source:
`https://dps.psx.com.pk/symbols` returns an **open JSON** list of all listed symbols
(verified: 1,068 symbols, 744 equities) with name + sector, and per-company HTML pages add
sector, registered address, free float, and shares. It covers **listed companies only**.

The authoritative full registrar is **SECP eServices** (keyed on the **CUIN** / SECP
registration number), but the SECP site returned **HTTP 403 (WAF)** and the eServices portal
**timed out** (firewalled from this environment). **FBR's Active Taxpayers List** (keyed on
the **NTN**) offers per-NTN online verification but no open bulk file was located.

## Next action

Use the PSX data portal API for listed companies. For the full register (CUIN, status,
directors), use SECP eServices from an unblocked network (do not bypass the WAF). For tax
status, use FBR ATL per-NTN verification. Note the three identifiers: **CUIN** (SECP),
**NTN** (FBR), **PSX symbol** (listed). Redact directors (personal data).
