# Source inventory — Indonesia

| Source | Type | Org | Access | Formats | Financials | Status |
|---|---|---|---|---|---|---|
| AHU Online (`ahu.go.id`) | Legal-entity registry | Ditjen AHU / Ministry of Law | Free search; paid profiles (PNBP); geo-blocked here | html, pdf | no (capital only) | blocked_by_payment |
| OSS (`oss.go.id`) | Business licensing (NIB) | Kementerian Investasi/BKPM | Per-company NIB search | html | no | useful_secondary_source |
| IDX (`idx.co.id`) | Listed financials | Bursa Efek Indonesia | Browser; Cloudflare-gated | json, pdf, xbrl | yes (listed) | blocked_by_authentication |
| Satu Data (`data.go.id`) | Open-data portal | Gov of Indonesia | Public | csv/xlsx/json | no (statistics) | useful_secondary_source |

## Identifiers

- **NIB — Nomor Induk Berusaha** — 13-digit business id (OSS). Modern primary id.
- **NPWP — Nomor Pokok Wajib Pajak** — tax id (15→16-digit).
- **Nomor SK AHU** — legal-entity decree number (pengesahan badan hukum).
- **VAT (PPN)** — no separate VAT number; collectors are **PKP**, identified by NPWP.

## Key facts

- Company identity is **split**: AHU (legal entity PT/CV/Yayasan, paid + geo-blocked
  here) and OSS (the NIB business id, per-company).
- **No open bulk register.** Satu Data has no register (regional statistics).
- **Financials**: **IDX** is open for **listed** companies (~900) but Cloudflare-
  gated for automation; **private-company financials are not open** (LKTP filed with
  the Ministry of Trade).
- Currency **IDR**. Directors/shareholders are personal data → redact.
- Environment block: ahu.go.id firewalled (DNS resolves; TCP timeout); idx.co.id
  Cloudflare-gated — no per-company values captured.
