# Indonesia — combined profile mapping

## Join keys & precedence

- **Primary join key: NIB** (13-digit, OSS) — the modern business id. **NPWP** (tax
  id) links AHU ↔ OSS ↔ tax; for listed companies the **IDX ticker** keys the
  listed entity (filings carry NPWP to join back).
- **Precedence**: **AHU** is authoritative for the **legal entity** (name, legal
  form, capital, directors, shareholders — paid); **OSS** is authoritative for the
  **NIB and activity (KBLI)**; **IDX** is authoritative for **listed financials**.
- **Access**: AHU paid + geo-blocked here; OSS per-company JS search; IDX
  Cloudflare-gated. No per-company values captured.

| Profile path | Source | Source path | Join key | Precedence | Notes |
|---|---|---|---|---|---|
| registration.nib | oss_nib | nib | NIB | authoritative | modern company id |
| registration.nomor_sk_ahu | ahu_legal_entity | nomor_sk_ahu | NPWP | authoritative | legal-entity key; paid |
| tax_identifiers.npwp | ahu_legal_entity | npwp | NPWP | authoritative | tax id |
| tax_identifiers.vat_id | n/a | — | — | n/a | no separate VAT (PPN; PKP via NPWP) |
| legal_identity.legal_name | ahu_legal_entity | nama_pt | NPWP | authoritative | OSS nama_pelaku_usaha as fallback |
| legal_identity.legal_form | ahu_legal_entity | jenis_badan_hukum | NPWP | authoritative | PT/CV/Yayasan |
| status.status_text | oss_nib | status_nib | NIB | authoritative | AHU status as alt |
| activity.kbli_codes | oss_nib | kbli | NIB | authoritative | KBLI 2020 |
| activity.business_scale | oss_nib | skala_usaha | NIB | authoritative | UMK/Non-UMK |
| registered_location.* | ahu_legal_entity | alamat | NPWP | authoritative | OSS alamat as alt |
| capital.* | ahu_legal_entity | modal_dasar/disetor | NPWP | authoritative (paid) | IDR |
| owners[] | ahu_legal_entity | pemegang_saham | NPWP | authoritative (paid) | REDACT natural persons |
| officers[] | ahu_legal_entity | pengurus | NPWP | authoritative (paid) | REDACT |
| listing.* | idx_listed_financials | KodeEmiten/... | ticker | authoritative (listed) | Cloudflare-gated |
| financial_statements[] | idx_listed_financials | Laporan Keuangan | ticker | authoritative (listed) | IDR; listed only |

## Freshness

- AHU / OSS: **live**. IDX: **quarterly/annual**. All constrained by access
  (paid / per-company / Cloudflare).

## Missing-data notes

- **No open bulk register**; AHU paid + geo-blocked, OSS per-company JS.
- **Private-company financials not open** — only IDX (listed, Cloudflare-gated).
- **No separate VAT number** (PPN; PKP status via NPWP).
- **Directors/shareholders** redacted as personal data (UU 27/2022 PDP).
- **No per-company values captured** (controls/blocks not bypassed).
