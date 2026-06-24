# OSS — Online Single Submission (NIB) Field Catalog

## Source Summary

- Country: Indonesia
- Source type: business_licensing
- Organization: Kementerian Investasi / BKPM
- URL: https://oss.go.id/
- License: not stated (verification use)
- Access: public per-company NIB search (JS SPA)
- Freshness: live
- Record shape: per-company NIB record via the public search
- Primary keys: NIB
- Join keys: NIB, NPWP

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| nib | NIB | Business id (13-digit) | string | identifier |  | modern primary id |
| nama_pelaku_usaha | Nama Pelaku Usaha | Business name | string | legal_name |  | |
| kbli | KBLI | Activity code(s) | array | activity |  | KBLI 2020 (~ISIC) |
| skala_usaha | Skala Usaha | Business scale | string | metadata | UMK/Non-UMK | |
| status_nib | Status NIB | NIB status | string | status |  | aktif/dicabut |
| alamat | Alamat | Address | string | address |  | |

## Interpretation Notes

- **OSS** (Online Single Submission, BKPM) issues the **NIB** — the modern business
  identification number that has become the primary operating identifier for
  Indonesian businesses — plus risk-based licenses and **KBLI** activity codes.
- The public **"Cari NIB"** search (`oss.go.id/id/pencarian`) is **per-company** and
  **JS-driven** (Next.js SPA); specific data endpoints were not openly enumerable on
  direct GET, and there is **no open bulk register**. OSS/BKPM publishes aggregate
  investment statistics rather than a company-by-company dataset.
- **Join**: the **NIB** is the modern join key; **NPWP** links to tax and to AHU.
- Documented from the public search model; no live per-company values captured.
