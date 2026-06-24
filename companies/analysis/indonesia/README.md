# Company data sources for Indonesia

## Status

- Official bulk data: **not found (open)** — the legal-entity registry (AHU) has no
  open bulk; profiles are paid (PNBP)
- Official API: **per-company only** — OSS issues/verifies the NIB; no open bulk/API
- Open data portal: **working** (`data.go.id`, Satu Data Indonesia) but **hosts no
  company register** (regional/sectoral statistics)
- License: AHU/OSS data is for verification; not open for bulk reuse
- Recommended ingestion path: **per-company lookup** (AHU profile + OSS NIB) + **IDX**
  for listed-company financials (browser)
- **Environment note:** `ahu.go.id` resolved via DNS (103.200.129.129) but was
  **firewalled (TCP/HTTP timeout)** from this environment, and `idx.co.id` is
  **Cloudflare-gated** (403) for automated requests — so parts of this were
  documented from public knowledge; no per-company values captured.

## Best source

Two complementary official systems:

- **AHU Online** (`ahu.go.id`, Direktorat Jenderal AHU, **Ministry of Law** /
  Kemenkumham) — the **legal-entity registry** for **PT** (Perseroan Terbatas), CV,
  Firma, **Yayasan** (foundations), Perkumpulan. Holds the company's legal identity
  (nama PT, nomor **SK pengesahan badan hukum**, NPWP, modal/capital, pengurus &
  pemegang saham). A public **"Pencarian Profil"** exists, but full profiles and
  documents are **paid (PNBP)**.
- **OSS** (`oss.go.id`, **Kementerian Investasi/BKPM**) — Online Single Submission;
  issues the **NIB** (Nomor Induk Berusaha, the modern business id) and risk-based
  licenses, with **KBLI** activity codes. Public **NIB search** ("Cari NIB"),
  per-company.

## Financial data — listed only (open via browser)

**IDX — Bursa Efek Indonesia** (`idx.co.id`) publishes **listed-company financial
statements** (laporan keuangan tahunan/triwulanan) and annual reports — the main
**open** financial source (~900 listed companies). It is public via the browser but
**Cloudflare-gated** for automated access. **Private-company financials** (filed as
LKTP under the Company Registration / Ministry of Trade obligation) are **not openly
public**.

## Identifiers & tax

- **NIB — Nomor Induk Berusaha** — 13-digit business identification number (OSS).
  The modern primary company id.
- **NPWP — Nomor Pokok Wajib Pajak** — tax id (15-digit, being aligned to the
  16-digit NIK).
- **Nomor SK AHU** — legal-entity decree number (pengesahan badan hukum).
- **VAT (PPN)** — no separate VAT number; collectors are **PKP** (Pengusaha Kena
  Pajak), identified by NPWP.
- Currency **IDR**. Legal forms: PT, **PT Tbk** (listed), CV, Firma, Yayasan,
  Koperasi, Perkumpulan.

## Next action

Use **AHU** per-company profile (paid PNBP) + **OSS** NIB search for identity, and
**IDX** for listed-company financials. There is **no open bulk register** and **no
open private financials**. Treat directors/shareholders (pengurus/pemegang saham) as
personal data and redact. (Re-probe AHU/IDX from an unblocked, non-Cloudflare-
challenged network.)
