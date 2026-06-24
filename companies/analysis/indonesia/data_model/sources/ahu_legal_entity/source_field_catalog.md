# AHU Online — Legal Entity Registry Field Catalog

## Source Summary

- Country: Indonesia
- Source type: official_registry
- Organization: Direktorat Jenderal AHU, Ministry of Law (Kemenkumham)
- URL: https://ahu.go.id/
- License: paid per company (PNBP)
- Access: free profile search; **paid** full profiles/documents; **geo-blocked** here
- Freshness: live register
- Record shape: per-company legal-entity profile (paid)
- Primary keys: Nomor SK AHU
- Join keys: NPWP, Nama PT

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| nama_pt | Nama PT | Company name | string | legal_name |  | display name |
| nomor_sk_ahu | Nomor SK AHU | Legal-entity decree number | string | identifier |  | legal-entity primary key |
| npwp | NPWP | Tax id (15→16-digit) | string | identifier |  | join key |
| jenis_badan_hukum | Jenis badan hukum | Legal form | string | legal_form | PT/Yayasan | |
| modal_dasar | Modal dasar | Authorized capital | decimal | financial |  | IDR; paid |
| modal_disetor | Modal disetor | Paid-up capital | decimal | financial |  | IDR; paid |
| pengurus | Pengurus | Directors / commissioners | array | person |  | PERSONAL DATA — redact; paid |
| pemegang_saham | Pemegang saham | Shareholders | array | ownership |  | PERSONAL DATA — redact; paid |
| alamat | Alamat | Address | string | address |  | |

## Interpretation Notes

- **AHU** is the authoritative legal-entity registry (Ministry of Law) for **PT**
  (Perseroan Terbatas), CV, Firma, **Yayasan**, Perkumpulan. It holds the legal
  identity: name, **Nomor SK** (decree of legal-entity ratification), NPWP, capital,
  and directors/shareholders.
- **Access**: a free **"Pencarian Profil"** search exists, but full profiles and
  documents are **paid (PNBP)**. There is **no open bulk/API**.
- **Environment**: `ahu.go.id` **resolved via DNS** (`103.200.129.129`) but
  **TCP/HTTP timed out** from this environment — documented from public knowledge;
  **no live values captured** (examples empty).
- **Personal data**: pengurus (directors/commissioners) and pemegang saham
  (shareholders) are personal data (UU 27/2022 PDP) — redact. Currency **IDR**.
- **Join**: links to OSS/tax via **NPWP**; the **Nomor SK AHU** is the legal-entity
  key.
