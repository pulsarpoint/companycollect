# Schema notes — Indonesia

## Identifiers

| Field | Description |
|---|---|
| **NIB — Nomor Induk Berusaha** | 13-digit business identification number (OSS). Modern primary company id. |
| **NPWP — Nomor Pokok Wajib Pajak** | Tax id (15-digit, being aligned to the 16-digit NIK). |
| **Nomor SK AHU** | Legal-entity decree number (pengesahan badan hukum, Ministry of Law). |
| **VAT (PPN)** | No separate VAT number; collectors are **PKP** (Pengusaha Kena Pajak), identified by NPWP. |

`NIB` is the modern join key (OSS); `NPWP` links tax; AHU holds the legal-entity
identity. For listed companies, the **IDX ticker** is an additional key.

## AHU legal-entity record (field model, from public docs)

| Field (id) | English | Notes |
|---|---|---|
| Nama PT | Company name | PT / PT Tbk / CV / Yayasan |
| Nomor SK AHU | Legal-entity decree number | pengesahan badan hukum |
| NPWP | Tax id | |
| Jenis badan hukum | Legal form | PT/CV/Firma/Yayasan/Perkumpulan |
| Modal dasar / disetor | Authorized / paid-up capital | IDR |
| Pengurus | Directors / commissioners | **PERSONAL DATA — redact** |
| Pemegang saham | Shareholders | **PERSONAL DATA — redact** |
| Alamat | Registered address | |
| Status | Status | aktif / dibubarkan |

## OSS record (NIB)

`NIB`, `nama pelaku usaha` (business name), **KBLI** (activity code), `skala usaha`
(business scale: UMK/non-UMK), `status NIB`, `alamat`.

## IDX (listed financials)

`ticker`, `company_name`, `listing_date`, `sector`, `financial_statements`
(laporan keuangan, IDR), `annual_report`. Listed companies only.

## Legal forms

| Local | English |
|---|---|
| PT (Perseroan Terbatas) | Limited liability company |
| PT Tbk (Terbuka) | Public / listed company |
| PT Persero | State-owned limited company |
| CV (Persekutuan Komanditer) | Limited partnership |
| Firma (Fa) | General partnership |
| Yayasan | Foundation |
| Koperasi | Cooperative |
| Perkumpulan | Association |

## Status values

`aktif` (active), `dibubarkan` (dissolved), `pailit` (bankrupt).

## Internal model mapping

```
company_id          <- NIB (13-digit) [or Nomor SK AHU for legal entity]
registration_number <- Nomor SK AHU / NIB
tax_id              <- NPWP
vat_id              <- none separate (PPN; PKP status via NPWP)
legal_name          <- Nama PT
company_type        <- Jenis badan hukum (PT/PT Tbk/CV/Yayasan)
status              <- Status (aktif/dibubarkan)
registered_address  <- Alamat
activity_code       <- KBLI (OSS)
capital             <- Modal dasar / disetor (IDR; AHU)
financials          <- IDX (listed only, IDR); private LKTP not open
owners/officers     <- Pengurus / Pemegang saham (PERSONAL DATA — redact)
country             <- "Indonesia"
```

## Encoding / formats

- UTF-8; Indonesian. Currency **IDR**. Dates dd-mm-yyyy.
- **No open bulk register**; listed financials open via browser (IDX, Cloudflare).
