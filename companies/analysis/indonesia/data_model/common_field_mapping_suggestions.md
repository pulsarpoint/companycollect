# Common field mapping suggestions — Indonesia

> Suggestion only for a future cross-country mapper. It does **not** constrain the
> country-specific Indonesia profile, which stays keyed on the NIB / NPWP.

| Common field | Indonesia source path | Notes |
|---|---|---|
| company_id | `registration.nib` (OSS NIB) | 13-digit; modern business id |
| registration_number | `registration.nomor_sk_ahu` (AHU) | legal-entity decree number (paid) |
| tax_id | `tax_identifiers.npwp` | 15→16-digit |
| vat_id | not_available_in_open_sources | no separate VAT (PPN; PKP via NPWP) |
| legal_name | `legal_identity.legal_name` | Nama PT / Nama Pelaku Usaha |
| status | `status.status_text` | aktif/dibubarkan/dicabut |
| legal_form | `legal_identity.legal_form` | PT/PT Tbk/CV/Yayasan |
| incorporation_date | not_available_in_open_sources | in paid AHU profile |
| dissolution_date | not_available_in_open_sources | in paid AHU profile |
| registered_address | `registered_location.registered_address` | AHU (paid) / OSS |
| activity_code | `activity.kbli_codes` | KBLI 2020 (~ISIC) |
| financials | `financial_statements[]` | IDX listed only, IDR (Cloudflare-gated); private not open |
| officers | `officers[]` (Pengurus) | PERSONAL DATA — redact; paid |
| owners | `owners[]` (Pemegang saham) | PERSONAL DATA — redact; paid |
| source_provenance | `source_provenance[]` |  |

## Cross-country notes

- Company identity is **split**: `company_id` = **NIB** (OSS), `registration_number`
  = **Nomor SK AHU** (legal entity), `tax_id` = **NPWP**; they join on NPWP.
- `vat_id` does not exist as a separate number (PPN; PKP status via NPWP).
- **Financials** are open only for **listed** companies (IDX, Cloudflare-gated);
  private financials are not open. Currency **IDR**.
- Treat directors/shareholders as personal data (UU 27/2022 PDP) — redact.
