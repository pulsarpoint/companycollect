# e-Business Register open data — company basic + general data (RIK) Field Catalog

## Source Summary

- Country: Estonia
- Source type: official_registry
- Organization: Registrite ja Infosüsteemide Keskus (RIK)
- URL: https://avaandmed.ariregister.rik.ee/sites/default/files/avaandmed/ettevotja_rekvisiidid__lihtandmed.csv.zip
- License: Creative Commons Attribution 4.0 (CC-BY 4.0)
- Access: public (free)
- Freshness: daily
- Record shape: one row per company (lihtandmed CSV, `;`-delimited, UTF-8 BOM); deeper fields in yldandmed JSON
- Primary keys: `ariregistri_kood`
- Join keys: `ariregistri_kood`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| ariregistri_kood | ariregistri_kood | Registry code (company id) | string | identifier | 11694365 | join key |
| nimi | nimi | Legal name | string | legal_name | 007 Autohaus osaühing | |
| ettevotja_oiguslik_vorm | ettevotja_oiguslik_vorm | Legal form | string | legal_form | Osaühing | OÜ/AS/… |
| kmkr_nr | kmkr_nr | VAT number | string | identifier | EE101335276 | EE + 9 digits |
| ettevotja_staatus | ettevotja_staatus | Status code | string | status | R | RIK codes |
| ettevotja_staatus_tekstina | ettevotja_staatus_tekstina | Status text | string | status | Registrisse kantud | |
| ettevotja_esmakande_kpv | ettevotja_esmakande_kpv | First registration date | date | date | 30.07.2009 | dd.mm.yyyy |
| ads_normaliseeritud_taisaadress | ads_normaliseeritud_taisaadress | Normalized full address | string | address | Harju maakond, Tallinn, … | ADS |
| asukoha_ehak_kood | asukoha_ehak_kood | EHAK admin code | string | geography | 8151 | EHAK |
| asukoha_ehak_tekstina | asukoha_ehak_tekstina | Location text | string | geography | Pirita linnaosa, Tallinn | |
| teabesysteemi_link | teabesysteemi_link | Register URL | string | metadata | https://ariregister.rik.ee/est/company/11694365 | back-link |

## Interpretation Notes

- **The open spine.** The basic `lihtandmed` CSV (verified: **373,025 companies**) gives clean identity, status,
  legal form, VAT and address keyed on the **registrikood** (8-digit). The richer `yldandmed` JSON (225 MB) adds
  deeper structured fields (capital, activities, contacts); not fully parsed here — additional fields are
  `raw_extension` until cataloged.
- **VAT = de-facto tax id.** Estonia exposes **KMKR** (VAT, `EE` + 9 digits) in the register; there is no
  separate open tax-id field, so `tax_id` and `vat_id` both map to KMKR.
- **Status coverage.** Open data covers entities with active, liquidation, or bankruptcy status; a separate
  dissolution date isn't a basic-data column — derive end-of-life from status.
- **Address** is available raw (`ettevotja_aadress`) and normalized (`ads_normaliseeritud_taisaadress`), plus an
  **EHAK** admin code for geography.
- **Formats:** CSV (basic), JSON/XML (general), and **Parquet** versions exist. `;`-delimited, UTF-8 BOM; dates
  dd.mm.yyyy.
- A real `sample_record.json` (007 Agent & Partners OÜ) is included from the downloaded CSV.
