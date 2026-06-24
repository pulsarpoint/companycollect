# Turkey — Schema Notes

## Identifiers

- **MERSIS no** — **16-digit** Central Registry System number — the company id.
- **Ticaret Sicil No** — trade-registry number assigned by the local registry
  office (needs the office to be globally unique).
- **VKN (Vergi Kimlik Numarası)** — **10-digit** tax id. Turkey has **VAT (KDV)**;
  the VKN is the tax/VAT identifier — **no separate VAT number**.
- **KAP id** — KAP's internal id for listed companies. **NACE** activity code.
- Join keys: **MERSIS no** / **VKN** across MERSIS, gazette, GİB; **KAP id** / name
  for listed financials.

## MERSIS per-company record (registry)

`mersis_no` (16-digit), `unvan` (title), `vkn` (tax id), `ticaret_sicil_no`,
`nace` (activity), `adres` (address), `sirket_turu` (company type: A.Ş. / Ltd.
Şti. / etc.), `durum` (status: active/dissolved). Free per-company query.

## KAP (listed) record + financials

`kapId`, `company_name` (unvan), ticker/stock code, city, sector, plus filed
**financial statements** (Bilanço / Gelir Tablosu — balance sheet / income
statement), disclosures (özel durum açıklamaları). Currency **TRY**. Per-company
pages at `/tr/sirket-bilgileri/ozet/{kapId}-{slug}`.

## Trade Registry Gazette

`ticaret_sicil_no`, `unvan`, `ilan_turu` (announcement type:
kuruluş/değişiklik/tasfiye), `ilan_tarihi` (date), `sicil_mudurlugu` (registry
office), `ilan_metni` (announcement text — may name directors, personal data).

## Dates, money, encoding

- Dates: `DD.MM.YYYY` (Turkish) — normalize to `YYYY-MM-DD`.
- Money: **TRY** (financials).
- Encoding: UTF-8 (Turkish characters: ç ğ ı ö ş ü İ).

## Internal model mapping

```text
company_id          <- MERSIS no (16-digit)
registration_number <- ticaret_sicil_no (trade-registry number) + MERSIS no
tax_id              <- VKN (10-digit)
vat_id              <- null (VAT/KDV uses the VKN; no separate VAT number)
legal_name          <- unvan (title)
company_type        <- sirket_turu (A.Ş. / Ltd. Şti. / ...)
status              <- durum (active/dissolved)
registered_address  <- adres
activity_code       <- NACE
financials          <- KAP (listed only; TRY)
officers            <- gazette / trade-registry (per-company; personal data, KVKK)
```
