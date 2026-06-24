# ЕГРЮЛ — EGRUL Field Catalog

> **PLANNING-ONLY.** The authoritative Unified State Register of Legal Entities
> (FNS). The free per-company extract (выписка, PDF) is public, but the FULL bulk
> (all entities, daily) is a **paid FTP subscription**. Cataloged from public docs;
> directors/founders are personal data (152-ФЗ).

## Source Summary

- Country: Russia
- Source type: official_registry
- Organization: Federal Tax Service (ФНС / FNS)
- URL: https://egrul.nalog.ru/
- License: free per-company / paid full bulk
- Access: free per-company extract; paid FTP for the full bulk
- Freshness: live register
- Record shape: per-company extract (выписка)
- Primary keys: ogrn
- Join keys: ogrn, inn

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| egrul.ogrn | ОГРН | OGRN (13-digit) | string | identifier | authoritative id |
| egrul.inn | ИНН | INN (10-digit) | string | identifier | tax id |
| egrul.kpp | КПП | KPP (9-digit) | string | identifier | |
| egrul.full_name | Полное наименование | Full name | string | legal_name | |
| egrul.legal_form | ОПФ/ОКОПФ | Legal form | string | legal_form | |
| egrul.status | Состояние | Status | string | status | active/liquidation/... |
| egrul.registration_date | Дата регистрации | Registration date | date | date | |
| egrul.directors_founders | Руководитель/Учредители | Directors/founders | array | person | **PERSONAL DATA (152-ФЗ)** |
| egrul.capital | Уставный капитал | Charter capital (RUB) | decimal | financial | |

## Interpretation Notes

- The authoritative register: OGRN/INN/KPP, full name, legal form (OKOPF), status,
  registration date, legal address, **directors/founders**, charter capital, OKVED,
  and the full change history.
- **Access**: free per-company extract (выписка) by OGRN/INN/name; the **full bulk**
  (all legal entities, daily) is a **paid FTP subscription**. GIR BO + RSMP cover
  identity/financials/activity openly; EGRUL adds directors/founders/capital/history.
- **Directors/founders** are personal data (152-ФЗ) — redact. No raw sample record
  (per-company extract / paid bulk).
