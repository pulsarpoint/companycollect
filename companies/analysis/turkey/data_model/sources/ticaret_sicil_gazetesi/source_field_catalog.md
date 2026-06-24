# Türkiye Ticaret Sicili Gazetesi Field Catalog

> **PLANNING-ONLY.** The official Trade Registry Gazette (TOBB) publishes company
> registrations/amendments/dissolutions, searchable per company / trade-registry
> number. No open bulk. Cataloged from public docs.

## Source Summary

- Country: Turkey
- Source type: official_gazette
- Organization: TOBB
- URL: https://www.ticaretsicil.gov.tr/
- License: public gazette
- Access: public per-company search
- Freshness: continuous (publication-driven)
- Record shape: per-company gazette announcements
- Primary keys: ticaret_sicil_no
- Join keys: ticaret_sicil_no, mersis_no

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| gazette.ticaret_sicil_no | Ticaret Sicil No | Trade-registry no | string | identifier | join to MERSIS |
| gazette.unvan | Unvan | Title | string | legal_name | |
| gazette.ilan_turu | İlan Türü | Announcement type | string | metadata | Kuruluş/Değişiklik/Tasfiye |
| gazette.ilan_tarihi | İlan Tarihi | Date | date | date | |
| gazette.sicil_mudurlugu | Sicil Müdürlüğü | Registry office | string | geography | |
| gazette.ilan_metni | İlan Metni | Text | string | document | **PERSONAL DATA possible (KVKK)** |

## Interpretation Notes

- The gazette is the source of company **events** (incorporation, amendments,
  capital changes, dissolution) and, within the announcement text, **directors/
  shareholders** — **personal data (KVKK)**, redact.
- Per-company search (by trade-registry number / title); no open bulk. Join to
  MERSIS by trade-registry number / name. No raw sample record.
