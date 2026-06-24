# РСМП — Unified Register of SMEs Field Catalog

## Source Summary

- Country: Russia
- Source type: official_registry
- Organization: Federal Tax Service (ФНС / FNS)
- URL: https://file.nalog.ru/opendata/7707329152-rsmp/
- License: open data (FNS)
- Access: public bulk (no key)
- Freshness: monthly
- Record shape: chunked XML files, one `Документ` per SME
- Primary keys: `ИННЮЛ` (or `ИННФЛ` for ИП), `ОГРН`
- Join keys: `ИННЮЛ`, `ОГРН`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| ОргВклМСП/@ИННЮЛ | ИННЮЛ | INN (legal entity) | string | identifier | tax id; join key |
| ОргВклМСП/@ОГРН | ОГРН | OGRN (13-digit) | string | identifier | company id |
| ОргВклМСП/@НаимОрг | НаимОрг | Full name | string | legal_name | |
| ОргВклМСП/@НаимОргСокр | НаимОргСокр | Short name | string | legal_name | |
| ИПВклМСП | ФИОИП/ИННФЛ/ОГРНИП | Individual entrepreneur | object | person | **PERSONAL DATA (152-ФЗ)** |
| СведМН/@Регион… | Регион/Район/Город/КодРегион | Location | string | geography | |
| СвОКВЭД/СвОКВЭДОсн/@КодОКВЭД | КодОКВЭД | Primary OKVED | string | activity | |
| СвОКВЭД/СвОКВЭДДоп | СвОКВЭДДоп | Additional OKVED | array | activity | |
| @ДатаВклМСП | ДатаВклМСП | Date included | date | date | |
| @ВидСубМСП | ВидСубМСП | 1 ЮЛ / 2 ИП | string | legal_form | |
| @КатСубМСП | КатСубМСП | micro/small/medium | string | metadata | 1/2/3 |
| ССЧР/@ВеличССЧР | ССЧР | Average headcount | integer | employment | |

## Interpretation Notes

- **Schema verified from the RSMP XSD** (`structure-12052026.xsd`, downloaded). The
  open bulk is monthly chunked XML in a ZIP; the latest archive (`data-10062026…`)
  is **~2.25 GB** (content-length 2,247,152,251) — too large to fetch here, so the
  catalog is from the official XSD.
- The register lists **all SMEs** (~6M): legal entities (ОргВклМСП) and individual
  entrepreneurs (ИПВклМСП). For an **ИП**, the block carries the person's name
  (`ФИОИП`) and personal INN (`ИННФЛ`) — **personal data (152-ФЗ)**, redact.
- **Join**: `ИННЮЛ`/`ОГРН` to GIR BO, EGRUL, and the FNS open sets.
- **Category** (`КатСубМСП`): 1 micro, 2 small, 3 medium. **Kind** (`ВидСубМСП`):
  1 legal entity, 2 individual entrepreneur.
- Encoding may be **Windows-1251** — convert to UTF-8 on ingest. No raw sample
  record (full archive not downloaded; XSD-derived).
