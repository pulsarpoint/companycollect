# KBO Public Search — Field Catalog

> Free **web** single-company lookup (kbopub, NL/FR/DE/EN); the **Public Search Web Service API is PAID**
> (~€50 / 2000 requests). For ingestion use `kbo_open_data` (free bulk). Same key (EnterpriseNumber).

## Source Summary

- Country: Belgium
- Source type: official_registry_search
- Organization: FOD Economie / SPF Économie
- URL: https://kbopub.economie.fgov.be/kbopub/zoeknummerform.html
- License: free web; web service paid
- Access: public (web) / paid (web service)
- Freshness: real-time
- Record shape: per-company HTML (web) / SOAP-XML (paid web service)
- Primary keys: `EnterpriseNumber`
- Join keys: `EnterpriseNumber`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Notes |
|---|---|---|---|---|---|
| enterpriseNumber | Ondernemingsnummer | Enterprise number | string | identifier | join |
| name | Naam/Dénomination | Name | string | legal_name | |
| legalForm | Rechtsvorm | Legal form | string | legal_form | |
| status | Status | Active/stopped | string | status | |
| address | Adres | Registered office | string | address | |
| activities | NACEBEL | Activities | array | activity | |
| establishments | Vestigingseenheden | Establishments | array | relationship | |

## Interpretation Notes

- **Lookup, not bulk** — overlaps the open data master; use it for authoritative single-company
  verification. The bulk (free) path is `kbo_open_data`; the **web service is paid** (avoid for ingestion).
- Same **EnterpriseNumber** key as everything else. No `sample_record.json` (web UI / paid service).
