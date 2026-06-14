# Biała lista podatników VAT — Field Catalog

## Source Summary

- Country: Poland
- Source type: official_tax_api (VAT bridge)
- Organization: Ministerstwo Finansów / KAS
- URL: `https://wl-api.mf.gov.pl/api/search/nip/{nip}?date=YYYY-MM-DD` (also /regon/, /bank-account/); daily flat file
- License: open (Ministerstwo Finansów public register)
- Access: public, no auth
- Freshness: daily
- Record shape: `{ result: { subject: {...}, requestId, requestDateTime } }`
- Primary keys: `result.subject.nip`
- Join keys: `nip`, `regon`, `krs`

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| result.subject.name | name | Name | string | legal_name | `POWSZECHNA KASA…` | |
| result.subject.nip | nip | Tax id | string | identifier | `5250007738` | PK; VAT=PL+NIP |
| result.subject.regon | regon | REGON (9) | string | identifier | `016298263` | bridge |
| result.subject.krs | krs | KRS number | string | identifier | `0000026438` | **NIP→KRS bridge** |
| result.subject.statusVat | statusVat | VAT status | string | status | `Czynny` | active/exempt |
| result.subject.workingAddress | workingAddress | Business address | string | address | `ŚWIĘTOKRZYSKA 36…` | |
| result.subject.accountNumbers | accountNumbers | Bank accounts | array | financial | `["521010…"]` | unique to this source |
| result.subject.representatives | representatives | Representatives | array | person | — | **PII** |
| result.subject.partners | partners | Partners | array | ownership | — | PII possible |
| result.subject.registrationLegalDate | registrationLegalDate | VAT reg date | date | date | — | |
| result.subject.removalDate | removalDate/restorationDate | VAT removal/restore | date | date | — | history |
| result.subject.hasVirtualAccounts | hasVirtualAccounts | Virtual accounts | boolean | metadata | — | |

## Interpretation Notes

- **The identifier bridge.** One lookup returns **NIP + REGON + KRS** together — the cleanest way to join
  the tax world (NIP) to the KRS spine and to REGON. Also the only open source of **registered bank
  accounts** and live **VAT status** (Czynny/Zwolniony).
- **Seed**: the **daily flat file** lists all NIP-account pairs → a population seed of active taxpayers;
  resolve each NIP's KRS, then enrich via the KRS API + RDF.
- **REGON length**: 9-digit here vs 14-digit in KRS — normalize to the 9-digit core.
- **PII**: representatives/partners may be natural persons — GDPR.
- Lookup also by `/regon/{regon}` and `/bank-account/{nrb}`. Documented daily request limits apply.
- See `sample_record.json` for a real trimmed record (PKO BP, NIP 5250007738).
