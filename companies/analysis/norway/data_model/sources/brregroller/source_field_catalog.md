# Enhetsregisteret (roles / roller) Field Catalog

## Source Summary

- Country: Norway
- Source type: official_registry_api (per-entity lookup)
- Organization: Brønnøysund Register Centre (Brreg)
- URL: https://data.brreg.no/enhetsregisteret/api/enheter/{orgnr}/roller
- License: NLOD 2.0
- Access: public (no auth) — **but contains personal data (GDPR)**
- Freshness: daily (delta feed /api/oppdateringer/roller)
- Record shape: object with `rollegrupper[]`, each having `type`, `sistEndret`, and `roller[]`;
  each role holds either a `person` or an `enhet`
- Primary keys: entity org number (path) + role identity
- Join keys: `{orgnr}` (owning entity), `roller[].enhet.organisasjonsnummer` (corporate holders)

## Fields

| Path | Source field | Meaning | Type | Semantic type | Examples | Notes |
|---|---|---|---|---|---|---|
| rollegrupper[].type.kode | type.kode | Role group code | string | person | DAGL, STYR, REVI | code list rollegruppetyper |
| rollegrupper[].type.beskrivelse | type.beskrivelse | Role group name | string | person | Daglig leder, Styre, Revisor | |
| rollegrupper[].sistEndret | sistEndret | Group last changed | date | date | 2026-02-17 | |
| rollegrupper[].roller[].type.kode | roller[].type.kode | Role code | string | person | DAGL, LEDE, MEDL, VARA, REVI | code list rolletyper |
| rollegrupper[].roller[].type.beskrivelse | ... | Role name | string | person | Styrets leder, Styremedlem | |
| ...person.navn.fornavn | person.navn.fornavn | First name | string | person | **PERSONAL DATA** | GDPR; redacted |
| ...person.navn.mellomnavn | person.navn.mellomnavn | Middle name | string | person | **PERSONAL DATA** | optional |
| ...person.navn.etternavn | person.navn.etternavn | Last name | string | person | **PERSONAL DATA** | GDPR; redacted |
| ...person.fodselsdato | person.fodselsdato | Birth date | date | person | **PERSONAL DATA** | full DoB, not national ID |
| ...person.erDoed | person.erDoed | Is deceased | boolean | person | false | personal data |
| ...enhet.organisasjonsnummer | enhet.organisasjonsnummer | Corporate role-holder org no. | string | relationship | 976389387 | join to brregenhet (e.g. auditor) |
| ...enhet.navn | enhet.navn | Corporate role-holder name | array | relationship | ["ERNST & YOUNG AS"] | |
| ...enhet.godkjenningsstatus | enhet.godkjenningsstatus | Approval status | string | status | Godkjent revisjonsforetak | |
| ...valgtAv.kode | valgtAv.kode | Elected-by code | string | person | AREP | employee rep; code list representanter |
| ...fratraadt | fratraadt | Resigned | boolean | status | false | filter for current officers |
| ...rekkefolge | rekkefolge | Order within group | integer | metadata | 0,1,2 | avregistrert sibling flag |

## Interpretation Notes

- **Personal data / GDPR**: this endpoint exposes natural persons' **names + full date of birth**.
  It is open under NLOD, but personal data must still be handled under GDPR — decide lawful basis,
  consider storing **birth year only** or omitting DoB, and keep it out of fixtures/sample records.
  Example values for person fields are intentionally **not stored** in the catalog or sample.
  The national identity number (*fødselsnummer*) is **NOT** here — it is only in the authenticated
  `autorisert-api` (Maskinporten), which is out of scope.
- **person vs enhet**: each role entry has *either* a `person` (natural person) *or* an `enhet`
  (company, e.g. an audit firm). Treat them as a discriminated union. Corporate holders give a
  join key (`enhet.organisasjonsnummer`) back to `brregenhet`.
- **Role groups** (`rollegrupper`): DAGL = general manager/CEO, STYR = board (with LEDE chair,
  MEDL member, VARA deputy), REVI = auditor. Other codes exist (e.g. contact person, accountant).
- **Currency of role**: filter `fratraadt=true` / `avregistrert=true` to get only current officers.
- **Per-entity lookup**: there is no single roles bulk in the open API; fetch per org number, or
  use the `/api/oppdateringer/roller` feed plus `/api/roller/totalbestand` where available.
- All descriptive text is Norwegian; English names are helper metadata only.
