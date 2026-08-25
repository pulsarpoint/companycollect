# Serbia — Schema Notes

## APR Companies Open Data API

Envelope:

```json
{
  "DatumPreseka": "2026-07-31",
  "Podaci": {
    "<maticni_broj>": { "...": "..." }
  }
}
```

- Primary key: eight-digit `matični broj`, stored as text to preserve leading
  zeroes.
- Encoding: UTF-8.
- Dates: ISO `YYYY-MM-DD`.
- Snapshot semantics: `DatumPreseka` describes the registry cut-off date.

Observed record fields:

| Field | Meaning | Internal mapping |
|---|---|---|
| `PoslovnoIme` | Business name | `legal_name` |
| `SifraOpstine` | Municipality code | `municipality_code` |
| `NazivOpstine` | Municipality name | `municipality` |
| `NazivStatus` | Registry status | `status` after controlled mapping |
| `DatumOsnivanja` | Incorporation date | `incorporation_date` |
| `NazivPravneForme` | Legal form | `company_type` |
| `SifraDelatnosti` | Registered activity code | `activity_code` |

Fields not present: PIB/VAT, full registered address, dissolution date,
representatives, directors, procurists, members/founders and beneficial owners.

## Proposed company projection

```text
company_id                 <- maticni broj
registration_number        <- maticni broj
tax_id                     <- null in open API; available in paid SP2
legal_name                 <- PoslovnoIme
company_type               <- NazivPravneForme
status                     <- mapped NazivStatus
incorporation_date         <- DatumOsnivanja
registered_address         <- null in open API; paid SP3
municipality_code          <- SifraOpstine
municipality               <- NazivOpstine
activity_code              <- SifraDelatnosti
country                    <- Serbia
source_snapshot_date       <- DatumPreseka
source_retrieved_at        <- collector timestamp
raw_record                 <- full source object
```

## Proposed representative relationship

The exact APR leaf schema is not public, so the following is an internal target
model rather than a claim about APR field names:

```text
company_id
representative_source_id       nullable until APR schema is known
representative_name
representative_kind            legal | other | procurist | board | branch
function_title
person_or_legal_entity
authority_mode                 individual | joint | countersignature | unknown
valid_from                     nullable
valid_to                       nullable
is_current
source_group                   SP3 | SP4 | SP6 | web_service
source_snapshot_or_event_at
source_retrieved_at
raw_record
```

Manual inspection of one public record on 2026-08-25 confirmed visible fields
for name (`Име и презиме`), function (`Функција`), a masked JMBG reveal control,
and independent representation (`Самостално заступа`). The actual person name
was redacted and JMBG was not revealed. These labels validate the target
semantics but do not establish the paid SP3/SP4 transport paths.

Do not manufacture a stable person ID from a name. Prefer an APR-provided
relationship/person identifier. If none exists, version the relationship per
company and retain the raw event/snapshot so identity reconciliation can be
revisited.

## Representative source mapping

| APR group | Intended use |
|---|---|
| SP3 `Zakonski zastupnici` | Legal/statutory representatives |
| SP4 `Ostali zastupnici` | Other representatives |
| SP4 `Prokuristi`, `Grupna prokura` | Procurists and joint procura |
| SP4 board groups | Director/supervisory/executive/management boards |
| SP5 `Članovi osnivači` | Founders/members, not representatives |
| SP6 `Zastupnici ogranaka` | Branch representatives |

## Proposed beneficial-owner relationship

Beneficial ownership is a separate APR CEV source, not an SP3/SP4 relationship.
The live portal requires eID/SSO, so this target is based on APR's current law
and public documentation rather than a copied record:

```text
company_id
owner_uid
source_person_key
person_kind                     domestic | foreign | refugee_or_displaced
name
personal_identifier_kind
personal_identifier_hmac        optional keyed HMAC; raw value forbidden
personal_identifier_issuing_country_code
birth_date / birth_place / birth_country_code
residence_country_code / stay_country_code / citizenship_country_codes
basis_code / basis_label_raw
ownership_percentage / voting_rights_percentage
acquired_on / registered_on / documents_registered_on
has_supporting_documents / supporting_document_count
trust_*
has_discrepancy / discrepancy_note
is_present / observed_at / source envelope
```

Use explicit availability around both people collections. `not_acquired` and
`access_restricted` must not be interpreted as a confirmed empty result.

## Validation rules

- Reject a payload without `DatumPreseka` or `Podaci`.
- Treat registration numbers as strings and validate eight digits where the
  register guarantees that format.
- Alert when the nested field set changes; do not silently discard new fields.
- Preserve the previous raw snapshot and content hash.
- For representative changes, model deletions/replacements explicitly; never
  infer that a missing row is historical without APR's change semantics.
- Never store raw JMBG, passport, identity-card, foreigner-number or
  refugee-card values in ClickHouse. If approved identity linking is necessary,
  transform with a secret-keyed HMAC-SHA256 before loading.
- Never infer statutory beneficial ownership from the public-search `Čланови`
  membership/shareholder section.
