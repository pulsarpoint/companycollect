# Serbia APR Company People — Observed Shape and ClickHouse Model

## Summary

APR company representatives and beneficial owners must be treated as two
independent sources:

1. **SP3/SP4 status data** supplies legal representatives, other
   representatives, directors, boards and procurists.
2. **APR CEV** supplies statutory beneficial owners under a separate eID or
   contracted service.

Buying SP2+SP3+SP4 can populate representative tables. It does not populate the
beneficial-owner tables. Each collection therefore has an acquisition
`availability` state so `records: []` cannot silently mean both “not purchased”
and “APR confirmed none.”

## One-Company Representative Observation

The APR public company page for MB `21141666` was inspected manually on
2026-08-25 after user-completed CAPTCHA. This was a semantic check, not an
automated extraction.

| Target field | Observed value |
|---|---|
| relationship section | natural-person legal representatives |
| `party_kind` | `natural_person` |
| `name` | present in UI; intentionally redacted from repository |
| `function_title` | `Директор` |
| `role_code` | `director` plus source relationship `legal_representative` |
| `personal_identifier_kind` | `jmbg` field/reveal control present |
| raw personal identifier | not revealed, not copied, must never enter ClickHouse |
| `represents_independently` | `true` (`Да`) |

The page also contained separate sections for other representatives, directors,
boards, procurists/group procura and members. Most representative sections had
no registered record for the inspected company. A member was visible under
`Чланови`, but that is a company membership/shareholding concept and is not
proof of statutory beneficial ownership.

APR prohibits automated collection from the public search. This one record
validates field meanings only; it does not reveal the paid SP3/SP4 transport
paths, stable identifiers, history fields or deletion semantics.

## Beneficial-Owner Target Shape

No live CEV owner record was accessed: the portal redirects to APR eID/SSO. The
model is instead based on APR's current documentation and the 2025 Act. It
covers:

- domestic, foreign and refugee/displaced-person identity regimes;
- personal name, pseudonymous identifier link, birth/residence/citizenship;
- APR basis codes `OSV*` and `TR*`;
- ownership and voting percentages where relevant;
- acquisition, data-recording and document-recording dates;
- trust context, supporting-document metadata and discrepancy state.

This is a semantic target contract, not a claim that the contracted payload is
JSON or uses these paths. The mapper must be finalized from APR's actual service
specification and redacted examples.

## ClickHouse Tables

Migration `000319_corpscout_rs_apr_company_people` creates four tables:

| Table | Grain | Engine | Reader rule |
|---|---|---|---|
| `rs_apr_company_representative_observations` | one immutable relationship state observed in a source run | `MergeTree` | audit/history input |
| `rs_apr_company_representatives_current` | latest resolved state per company + relationship | `ReplacingMergeTree(resolved_at)` | query with `FINAL WHERE is_current` |
| `rs_apr_company_beneficial_owner_observations` | one immutable owner/basis state observed in a source run | `MergeTree` | restricted audit/history input |
| `rs_apr_company_beneficial_owners_current` | latest resolved state per company + owner relationship | `ReplacingMergeTree(resolved_at)` | query with `FINAL WHERE is_current` and restricted role |

The migration also adds Serbia-required values to
`company_person_role_type`: legal/other representative, director, supervisory,
executive and management board member, procurist/group procurist, and
beneficial owner.

## Key and Version Rules

- `company_id`: eight-digit matični broj stored as text.
- `relationship_uid`: SHA-256 of a versioned namespace plus company id and an
  APR-stable relationship/person key. Do not hash a name as identity.
- `owner_uid`: SHA-256 of a versioned namespace plus company id, APR-stable
  person key and the relationship/basis discriminator. One person can have more
  than one legal basis over time.
- `source_record_uid`: stable APR delivery/event id when available; otherwise a
  documented deterministic source-record key.
- `state_fingerprint`: SHA-256 over canonical semantic payload only, excluding
  run/observation timestamps.
- `is_present`/`is_current`: explicit tombstones. Never infer deletion merely
  because a record is absent until APR defines snapshot/change semantics.
- `observed_at`: collector observation time; do not substitute a business date.
- `source_effective_from/to` and `acquired_on`: source business-effective dates.

If APR supplies no stable relationship/person identifier, retain source rows
and version relationships per company; do not falsely merge people by name.

## Security and Privacy

- Never persist or log raw JMBG, passport, foreign identity-card,
  foreigner-number or refugee-card values.
- If approved deterministic linking is necessary, normalize inside a protected
  transform and emit only `HMAC-SHA256(secret, identifier)`. A plain SHA-256 is
  vulnerable to enumeration.
- Protect HMAC, birth and discrepancy columns with ClickHouse roles/column
  grants. Keep the key outside Dagster, ClickHouse and raw open-data storage.
- Supporting-document contents require encrypted object storage, retention and
  audit policy; the tables store only presence/count metadata.
- Use synthetic or fully redacted fixtures. Do not commit real CEV records.

## Acquisition-to-Load Flow

```text
APR SP2+SP3+SP4 delivery          APR CEV contracted delivery
          |                                   |
          v                                   v
restricted immutable raw storage + payload hash + access audit
          |                                   |
          v                                   v
validate contract schema, source keys, effective dates and tombstones
          |                                   |
          +-------- protected PII transform --+
                               |
                               v
append *_observations -> resolve current rows with explicit tombstones
                               |
                               v
publish availability=complete/partial and records[] to company profile
```

Until the source is acquired, publish:

```json
{
  "officers": {"availability": "not_acquired", "records": []},
  "beneficial_owners": {"availability": "not_acquired", "records": []}
}
```

After SP2+SP3+SP4 but before CEV acquisition, only `officers.availability`
changes. `beneficial_owners.availability` stays `not_acquired` or
`access_restricted`.

## Implementation Gate

Do not build the ingestion client from UI labels. Request from APR:

- redacted SP3/SP4 and CEV payload examples plus schemas;
- stable person, relationship, owner and change-event identifiers;
- full-snapshot versus delta behavior and explicit deletion semantics;
- field code lists, dates/timestamps and representation-authority rules;
- authentication, sandbox, quotas, fees and SLA;
- lawful retention, caching, display and redistribution terms;
- exact treatment of JMBG/foreign identifiers, documents and discrepancy notes.

The source catalogs and implementation handoffs under `data_model/sources/`
should be updated with the real transport paths before any collector is enabled.
