# Pulling Serbian APR Companies and Company Representatives

## Decision

Use two APR products:

1. The open Companies API for the monthly company backbone.
2. APR's paid status-data service for representatives.

The open API alone cannot satisfy the representative requirement.

Beneficial owners are deliberately out of scope for SP2+SP3+SP4. They belong to
APR's separate Central Register of Beneficial Owners (CEV), with its own eID or
contracted-service access and privacy requirements.

## Capability matrix

| Requirement | Open Companies API | One-off paid data | Contracted web service |
|---|---:|---:|---:|
| Matični broj, name, status, legal form | Yes | Yes | Yes |
| Municipality | Yes | Yes | Yes |
| PIB | No | SP2 | Request group |
| Full registered address | No | SP3 | Request group |
| Legal representatives | No | SP3 | Request group |
| Other representatives/procurists/boards | No | SP4 | Request group |
| Members/founders | No | SP5 | Request group |
| Branch representatives | No | SP6 | Request group |
| Daily change feed | No | No | Yes |

## Backfill cost indication

Using the 2026 published per-subject schedule and the current open-feed count of
133,634 only as a mechanical estimate:

- SP2 + SP3: `133,634 × 30 RSD = 4,009,020 RSD`
- SP2 + SP3 + SP4: `133,634 × 35 RSD = 4,677,190 RSD`

APR must provide the actual quote. The population selected, deleted-company
coverage, contract terms and other charging rules may change the total.

## Recommended pipeline

```text
APR open JSON (monthly)
    -> immutable raw snapshot + SHA-256
    -> validate envelope, date, count and fields
    -> upsert company core by matični broj

APR SP2+SP3(+SP4) backfill
    -> immutable raw XLSX/MDB
    -> normalize representative relationships
    -> mark current baseline

APR web-service changes (daily)
    -> immutable raw event batches
    -> idempotent staging by APR event/change identifier
    -> close/open representative relationship versions
    -> reconcile periodically against per-MB snapshots
```

## Public search policy

Do not automate `pretraga.apr.gov.rs`. APR explicitly disallows automatic tools
against public search results and uses reCAPTCHA. It can be used by a human for
spot checks only.

A manual spot check on 2026-08-25 confirmed the visible legal-representative
shape: person name, function, masked JMBG control and whether the person acts
independently. The personal name was not saved and JMBG was not revealed. The
UI also exposes a separate `Чланови` section; membership must not be treated as
proof of beneficial ownership.

## Request to send APR

Ask `apr-podaci@apr.gov.rs` for:

- technical documentation and a sample for the company status web service;
- authentication, sandbox and production onboarding steps;
- the exact data-group identifiers corresponding to SP3 and SP4;
- representative field definitions and example values;
- stable IDs and change-event/deletion semantics;
- effective dates and joint/countersignature representation;
- fees, quotas and rate limits for the intended company population;
- retention, caching, display and redistribution rights;
- confirmation of which natural-person identifiers are included or suppressed.

## Go ingestion outline

The existing downloader can fetch the public snapshot. For production, add a
small concrete APR client only after APR supplies its protocol documentation.
Keep the integration direct: download raw data, validate it, normalize it, and
log failures once at the collector/worker boundary. Do not introduce an
interface until there is a second real implementation or a stable boundary that
requires one.
