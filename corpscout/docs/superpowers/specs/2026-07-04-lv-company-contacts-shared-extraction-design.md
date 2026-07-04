# Latvia Company Contacts + Shared Contact Extraction — Design

**Date:** 2026-07-04
**Status:** Approved design, pending user review of this spec
**Scope:** `corpscout/dagster_v3` (new shared module + Czech refactor + Latvia
asset) + one ClickHouse migration

## Problem

Latvia UR exposes no structured contact fields, but 1,330 of 485,380
`lv_companies` rows carry a domain **as the company's legal name**
(`SIA "cenuklubs.lv"`, `IK Akmenkalis.com`, `Sabiedrība ar ierobežotu
atbildību "Metinājumi.lv"`). Czech ARES has the identical problem and a
working solution (`czech_ares/contacts.py` → `cz_company_contacts`), but its
extraction/validation/write machinery is country-private, and the same logic
is re-implemented in different flavors in Estonia and Brazil. More countries
with unstructured contact hints are coming (Sweden's design doc already
defers exactly this).

## Decisions (user-confirmed)

1. **Shared module now, Czech rewired now**: generic extraction machinery
   moves from `czech_ares/contacts.py` into a central module; Czech becomes
   a thin consumer in the same project. Estonia/Brazil keep their own logic
   for now (different extraction style: denylist + email-suffix uniqueness);
   consolidating them is future work.
2. **IDN support ships with the move** ("should definitely be done"):
   Latvia has diacritic domains (`Metinājumi.lv`) that the Czech ASCII-only
   regex silently misses. Czech inherits the improvement.
3. **Domain-graph wiring is a known follow-up** (user-acknowledged):
   `cz_company_contacts` and the new `lv_company_contacts` are not yet fed
   into the shared `corpscout.domains` graph; a separate small project wires
   both.

## Component 1: shared module `src/dagster_v3/contact_extraction.py`

Sibling of the existing shared `domains.py` (which stays unchanged — it owns
URL/domain normalization: `root_domain`, `website_host`, …). The new module
owns contact-candidate extraction, domain validation, and the atomic write,
moved from `czech_ares/contacts.py` and generalized:

- `ContactCandidate` dataclass (record_id, contact_type `domain`|`email`,
  contact_value, domain) — `record_id` replaces the Czech-specific `ico`.
- `EMAIL_RE`, `DOMAIN_RE` — moved, with the **IDN extension**: the
  domain/email label character classes accept unicode letters in addition
  to `[A-Z0-9-]` (Python `re` with explicit `\w`-style unicode letter
  handling; exact regex fixed at plan time with tests for
  `Metinājumi.lv`, mixed-script rejection is not attempted — public-suffix
  validation via tldextract remains the gate).
- `CANDIDATE_TEXT_FILTER` — the ClickHouse `match()` prefilter constant,
  extended for unicode labels, used by per-country candidate scans.
- `extract_contact_candidates(record_id, text)` +
  `extract_contact_candidates_by_domain(rows)` — moved as-is otherwise;
  normalization still delegates to `domains.root_domain`/`website_host`
  (tldextract handles unicode domains natively).
- Domain validation (moved): CommonCrawl lookup → `confidence 0.95,
  domain_source 'commoncrawl'`; DNS parent-zone NS resolution fallback
  (dnspython) → `0.70, 'dns'`; unresolvable → dropped. **IDN handling:**
  domains are stored lowercase-unicode; the DNS path idna-encodes before
  querying (`"metinājumi.lv" → "xn--metinjumi-y2b.lv"`); the CommonCrawl
  lookup tries both the unicode and idna forms.
- `replace_contact_table(clickhouse_client, *, table, rows, batch_size)` —
  the generic stage/`EXCHANGE TABLES` atomic replace (moved from
  `_replace_contact_table`), parameterized by target table.

`czech_ares/contacts.py` shrinks to Czech specifics: the `cz_companies`
candidate-prefilter SQL with keyset pagination, ico↔record_id mapping, the
`cz_company_contacts` row shape, and the asset entry point — all calling the
shared module. **Regression guarantee: the full existing Czech test suite
stays green.** Generic tests (extraction, validation, writer, nameserver
resolution) move to `tests/test_contact_extraction.py`, generalized and
extended with IDN cases; Czech keeps its SQL/pagination/migration-shape/job
tests.

## Component 2: Latvia

### Migration `0000NN_corpscout_lv_company_contacts` (NN = highest+1)

Identical shape to `000083_corpscout_cz_company_contacts` with `regcode`
replacing `ico`:

```sql
CREATE DATABASE IF NOT EXISTS corpscout;

CREATE TABLE IF NOT EXISTS corpscout.lv_company_contacts
(
    source_slug LowCardinality(String),
    source_record_id String,
    regcode String,
    contact_type LowCardinality(String),
    contact_value String,
    domain String,
    domain_source LowCardinality(String),
    confidence Float32,
    resolved_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(resolved_at)
ORDER BY (regcode, contact_type, contact_value);
```

Naming follows the repo convention (`cz_company_contacts`,
`ee_company_contacts` → `lv_company_contacts`). Down: drop table. Entry
appended to `EXPECTED_MIGRATIONS`.

### Asset `defs/latvia_ur/contacts.py`

`latvia_ur_clickhouse_company_contacts`: dep `latvia_ur_clickhouse_companies`,
group `latvia_ur`, wired into `latvia_ur_register_job`'s selection (third
leaf beside translation and classification). Flow mirrors Czech:

1. Candidate scan over `corpscout.lv_companies` filtered by the shared
   prefilter on `legal_name` (which covers `name_in_quotes` as a substring;
   unquoted forms like `IK 24dressup.lv` are caught too), keyset-paginated
   by `regcode`.
2. `extract_contact_candidates(regcode, legal_name)` — legal-form noise
   (`SIA`, `IK`, `Sabiedrība ar ierobežotu atbildību`, quotes) never matches
   the domain regex, so no Latvia-specific stripping is needed; the
   public-suffix check drops false positives.
3. Shared validation (CommonCrawl → DNS), `source_slug='latvia_ur'`,
   `source_record_id=regcode`.
4. Shared atomic replace into `corpscout.lv_company_contacts`.

MaterializeResult metadata: candidates scanned, rows written, by
domain_source counts.

## Error handling

Unchanged from Czech semantics: unresolvable domains are dropped (not
low-confidence rows); DNS resolver failures degrade per-domain, never crash
the run; the stage/EXCHANGE write is atomic (a failed run leaves the old
table intact); re-runs fully replace the table (source data is small — 1.3k
candidates — so full recompute per run is the right simplicity).

## Testing

- `tests/test_contact_extraction.py`: moved+generalized Czech tests
  (extraction ordering/dedup, email vs domain typing, validation
  confidence/labels, batched writer, nameserver resolution mocked) plus IDN:
  `Metinājumi.lv` extracts, idna-encodes for DNS, stores lowercase unicode;
  ASCII behavior byte-compatible with the old Czech regex for old inputs.
- Czech suite: green, unmodified assertions except import paths.
- Latvia: extraction against real samples (`'SIA "cenuklubs.lv"'` →
  domain `cenuklubs.lv`; `'IK Akmenkalis.com'` → `akmenkalis.com`;
  plain names extract nothing), SQL-shape test for the candidate scan,
  migration contract entry, register-job membership pin.
- Live smoke: run the scan against real ClickHouse (expect ≈1,330 candidate
  rows), validate a sample end-to-end if the network allows DNS.

## Out of scope

- Wiring `cz_company_contacts`/`lv_company_contacts` into the shared
  `corpscout.domains` graph (acknowledged follow-up).
- Rewiring Estonia/Brazil onto the shared module.
- Phone extraction (neither register embeds phones in names).
- Email-suffix→domain inference with denylists (Estonia/Brazil style) — the
  shared module only ships what Czech/Latvia need; the denylist
  consolidation lands when Estonia/Brazil migrate.
