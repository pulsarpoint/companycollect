# Curated technology definitions

`technology_aliases.json` contains file-curated accepted technology synonyms.
The existing `technology_catalog_clickhouse` asset loads it, validates every target
against the merged catalog, and publishes `corpscout.technology_aliases` using migration
`000388_corpscout_technology_aliases`. Aliases are published after the catalog and DNS
fingerprints; the publish log is appended only after all three tables finish.
Migration `000389_corpscout_technology_proposals` adds crawler proposals and the
administrator review ledger. The asset merges approved new entries as an `admin_review`
catalog layer and combines explicitly approved aliases with this file. Those reviews
remain inputs on every refresh; approval alone does not publish a new snapshot.

The initial file deliberately contains no aliases. In the real-job audit, `Git`, `git`,
and `GIT` resolve through case-insensitive catalog matching. Other extracted labels
need new catalog entries or compound/category handling, not invented alias targets.

Each accepted synonym uses this structure (illustrative entry, not currently loaded):

```json
{
  "schema_version": "1.0",
  "aliases": [
    {
      "alias": "AWS",
      "technology": "Amazon Web Services",
      "reviewed_by": "reviewer identity",
      "reviewed_at": "2026-09-07",
      "review_note": "Reviewed abbreviation for the same cloud platform.",
      "source_references": ["https://aws.amazon.com/what-is-aws/"]
    }
  ]
}
```

The loader derives `alias_key` using Unicode NFKC, case folding, and whitespace
normalization. It preserves punctuation, including `C++` and `C#`. `technology` must
be the exact existing canonical key. Normalized canonical names cannot be added as
aliases, and duplicate/conflicting alias keys fail validation. Accepted aliases cannot
target other aliases. Missing catalog technologies must be reviewed and added through
`technologies.json` or the backoffice proposal review page first.

The alias table contains accepted mappings only. Crawler candidates belong in `new_tech`,
with decisions in `technology_proposal_reviews`. The publisher supplies `match_mode=case_insensitive`,
`review_status=accepted`, `source=custom`, the alias file SHA-256, run ID, and timestamp.
Consumers should try an exact canonical name, then a unique normalized canonical name,
then accepted aliases. A normalized catalog collision remains ambiguous when no exact
canonical match exists. Persist canonical names as identities; do not lowercase the
catalog or use string similarity as an automatic alias decision.

The file is required. Invalid JSON, missing fields, missing targets, conflicting names,
or a missing file fail before any catalog/icon publication. An explicit `aliases: []`
is valid and removes the file-curated aliases; accepted administrator aliases remain.
Each table exchange is atomic; the three exchanges are not one database transaction.

Alias content participates in the catalog asset's definition hash. Materialization
metadata includes `alias_rows` and `alias_source_version`; the existing publish log's
definition hash includes this file. Apply the migration before deploying the updated
asset. Packaging the Dagster source includes this JSON file alongside the other custom
definitions. No crawler/model call is required to publish curated aliases.
Materialization also records `reviewed_technology_rows` and `reviewed_source_version`.
Conflicting approved identities, aliases or changed category meanings fail validation
before catalog publication. A later review supersedes an earlier decision without
deleting either the proposal evidence or the audit history.

Focused checks from the Dagster project:

```sh
uv run pytest tests/test_technology_aliases.py tests/test_technology_catalog.py tests/test_clickhouse_migrations.py -q
uv run pytest tests/test_technology_aliases_clickhouse.py -q
uv run dg check defs
```

The integration test creates a disposable, loopback-only Docker ClickHouse. It runs the
actual catalog asset with fixed source/icon inputs, checks populated and empty alias
publication, and verifies that invalid input preserves the previously published data.
