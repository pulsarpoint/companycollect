# provider_recon

Collects service-provider evidence and publishes one `provider-recon/v1` JSON
document per provider. Sources:
- official IP-range feeds
- BGP announcements via RIPEstat
- hand-written DNS/HTTP/PTR rules

Design: `docs/superpowers/specs/2026-09-27-provider-recon-service-design.md`.

## Commands

```bash
make build
bin/provider-recon validate                      # check definitions/*.yaml
bin/provider-recon collect -out /tmp/recon       # dry run to a local directory
bin/provider-recon collect                       # publish to S3
bin/provider-recon collect -provider aws         # one provider
bin/provider-recon schema                        # regenerate definitions/schema.json
bin/provider-recon restore -provider aws -collector aws_ip_ranges -removed-since 2026-10-03   # undo wrong removals
make live                                        # hit every real feed (format-drift check)
```

`collect` exit codes:
- 0: ok
- 1: error (nothing published when the definitions are invalid)
- 2: published, but some collector was stale or failed

## Output (bucket `provider-recon`)

- `providers/<slug>/latest.json`: the current document, rewritten every run.
- `providers/<slug>/history/<YYYYMMDDTHHMMSSZ>.json`: written only when the content hash changes.
- `changes/<run_id>.json`: what changed per provider (added/removed evidence, feed versions) and collector issues.

The content hash covers identity + evidence and ignores sync tokens. A feed
that republishes identical ranges therefore creates no history. A failing
feed keeps the previous ranges (`stale`); the provider is never emptied.

## Removal lifecycle

Nothing disappears the moment a feed stops listing it. Every item has a
`status`:
- `active`
- `missing`: absent from a successful fetch since `missing_since`
- `removed`: with `removed_at` and a `removal_action` of `grace_expired` or
  `definition_removed`

Items also carry `first_seen` and `last_seen`.

- A missing range is removed after the feed's `removal_grace_days`
  (default 7; Azure 14). While a feed fails, nothing moves.
- No threshold blocks a large drop. Every run's per-feed churn is in the
  change manifest, and the backoffice highlights unusual updates.
- A wrong removal is undone with:

  ```bash
  bin/provider-recon restore -provider <slug> -collector <id> -removed-since <YYYY-MM-DD>
  ```

  It sets `restored_at`, and restores only grace-expired removals. Fix the
  collector first, then restore, then collect.
- Removed items stay in `latest.json` indefinitely, with `removed_at` and
  `removal_action`, so the document is the complete timeline (owner ruling
  2026-09-28). History objects and the ClickHouse timeline keep them too.

## Environment

`CORPSCOUT_S3_ENDPOINT`, `CORPSCOUT_S3_ACCESS_KEY`, `CORPSCOUT_S3_SECRET_KEY`;
optional `PROVIDER_RECON_BUCKET` (default `provider-recon`).

## Adding a provider

1. Create `definitions/<slug>.yaml`. The editor schema is `definitions/schema.json`.
2. Service keys are `<slug>.<name>`, and service types come from the closed list in `internal/model`.
3. Attach feeds with `tag_map`:
   - exact tags or `*` globs
   - `""` ignores a tag
   - `generic_tags` drops umbrella ranges that repeat a specific tag's CIDR
4. Run `bin/provider-recon validate`, then a local `collect -provider <slug> -out /tmp/x`.

## dns-detect

Resolves one DNS record at a time into the services it proves: `(service_type,
provider)` with the evidence and the record's own window. A domain's history
is a view over the per-record results. Design:
`docs/superpowers/specs/2026-09-28-dns-detect-service-design.md`.

Packages: `internal/detect/hosts` (normalisation, public-suffix provider keys),
`internal/detect/knowledge` (index compiled from provider documents over
`internal/matcher`), `internal/detect/resolve` (routing, analyzers,
`LabelHost`), and `cmd/dns-detect`.

```bash
go run ./cmd/dns-detect resolve -knowledge DIR < records.ndjson > results.ndjson
```

`DIR` holds provider documents as `<slug>.json` (a flattened copy of
`providers/<slug>/latest.json`). Each input record gives one output line, in
order:

```json
{"record_id":"a","root_domain":"a.se","name":"a.se.","type":"NS","value":"ns1.loopia.se.","first_seen":"2026-09-01","last_seen":"2026-09-23"}
```
```json
{"knowledge_version":"sha256:…","record_id":"a","results":[{"record_id":"a","root_domain":"a.se","record_name":"a.se","record_type":"NS","analyzer":"ns","subject":"ns1.loopia.se","service_type":"dns","provider_key":"loopia","provider_slug":"loopia","service_key":"loopia.dns","rule_id":"loopia/loopia.dns/NS/target suffix loopia.se","confidence":1,"fallback":false,"valid_from":"2026-09-01","valid_to":"2026-09-23"}],"findings":[]}
```

Routing (slice 1):
- **apex NS**: `ns`, gives `dns`.
- **apex SOA**: `soa`, the MNAME host, gives `dns` with `fallback: true`.
- **apex MX**: `mx`, gives `email`. `0 .` is the finding `null_mx`.
- **apex/www CNAME**: `cname`, gives `hosting` unless a rule says otherwise. A
  target inside the domain itself (www → apex) is only the finding
  `cname_within_domain`: where that name is served from is the A/AAAA
  evidence's job (slice 3).
- **Everything else** gives no result yet.

Fallback rows count only where no non-fallback row of the same service type
covers the same time. The history view applies that (slice 4).

Without a rule, a host's registrable domain decides:
- the domain itself gives `self-hosted`;
- a provider key gives that provider;
- anything else gives an unmapped key.

Test fixtures in `internal/detect/knowledge/testdata/providers` are real
documents. Refresh them with the loop in the slices 0–1 plan (Task 2, Step 1).
