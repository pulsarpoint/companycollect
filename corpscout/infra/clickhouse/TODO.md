# TODO — backup rollout follow-ups

Context: the clickhouse-backup sidecar was deployed 2026-07-14. The first
full backup (`shard{shard}-full-20260714142423`, ~456 GiB) started uploading
to the B2 bucket `main-ch-backup` at 14:24 UTC, ETA ~21:30–22:00 UTC.
Until `metadata.json` is written at the very end, `list remote` shows the
backup as `broken (can't stat metadata.json)` — that is normal for an
in-progress upload.

Check whether it finished:

```bash
ssh companycollect 'docker exec clickhouse-clickhouse-backup-1 clickhouse-backup list remote'
# done when the full- entry shows a size and no "broken" marker
```

- [ ] **1. Restore drill (after the full upload completes).** Restore one
  small table from B2 into a scratch `backup_verify` database, compare row
  counts with the source table, then drop `backup_verify`. Proves the whole
  chain (B2 auth, download, metadata, part attach) end to end.
  Plan: `docs/superpowers/plans/2026-07-14-clickhouse-b2-backup.md`, Task 3.

- [ ] **2. Final whole-branch review.** Fresh review of the complete diff
  (base `19b5fe29`) across compose sidecar, deploy, README, and the logged
  minor findings; merge verdict ends the plan.

- [ ] **3. First incremental check (~2026-07-15 14:24 UTC).** Confirm an
  `increment-...` entry appears in `list remote` and is small relative to
  the full. This is the moment the weekly-full + daily-incremental design
  proves itself.

- [ ] **4. Decide on `SKIP_TABLES` for `_tmp_*` loader tables (optional).**
  The full backup includes transient `_tmp_*` tables (e.g.
  `_tmp_br_companies_*`). If item 3 shows bloated incrementals because of
  them, add a `SKIP_TABLES` pattern to the sidecar env in
  `docker-compose.yml` + `vars.yml` and re-run `ansible-playbook install.yml`.

Delete this file when everything above is done.
