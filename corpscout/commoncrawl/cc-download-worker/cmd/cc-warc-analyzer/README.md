# cc-warc-analyzer

`cc-warc-analyzer` compares Common Crawl download strategies without downloading WARC content or writing
to RustFS. It builds or reuses the same `pagesN` worklists as `cc-download-worker`, so the estimates cover
the exact pages that would be staged.

From the `commoncrawl/` directory:

```bash
make download

./cc-download-worker/bin/cc-warc-analyzer \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl CC-MAIN-2026-25 \
  --mode tech \
  --parts 85-94
```

The analyzer compares:

- `exact_records`: one range request per selected page, with no junk bytes.
- `bounded_gap_*`: combine physically nearby selected records up to `--max-range-bytes`.
- `junk_threshold_*`: combine records only while junk remains below each configured percentage.
- `selected_warc_span_lower_bound`: one span between the first and last selected record in each WARC.
- `hybrid_whole_warc_*pct`: download a complete WARC when selected compressed bytes meet the configured
  percentage; otherwise use exact selected-record requests.
- `whole_warc_objects`: exact complete-object sizes obtained with HTTP metadata requests.

Each range policy is evaluated at three scopes:

- `output_chunk`: safe with the existing 256 MiB pack and resume contract.
- `part`: maximum locality available inside each original Common Crawl index part.
- `part_block`: theoretical locality across the entire requested `--parts` block.

Whole-WARC size checks use `HEAD`, falling back to a one-byte range metadata request. Results are cached
under `<base>/<crawl>/analysis/warc_sizes.json`; use `--whole-warc-sizes=false` to run only index-based
analysis.

By default, the requested range is filtered through the same local completion markers as `cc-crawl`:
`<base>/<crawl>/crawl/out_<mode>_<part>.loaded`. Only missing parts contribute selected bytes to WARC
coverage and hybrid decisions. Use `--skip-loaded=false` only when intentionally analyzing completed
parts again.

The analyzer also creates a resumable SQLite plan under
`<base>/<crawl>/download/plans/pagesN/<mode>_parts_<range>.sqlite`. It contains normalized page,
chunk, and WARC rows; pending/committed state; exact object sizes; cache state; and the active
whole-WARC versus exact-range strategy. RustFS chunk manifests remain authoritative when the downloader
later reconciles completion state.

The readable SQLite views are `page_plan` and `warc_plan`. For example:

```sql
SELECT warc_filename, pending_pages, pending_bytes, selected_percent, strategy
FROM warc_plan
ORDER BY selected_percent DESC
LIMIT 20;
```

Use `--check` to inspect an existing plan without building worklists, making network requests, or changing
SQLite state:

```bash
./cc-download-worker/bin/cc-warc-analyzer \
  --base /opt/companycollect/corpscout/commoncrawl/data \
  --crawl CC-MAIN-2026-25 \
  --mode tech \
  --parts 85-150 \
  --check
```

The check reports complete-WARC and individual-page totals, estimated requests/source bytes/junk,
complete-WARC utilization in ten-percentage-point buckets, and the highest-utilization individual WARC
objects.

Useful controls:

| Flag | Default | Meaning |
|---|---:|---|
| `--parts` | required | One part or inclusive block such as `85-94` or `85-114`. |
| `--mode` | `tech` | Marker namespace used to exclude completed parts (`tech` or `industry`). |
| `--skip-loaded` | `true` | Exclude parts already carrying a local `.loaded` marker. |
| `--pages-per-domain` | `25` | Page-selection policy shared with the downloader. |
| `--max-range-bytes` | `16 MiB` | Maximum candidate coalesced range. |
| `--junk-thresholds` | `10,25,50,75` | Maximum junk percentages compared by the planner. |
| `--whole-warc-thresholds` | `25,50,75` | Selected compressed-byte coverage that triggers a complete WARC download. |
| `--whole-warc-sizes` | `true` | Measure exact sizes of all referenced WARC objects. |
| `--warc-head-concurrency` | `32` | Concurrent WARC size metadata requests. |
| `--plan-db` | derived | SQLite planning/progress database path override. |
| `--plan-whole-warc-threshold` | `50` | Threshold persisted as the active strategy in SQLite. |
| `--plan-stats-limit` | `10` | Highest-utilization per-WARC rows written to the JSON log. |
| `--check` | `false` | Read and report the existing SQLite plan without modifying it. |

The reusable [`rangeplanner`](../../rangeplanner/) package contains the physical range planner and
statistics. Once a policy is selected from real results, `cc-download-worker` can use the same package to
execute it without duplicating the algorithm.
