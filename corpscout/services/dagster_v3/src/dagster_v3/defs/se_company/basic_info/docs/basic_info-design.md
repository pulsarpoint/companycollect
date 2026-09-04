# se_company.basic_info

The basic-info entity of the 2026-09-03 SE basic-info design
(`docs/superpowers/specs/2026-09-03-se-company-basic-info-design.md`).

| Module | Responsibility |
| --- | --- |
| `tables.py` | Table names and column tuples, pinned against migrations 000376-000379 |
| `precedence.py` | `BASIC_INFO_PRECEDENCE`: numbers per field per source; the reviewer is a source ranked 10000 |
| `fold.py` | `fold_basic_info`: pure, one company, highest precedence wins, ties to newest `observed_at` then smaller uid; no row without a register legal name |
| `batch.py` | Reads current suggestion rows (`FINAL`), folds in pages of 20,000, rewrites every folded company's main row plus one history row per change |
| `assets.py` | `se_company_basic_info_fold` (64 hash buckets, `multi_run(1)`, pool `se_company_basic_info_fold`), `se_company_basic_info_fold_companies` (targeted), `se_company_basic_info_precedence_clickhouse` |

`batch.py` writes the history row before the main row: the two statements are not one
transaction, so a failure between them costs a duplicate history row on retry rather than
a published main row with no first-publish history entry. `fold.py` treats an empty string
like NULL — a source never "says empty", so `''` never supplies a field and never wins —
and normalises a naive `observed_at` to UTC before comparing across suggestions, so ties
break consistently regardless of a source's tz-awareness.

Suggestion rows come from the slice-2 extractors (`se_basic_info_suggestions_<source>`) and,
for the `reviewer` source, from the backoffice (slice 3). NULL in a value column is "no
opinion". There is no content hash: an extractor writes a new row when the source's
current record has a newer `observed_at` than the current suggestion row, and the fold
rewrites the main row of every company it folds (so `folded_at` advances and the
changed-only selection converges) while adding a history row only where a value or
source changed.

Operating the fold: materialize one `bucket_NN` partition or launch a backfill of all 64
from the UI; `changed_only` (default true) skips companies folded after their newest
suggestion. `se_company_basic_info_fold_companies` takes `company_ids` and re-folds them
whatever their bucket. Nothing is scheduled. Resolved 2026-09-04: every folded company is
rewritten with a new main row, so `folded_at` advances on every fold and `changed_only`
converges instead of re-selecting an unchanged company forever; `page_size` (default
20,000) is the knob to lower if a run's per-page memory presses the host.
