# Technology mapping audit — 7 September 2026

Audited the latest saved real-job technology extraction against a read-only snapshot of
`corpscout.technology_catalog`: **7,981 canonical entries**. The primary sample contains
**11 observations / 11 distinct labels from one job page and one named employer**.

Normal case-insensitive matching resolves **`Git` to canonical `git` without an alias**.
Baseline coverage is **1/11 (9.09%)**; this sample needs **zero accepted aliases**, so
coverage stays 1/11 after the alias step. Eight labels need catalog entries; two labels
need compound/category handling. This small sample is insufficient to estimate coverage
across companies or job types.

Correction: the first report treated capitalization as an alias and credited it with a
coverage increase. That was an overly strict baseline. The resolver and regenerated
audit now include unique normalized catalog matches in the baseline.

| Raw technology | Outcome | Reason / next action |
|---|---|---|
| Git | Normal catalog match → `git` | Unique case-insensitive match; no alias needed |
| Azure DevOps | Missing from catalog | Add a separate tool-suite entry; do not map to Azure cloud |
| MATLAB | Missing from catalog | Review adding MATLAB; Matchlab is not an equivalent |
| MFC | Missing from catalog | Review adding Microsoft Foundation Class Library, then its MFC alias |
| Yocto | Missing from catalog | Review adding the named embedded-Linux tooling/project |
| Windows | Missing from catalog | General Windows is absent; the source does not specify Windows Server or CE |
| Linux | Missing from catalog | General Linux is absent; a distribution such as Ubuntu cannot be inferred |
| TCP/IP | Missing from catalog | Review a protocol-suite entry |
| Windows APIs | Missing from catalog | Review an API-family entry; Microsoft HTTPAPI is more specific |
| C/C++ | Ambiguous compound | C exists, C++ has no exact entry; the combined label cannot map to C alone |
| sonar hardware | Generic category | No named product; catalog Sonar/SonarQube describe unrelated software |

The catalog covers many website-detectable products and specific operating-system
distributions, while this job describes development tools and broader platform families.
This source primarily exposes catalog gaps rather than synonym gaps.

## Deliverables

- [technology_mapping_audit.json](technology_mapping_audit.json): all 11 labels, frequencies,
  source quotations and locators, catalog candidates/metadata, reviewed decisions, mapped
  observations, before/after counts, and a separate historical comparison.
- [technology_aliases.json](technology_aliases.json): an empty accepted-alias list, with a
  mapping version and catalog snapshot hash. No synonym in this real-page sample has an
  existing equivalent target that needs an alias. No ClickHouse table was populated.
- [reviewed_decisions.json](reviewed_decisions.json): explicit semantic decisions for this
  sample. Reviewed by the assistant; this is not a claim of independent human review.
- [inputs.json](inputs.json): explicit primary/historical/excluded input selection.
- [audit.py](audit.py): deterministic replay of candidate generation, source checks,
  reviewed alias application, and JSON output generation.
- [test_audit.py](test_audit.py): focused tests for false matches, invalid aliases, evidence
  corruption, preserved qualifiers, and unassessed historical schema handling.

## Source selection and validation

Primary source: [Kongsberg Software Developer vacancy](https://www.kongsberg.com/careers/vacancies/software-developer/),
using the saved native Crawl4AI HTML fetched on 6 September and the latest extraction
artifact `data/technology-software-developer-v2/result.json`. The source snapshot is
`b29257ab0b0d7d8564b29121750fa50bb01bc13732bc7dc4ef95fe5dc2a88144`.

That artifact is an extraction diagnostic with page metadata, not a complete ResearchResult.
The importer handles its format explicitly. Its 11 records remain source-matched; the
audit also rechecks the saved HTML digest, window bounds, and each quotation's presence.
Quotation presence does not independently verify company attribution or claim semantics.

The earlier `technology-software-developer-v1` has 13 observations / 10 distinct labels
from the same page snapshot. Its separate baseline mapping coverage is 1/13 observations
(7.69%) and 1/10 labels (10%), unchanged by aliases. The earlier three reviewable records remain marked
as such even when today's punctuation-tolerant source check finds their fragments.
We do not sum the two runs as independent jobs or evidence.

Both synthetic technology fixtures and both schema 1.0 Kongsberg runs are excluded from
primary coverage. The older 40-page job-list benchmark did not extract technology claims;
this audit does not claim to cover those 40 pages.

## How mappings were produced

The script first checks exact canonical names, then unique normalized catalog matches,
then accepted aliases. Normalization applies Unicode NFKC, case folding and whitespace
normalization while preserving punctuation. If several canonical entries share the
normalized key and there is no exact match, the result is ambiguous.

The catalog contains 16 such collision groups, listed in the audit. For example,
`Moxie` and `mOxie` share a normalized key. Existing exact names keep their identity;
an input of `MOXIE` requires disambiguation. Catalog entries were not merged or renamed.

The report also retrieves four nearby catalog names using case-folded string similarity
and includes related entries inspected during review. These scores are retrieval aids;
they never accept a mapping.
For example, MATLAB retrieves Matchlab, and Windows retrieves Windows CE. Neither is
accepted. The explicit review file decides which candidate is the same technology.

The Git identity was also checked against the catalog's `git-scm.com` website and
the [Git project's description](https://git-scm.com/about). The source describes Git in
a version-control/CI context. Microsoft's documentation supports keeping
[Azure DevOps](https://learn.microsoft.com/en-us/azure/devops/user-guide/what-is-azure-devops?view=azure-devops)
as a distinct development suite and identifies
[MFC](https://learn.microsoft.com/en-us/cpp/mfc/mfc-desktop-applications?view=msvc-170)
as Microsoft Foundation Class Library. The missing MATLAB identity was checked against
[MathWorks](https://www.mathworks.com/products/matlab.html).

`Git`, `git`, and `GIT` resolve to the same canonical `git` entry. `C`, `C++`, and `C#`
remain distinct. Accepted synonym keys also use case-insensitive normalization; aliases
cannot duplicate or override normalized canonical names. The AWS alias in unit tests is
a synthetic test of synonym resolution, not a seed derived from this real-page sample.

The audit uses no paid model calls, makes no database writes, and leaves source extraction
artifacts unchanged. Original company names, relationship, scope, alternative groups,
job URLs, and source references are preserved in the mapped observation output.

## Replay

From `corpscout/services/company_research`:

```sh
.venv/bin/python technology_mapping/audit.py \
  --catalog data/technology-mapping-audit/catalog.json

.venv/bin/python -m unittest discover \
  -s technology_mapping -p 'test_*.py' -v

uvx --offline ruff check technology_mapping
uvx --offline ty check technology_mapping --python .venv/bin/python
```

The full catalog snapshot remains in the ignored data folder; its SHA-256 is
`1262e21751af34816b4aff563082dbbb87c1a7c372cb321a9b9e925a8745c68e`.
The audit embeds the relevant candidate metadata and records the read-only export query.
Replay needs that snapshot and the saved HTML/result artifacts. A different catalog
snapshot requires a new review, rather than silently applying stale decisions.

The next useful work is to review the eight missing catalog concepts and compound/category
handling, then extend the real-job technology corpus. Those changes can increase useful
coverage substantially more than adding spelling variants for this sample.
