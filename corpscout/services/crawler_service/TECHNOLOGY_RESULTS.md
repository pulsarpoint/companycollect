# Technology extraction — 6 September 2026

Version 0.2.0 adds an eighth objective, `technology_signals`, to selection and
extraction. It emits schema 1.1 and a `technology_summary` suitable for grouping
observations without equating candidate requirements with deployed technology.

## Real job description

The package fetched the Kongsberg Software Developer advertisement with Crawl4AI.
The final prompt was also tested on that same saved native cleaned HTML, keeping
the input unchanged between prompt iterations.

Final extraction: **11 technology observations, all source matched**, attributed to
Kongsberg Discovery Canada Ltd. The final extraction used one model call, with no
timeout or schema correction. This is a source-presence result, not a recall score.

| Statement type | Observed technologies |
|---|---|
| Stated role usage | C/C++, Windows, Linux, sonar hardware |
| Required experience | TCP/IP, Windows APIs, MFC, Yocto, Git, Azure DevOps |
| Preferred experience | MATLAB |

The Git/Azure DevOps alternative is preserved. Every observation retains the job
URL, employer, role, context and exact source evidence. The summary preserves
statement type and scope; it does not promote all these observations to a
company-wide installed stack.

Local artifacts:

- [Initial complete URL-to-JSON run](data/technology-software-developer-v1/result.json)
- [Final extraction from the same native snapshot](data/technology-software-developer-v2/result.json)
- [Native HTML](data/technology-software-developer-v2/html/p0001.html)

The first prompt returned 13 observations with 10 passing its source checks. Three
quotes failed because inline HTML introduced a space before a full stop. Evidence
normalization now tolerates this spacing, while preserving words and punctuation.
The prompts differ, so the 13-versus-11 count is not a model repeatability measure;
neither run has an exhaustive manually annotated recall baseline.

## Controlled classification fixture

[Fixture](tests/fixtures/technology_job.html) contains two fictional ads. The final
DeepSeek response returned all **10 expected company/technology/type/scope tuples**,
with no unexpected tuples:

| Source statement | Expected interpretation |
|---|---|
| Team runs Kubernetes and develops tools in Python | Stated use, team scope |
| AWS or Azure experience required | Two requirement observations with the same alternative group |
| Java is nice-to-have | Preferred experience |
| Migrating from Oracle to PostgreSQL | Oracle being replaced; PostgreSQL planned adoption |
| Team does not use MongoDB | Explicit non-use |
| Rust experience not required | Mentioned/non-requirement, no assertion about use |
| HireCo employee maintains client ShopCo's SAP system | Client usage assigned to ShopCo, employer retained as HireCo |
| Website powered by WordPress | Excluded as unrelated website tooling |

Nine observations passed evidence checks. Kubernetes remains `needs_review` because
the model added a full stop to a quotation where the source sentence continued.
It is retained in the records and excluded from the analytical summary. The source
checker does not remove intervening words or accept arbitrary paraphrases.

[Final fixture output](data/technology-fixture-v2/result.json) and the earlier
`data/technology-fixture-v1/` output are preserved. The earlier response missed the
Rust non-requirement and usually omitted attribution quotations. The revised prompt
adds a complete JSON example and explicitly requires repeated attribution evidence.

## Automated checks

All **22 tests** pass, including a real-browser flow from sitemap to jobs list to
individual job description selected for `technology_signals`. Checks cover:

- Required experience stays separate from stated use in summaries.
- Client attribution and unknown-company exclusions.
- Distinct job URL counts across repeated windows and tracking parameters.
- Separate scope, date and alternative groups.
- Missing technology/company/job evidence stays reviewable.
- Short names such as C and R do not match inside C++/C# or unrelated words.
- Inline punctuation spacing, existing evidence rules and previous crawler behavior.

Ruff, ty and the version 0.2.0 wheel build pass. These targeted checks do not yet
establish extraction accuracy across employers, languages and job families. Company
and technology aliases and mirrored job ads are not globally resolved.
